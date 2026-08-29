"""
WNBA FIRST BASKET TRACKER -- LIVE PLAY BY PLAY (opening plays, real-time)
----------------------------------------------------------------------------
The main pipeline (a separate, private repo) only captures a game's
opening plays once it's logged the ENTIRE game as final -- that can be
1-2+ hours after the first basket itself actually happened.

This script closes that gap: it checks TODAY's scoreboard directly (no
dependency on any other tab or script), and for any game currently in
progress or just finished, fetches and upserts its opening plays into the
'Play By Play' tab right away. Runs on a schedule via live-playbyplay.yml
in this same repo.

Captures every play up through the point where BOTH teams have logged
their own first scoring play -- not a fixed early-game time window. A
fixed window (this used to stop at the first 4 minutes of Q1) silently
drops the "First Team Basket" market whenever the slower-scoring team
doesn't get on the board that early, which happens often enough to
matter. There's no reliable time bound for "how long until both teams
have scored," so this just keeps capturing until it actually happens (or
the game ends).

Safe to re-run: upserts (replaces) rows for any event ID it touches, never
appends duplicates. Skips games it has already fully captured (an
existing row already proves both teams have a logged scoring play) to
keep repeat runs cheap -- including already-final games, since a game
being over doesn't mean a *previous* run captured both teams before this
fix landed.

Lives in its own public repo (separate from the actual picks/scoring
pipeline) specifically so it can poll frequently without any GitHub
Actions minutes budget concern -- public repos get unlimited minutes on
standard runners, private repos are capped. This script contains no
prediction/scoring logic, just a live ESPN scoreboard watcher, so keeping
it public costs nothing competitively.

Also double-duties as the trigger for an extra, one-off run of the main
pipeline's intraday-updates workflow a few minutes into a game's Q1 --
that workflow otherwise only runs on a fixed 2-hour clock, which can leave
picks/injury status stale for up to ~2 hours right after a game actually
tips off. Requires a MAIN_PIPELINE_DISPATCH_TOKEN secret (a GitHub token
with permission to dispatch workflows on the private pipeline repo); if
that secret isn't set, this feature just quietly no-ops.
"""

import os
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
import sheets as sheets_client
from curl_cffi import requests
from google.oauth2.service_account import Credentials
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

CREDENTIALS_PATH = os.environ.get("GOOGLE_CREDENTIALS_PATH", "gcreds.json")
SPREADSHEET_ID = "1cHokxmusavnAfYr0DqJkNSWZ7Eb8_74L5YGAdleGAuQ"
PBP_SHEET_NAME = "Play By Play"
SPORT_PATH = "basketball/wnba"
LOCAL_TZ = ZoneInfo("America/New_York")

# Same filter the main pipeline uses to keep All-Star/exhibition games out
# of anything real -- duplicated here since this repo is intentionally
# self-contained and independent of the main pipeline's codebase.
REAL_TEAMS = {
    "Las Vegas Aces", "New York Liberty", "Connecticut Sun", "Indiana Fever",
    "Chicago Sky", "Atlanta Dream", "Washington Mystics", "Minnesota Lynx",
    "Phoenix Mercury", "Los Angeles Sparks", "Seattle Storm", "Dallas Wings",
    "Golden State Valkyries", "Toronto Tempo", "Portland Fire",
}

TEAM_ABBR = {
    "Las Vegas Aces": "LVA", "New York Liberty": "NYL", "Connecticut Sun": "CON",
    "Indiana Fever": "IN", "Chicago Sky": "CHI", "Atlanta Dream": "ATL",
    "Washington Mystics": "WAS", "Minnesota Lynx": "MIN", "Phoenix Mercury": "PHX",
    "Los Angeles Sparks": "LA", "Seattle Storm": "SEA", "Dallas Wings": "DAL",
    "Golden State Valkyries": "GSV", "Toronto Tempo": "TOR", "Portland Fire": "POR",
}

PBP_HEADER = ["Event ID", "Date", "Matchup", "Period", "Clock", "Seconds Remaining", "Team",
              "Away Score", "Home Score", "Play", "Scoring Play (Y/N)"]

FB_SHEET_NAME = "Live First Basket"
FB_HEADER = ["Event ID", "Date", "Matchup", "Player", "Athlete ID", "Team", "Position",
             "Method", "Period", "Clock", "Captured At"]

# The main pipeline's intraday refresh (injuries/picks/odds) otherwise only
# runs on a fixed 2-hour clock, which can leave picks stale for up to ~2
# hours right after a game actually tips off -- exactly when an OUT/GTD
# player's status is most likely to have just changed. This repo already
# polls every 5 minutes during game windows, so it double-duties as the
# trigger: once a game is a few minutes into Q1, fire an extra one-off
# intraday run via the GitHub API instead of waiting for the clock.
MAIN_PIPELINE_REPO = "drewgp1127/bball-bots"
MAIN_PIPELINE_INTRADAY_WORKFLOW = "intraday-updates.yml"
MAIN_PIPELINE_BRANCH = "master"
DISPATCH_LOG_SHEET_NAME = "Post-Tip Dispatch Log"
DISPATCH_LOG_HEADER = ["Event ID", "Date", "Matchup", "Dispatched At"]

# Second dispatch, fired at a different moment and for a different
# reason: once BOTH teams have logged their own first scoring play, the
# first basket and both first-team-baskets are decided, so the picks
# riding on them can be graded immediately instead of sitting Pending
# until the 8am pipeline builds Game Log. Kept in its own log tab so the
# two dispatches dedupe independently -- a game fires each exactly once.
MAIN_PIPELINE_GRADING_WORKFLOW = "live-grading.yml"
GRADING_DISPATCH_LOG_SHEET_NAME = "Live Grading Dispatch Log"
# Widened past a single instant to reliably catch it despite 5-minute
# polling -- WNBA quarters are 10 minutes, so this is elapsed game time of
# roughly 3-9 minutes into Q1.
POST_TIP_WINDOW_SECONDS_REMAINING = (60, 420)

# Simple in-run cache -- same athlete can be the first-basket scorer looked
# up at most once per run anyway, but avoids re-hitting ESPN if it ever is.
_athlete_cache = {}


def _is_retryable(exc):
    """5xx, timeouts, and connection errors only -- never 4xx (won't fix
    itself on retry)."""
    if isinstance(exc, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
        return True
    if isinstance(exc, requests.exceptions.HTTPError):
        resp = getattr(exc, "response", None)
        return resp is not None and resp.status_code >= 500
    return False


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=2, max=8),
    retry=retry_if_exception(_is_retryable),
    reraise=True,
)
def _get(url, **kwargs):
    """Same retry policy as the main pipeline's ESPNSource (firstiq/sources/
    espn.py) -- 3 attempts, ~2s/4s/8s exponential backoff, only on 5xx/
    timeout/connection errors. This repo intentionally stays independent of
    firstiq (no shared import, see module docstring), so the policy is
    duplicated here rather than shared."""
    resp = requests.get(url, impersonate="chrome", timeout=15, **kwargs)
    resp.raise_for_status()
    return resp


def get_player(athlete_id):
    """Resolves an athlete ID to name/position via ESPN's athlete endpoint --
    same source and shape the main pipeline's own lookup uses, kept
    independent here since this repo is intentionally self-contained."""
    if athlete_id in _athlete_cache:
        return _athlete_cache[athlete_id]
    url = f"https://sports.core.api.espn.com/v2/sports/basketball/leagues/wnba/athletes/{athlete_id}?lang=en&region=us"
    try:
        data = _get(url).json()
        name = data.get("fullName") or data.get("displayName") or "UNKNOWN"
        pos = data.get("position", {}).get("abbreviation", "")
        result = {"name": name, "position": pos[0] if pos else ""}
    except Exception:
        # Don't cache a transient lookup failure as a resolved "UNKNOWN"
        # athlete -- that would permanently poison this event's first-basket
        # record for the rest of the run. Return None so the caller retries
        # the whole event on the next poll instead.
        return None
    _athlete_cache[athlete_id] = result
    return result


def get_abbr(full_name, espn_fallback):
    return TEAM_ABBR.get(full_name, espn_fallback)


def parse_clock_to_seconds(display_value):
    try:
        minutes, seconds = display_value.split(":")
        return int(minutes) * 60 + int(seconds)
    except (ValueError, AttributeError):
        return None


def event_already_captured(rows_for_event):
    """True if the already-captured rows for this event show both teams
    have logged their own first scoring play -- meaning there's nothing
    more to gain by re-fetching, since fetch_game_data() stops right after
    that point. Deliberately does NOT special-case a "post"/final game:
    a game being over doesn't mean a previous run under the old fixed-
    window logic actually captured both teams."""
    scoring_teams = {row[6] for row in rows_for_event if len(row) > 10 and row[10] == "Y"}
    return len(scoring_teams) >= 2


def current_period_and_clock(event):
    """Reads (period, seconds_remaining) straight off the scoreboard's own
    status block for this event -- no extra API call needed."""
    status = event.get("status", {})
    period = status.get("period")
    seconds_remaining = parse_clock_to_seconds(status.get("displayClock", ""))
    return period, seconds_remaining


def is_a_few_minutes_into_first_quarter(event):
    period, seconds_remaining = current_period_and_clock(event)
    if period != 1 or seconds_remaining is None:
        return False
    low, high = POST_TIP_WINDOW_SECONDS_REMAINING
    return low <= seconds_remaining <= high


def trigger_main_pipeline_workflow(token, workflow):
    """Fires a workflow_dispatch on one of the main (private) pipeline
    repo's workflows. Returns True on success; never raises, so a
    dispatch failure never takes down this run's actual play-by-play work."""
    url = f"https://api.github.com/repos/{MAIN_PIPELINE_REPO}/actions/workflows/{workflow}/dispatches"
    try:
        resp = requests.post(
            url,
            json={"ref": MAIN_PIPELINE_BRANCH},
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
            },
            impersonate="chrome",
            timeout=15,
        )
        return resp.status_code == 204
    except Exception as e:
        print(f"    Could not trigger main pipeline workflow {workflow}: {e}")
        return False


def get_todays_events():
    # datetime.now() is naive and reads UTC on GitHub Actions runners --
    # between 8pm-midnight ET, UTC has already rolled to the next calendar
    # date, which would silently pull tomorrow's slate instead of today's.
    target_str = datetime.now(LOCAL_TZ).strftime("%Y%m%d")
    url = f"https://site.api.espn.com/apis/site/v2/sports/{SPORT_PATH}/scoreboard?dates={target_str}"
    return _get(url).json().get("events", [])


def format_shot_method(type_text, points):
    """Turns ESPN's verbose shot-type text (e.g. "Pullup Jump Shot",
    "Driving Layup Shot") plus the point value into a short label like
    "3-Pointer" or "Layup", matching the plain "layup, 3pt, etc." vocab
    the tracker/notification should show instead of ESPN's raw wording."""
    t = (type_text or "").lower()
    if "dunk" in t:
        base = "Dunk"
    elif "layup" in t:
        base = "Layup"
    elif "hook" in t:
        base = "Hook Shot"
    elif "tip" in t:
        base = "Tip-In"
    else:
        base = "Jump Shot"
    if points == 3:
        return "3-Pointer" if base == "Jump Shot" else f"3PT {base}"
    return base


def find_first_basket(plays, team_id_to_name):
    """Scans ALL plays (not just the first-4-minutes window below) for the
    game's actual first made shot -- same definition the main pipeline's
    analyze_game() uses (first scoringPlay with a shooter), just evaluated
    live instead of after the whole game is final."""
    for play in plays:
        if not play.get("scoringPlay", False):
            continue
        participants = play.get("participants", [])
        athlete_id = participants[0]["athlete"]["id"] if participants else None
        if not athlete_id:
            continue
        player = get_player(athlete_id)
        if player is None:
            return None  # unresolved lookup -- retry this event on the next poll
        team_id = play.get("team", {}).get("id", "")
        points = play.get("scoreValue") or play.get("pointsAttempted")
        return {
            "name": player["name"],
            "athlete_id": athlete_id,
            "position": player["position"],
            "team": team_id_to_name.get(team_id, team_id or ""),
            "method": format_shot_method(play.get("type", {}).get("text", ""), points),
            "period": play.get("period", {}).get("number"),
            "clock": play.get("clock", {}).get("displayValue", ""),
        }
    return None


def fetch_game_data(event_id):
    """Returns (rows, fully_captured, first_basket) from a single fetch.
    rows covers every play up through the point where both teams have
    logged their own first scoring play (fully_captured=True at that
    point) -- see the module docstring for why this isn't a fixed time
    window. first_basket is the resolved scorer of the game's actual first
    basket (see find_first_basket), independent of that per-team cutoff."""
    url = f"https://site.api.espn.com/apis/site/v2/sports/{SPORT_PATH}/summary?event={event_id}"
    data = _get(url).json()
    if "plays" not in data:
        raise ValueError(f"Event {event_id}: summary response has no 'plays' key -- ESPN response shape may have changed.")
    plays = data["plays"]
    competitors = data["header"]["competitions"][0]["competitors"]
    team_id_to_name = {c["team"]["id"]: c["team"]["displayName"] for c in competitors}

    first_basket = find_first_basket(plays, team_id_to_name)

    rows = []
    teams_scored = set()
    fully_captured = False
    for play in plays:
        period = play.get("period", {}).get("number")
        if period is None:
            continue
        clock_display = play.get("clock", {}).get("displayValue", "")
        seconds_remaining = parse_clock_to_seconds(clock_display)
        if seconds_remaining is None:
            continue
        team_id = play.get("team", {}).get("id", "")
        team_name = team_id_to_name.get(team_id, team_id or "")
        away_score = play.get("awayScore", "")
        home_score = play.get("homeScore", "")
        text = play.get("text", "")
        is_score = play.get("scoringPlay", False)
        rows.append((period, clock_display, seconds_remaining, team_name, away_score, home_score, text, "Y" if is_score else "N"))
        if is_score and team_name:
            teams_scored.add(team_name)
        if len(teams_scored) >= 2:
            fully_captured = True
            break
    return rows, fully_captured, first_basket


def get_or_create_tab(spreadsheet, sheet_name, header):
    try:
        return spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        print(f"'{sheet_name}' tab doesn't exist yet -- creating it.")
        ws = spreadsheet.add_worksheet(title=sheet_name, rows=2000, cols=len(header))
        ws.update([header], "A1", value_input_option="USER_ENTERED")
        return ws


def main():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scopes)
    # sheets_client.authorize(), not gspread.authorize(): this script is
    # the heaviest Sheets consumer in the system (a poll every 5 minutes,
    # four full-tab reads and a whole-tab rewrite each time) and shares one
    # service account -- and therefore one per-minute quota -- with the main
    # pipeline. Bare gspread turns a 429 into a dead run. See sheets.py.
    client = sheets_client.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)

    print("Checking today's scoreboard for live/finished games...")
    events = get_todays_events()

    pbp_ws = get_or_create_tab(spreadsheet, PBP_SHEET_NAME, PBP_HEADER)
    existing_values = pbp_ws.get_all_values()
    if not existing_values:
        existing_values = [PBP_HEADER]
    existing_rows = existing_values[1:]

    fb_ws = get_or_create_tab(spreadsheet, FB_SHEET_NAME, FB_HEADER)
    fb_existing_values = fb_ws.get_all_values()
    if not fb_existing_values:
        fb_existing_values = [FB_HEADER]
    fb_existing_rows = fb_existing_values[1:]
    fb_known_event_ids = {r[0] for r in fb_existing_rows if r}

    dispatch_token = os.environ.get("MAIN_PIPELINE_DISPATCH_TOKEN")
    dispatch_ws = get_or_create_tab(spreadsheet, DISPATCH_LOG_SHEET_NAME, DISPATCH_LOG_HEADER)
    dispatch_existing_values = dispatch_ws.get_all_values()
    if not dispatch_existing_values:
        dispatch_existing_values = [DISPATCH_LOG_HEADER]
    dispatched_event_ids = {r[0] for r in dispatch_existing_values[1:] if r}

    grading_dispatch_ws = get_or_create_tab(spreadsheet, GRADING_DISPATCH_LOG_SHEET_NAME, DISPATCH_LOG_HEADER)
    grading_dispatch_values = grading_dispatch_ws.get_all_values()
    if not grading_dispatch_values:
        grading_dispatch_values = [DISPATCH_LOG_HEADER]
    grading_dispatched_event_ids = {r[0] for r in grading_dispatch_values[1:] if r}

    updates = {}  # event_id -> new_rows, only for events actually re-fetched
    fb_new_rows = []  # newly-identified first baskets this run
    grade_ready = []  # events whose opening just became fully decided
    today_str = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d")
    now_str = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")

    for event in events:
        state = event["status"]["type"]["state"]
        if state == "pre":
            continue  # hasn't tipped off yet, nothing to fetch

        season_slug = event.get("season", {}).get("slug", "unknown")
        if season_slug != "regular-season":
            continue

        competitors = event["competitions"][0]["competitors"]
        team_names = {c["team"]["displayName"] for c in competitors}
        if not team_names.issubset(REAL_TEAMS):
            continue

        home_c = next(c for c in competitors if c["homeAway"] == "home")["team"]
        away_c = next(c for c in competitors if c["homeAway"] == "away")["team"]
        home = get_abbr(home_c["displayName"], home_c.get("abbreviation", ""))
        away = get_abbr(away_c["displayName"], away_c.get("abbreviation", ""))
        matchup = f"{away} @ {home}"
        event_id = event["id"]

        if (
            dispatch_token
            and event_id not in dispatched_event_ids
            and is_a_few_minutes_into_first_quarter(event)
        ):
            print(f"  {matchup} is a few minutes into Q1 -- triggering an early intraday pipeline refresh...")
            if trigger_main_pipeline_workflow(dispatch_token, MAIN_PIPELINE_INTRADAY_WORKFLOW):
                # Log immediately, before doing anything else that could fail --
                # otherwise a later exception this run leaves the dispatch fired
                # but unrecorded, and the next run fires it again.
                dispatch_ws.append_row([event_id, today_str, matchup, now_str], value_input_option="USER_ENTERED")
                dispatched_event_ids.add(event_id)
                print("    Triggered and logged.")
            else:
                print("    Dispatch failed -- will retry next run.")

        rows_for_event = [r for r in existing_rows if r and r[0] == event_id]
        if rows_for_event and event_already_captured(rows_for_event):
            # Nothing left to fetch for this game -- but if its grading
            # dispatch never went out (it failed, or this game was
            # captured before live grading existed), fire it now. The
            # plays are already on the sheet, so the grader has what it
            # needs immediately.
            if dispatch_token and event_id not in grading_dispatched_event_ids:
                grade_ready.append((event_id, matchup))
            continue

        print(f"  Fetching {matchup} (event {event_id}, status: {state})...")
        try:
            plays, fully_captured, first_basket = fetch_game_data(event_id)
        except Exception as e:
            print(f"    Could not fetch: {e}")
            continue

        if first_basket and event_id not in fb_known_event_ids:
            fb_new_rows.append([event_id, today_str, matchup, first_basket["name"], first_basket["athlete_id"],
                                 first_basket["team"], first_basket["position"], first_basket["method"],
                                 first_basket["period"], first_basket["clock"], now_str])
            fb_known_event_ids.add(event_id)
            print(f"    First basket: {first_basket['name']} ({first_basket['team']}) -- {first_basket['method']}")

        if not plays and not fully_captured:
            print("    Nothing capturable yet -- will retry next run.")
            continue

        new_rows = [[event_id, today_str, matchup, *p] for p in plays]
        updates[event_id] = new_rows
        status_note = "both teams scored, fully captured" if fully_captured else "still waiting on one team, will refine next run"
        print(f"    Captured {len(new_rows)} play(s) -- {status_note}.")

        # Both teams have scored, so FB and both FTs are settled for this
        # game. Queue a grading dispatch -- but don't fire it here: the
        # plays it needs are only written to the sheet at the end of this
        # run, and dispatching first would race the grader against data
        # that isn't there yet.
        if fully_captured and dispatch_token and event_id not in grading_dispatched_event_ids:
            grade_ready.append((event_id, matchup))

    if fb_new_rows:
        print(f"Writing Live First Basket: {len(fb_new_rows)} new game(s).")
        fb_ws.append_rows(fb_new_rows, value_input_option="USER_ENTERED")

    if updates:
        # RE-READ before writing back, and rebuild from the fresh copy.
        #
        # This is a clear()+full-rewrite, so it replaces the entire tab with
        # whatever this run believes it should contain. `existing_rows` was
        # read at the top of main(), before a scoreboard fetch and one ESPN
        # summary fetch per live game -- easily tens of seconds. Anything
        # appended to Play By Play inside that window is absent from the
        # snapshot and would be silently erased by the write-back. The
        # victim gets no error; only the destroying run would know, and it
        # doesn't look.
        #
        # The other writer is a different REPOSITORY: the main pipeline's
        # wnba_playbyplay_tab.py appends historical games as step 7 of its
        # daily run. GitHub concurrency groups are scoped per-repo, so this
        # repo's `live-pbp` group cannot see that job and provides no
        # protection against it. Today's clocks keep them apart (8:17am ET
        # vs. game hours) but nothing enforces that, and a workflow_dispatch
        # answers to no clock at all.
        #
        # Re-reading immediately before the write is what makes the window
        # small enough not to matter, rather than relying on the schedule.
        current_values = pbp_ws.get_all_values()
        current_rows = current_values[1:] if current_values else []

        appeared = len(current_rows) - len(existing_rows)
        if appeared > 0:
            print(f"  NOTE: {appeared} row(s) appeared in '{PBP_SHEET_NAME}' since this run "
                  f"started -- most likely the main pipeline's own play-by-play step. "
                  f"Merging them in rather than overwriting them.")

        kept_rows = [r for r in current_rows if r and r[0] not in updates]
        all_rows = kept_rows + [row for rows in updates.values() for row in rows]

        print(f"Writing Play By Play: {len(kept_rows)} untouched row(s) kept, {len(updates)} game(s) updated.")
        pbp_ws.clear()
        pbp_ws.update(range_name="A1", values=[PBP_HEADER], value_input_option="USER_ENTERED")
        if all_rows:
            pbp_ws.update(range_name=f"A2:K{len(all_rows) + 1}", values=all_rows, value_input_option="RAW")

        # Assert the rewrite did what it claimed. A clear() that succeeded
        # followed by an update() that partially failed leaves the tab
        # truncated, and every consumer downstream reads that as "these
        # games have no plays" rather than as an error.
        written = len(pbp_ws.get_all_values()) - 1
        if written != len(all_rows):
            print(f"  WARNING: expected {len(all_rows)} data row(s) in '{PBP_SHEET_NAME}' "
                  f"after the rewrite, found {written}. The tab may be truncated -- "
                  f"the next poll will rebuild it, but check before trusting live grading.")
    else:
        print("No new plays to write.")

    # Only now that the plays are actually on the sheet is it safe to ask
    # the main pipeline to grade off them. One dispatch covers every game
    # that became decided this run -- the grader scans all pending picks,
    # so firing it once per run is enough no matter how many games landed.
    if grade_ready:
        names = ", ".join(m for _, m in grade_ready)
        print(f"Opening decided for {names} -- triggering live grading...")
        if trigger_main_pipeline_workflow(dispatch_token, MAIN_PIPELINE_GRADING_WORKFLOW):
            grading_dispatch_ws.append_rows(
                [[event_id, today_str, matchup, now_str] for event_id, matchup in grade_ready],
                value_input_option="USER_ENTERED",
            )
            print("    Triggered and logged.")
        else:
            print("    Grading dispatch failed -- will retry next run "
                  "(the scheduled live-grading run is the backstop).")

    print("Done.")


if __name__ == "__main__":
    main()
