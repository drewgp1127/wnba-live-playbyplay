"""
WNBA FIRST BASKET TRACKER -- LIVE PLAY BY PLAY (first 4 minutes, real-time)
----------------------------------------------------------------------------
The main pipeline (a separate, private repo) only captures a game's
first-4-minutes Q1 window once it's logged the ENTIRE game as final --
that can be 1-2+ hours after the first basket itself actually happened.

This script closes that gap: it checks TODAY's scoreboard directly (no
dependency on any other tab or script), and for any game currently in
progress or just finished, fetches and upserts its first-4-minutes Q1
window into the 'Play By Play' tab right away. Runs on a schedule via
live-playbyplay.yml in this same repo.

Safe to re-run: upserts (replaces) rows for any event ID it touches, never
appends duplicates. Skips games it has already fully captured (either
ESPN reports the game "post"/final, or an existing row already proves the
first-4-minutes window had already closed as of that capture) to keep
repeat runs cheap.

Lives in its own public repo (separate from the actual picks/scoring
pipeline) specifically so it can poll frequently without any GitHub
Actions minutes budget concern -- public repos get unlimited minutes on
standard runners, private repos are capped. This script contains no
prediction/scoring logic, just a live ESPN scoreboard watcher, so keeping
it public costs nothing competitively.
"""

import os
from datetime import datetime
from zoneinfo import ZoneInfo
from curl_cffi import requests
import gspread
from google.oauth2.service_account import Credentials

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

# First 4 minutes of Q1 only, same window the main pipeline uses.
FIRST_N_MINUTES = 4
MIN_SECONDS_REMAINING = (10 - FIRST_N_MINUTES) * 60  # 360

# Simple in-run cache -- same athlete can be the first-basket scorer looked
# up at most once per run anyway, but avoids re-hitting ESPN if it ever is.
_athlete_cache = {}


def get_player(athlete_id):
    """Resolves an athlete ID to name/position via ESPN's athlete endpoint --
    same source and shape the main pipeline's own lookup uses, kept
    independent here since this repo is intentionally self-contained."""
    if athlete_id in _athlete_cache:
        return _athlete_cache[athlete_id]
    url = f"https://sports.core.api.espn.com/v2/sports/basketball/leagues/wnba/athletes/{athlete_id}?lang=en&region=us"
    resp = requests.get(url, impersonate="chrome", timeout=15)
    if resp.status_code != 200:
        result = {"name": "UNKNOWN", "position": ""}
    else:
        data = resp.json()
        name = data.get("fullName") or data.get("displayName") or "UNKNOWN"
        pos = data.get("position", {}).get("abbreviation", "")
        result = {"name": name, "position": pos[0] if pos else ""}
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


def row_proves_window_closed(row):
    """True if an already-captured row for this event proves the first-4-
    minutes window had already fully elapsed as of that capture (period
    moved past 1, or the clock had already ticked below the threshold) --
    meaning there's nothing more to gain by re-fetching this event."""
    if len(row) < 6:
        return False
    try:
        period = int(row[3])
        seconds_remaining = int(row[5])
    except (ValueError, TypeError):
        return False
    return period > 1 or seconds_remaining < MIN_SECONDS_REMAINING


def get_todays_events():
    # datetime.now() is naive and reads UTC on GitHub Actions runners --
    # between 8pm-midnight ET, UTC has already rolled to the next calendar
    # date, which would silently pull tomorrow's slate instead of today's.
    target_str = datetime.now(LOCAL_TZ).strftime("%Y%m%d")
    url = f"https://site.api.espn.com/apis/site/v2/sports/{SPORT_PATH}/scoreboard?dates={target_str}"
    resp = requests.get(url, impersonate="chrome", timeout=15)
    resp.raise_for_status()
    return resp.json().get("events", [])


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
    """Returns (rows, window_closed, first_basket) from a single fetch.
    rows/window_closed cover the first-4-minutes Play By Play capture as
    before; first_basket is the resolved scorer of the game's actual first
    basket (see find_first_basket), independent of that 4-minute window."""
    url = f"https://site.api.espn.com/apis/site/v2/sports/{SPORT_PATH}/summary?event={event_id}"
    resp = requests.get(url, impersonate="chrome", timeout=15)
    resp.raise_for_status()
    data = resp.json()
    plays = data.get("plays", [])
    competitors = data["header"]["competitions"][0]["competitors"]
    team_id_to_name = {c["team"]["id"]: c["team"]["displayName"] for c in competitors}

    first_basket = find_first_basket(plays, team_id_to_name)

    rows = []
    window_closed = False
    for play in plays:
        period = play.get("period", {}).get("number")
        if period is None:
            continue
        if period > 1:
            window_closed = True
            continue
        clock_display = play.get("clock", {}).get("displayValue", "")
        seconds_remaining = parse_clock_to_seconds(clock_display)
        if seconds_remaining is None:
            continue
        if seconds_remaining < MIN_SECONDS_REMAINING:
            window_closed = True
            continue
        team_id = play.get("team", {}).get("id", "")
        team_name = team_id_to_name.get(team_id, team_id or "")
        away_score = play.get("awayScore", "")
        home_score = play.get("homeScore", "")
        text = play.get("text", "")
        is_score = play.get("scoringPlay", False)
        rows.append((period, clock_display, seconds_remaining, team_name, away_score, home_score, text, "Y" if is_score else "N"))
    return rows, window_closed, first_basket


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
    client = gspread.authorize(creds)
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

    updates = {}  # event_id -> new_rows, only for events actually re-fetched
    fb_new_rows = []  # newly-identified first baskets this run
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

        rows_for_event = [r for r in existing_rows if r and r[0] == event_id]
        already_settled = state == "post" or any(row_proves_window_closed(r) for r in rows_for_event)
        if rows_for_event and already_settled:
            continue

        print(f"  Fetching {matchup} (event {event_id}, status: {state})...")
        try:
            plays, window_closed, first_basket = fetch_game_data(event_id)
        except Exception as e:
            print(f"    Could not fetch: {e}")
            continue

        if first_basket and event_id not in fb_known_event_ids:
            fb_new_rows.append([event_id, today_str, matchup, first_basket["name"], first_basket["athlete_id"],
                                 first_basket["team"], first_basket["position"], first_basket["method"],
                                 first_basket["period"], first_basket["clock"], now_str])
            fb_known_event_ids.add(event_id)
            print(f"    First basket: {first_basket['name']} ({first_basket['team']}) -- {first_basket['method']}")

        if not plays and not window_closed:
            print("    Nothing capturable yet -- will retry next run.")
            continue

        new_rows = [[event_id, today_str, matchup, *p] for p in plays]
        updates[event_id] = new_rows
        status_note = "window closed, fully captured" if window_closed else "still filling in, will refine next run"
        print(f"    Captured {len(new_rows)} play(s) -- {status_note}.")

    if fb_new_rows:
        print(f"Writing Live First Basket: {len(fb_new_rows)} new game(s).")
        fb_ws.append_rows(fb_new_rows, value_input_option="USER_ENTERED")

    if not updates:
        print("Nothing new to update.")
        return

    kept_rows = [r for r in existing_rows if r and r[0] not in updates]
    all_rows = kept_rows + [row for rows in updates.values() for row in rows]

    print(f"Writing Play By Play: {len(kept_rows)} untouched row(s) kept, {len(updates)} game(s) updated.")
    pbp_ws.clear()
    pbp_ws.update(range_name="A1", values=[PBP_HEADER], value_input_option="USER_ENTERED")
    if all_rows:
        pbp_ws.update(range_name=f"A2:K{len(all_rows) + 1}", values=all_rows, value_input_option="RAW")
    print("Done.")


if __name__ == "__main__":
    main()
