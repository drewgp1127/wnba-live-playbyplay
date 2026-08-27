"""
WNBA LIVE PLAY BY PLAY -- SCHEDULE GATE
----------------------------------------------------------------------------
Decides whether live-playbyplay.yml's polling loop is actually worth running
right now, based on TODAY's real game times -- not a blind guessed clock
window. The workflow used to fire 3 fixed ~4.5h windows a day (10am, 2:30pm,
7pm ET) regardless of whether a game was anywhere near those windows; on
2026-08-26 the 7pm ET firing silently never happened at all (GitHub's
`schedule` trigger is documented as best-effort), leaving the tracker dead
for 5+ hours during that night's games with nothing to notice or recover.

This script replaces "3 big blind windows" with "check every hour, only do
real work when a game is actually relevant." Exit code is the signal the
workflow branches on:
  0 (poll)  -- at least one of today's real, regular-season games has
               either already started/finished, or tips off within
               LOOKAHEAD_MINUTES.
  1 (skip)  -- nothing relevant is happening; the hourly check-in can exit
               near-instantly instead of burning ~4.5h for no reason.

Deliberately fails OPEN (exit 0, i.e. "poll") on any error fetching the
scoreboard -- this repo has no GitHub Actions minutes budget concern (see
wnba_playbyplay_live.py's module docstring), so an unnecessary poll costs
nothing, while a wrongly-skipped one could silently miss a live game.

Reuses get_todays_events()/REAL_TEAMS from wnba_playbyplay_live.py rather
than re-implementing the ESPN scoreboard fetch -- same repo, same source of
truth for "what counts as a real game today."
"""

from datetime import datetime, timezone

from wnba_playbyplay_live import REAL_TEAMS, get_todays_events

LOOKAHEAD_MINUTES = 15


def _game_is_relevant_now(event, now_utc):
    state = event.get("status", {}).get("type", {}).get("state")
    if state != "pre":
        return True  # already live or finished -- may still need capturing

    competitors = event.get("competitions", [{}])[0].get("competitors", [])
    team_names = {c.get("team", {}).get("displayName") for c in competitors}
    if not team_names.issubset(REAL_TEAMS):
        return False  # exhibition/All-Star -- never relevant

    date_str = event.get("date")  # ESPN gives this as an ISO8601 UTC string
    if not date_str:
        return False
    try:
        tip_utc = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except ValueError:
        return False
    minutes_until_tip = (tip_utc - now_utc).total_seconds() / 60
    return 0 <= minutes_until_tip <= LOOKAHEAD_MINUTES


def should_poll():
    now_utc = datetime.now(timezone.utc)
    try:
        events = get_todays_events()
    except Exception as e:
        print(f"Could not fetch today's scoreboard ({e}) -- failing open, will poll.")
        return True

    for event in events:
        if event.get("season", {}).get("slug") != "regular-season":
            continue
        if _game_is_relevant_now(event, now_utc):
            return True
    return False


if __name__ == "__main__":
    if should_poll():
        print("A game is live, finished, or starting soon -- polling.")
        raise SystemExit(0)
    print("Nothing relevant right now -- skipping this hour's run.")
    raise SystemExit(1)
