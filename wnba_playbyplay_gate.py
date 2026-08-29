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
  0 (poll)  -- at least one of today's real, regular-season games is live,
               tips off within LOOKAHEAD_MINUTES, or finished recently
               enough to still need capturing.
  1 (skip)  -- nothing relevant is happening; the hourly check-in can exit
               near-instantly instead of burning ~4.5h for no reason.

The workflow calls this twice: once before starting the poll loop, and again
between poll cycles so the loop can stop as soon as the slate is over
instead of holding the `live-pbp` concurrency group for its full ~265-minute
ceiling. That early exit is what makes the hourly check-in actually hourly --
otherwise one "poll" decision blocks the next ~4 firings whether or not
there's still a game on.

A finished game stays relevant for POST_GAME_GRACE_MINUTES after tip rather
than for the rest of the calendar day. It has to stay relevant for a while:
a game being over doesn't mean a previous run actually captured both teams'
first scoring plays, and the live-grading dispatch may still be owed. But
keeping it relevant indefinitely (which is what `state != "pre"` did) meant
the first game to go final pinned the poll loop for every remaining hour of
the day.

Deliberately fails OPEN (exit 0, i.e. "poll") on any error deciding -- this
repo has no GitHub Actions minutes budget concern (see
wnba_playbyplay_live.py's module docstring), so an unnecessary poll costs
nothing, while a wrongly-skipped one could silently miss a live game. Note
that the fail-open guard wraps the whole decision, not just the scoreboard
fetch: an unhandled shape change anywhere in here would otherwise exit
non-zero, which the workflow reads as "skip" -- a silent no-op on a run that
still reports green.

Reuses get_todays_events()/REAL_TEAMS from wnba_playbyplay_live.py rather
than re-implementing the ESPN scoreboard fetch -- same repo, same source of
truth for "what counts as a real game today."
"""

from datetime import datetime, timezone

from wnba_playbyplay_live import REAL_TEAMS, get_todays_events

# Must comfortably exceed the cron interval, or there is a systematic blind
# window before every tip. With hourly firings and a 15-minute lookahead, a
# game tipping 20+ minutes after a firing is skipped by that firing and only
# picked up by the next one, up to an hour later. Observed live on
# 2026-08-28: the 23:07Z run skipped, POR @ ATL tipped in the gap, and by the
# time polling started the game was well into Q1 -- its post-tip intraday
# dispatch window (60-420s remaining in Q1, read off the CURRENT scoreboard
# clock) had already passed and that dispatch never fired at all. The plays
# and the first basket were still captured, since the summary endpoint
# returns the whole game retroactively, but the dispatch is gone for good.
#
# 75 = the 60-minute cron interval plus margin for GitHub's delivery lag,
# which has been running 7-13 minutes late. Starting a poll loop early costs
# nothing (public repo, unlimited minutes) and the loop now re-gates between
# cycles, so an early start exits on its own once the slate is over.
LOOKAHEAD_MINUTES = 75
# How long after tip-off a finished game still counts as worth polling for.
# Long enough to cover a full game plus overtime plus a late-posted final
# (and to backfill an evening slate the tracker slept through), short enough
# that the day's games stop launching poll loops overnight.
POST_GAME_GRACE_MINUTES = 360


def _tip_time(event):
    """ESPN gives the scheduled tip as an ISO8601 UTC string."""
    date_str = event.get("date")
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except ValueError:
        return None


def _game_is_relevant_now(event, now_utc):
    competitors = (event.get("competitions") or [{}])[0].get("competitors", [])
    team_names = {c.get("team", {}).get("displayName") for c in competitors}
    if not team_names:
        # Couldn't read who's playing at all. Can't rule this out as a real
        # live game, and an extra poll is free -- fail open rather than
        # silently ignoring an event whose shape we don't recognise.
        return True
    if not team_names.issubset(REAL_TEAMS):
        return False  # exhibition/All-Star -- never relevant

    state = event.get("status", {}).get("type", {}).get("state")
    if state == "in":
        return True  # tipped off and still going -- always worth polling

    tip_utc = _tip_time(event)
    if tip_utc is None:
        return False
    minutes_from_tip = (now_utc - tip_utc).total_seconds() / 60

    if state == "post":
        return 0 <= minutes_from_tip <= POST_GAME_GRACE_MINUTES
    # "pre" (or an unrecognised state): once tip-off is close enough that the
    # NEXT cron firing would already be too late -- see LOOKAHEAD_MINUTES.
    return -LOOKAHEAD_MINUTES <= minutes_from_tip <= 0


def should_poll():
    now_utc = datetime.now(timezone.utc)
    try:
        for event in get_todays_events():
            if event.get("season", {}).get("slug") != "regular-season":
                continue
            if _game_is_relevant_now(event, now_utc):
                return True
        return False
    except Exception as e:
        print(f"Could not decide from today's scoreboard ({e}) -- failing open, will poll.")
        return True


if __name__ == "__main__":
    if should_poll():
        print("A game is live, starting soon, or recently finished -- polling.")
        raise SystemExit(0)
    print("Nothing relevant right now -- skipping this hour's run.")
    raise SystemExit(1)
