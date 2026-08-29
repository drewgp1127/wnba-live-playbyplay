"""
WNBA LIVE PLAY BY PLAY -- SELF-CHAIN HANDOFF
----------------------------------------------------------------------------
Starts a fresh run of this same workflow when the current one is about to
end while games are still live.

Why this exists: GitHub's `schedule` trigger is best-effort, and for this
repo it has been failing outright -- 0 firings delivered in the 21+ hours
after the hourly cron landed on 2026-08-27, with the workflow active, the
YAML valid, and manual dispatch working fine. The poll loop is capped at
~265 minutes (GitHub's hard runner ceiling is 360), so when that cap is hit
mid-slate the tracker goes dark until the next scheduled firing actually
arrives -- which is exactly the thing that cannot be relied on.

This closes the loop: the run hands off to its own successor via the API, so
a single delivered `schedule` event is enough to keep coverage alive for as
long as games are on. The cron becomes the cold-start/recovery path rather
than the thing every hour depends on.

Deliberately fails CLOSED (no handoff) on any error, which is the opposite
of wnba_playbyplay_gate.py's fail-open stance -- and intentionally so. An
unnecessary poll costs nothing, but an unnecessary *handoff* chains another
4.4-hour job, and a persistent ESPN outage under fail-open would chain runs
back-to-back forever. The scheduled cron is the backstop for the error case,
so when in doubt this stops and lets the schedule take over.

Requires a token with `actions: write` on THIS repo, as either
SELF_DISPATCH_TOKEN or the existing MAIN_PIPELINE_DISPATCH_TOKEN. The
built-in GITHUB_TOKEN cannot be used: GitHub deliberately refuses to start a
new workflow run from an event raised with it, precisely to prevent the kind
of recursion this script does on purpose. With no usable token the script
just no-ops and the cron remains the only trigger.
"""

import os
from datetime import datetime, timezone

from curl_cffi import requests

from wnba_playbyplay_gate import _game_is_relevant_now
from wnba_playbyplay_live import get_todays_events

THIS_REPO = "drewgp1127/wnba-live-playbyplay"
THIS_WORKFLOW = "live-playbyplay.yml"
BRANCH = "master"


def should_hand_off():
    """True only if we're confident a game still needs covering. Any doubt
    (fetch failure, unexpected shape) means don't chain -- see module docs."""
    now_utc = datetime.now(timezone.utc)
    try:
        for event in get_todays_events():
            if event.get("season", {}).get("slug") != "regular-season":
                continue
            if _game_is_relevant_now(event, now_utc):
                return True
        return False
    except Exception as e:
        print(f"Couldn't confirm the slate is still live ({e}) -- not chaining; "
              "the scheduled run is the backstop.")
        return False


def dispatch_self(token):
    url = f"https://api.github.com/repos/{THIS_REPO}/actions/workflows/{THIS_WORKFLOW}/dispatches"
    try:
        resp = requests.post(
            url,
            json={"ref": BRANCH},
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
            },
            impersonate="chrome",
            timeout=15,
        )
        if resp.status_code == 204:
            return True
        print(f"    Handoff rejected (HTTP {resp.status_code}) -- token likely lacks "
              "`actions: write` on this repo.")
        return False
    except Exception as e:
        print(f"    Handoff failed: {e}")
        return False


def main():
    token = os.environ.get("SELF_DISPATCH_TOKEN") or os.environ.get("MAIN_PIPELINE_DISPATCH_TOKEN")
    if not token:
        print("No self-dispatch token set -- skipping handoff. "
              "The next scheduled firing is the only thing that will restart polling.")
        return
    if not should_hand_off():
        print("Slate is over -- no handoff needed.")
        return
    print("Games still live and this run is ending -- starting a fresh run...")
    if dispatch_self(token):
        print("    Handed off.")


if __name__ == "__main__":
    main()
