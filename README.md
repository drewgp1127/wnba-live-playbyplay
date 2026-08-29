# WNBA Live Play By Play

Watches today's WNBA scoreboard and captures each game's opening plays into
the `Play By Play` / `Live First Basket` tabs of the tracker sheet as soon as
they happen, instead of waiting for the main pipeline to log the whole game
as final (1-2+ hours later).

Public by design: this repo holds no prediction or scoring logic, just an
ESPN watcher, so it can poll frequently on unlimited public-repo Actions
minutes.

## How it gets triggered

This is the fragile part, and it's worth understanding before changing it.

`live-playbyplay.yml` fires on `schedule` (`:00` and `:30`). Each firing runs
`wnba_playbyplay_gate.py`, which looks at today's *real* game times and exits
`0` (poll) or `1` (skip); the poll loop only runs on `0`. The loop polls every
5 minutes, re-checks the gate between cycles, and stops as soon as the slate
is over so it doesn't hold the `live-pbp` concurrency group needlessly.

**GitHub's `schedule` trigger is best-effort and has failed hard here.** On
2026-08-27 it delivered **0 of ~21** expected hourly firings while the
workflow was `active`, the YAML valid, the repo public and un-archived, and
manual dispatch working fine. Earlier: the 5-minute cron landed roughly
hourly (`9d6564a`), and one of the 3 fixed windows vanished entirely
(`1829526`). Do not assume a scheduled firing will arrive.

`wnba_playbyplay_selfchain.py` is the mitigation: when a run ends with games
still live, it starts a fresh run via the API, so **one** delivered firing
sustains coverage for a whole slate. The cron is the cold-start path, not an
hourly dependency.

## Required secrets

| Secret | Required? | What it does |
|---|---|---|
| `GOOGLE_CREDENTIALS_JSON` | **Yes** | Service account JSON for the tracker sheet. Without it the run fails. |
| `SELF_DISPATCH_TOKEN` | Strongly recommended | Lets a run hand off to its successor. Without it, coverage ends whenever the ~265-minute loop cap is hit and nothing restarts until a scheduled firing actually lands. |
| `MAIN_PIPELINE_DISPATCH_TOKEN` | Optional | Dispatches the private pipeline's `intraday-updates.yml` (post-tip) and `live-grading.yml` (once both teams have scored). Also used as a fallback for the handoff if `SELF_DISPATCH_TOKEN` is unset. |

### Creating `SELF_DISPATCH_TOKEN`

1. GitHub → Settings → Developer settings → Personal access tokens → **Fine-grained tokens** → *Generate new token*
2. **Repository access**: Only select repositories → `drewgp1127/wnba-live-playbyplay`
3. **Permissions** → Repository permissions → **Actions: Read and write**
4. Generate, copy the token
5. In this repo: Settings → Secrets and variables → Actions → *New repository secret*, named `SELF_DISPATCH_TOKEN`

Set an expiry you'll actually renew — when it lapses the handoff silently
stops and the tracker quietly goes back to depending on the cron alone.

> `GITHUB_TOKEN` **cannot** be used here. GitHub deliberately refuses to start
> a new workflow run from an event raised with it, to prevent exactly the kind
> of recursion the handoff performs on purpose. With no usable token the
> handoff step logs and no-ops.

## Failure modes worth knowing

- **The gate fails open, the handoff fails closed.** An unnecessary poll is
  free; an unnecessary handoff chains another 4.4-hour job, so anything
  uncertain stops chaining and defers to the cron.
- **The gate step is `continue-on-error: true`** — that's how "skip" is
  signalled. So a *crash* in the gate also reads as "skip", and the run still
  reports green. `should_poll()` wraps its whole body to avoid this; keep it
  that way when editing.
- **A run that dies mid-write can leave `Play By Play` empty**, since the
  script does `clear()` then rewrites the whole tab.
