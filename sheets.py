"""
WNBA LIVE PLAY BY PLAY -- hardened Google Sheets client.

Sheets enforces a per-minute, per-user read quota, and it is enforced per
SERVICE ACCOUNT -- not per repo. This repo shares one service account with
the main pipeline, so both are drawing on the same quota bucket.

That matters more here than anywhere else in the system. This is the most
quota-hungry component by a wide margin: it polls every 5 minutes for
~4.4 hours during game windows, and each poll does a full get_all_values()
on four tabs plus a whole-tab rewrite of Play By Play. Everything else in
the system runs a handful of times a day.

The main pipeline learned this the hard way on 2026-08-27, when a 429 on
a Sheets read killed a run outright (gspread raises APIError and the step
dies), and fixed it with exponential backoff in firstiq/sheets.py. This
repo is deliberately independent of that codebase -- it is public, the
pipeline is private, and it can't import across that line -- so it never
received the fix and kept calling gspread.authorize() bare. This module is
that fix, duplicated here on purpose for the same reason _get()'s retry
policy is duplicated in wnba_playbyplay_live.py.

authorize() returns an ordinary gspread client whose single HTTP choke
point is wrapped in backoff. Every read and write in gspread funnels
through that one method, so one wrapper covers the whole surface and
callers keep using the normal gspread API.

    import sheets
    client = sheets.authorize(creds)
"""

import gspread
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

# 429 is the quota ceiling this exists for; the 5xx entries are Google's
# ordinary transient failures. Every other 4xx (bad sheet ID, revoked
# credentials, malformed range) is a real error that will not fix itself.
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


def _status_code(exc):
    """gspread has moved this around between majors -- 6.x sets both
    APIError.code and APIError.response, older versions only the latter."""
    code = getattr(exc, "code", None)
    if isinstance(code, int):
        return code
    resp = getattr(exc, "response", None)
    return getattr(resp, "status_code", None)


def _is_retryable(exc):
    return isinstance(exc, gspread.exceptions.APIError) and _status_code(exc) in _RETRYABLE_STATUS


def authorize(creds):
    """gspread.authorize(), plus retry on quota and transient errors.

    5 attempts with ~5s/10s/20s/40s jittered backoff. The read quota is
    per *minute*, so the point of the long tail is to still be trying once
    the window has rolled over rather than to retry quickly.

    The polling loop sleeps 300s between runs, so even a full 5-attempt
    backoff finishes comfortably inside one poll interval -- a retried run
    delays the next poll's start, it never overlaps it.
    """
    client = gspread.authorize(creds)

    # gspread 6 moved transport onto client.http_client; 5.x kept
    # request() on the client itself. Wrap whichever one is really there.
    transport = getattr(client, "http_client", client)
    inner = transport.request

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=5, max=60),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
    )
    def request(*args, **kwargs):
        return inner(*args, **kwargs)

    transport.request = request
    return client
