"""
Which HTTP methods the Sheets client is allowed to retry.

WHY THIS FILE EXISTS: the retry wrapper in sheets.py used to retry every
request, including POST. `values:append` is a POST, and Google can commit
an append and STILL return a 5xx -- the row is in the sheet, the caller
sees a failure, and a blind retry appends it a second time.

This repo is the one that appends. wnba_playbyplay_live.py appends to
'Live First Basket', to the dispatch tab and to the grading dispatch tab,
and the FirstIQ app reads 'Live First Basket' directly to show which
player scored first. A duplicated row there is one game's first basket
recorded twice, which the app then joins to its slate. Nothing downstream
dedupes it and nothing logs that it happened -- which is why the rule
needs a test rather than a comment.

    pip install pytest && python -m pytest tests/ -q
"""

import sheets


class TestRequestMethodDetection:
    """gspread calls request(method, endpoint, ...) -- positionally in
    some versions, by keyword in others. Both have to be readable, or the
    gate falls back to 'unknown' and nothing is ever retried."""

    def test_a_positional_method_is_found(self):
        assert sheets._request_method(("get", "/v4/spreadsheets/x"), {}) == "GET"

    def test_a_keyword_method_is_found(self):
        assert sheets._request_method((), {"method": "post"}) == "POST"

    def test_the_verb_is_upper_cased(self):
        assert sheets._request_method(("PuT", "/x"), {}) == "PUT"

    def test_an_undeterminable_method_is_none(self):
        assert sheets._request_method((), {}) is None

    def test_a_non_string_method_is_none(self):
        """Rather than raising inside the transport wrapper, which would
        turn an odd call into a crash on every request."""
        assert sheets._request_method((None,), {}) is None


class TestRetryGate:
    def test_reads_are_retried(self):
        """The quota this wrapper exists for is a READ quota."""
        assert "GET" in sheets._RETRYABLE_METHODS

    def test_range_writes_are_retried(self):
        """values.update is a PUT: writing the same range with the same
        body twice leaves the same sheet, so a retry costs nothing."""
        assert "PUT" in sheets._RETRYABLE_METHODS

    def test_appends_are_not_retried(self):
        """THE regression. A retried append duplicates a row that Google
        already committed, in the tab the app reads."""
        assert "POST" not in sheets._RETRYABLE_METHODS

    def test_an_unknown_method_is_not_retried(self):
        """Fail closed. Guessing wrong here costs one retry; guessing
        wrong the other way costs a duplicate row nobody can see."""
        assert sheets._request_method((), {}) not in sheets._RETRYABLE_METHODS


def test_no_worksheet_write_relies_on_gspread_argument_order():
    """gspread has swapped update()'s argument order once already and
    accepts the old form only through a shim that detects (str, list).
    A positional call is a bet on which major version is installed at run
    time -- and this process runs unattended on a 300s loop.

    Asserts the ABSENCE of the positional form rather than the presence of
    a keyword one. The first version of this test looked for
    `ws.update(range_name=` in the file, which passed vacuously: the
    substring also matches `pbp_ws.update(range_name=` on a different line,
    so it stayed green with the bug reintroduced.
    """
    import re

    source = open("wnba_playbyplay_live.py", encoding="utf-8").read()
    # `.update(` whose first argument is a list or a bare string, i.e. the
    # positional (values, range) or (range, values) forms.
    positional = re.findall(r"\.update\(\s*(?:\[|[\"'f])", source)
    assert not positional, (
        f"{len(positional)} worksheet write(s) pass update() arguments "
        f"positionally. Use range_name=/values= -- the gspread 6 shim that "
        f"makes the old order work is a deprecation, not a contract."
    )
