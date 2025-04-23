"""Tests for the TimestampResolution to_timedelta method and related functionalities."""

from datetime import timedelta

import rustvello


def test_timestamp_resolution_to_timedelta() -> None:
    """Test TimestampResolution to_timedelta method."""
    test_cases = [
        (rustvello.TimestampResolution.Weeks, timedelta(weeks=1)),
        (rustvello.TimestampResolution.Days, timedelta(days=1)),
        (rustvello.TimestampResolution.Hours, timedelta(hours=1)),
        (rustvello.TimestampResolution.Minutes, timedelta(minutes=1)),
        (rustvello.TimestampResolution.Seconds, timedelta(seconds=1)),
        (rustvello.TimestampResolution.Microseconds, timedelta(microseconds=1)),
        (rustvello.TimestampResolution.Milliseconds, timedelta(milliseconds=1)),
    ]
    for resolution, expected_delta in test_cases:
        result = resolution.to_timedelta()
        assert isinstance(result, timedelta), f"Expected timedelta for {resolution}"
        assert result == expected_delta, f"Mismatch for {resolution}"

    # Years and Months tests
    years_delta = rustvello.TimestampResolution.Years.to_timedelta()
    assert isinstance(years_delta, timedelta)
    assert timedelta(days=365) <= years_delta <= timedelta(days=366)

    months_delta = rustvello.TimestampResolution.Months.to_timedelta()
    assert isinstance(months_delta, timedelta)
    assert timedelta(days=27) <= months_delta <= timedelta(days=31)
