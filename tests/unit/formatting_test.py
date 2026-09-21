"""Unit tests for pudl_diff.formatting."""

from pudl_diff.formatting import (
    format_bytes,
    format_duration,
    format_percent,
    format_signed_percent,
)


def test_format_bytes():
    """Format bytes."""
    assert format_bytes(0) == "0 B"
    assert format_bytes(999) == "999 B"
    assert format_bytes(1_500) == "1.5 KB"
    assert format_bytes(512_000_000) == "512.0 MB"
    assert format_bytes(21_394_456_576) == "21.4 GB"
    assert format_bytes(-2_500_000) == "-2.5 MB"
    assert format_bytes(2_500_000, signed=True) == "+2.5 MB"
    assert format_bytes(0, signed=True) == "0 B"


def test_format_percent():
    """Format percent."""
    assert format_percent(0, 100) == "0%"
    assert format_percent(0, 0) == "0%"
    assert format_percent(1, 0) == "n/a"
    assert format_percent(1, 100) == "1.00%"
    assert format_percent(1, 1_000_000) == "<0.01%"
    assert format_percent(250, 100) == "250%"
    assert format_percent(12_345, 100_000) == "12.35%"


def test_format_duration():
    """Format duration."""
    assert format_duration(0.0432) == "0.043s"
    assert format_duration(38.0) == "38.000s"
    assert format_duration(125.4) == "2m 05.4s"
    assert format_duration(3723.0) == "1h 02m 03s"


def test_format_signed_percent():
    """Format signed percent."""
    assert format_signed_percent(0) == "0%"
    assert format_signed_percent(1.234) == "+1.23%"
    assert format_signed_percent(-1.234) == "-1.23%"
    assert format_signed_percent(0.001) == "+<0.01%"
    assert format_signed_percent(-0.001) == "-<0.01%"
