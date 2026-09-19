"""Unit tests for pudl.validate.diff.formatting."""

from pudl.validate.diff.formatting import format_bytes


def test_format_bytes():
    assert format_bytes(0) == "0 B"
    assert format_bytes(999) == "999 B"
    assert format_bytes(1_500) == "1.5 KB"
    assert format_bytes(512_000_000) == "512.0 MB"
    assert format_bytes(21_394_456_576) == "21.4 GB"
    assert format_bytes(-2_500_000) == "-2.5 MB"
    assert format_bytes(2_500_000, signed=True) == "+2.5 MB"
    assert format_bytes(0, signed=True) == "0 B"
