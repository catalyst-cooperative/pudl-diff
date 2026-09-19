"""Human-readable formatting of sizes, durations, and percentages."""


def format_bytes(num_bytes: int, *, signed: bool = False) -> str:
    """Format a byte count in decimal units, e.g. ``21.4 GB`` or ``512 B``.

    Args:
        num_bytes: The size to format. May be negative, for a change in size.
        signed: Whether to prefix a positive size with ``+``. Negative sizes
            always get a ``-``.
    """
    magnitude = abs(num_bytes)
    for unit, factor in (("GB", 10**9), ("MB", 10**6), ("KB", 10**3)):
        if magnitude >= factor:
            text = f"{magnitude / factor:.1f} {unit}"
            break
    else:
        text = f"{magnitude} B"
    if num_bytes < 0:
        return f"-{text}"
    return f"+{text}" if signed and num_bytes > 0 else text


def format_elapsed(seconds: float) -> str:
    """Format a duration with enough precision to be meaningful for fast tables."""
    return f"{seconds:.3f}s"


def format_duration(seconds: float) -> str:
    """Format the duration of a whole run, which may be minutes or hours long."""
    if seconds < 60:
        return f"{seconds:.3f}s"
    minutes, secs = divmod(seconds, 60)
    hours, minutes = divmod(int(minutes), 60)
    if hours:
        return f"{hours}h {minutes:02d}m {secs:02.0f}s"
    return f"{minutes}m {secs:04.1f}s"


def format_percent(count: int, total: int | None) -> str:
    """``count`` as a percentage of ``total``, with useful precision when small."""
    if count == 0:
        return "0%"
    if not total:
        return "n/a"
    percent = 100 * count / total
    if percent < 0.01:
        return "<0.01%"
    if percent >= 100:
        return f"{percent:,.0f}%"
    return f"{percent:.2f}%"


def format_signed_percent(percent: float) -> str:
    """A percentage change, always signed, with useful precision when small."""
    if percent == 0:
        return "0%"
    if abs(percent) < 0.01:
        return f"{'+' if percent > 0 else '-'}<0.01%"
    return f"{percent:+,.2f}%"
