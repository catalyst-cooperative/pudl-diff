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
