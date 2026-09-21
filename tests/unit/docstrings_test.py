"""Check that the source's docs are written in Markdown, not reStructuredText."""

import re
from pathlib import Path

import pytest

SOURCE_FILES = sorted(Path(__file__).parents[2].glob("src/pudl_diff/*.py"))

# The documentation is built from Markdown by Zensical and mkdocstrings, and the report
# schema's descriptions are these docstrings too, so Sphinx syntax would show up
# verbatim in both.
RST_PATTERNS = {
    "a Sphinx role, like :attr:`name`": re.compile(r":[a-z]+:`"),
    "an RST literal, like ``name``, instead of a Markdown code span": re.compile(
        r"(?<!`)``(?!`)"
    ),
    "a Sphinx attribute comment, `#:`, instead of a docstring": re.compile(r"^\s*#:"),
}


def test_there_are_source_files_to_check():
    assert SOURCE_FILES


@pytest.mark.parametrize("path", SOURCE_FILES, ids=lambda path: path.name)
def test_source_is_written_in_markdown(path: Path):
    found = [
        f"{path.name}:{number}: {what}"
        for number, line in enumerate(path.read_text().splitlines(), start=1)
        for what, pattern in RST_PATTERNS.items()
        if pattern.search(line)
    ]

    assert not found
