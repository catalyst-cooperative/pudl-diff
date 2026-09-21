"""The parts of a PUDL datapackage descriptor that the tool reads.

A datapackage descriptor is a JSON file describing the tables of a dataset. These
types say what the tool relies on in it, and nothing more: PUDL's descriptors have
many more fields, which are ignored.
"""

from typing import NotRequired, TypedDict


class Field(TypedDict):
    """A column of a table."""

    name: str
    """The column's name."""


class TableSchema(TypedDict):
    """The columns of a table, and its primary key."""

    fields: list[Field]
    """The table's columns, in order."""
    primaryKey: NotRequired[list[str]]
    """The names of the columns that uniquely identify a row, if it has any."""


class Resource(TypedDict):
    """A table of the dataset."""

    name: str
    """The table's name, which is also the name of its Parquet file."""
    schema: TableSchema
    """The table's columns, and its primary key."""


class Datapackage(TypedDict):
    """A dataset's descriptor, of its tables and where the dataset came from."""

    resources: NotRequired[list[Resource]]
    """The dataset's tables."""
    id: NotRequired[str]
    """The dataset's build UUID."""
    created: NotRequired[str]
    """UTC ISO-8601 timestamp of when the dataset was built."""
    git_sha: NotRequired[str]
    """The git commit SHA of the PUDL code that built the dataset."""
    git_tags: NotRequired[list[str]]
    """The git tags on that commit."""
