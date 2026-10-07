"""Unit tests for pudl_diff.report_schema."""

import copy
import json
import re
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from pudl_diff.report_schema import (
    DOCS_PATH,
    SCHEMA_PATH,
    main,
    report_docs_text,
    report_json_schema,
    report_json_schema_text,
    schema_models,
    schema_type,
)
from pudl_diff.runner import run_dataset_diff
from pudl_diff.table_report import DiffOptions


def _schema_objects(schema: dict) -> dict[str, dict]:
    """The report's schema, and each of the models it refers to, by name."""
    return {"PudlDiffReport": schema, **schema["$defs"]}


def test_every_field_of_the_report_schema_is_described():
    """Every field of every model has a description, as they are the reference documentation.

    The descriptions are the models' attribute docstrings, so a field added without one
    would be undocumented in the schema and on the documentation site.
    """
    schema = report_json_schema()

    undescribed = [
        f"{model}.{field}"
        for model, definition in _schema_objects(schema).items()
        for field, spec in definition.get("properties", {}).items()
        if not spec.get("description", "").strip()
    ]

    assert not undescribed


def test_report_schema_descriptions_are_plain_text():
    """The schema's descriptions are Markdown, without Sphinx syntax or hard line breaks.

    They are the models' docstrings, which are wrapped to the width of the source, and
    may have been written with reStructuredText. In the schema, each paragraph is a single
    line, and only the items of a bullet list keep their own lines.
    """
    descriptions = [
        node["description"]
        for node in _walk(report_json_schema())
        if isinstance(node.get("description"), str)
    ]

    assert descriptions
    for description in descriptions:
        # No Sphinx roles or reStructuredText literals, just Markdown...
        assert not re.search(r":[a-z]+:`|``", description)
        # ...and no line breaks within a paragraph, other than between bullet points.
        for paragraph in description.split("\n\n"):
            assert all(line.startswith("* ") for line in paragraph.splitlines()[1:]), (
                paragraph
            )


def test_report_schema_descriptions_use_json_words_for_python_constants():
    """`None`, `True` and `False` in a description are written as JSON's `null`, `true` and `false`.

    The schema describes JSON, which is what its readers see, and not the Python that
    the models are written in.
    """
    descriptions = [
        node["description"]
        for node in _walk(report_json_schema())
        if isinstance(node.get("description"), str)
    ]

    assert any("`null`" in d for d in descriptions)
    assert any("`true`" in d for d in descriptions)
    for description in descriptions:
        assert not re.search(r"`(None|True|False|none)`", description), description


def _walk(node):
    if isinstance(node, dict):
        yield node
        for child in node.values():
            yield from _walk(child)
    elif isinstance(node, list):
        for child in node:
            yield from _walk(child)


def test_report_schema_requires_every_field_and_marks_derived_ones_read_only():
    """Every field is required in the schema, and derived ones are read only.

    Fields with defaults are still always written to a report, so a reader can rely on them
    being there. Fields that are derived from others, like `is_identical`, are marked read
    only, as they're written for convenience but not read back.
    """
    schema = report_json_schema()

    # Fields with defaults, like schema_version and error, are always written.
    assert {"schema_version", "error", "success", "is_identical"} <= set(
        schema["required"]
    )
    assert schema["properties"]["is_identical"]["readOnly"] is True
    assert "readOnly" not in schema["properties"]["created"]


def test_the_row_diff_sections_are_discriminated_by_status():
    """`pk_diff` and `non_pk_diff` are each one of two kinds of section, told apart by `status`.

    A `"compared"` section is the summary of that kind of comparison, and a `"skipped"` one
    is a stand-in that says why there isn't one.
    """
    schema = report_json_schema()
    row_diff = schema["$defs"]["RowDiffSummary"]["properties"]

    for section, compared in [
        ("pk_diff", "PkRowDiffSummary"),
        ("non_pk_diff", "NonPkRowDiffSummary"),
    ]:
        assert row_diff[section]["discriminator"] == {
            "propertyName": "status",
            "mapping": {
                "compared": f"#/$defs/{compared}",
                "skipped": "#/$defs/RowDiffSectionSkipped",
            },
        }


def test_the_committed_report_schema_is_up_to_date():
    """The JSON Schema in the repository is what the report's models generate now.

    It is published with the documentation, and used by others to validate reports, so it
    mustn't fall behind the code. The pre-commit hook updates it, and this checks that it was.
    """
    committed = SCHEMA_PATH.read_text(encoding="utf-8")

    assert committed == report_json_schema_text(), (
        f"{SCHEMA_PATH} is out of date with the report's models. Update it with "
        "`pixi run python -m pudl_diff.report_schema`."
    )
    assert json.loads(committed) == report_json_schema()


def test_the_committed_report_documentation_is_up_to_date():
    """The page documenting the report's fields is what the report's models generate now."""
    assert DOCS_PATH.read_text(encoding="utf-8") == report_docs_text(), (
        f"{DOCS_PATH} is out of date with the report's models. Update it with "
        "`pixi run python -m pudl_diff.report_schema`."
    )


def test_main_updates_out_of_date_files_and_reports_it(tmp_path: Path):
    """The generator rewrites the schema and its documentation if they're out of date.

    It exits 1 if it changed either, and 0 if it changed nothing, so that as a pre-commit
    hook it fails when it updates a file, and the change gets committed.
    """
    schema_path = tmp_path / "schema.json"
    docs_path = tmp_path / "report_schema.md"

    assert main(schema_path, docs_path) == 1  # written, because they didn't exist
    assert schema_path.read_text(encoding="utf-8") == report_json_schema_text()
    assert docs_path.read_text(encoding="utf-8") == report_docs_text()
    assert main(schema_path, docs_path) == 0  # nothing to change
    schema_path.write_text("{}")
    assert main(schema_path, docs_path) == 1
    assert schema_path.read_text(encoding="utf-8") == report_json_schema_text()
    docs_path.write_text("")
    assert main(schema_path, docs_path) == 1
    assert docs_path.read_text(encoding="utf-8") == report_docs_text()


def test_the_report_documentation_lists_every_model_and_links_between_them():
    """The documentation has a section for each model, and models link to the ones they use."""
    text = report_docs_text()

    assert "## PudlDiffReport" in text
    assert "## TableDiffReport" in text
    assert "[TableDiffReport](#tablediffreport)" in text


def test_the_report_schema_is_a_valid_json_schema():
    """The generated schema is itself a valid JSON Schema, so that tools can use it."""
    Draft202012Validator.check_schema(report_json_schema())


@pytest.fixture
def report_documents(tmp_path: Path, write_two_datasets) -> dict[str, dict]:
    """Reports that cover the ways a comparison can turn out, as parsed JSON."""
    left, right = write_two_datasets(
        tmp_path / "some",
        {"same": ["a", "b"], "changed": ["a", "b"], "gone": ["a"]},
        {"same": ["a", "b"], "changed": ["a", "c"], "new": ["a"]},
    )
    nothing_left, nothing_right = write_two_datasets(
        tmp_path / "none", {"only_left": ["a"]}, {"only_right": ["a"]}
    )
    reports = {
        "changes": run_dataset_diff(left, right, tmp_path / "out"),
        "skipped rows": run_dataset_diff(
            left,
            right,
            tmp_path / "out2",
            options=DiffOptions(max_compare_rows=1),
        ),
        "failed table": run_dataset_diff(
            left, right, tmp_path / "out3", table_names=["not_a_table"]
        ),
        "nothing to compare": run_dataset_diff(
            nothing_left, nothing_right, tmp_path / "out4"
        ),
    }
    return {name: json.loads(r.model_dump_json()) for name, r in reports.items()}


def test_reports_validate_against_the_schema(report_documents):
    """Reports of every kind of outcome are valid according to the schema.

    They cover changes found, a comparison skipped for having too many rows, a table that
    failed, and a run with nothing to compare, so that the schema describes the reports
    that are actually written, and not only the successful ones.
    """
    validator = Draft202012Validator(report_json_schema())

    for name, document in report_documents.items():
        errors = list(validator.iter_errors(document))
        assert not errors, (name, [e.message for e in errors])


def test_the_schema_rejects_reports_that_are_not_valid(report_documents):
    """The schema isn't so loose that it accepts a report that is wrong.

    A report is broken in each of a series of ways, and each is rejected: a missing
    field, one of the wrong type, and a row diff section that is not one of its two kinds.
    """
    validator = Draft202012Validator(report_json_schema())
    document = report_documents["changes"]

    def problems(change) -> list[str]:
        broken = copy.deepcopy(document)
        change(broken)
        return [e.message for e in validator.iter_errors(broken)]

    assert problems(lambda d: d.pop("summary"))
    assert problems(lambda d: d.pop("success"))
    assert problems(lambda d: d.update(created=123))
    assert problems(lambda d: d["tables"]["changed"].update(is_identical="no"))
    assert problems(lambda d: d["tables"]["changed"]["row_diff"].pop("pk_diff"))
    # A row diff section has to be one of the two variants its status names.
    assert problems(
        lambda d: d["tables"]["changed"]["row_diff"]["pk_diff"].update(status="other")
    )
    assert problems(
        lambda d: d["tables"]["changed"]["row_diff"]["non_pk_diff"].update(
            skipped_reason="because"
        )
    )


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        ({"type": "string"}, "string"),
        ({"anyOf": [{"type": "integer"}, {"type": "null"}]}, "integer or null"),
        ({"type": "array", "items": {"type": "string"}}, "array of string"),
        (
            {"type": "object", "additionalProperties": {"type": "integer"}},
            "object mapping names to integer",
        ),
        (
            {
                "type": "array",
                "prefixItems": [{"type": "string"}, {"type": "string"}],
            },
            "[string, string]",
        ),
        ({"const": "skipped", "type": "string"}, '"skipped"'),
        ({"enum": ["a", "b"], "type": "string"}, '"a" or "b"'),
        ({"$ref": "#/$defs/DiffOptions"}, "DiffOptions"),
        (
            {
                "oneOf": [{"$ref": "#/$defs/A"}, {"$ref": "#/$defs/B"}],
                "discriminator": {"propertyName": "status"},
            },
            "A or B, told apart by `status`",
        ),
    ],
)
def test_schema_type_describes_a_property_in_a_few_words(spec, expected):
    """The type of a property is summarised in a few words, for the documentation."""
    assert schema_type(spec) == expected


def test_schema_type_can_link_the_models_it_refers_to():
    """The names of models in a type can be written as links, to their sections of the page."""
    spec = {"anyOf": [{"$ref": "#/$defs/DiffOptions"}, {"type": "null"}]}

    assert schema_type(spec, lambda name: f"<{name}>") == "<DiffOptions> or null"


def test_schema_models_lists_the_report_first_and_then_each_model_it_refers_to():
    """The documentation is ordered with the report first, then each model as it's reached.

    Every model in the schema is in the list once, and the derived fields are marked.
    """
    schema = report_json_schema()

    models = schema_models(schema)

    names = [model["name"] for model in models]
    assert names[0] == "PudlDiffReport"
    assert sorted(names[1:]) == sorted(schema["$defs"])
    assert len(names) == len(set(names))
    report = models[0]
    assert [f["name"] for f in report["fields"]] == list(schema["properties"])
    derived = {f["name"] for f in report["fields"] if f["derived"]}
    assert derived == {"success", "is_identical"}
    assert all(f["description"] for model in models for f in model["fields"])
