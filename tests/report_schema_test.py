"""Unit tests for pudl.validate.diff.report_schema."""

import copy
import json
import re
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from pudl.validate.diff.report_schema import (
    SCHEMA_PATH,
    main,
    report_json_schema,
    report_json_schema_text,
    schema_models,
    schema_type,
)
from pudl.validate.diff.runner import run_dataset_diff
from pudl.validate.diff.table_report import DiffOptions


def _schema_objects(schema: dict) -> dict[str, dict]:
    """The report's schema, and each of the models it refers to, by name."""
    return {"PudlDiffReport": schema, **schema["$defs"]}


def test_every_field_of_the_report_schema_is_described():
    schema = report_json_schema()

    undescribed = [
        f"{model}.{field}"
        for model, definition in _schema_objects(schema).items()
        for field, spec in definition.get("properties", {}).items()
        if not spec.get("description", "").strip()
    ]

    assert not undescribed


def test_report_schema_descriptions_are_plain_text():
    descriptions = [
        node["description"]
        for node in _walk(report_json_schema())
        if isinstance(node.get("description"), str)
    ]

    assert descriptions
    for description in descriptions:
        # No Sphinx roles...
        assert not re.search(r":(class|func|attr|data|meth|obj|exc):`", description)
        # ...and no line breaks within a paragraph, other than between bullet points.
        for paragraph in description.split("\n\n"):
            assert all(line.startswith("* ") for line in paragraph.splitlines()[1:]), (
                paragraph
            )


def _walk(node):
    if isinstance(node, dict):
        yield node
        for child in node.values():
            yield from _walk(child)
    elif isinstance(node, list):
        for child in node:
            yield from _walk(child)


def test_report_schema_requires_every_field_and_marks_derived_ones_read_only():
    schema = report_json_schema()

    # Fields with defaults, like schema_version and error, are always written.
    assert {"schema_version", "error", "success", "is_identical"} <= set(
        schema["required"]
    )
    assert schema["properties"]["is_identical"]["readOnly"] is True
    assert "readOnly" not in schema["properties"]["created"]


def test_the_row_diff_sections_are_discriminated_by_status():
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
    committed = SCHEMA_PATH.read_text()

    assert committed == report_json_schema_text(), (
        f"{SCHEMA_PATH} is out of date with the report's models. Update it with "
        "`pixi run python -m pudl.validate.diff.report_schema`."
    )
    assert json.loads(committed) == report_json_schema()


def test_main_updates_an_out_of_date_schema_file_and_reports_it(tmp_path: Path):
    path = tmp_path / "schema.json"

    assert main(path) == 1  # written, because it didn't exist
    assert path.read_text() == report_json_schema_text()
    assert main(path) == 0  # nothing to change
    path.write_text("{}")
    assert main(path) == 1
    assert path.read_text() == report_json_schema_text()


def test_the_report_schema_is_a_valid_json_schema():
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
    validator = Draft202012Validator(report_json_schema())

    for name, document in report_documents.items():
        errors = list(validator.iter_errors(document))
        assert not errors, (name, [e.message for e in errors])


def test_the_schema_rejects_reports_that_are_not_valid(report_documents):
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
            "A or B, told apart by ``status``",
        ),
    ],
)
def test_schema_type_describes_a_property_in_a_few_words(spec, expected):
    assert schema_type(spec) == expected


def test_schema_type_can_link_the_models_it_refers_to():
    spec = {"anyOf": [{"$ref": "#/$defs/DiffOptions"}, {"type": "null"}]}

    assert schema_type(spec, lambda name: f"<{name}>") == "<DiffOptions> or null"


def test_schema_models_lists_the_report_first_and_then_each_model_it_refers_to():
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
