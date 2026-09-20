"""The JSON Schema of the PUDL Diff JSON report, and its rendering as documentation.

The schema is generated from the report's Pydantic models, so it always matches the
code. A copy is committed at :data:`SCHEMA_PATH` so that it can be linked to and used
without running any PUDL code, and a test checks that the copy is up to date. To
update it, run ``pixi run python -m pudl.validate.diff.report_schema``, which is also
what the ``pudl-diff-schema`` pre-commit hook does.
"""

import json
import re
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pudl import PUDL_DOCS_PATH
from pudl.validate.diff.dataset_report import PudlDiffReport

SCHEMA_PATH = PUDL_DOCS_PATH / "_static/pudl_diff_report.schema.json"
"""Where the committed copy of the report's JSON Schema is."""

_SPHINX_ROLE = re.compile(r":(?:class|func|attr|data|exc|meth|obj|mod):`~?\.?([^`]+)`")
_ROOT_MODEL = "PudlDiffReport"


def _plain_description(description: str) -> str:
    """A docstring as the description of a JSON Schema property.

    The Sphinx roles (like ``:attr:`success```) become plain code spans, ``None``,
    ``True`` and ``False`` become JSON's ``null``, ``true`` and ``false``, and the
    lines of each paragraph are joined, so that the text reads well without being
    rendered as reStructuredText, and isn't broken at the docstring's line width.
    Bullet points keep their own lines.
    """
    text = _SPHINX_ROLE.sub(r"``\1``", description)
    # The schema is of JSON, so its descriptions should use JSON's words.
    text = re.sub(r"``(None|True|False)``", lambda m: f"``{m[1].lower()}``", text)
    paragraphs = []
    for paragraph in text.split("\n\n"):
        lines: list[str] = []
        for line in paragraph.splitlines():
            if lines and not line.lstrip().startswith("* "):
                lines[-1] += " " + line.strip()
            else:
                lines.append(line.strip())
        paragraphs.append("\n".join(lines))
    return "\n\n".join(paragraphs)


def report_json_schema() -> dict[str, Any]:
    """The JSON Schema of the JSON report, which every ``PudlDiffReport`` conforms to.

    Generated from the report's models, so that the schema and the descriptions of
    what each field means always match the code. It describes the report as it is
    written (its ``serialization`` schema): fields that have a default value are
    still always present, and fields derived from others, like
    :attr:`PudlDiffReport.is_identical`, are listed and marked read-only.
    """
    schema = PudlDiffReport.model_json_schema(mode="serialization")

    def clean(node: Any) -> None:
        if isinstance(node, dict):
            if isinstance(node.get("description"), str):
                node["description"] = _plain_description(node["description"])
            for child in node.values():
                clean(child)
        elif isinstance(node, list):
            for child in node:
                clean(child)

    clean(schema)
    return schema


def report_json_schema_text() -> str:
    """:func:`report_json_schema` as the text of the committed schema file."""
    return json.dumps(report_json_schema(), indent=2) + "\n"


def _model_name(ref: str) -> str:
    return ref.removeprefix("#/$defs/")


def schema_type(spec: dict[str, Any], link: Callable[[str], str] = str) -> str:
    """Describe the type of a property of the schema in a few words.

    Args:
        spec: The property's schema.
        link: How to write the name of a model that the property refers to.
    """
    if "$ref" in spec:
        return link(_model_name(spec["$ref"]))
    if "const" in spec:
        return json.dumps(spec["const"])
    if "enum" in spec:
        return " or ".join(json.dumps(value) for value in spec["enum"])
    for key in ("anyOf", "oneOf"):
        if key in spec:
            label = " or ".join(schema_type(option, link) for option in spec[key])
            if "discriminator" in spec:
                field = spec["discriminator"]["propertyName"]
                label += f", told apart by ``{field}``"
            return label
    kind = spec.get("type")
    if kind == "array":
        if "prefixItems" in spec:
            items = ", ".join(schema_type(item, link) for item in spec["prefixItems"])
            return f"[{items}]"
        return f"array of {schema_type(spec.get('items', {}), link)}"
    if kind == "object" and "additionalProperties" in spec:
        values = schema_type(spec["additionalProperties"], link)
        return f"object mapping names to {values}"
    return str(kind or "any")


def schema_models(
    schema: dict[str, Any], link: Callable[[str], str] = str
) -> list[dict[str, Any]]:
    """Flatten a report schema into a list of models, for rendering as documentation.

    The report's own model comes first, followed by the models it refers to, in the
    order that they're first mentioned. Each is a dictionary with its ``name``,
    ``description``, and ``fields``: a list of dictionaries with each field's
    ``name``, ``type``, ``description`` and whether it is ``derived`` from other fields.
    """
    models = {_ROOT_MODEL: schema, **schema.get("$defs", {})}
    order = [_ROOT_MODEL]
    for name in order:
        for spec in models[name].get("properties", {}).values():
            for node in _walk(spec):
                if "$ref" in node and (ref := _model_name(node["$ref"])) not in order:
                    order.append(ref)
    return [
        {
            "name": name,
            "description": models[name].get("description", ""),
            "fields": [
                {
                    "name": field,
                    "type": schema_type(spec, link),
                    "description": spec.get("description", ""),
                    "derived": bool(spec.get("readOnly")),
                }
                for field, spec in models[name].get("properties", {}).items()
            ],
        }
        for name in order
    ]


def _walk(node: Any) -> Any:
    """Each dictionary in a nested structure of dictionaries and lists."""
    if isinstance(node, dict):
        yield node
        for child in node.values():
            yield from _walk(child)
    elif isinstance(node, list):
        for child in node:
            yield from _walk(child)


def main(path: Path = SCHEMA_PATH) -> int:
    """Write the JSON Schema of the report to ``path``, if it has changed.

    Returns:
        ``1`` if the file was out of date and had to be updated, so that this can be
        used as a pre-commit hook, and ``0`` otherwise.
    """
    text = report_json_schema_text()
    if path.exists() and path.read_text() == text:
        return 0
    path.write_text(text)
    print(f"Updated {path}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
