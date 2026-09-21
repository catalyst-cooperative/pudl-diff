"""The base class of the models that make up the JSON report."""

import pydantic


class ReportModel(pydantic.BaseModel):
    """A model in the PUDL Diff JSON report.

    The report's JSON Schema is generated from these models, so that the
    documentation of what each field means and how it is validated can't drift from
    the code:

    * A field's docstring, the string right after its definition, is the `description`
      of the field in the schema.
    * Fields that have a default value are still always written to the JSON, so
      the schema of the report lists them as required.
    """

    model_config = pydantic.ConfigDict(
        use_attribute_docstrings=True,
        json_schema_serialization_defaults_required=True,
    )
