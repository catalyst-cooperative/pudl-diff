# PUDL Diff: Compare Two Sets of PUDL Outputs

<!-- readme-intro -->

[![pytest](https://github.com/catalyst-cooperative/pudl-diff/actions/workflows/pytest.yml/badge.svg)](https://github.com/catalyst-cooperative/pudl-diff/actions/workflows/pytest.yml)
[![Codecov Test Coverage](https://img.shields.io/codecov/c/github/catalyst-cooperative/pudl-diff?style=flat&logo=codecov)](https://codecov.io/gh/catalyst-cooperative/pudl-diff)
[![Documentation](https://img.shields.io/github/deployments/catalyst-cooperative/pudl-diff/github-pages?style=flat&logo=githubpages&label=docs)](https://docs.catalyst.coop/pudl-diff)
[![PyPI Latest Version](https://img.shields.io/pypi/v/catalystcoop.pudl_diff?style=flat&logo=python)](https://pypi.org/project/catalystcoop.pudl_diff/)
[![Supported Python Versions](https://img.shields.io/pypi/pyversions/catalystcoop.pudl_diff?style=flat&logo=python)](https://pypi.org/project/catalystcoop.pudl_diff/)

`pudl_diff` compares two sets of
[PUDL](https://github.com/catalyst-cooperative/pudl) Parquet outputs, table by table,
and reports on what changed: the schema (columns and dtypes), the row counts, and the
rows themselves, matched on primary keys where a table has them. It's useful for
confirming that a change to PUDL's code, its dependencies, or its raw inputs did, or
didn't, change the data, and for seeing how nightly builds and stable releases differ.
It reads local directories or remote buckets, and works on tables too big for memory.

It produces:

- a colorized summary in the terminal, one line per table plus totals,
- a JSON report of the whole comparison, described by a published
    [JSON Schema](https://docs.catalyst.coop/pudl-diff/report_schema/), and
- Parquet files holding the rows that differ, for tables that do.

An existing report can be shown again as the same terminal summary, without comparing
anything, with `pudl_diff --from-report`.

## Installation

```bash
uv pip install catalystcoop.pudl_diff
```

`pudl_diff` doesn't depend on PUDL itself. If PUDL is installed in the same
environment it is used for its metadata and defaults, and otherwise `pudl_diff` falls
back on PUDL's published outputs.

## Usage

Compare a table from the last nightly build against your local outputs in
`$PUDL_OUTPUT/parquet`:

```bash
pudl_diff out_eia__yearly_generators
```

Compare every table in two datasets, local or remote, writing the reports to `diffs`:

```bash
pudl_diff --left s3://pudl.catalyst.coop/stable --right ~/my_outputs --output-path diffs
```

See the [documentation](https://docs.catalyst.coop/pudl-diff/usage/) for more, or run
`pudl_diff --help`.

## Development

- Install [pixi](https://pixi.sh) if you don't already have it.
- Run `pixi install` to create the development environment, and `pixi run prek install`
    to install the [pre-commit hooks](https://pre-commit.com/) defined in
    `.pre-commit-config.yaml`, using [prek](https://prek.j178.dev/) as the runner.
- Run `git config merge.ours.driver true` so the `merge=ours` rule in `.gitattributes`
    (which keeps your side of `pixi.lock` on conflict) takes effect.
- Run `pixi run test`, `pixi run lint` and `pixi run format`. See `AGENTS.md` for the
    rest of the tasks. The repository follows the layout of Catalyst's Python template,
    [cheshire](https://github.com/catalyst-cooperative/cheshire).

## About Catalyst Cooperative

[Catalyst Cooperative](https://catalyst.coop) is a small group of data wranglers and
policy wonks organized as a worker-owned cooperative consultancy. Our goal is a more
just, livable, and sustainable world. We integrate public data and perform custom
analyses to inform public policy
([Hire us!](https://catalyst.coop/hire-catalyst)). Our focus is primarily on
mitigating climate change and improving electric utility regulation in the United
States.

### Contact Us

- For general support, questions, or other conversations around the project that
    might be of interest to others, check out the
    [GitHub Discussions](https://github.com/catalyst-cooperative/pudl/discussions).
- If you'd like to get occasional updates about our projects
    [sign up for our email list](https://catalyst.coop/updates/).
- Want to schedule a time to chat with us one-on-one? Join us for
    [Office Hours](https://calend.ly/catalyst-cooperative/pudl-office-hours).
- More info on our website: <https://catalyst.coop>
- For private communication about the project or to hire us to provide customized data
    extraction and analysis, you can email the maintainers:
    [pudl@catalyst.coop](mailto:pudl@catalyst.coop).
