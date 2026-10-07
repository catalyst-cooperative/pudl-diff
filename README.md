# PUDL Diff: Compare Two Sets of PUDL Outputs

<!-- readme-intro -->

[![Project Status: Active](https://www.repostatus.org/badges/latest/active.svg)](https://www.repostatus.org/#active)
[![pytest](https://github.com/catalyst-cooperative/pudl-diff/actions/workflows/pytest.yml/badge.svg)](https://github.com/catalyst-cooperative/pudl-diff/actions/workflows/pytest.yml)
[![pyrefly](https://img.shields.io/github/actions/workflow/status/catalyst-cooperative/pudl-diff/pytest.yml?branch=main&label=pyrefly)](https://github.com/catalyst-cooperative/pudl-diff/actions/workflows/pytest.yml)
[![Codecov Test Coverage](https://img.shields.io/codecov/c/github/catalyst-cooperative/pudl-diff?style=flat&logo=codecov)](https://codecov.io/gh/catalyst-cooperative/pudl-diff)
[![Documentation](https://img.shields.io/github/deployments/catalyst-cooperative/pudl-diff/github-pages?style=flat&logo=githubpages&label=docs)](https://docs.catalyst.coop/pudl-diff)
[![PyPI Latest Version](https://img.shields.io/pypi/v/catalystcoop.pudl_diff?style=flat&logo=python)](https://pypi.org/project/catalystcoop.pudl_diff/)
[![conda-forge Version](https://img.shields.io/conda/vn/conda-forge/catalystcoop.pudl_diff?style=flat&logo=condaforge)](https://anaconda.org/conda-forge/catalystcoop.pudl_diff)
[![Supported Python Versions](https://img.shields.io/pypi/pyversions/catalystcoop.pudl_diff?style=flat&logo=python)](https://pypi.org/project/catalystcoop.pudl_diff/)
[![License: MIT](https://img.shields.io/github/license/catalyst-cooperative/pudl-diff)](https://github.com/catalyst-cooperative/pudl-diff/blob/main/LICENSE.txt)
[![Formatted by ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![pre-commit CI](https://results.pre-commit.ci/badge/github/catalyst-cooperative/pudl-diff/main.svg)](https://results.pre-commit.ci/latest/github/catalyst-cooperative/pudl-diff/main)

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
# or
pip install catalystcoop.pudl_diff
# or
conda install -c conda-forge catalystcoop.pudl_diff
# or
pixi add catalystcoop.pudl_diff
```

Python 3.14 or newer is required.

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

## Related Projects

- [PUDL](https://github.com/catalyst-cooperative/pudl), the Public Utility Data
    Liberation Project, whose outputs `pudl_diff` compares, and whose nightly builds
    and stable releases are its default inputs.
- [PUDL Data Viewer](https://data.catalyst.coop) for browsing and querying PUDL's
    published data.
- [PUDL Examples](https://docs.catalyst.coop/pudl-examples), notebooks that work with
    PUDL data.
- [Catalyst Agent Skills](https://docs.catalyst.coop/agent-skills), for helping
    LLM-based agents work with PUDL data and code.
- [FERC XBRL Extractor](https://docs.catalyst.coop/ferc-xbrl-extractor), which
    produces some of the data that goes into PUDL.
- [All of Catalyst's documentation](https://docs.catalyst.coop).

## Contributing

Bug reports, questions and pull requests are welcome in the
[issue tracker](https://github.com/catalyst-cooperative/pudl-diff/issues).
Please follow our [Code of Conduct](https://docs.catalyst.coop/pudl-diff/code_of_conduct/).
The [changelog](https://docs.catalyst.coop/pudl-diff/changelog/) lists what has
changed, and why.

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
