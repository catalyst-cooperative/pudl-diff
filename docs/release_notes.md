# PUDL Diff Release Notes

## X.Y.Z (YYYY-MM-DD)

<!-- Copy this section to the top of the file for each new release: fill in the real
version number and date above, and delete whichever subheadings below don't apply.

Agents: don't fill in real content here -- this is a reusable template, not a place to
record actual changes. Add your change to the section for the upcoming/unreleased
version instead: that's the first real, numbered `## X.Y.Z (YYYY-MM-DD)` section below
this one. If it doesn't exist yet, create it (with a placeholder date, or the expected
release date if known) by copying this template immediately below it, same as you
would when cutting an actual release. See AGENTS.md's Documentation section for more. -->

### What's New?

- Briefly describe the substantial changes to the code in here when you make a PR.
- You can refer to the relevant pull request like this: !1
- Don't hesitate to give shoutouts to folks who contributed, e.g. @cmgosnell
- You can link to issues that were closed like this: #2, #3, #4

### Bug Fixes

- Note any known bugs that are fixed by the release, and refer to the relevant issues.

### Known Issues

- List any remaining known problems, and link to their issues too.

## 0.1.0 (2026-XX-XX)

### What's New?

- The first release of `pudl_diff`, a tool for comparing two sets of PUDL Parquet
    outputs table by table (schema, row counts, and row contents) and reporting on
    what changed, as a colorized terminal summary and a JSON report described by a
    published JSON Schema. It began life inside the
    [PUDL repository](https://github.com/catalyst-cooperative/pudl).
- By default (`--quiet`), the live table lists only the tables that aren't
    identical, and leaves out the size columns.
    `--verbose` lists every table with its sizes, as before.
- The live table is more compact: a table's status is an emoji (✅ identical,
    ⚠️ changed, ❌ error) with no heading, and whether it has a primary key is 🔑 or
    🚫, under a `PK` heading.
