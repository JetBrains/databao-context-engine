[![official project](https://jb.gg/badges/official.svg)](https://confluence.jetbrains.com/display/ALL/JetBrains+on+GitHub)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/JetBrains/databao-context-engine/blob/main/LICENSE)

[//]: # ([![PyPI version]&#40;https://img.shields.io/pypi/v/databao-context-engine.svg&#41;]&#40;https://pypi.org/project/databao-context-engine&#41;)

[//]: # ([![Python versions]&#40;https://img.shields.io/pypi/pyversions/databao-context-engine.svg&#41;]&#40;https://pypi.org/project/databao-context-engine/&#41;)


<h1 align="center">Databao Context Engine</h1>
<p align="center">
 <b>Semantic context for your LLMs — generated automatically.</b><br/>
 No more copying schemas. No manual documentation. Just accurate answers.
</p>
<p align="center">
 <a href="https://databao.app">Website</a> 

[//]: # (•)

[//]: # ( <a href="#quickstart">Quickstart</a> •)

[//]: # ( <a href="#supported-data-sources">Data Sources</a> •)

[//]: # ( <a href="#contributing">Contributing</a>)
</p>

---

## What is Databao Context Engine?

Databao Context Engine **automatically generates governed semantic context** from your databases, BI tools, documents, and spreadsheets.

Integrate it with any LLM to deliver **accurate, context-aware answers** — without copying schemas or writing documentation by hand.

```
Your data sources → Context Engine → Unified semantic graph → Any LLM
```

## Why choose Databao Context Engine?

| Feature                    | What it means for you                                          |
|----------------------------|----------------------------------------------------------------|
| **Auto-generated context** | Extracts schemas, relationships, and semantics automatically   |
| **Runs locally**           | Your data never leaves your environment                        |
| **MCP integration**        | Works with Claude Desktop, Cursor, and any MCP-compatible tool |
| **Multiple sources**       | Databases, dbt projects, spreadsheets, documents               |
| **Built-in benchmarks**    | Measure and improve context quality over time                  |
| **LLM agnostic**           | OpenAI, Anthropic, Ollama, Gemini — use any model              |
| **Governed & versioned**   | Track, version, and share context across your team             |
| **Dynamic or static**      | Serve context via MCP server or export as artifact             |

# Prerequisites

This README assumes you will use `uv` as your package manager.

You can install it following the instructions [here](https://docs.astral.sh/uv/getting-started/installation/)

If you are going to push to the repository, please make sure to install git pre-commit hooks by running

```bash
  uv run pre-commit install
```

# How to run?

You can run it with:

```bash
  uv run dce info
```

Not providing the `info` subcommand or using the `--help` flag will show the help screen for the command.

## Using the dce command directly

To be able to use the `dce` command directly (without using `uv run` or `python`) there are two options.

### Installing dce locally

For that one needs to:

1. Build the project by running

```bash
  uv build
```

2. Installing the project on our machine by running:

```bash
  uv tool install -e .
```

This second step will install the `dce` script on your machine and add it into your path.

### Create dce alias using nix

This method will simply create a new shell environment with `dce` alias. For that one needs to install `nix` package
manager (https://nixos.org/download/). After that one could simply run in the project root

```bash
$ nix-shell
```

which is a short version of `$ nix-shell shell.nix`.

Alternatively, one could specify the path to the project repository

```bash
$ nix-shell {path_to_dce_repository}
```

After that, you can then directly use:

```bash
  dce --help
```

Note: when we actually release our built Python package, users that don't use `uv` will still be able to install the CLI
by using `pipx install` instead.

# Running Mypy

[mypy](https://mypy.readthedocs.io/en/stable/getting_started.html) has been added to the project for type checking.

You can run it with the following:

```bash
  uv run mypy src --exclude "test_*" --exclude dist
```

NB: the above runs type checking on all files within the `src` directory, excluding all test files.

# Running tests

You can run the tests with:

```bash
  uv run pytest
```

(there is currently one test succeeding and one test failing in the project)

# Generating JSON Schemas for our plugin's config files

To be able to build a datasource, each plugin requires a yaml config file that describes how to connect to the
datasource,
as well as other information needed to customise the plugin.

To document what each config file should look like, we can generate a JSON schema describing the fields allowed in that
file.

You can generate all JSON schemas for all plugins by running:

```bash
  uv run generate_configs_schemas
```

Some options can be provided to the command to choose which plugins to include or exclude from the generation.
To see the options available, you can refer to the help:

```bash
  uv run generate_configs_schemas --help
```

