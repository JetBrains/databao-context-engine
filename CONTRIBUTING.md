# Contributing to `databao-context-engine`

`databao-context-engine` is open source software. We welcome and encourage everyone to contribute code, documentation,
issues or just raise any questions you might have.

## Table of Contents

- [Setting up an environment](#setting-up-an-environment)
    - [Pre-commit](#pre-commit)
    - [Docker](#docker)
    - [Make](#make)
- [Running `dce` locally](#running-dce-locally)
    - [Using the `dce` command directly](#using-the-dce-command-directly)
        - [Installing `dce` locally](#installing-dce-locally)
        - [Create `dce` alias](#create-dce-alias)
        - [Create alias using `nix` shell](#create-alias-using-nix-shell)
- [Testing](#testing)
    - [Running test commands](#running-test-commands)
    - [Running linters](#running-linters)
    - [Running Mypy](#running-mypy)
- [Generating JSON Schemas for our plugin's config files](#generating-json-schemas-for-our-plugins-config-files)

## Setting up an environment

These are the tools used in `databao-context-engine` development and testing:

- [`uv`](https://docs.astral.sh/uv) is our package and project manager. It's used to build, manage environments, run
  tests and linters. This is the **only mandatory tool** to run project locally (except `docker` which is used only for
  testing), for installation follow the
  instructions [here](https://docs.astral.sh/uv/getting-started/installation/)
- [`pytest`](https://docs.pytest.org/en/latest/) is our test framework
- [`ruff`](https://docs.astral.sh/ruff/) for code linting and formatting
- [`mypy`](https://mypy.readthedocs.io/en/stable/) for static type checking
- [`ruff-pre-commit`](https://github.com/astral-sh/ruff-pre-commit) is a [`pre-commit`](https://pre-commit.com/) hook
  for `ruff`

### Pre-commit

If you are going to push to the repository, please make sure to install git pre-commit hooks by running.

```shell
  uv run pre-commit install
```

### Docker

Some tests rely on [`testcontainers`](https://testcontainers-python.readthedocs.io/en/latest/) thus docker
installation is mandatory to run them. Specific instructions for your OS can be
found [here](https://docs.docker.com/get-docker/).

### Make

[`make`](https://www.gnu.org/software/make/) is used just for running high level aggregated commands. It's not
mandatory, but it's usually installed by default on many distributions. Running `make` without target by default
will execute every step required before pushing to repository:

- check code formatting
- check static type checks
- run tests with all extras environment
- run special tests with a recommended environment

## Running `dce` locally

You can run `dce` with this command:

```shell
  uv run dce info
```

Not providing the `info` subcommand or using the `--help` flag will show the help screen for the command.

### Using the `dce` command directly

To be able to use the `dce` command directly (without using `uv run` or `python`) there are two options.

#### Installing `dce` locally

For development purposes or just to try the library, one could install `dce` locally.

##### Using `uv`

For that one needs to:

1. Build the project by running:

```shell
  uv build
```

2. Installing the project on local machine by running:

```shell
  uv tool install -e .
```

This second step will install the `dce` script on your machine and add it into your path.

##### Using `pipx`

Alternatively, one could run `pipx install` instead.

#### Create `dce` alias

To create a `dce` alias in your terminal one needs to run:

```shell
alias dce='uv --project ${projectDir} run dce'
```

After that, you can then directly use from any folder in your file system:

```shell
  dce --help
```

#### Create alias using `nix` shell

This method will simply create a new shell environment with `dce` alias for you automatically. For that one needs to
install `nix` package manager (https://nixos.org/download/). After that one could simply run:

```shell
$ nix-shell {path_to_dce_repository}
```

This will make sure `dce` command is available in the current terminal session.

## Testing

For testing we use `uv` and `pytest` framework. `uv` manages environments and dependencies automatically. Currently,
there are two environments which we test:

1. Main testing environment includes all `dce`
   package [optional-dependencies](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/#dependencies-optional-dependencies)
2. Another testing environment is for `recommended` set of extras

### Running test commands

To run all tests excluding `recommended` environment which is the majority of all tests:

```shell
  uv run --all-extras pytest
```

To run tests for the `recommended` set of extras:

```shell
uv run --isolated --extra recommended pytest tests/recommended_extras --run-recommended-extras
```

To run them all together with one command - `make test`

### Running linters

To make sure there are no linting errors you can run:

```shell
uv run ruff check
```

You can also run linters to make sure that code is correctly formatted:

```shell
uv run ruff format --check
```

You can run both these commands with `make lint`

### Running Mypy

You can run mypy to staticaly check types with the following command:

```shell
  uv run mypy . --exclude dist --exclude docs
```

or just with `make mypy` so you don't have to remember all the arguments.

## Generating JSON Schemas for our plugin's config files

To be able to build a datasource, each plugin requires a YAML config file that describes how to connect to the
datasource, as well as other information needed to customize the plugin.

To document what each config file should look like, we can generate a JSON schema describing the fields allowed in that
file.

You can generate all JSON schemas for all plugins by running:

```shell
  uv run generate_configs_schemas
```

Some options can be provided to the command to choose which plugins to include or exclude from the generation.
To see the options available, you can refer to the help:

```shell
  uv run generate_configs_schemas --help
```