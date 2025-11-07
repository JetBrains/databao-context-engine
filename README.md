# Prerequisites

This README assumes you will use `uv` as your package manager.

You can install it following the instructions [here](https://docs.astral.sh/uv/getting-started/installation/)

# How to run?

The project doesn't do much yet, it only exposes a nemory CLI, that has an info subcommand.

You can run it with:

```bash
  uv run nemory info
```

Not providing the `info` subcommand or using the `--help` flag will show the help screen for the command.

## Using the nemory command directly

To be able to use the `nemory` command directly (without using `uv run` or `python`), we need to:

1. Build the project by running

```bash
  uv build
```

2. Installing the project on our machine by running:

```bash
  uv tool install -e .
```

This second step will install the `nemory` script on your machine and add it into your path.

You can then directly use:

```bash
  nemory --help
```

Note: when we actually release our built Python package, users that don't use `uv` will still be able to install the CLI
by using `pipx install` instead.

# Running Mypy

[mypy](https://mypy.readthedocs.io/en/stable/getting_started.html) has been added to the project for type checking.

You can run it with the following:

```bash
  uv run mypy src --exclude "test_*"
```

NB: the above runs type checking on all files within the `src` directory, excluding all test files.

# Running tests

You can run the tests with:

```bash
  uv run pytest
```

(there is currently one test succeeding and one test failing in the project)

