.DEFAULT_GOAL:=test

.PHONY: test
test: sync lint mypy
	# run all tests with including all extras to the environment
	@uv run --all-extras pytest -s -vv

.PHONY: lint
lint:
	@uv run ruff check
	@uv run ruff format --check

.PHONY: format
format:
	@uv run ruff format

.PHONY: mypy
mypy:
	@uv run mypy . --exclude dist --exclude docs

.PHONY: sync
sync:
	@uv sync --locked --all-extras --dev