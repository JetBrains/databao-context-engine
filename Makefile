.DEFAULT_GOAL:=test

.PHONY: test
test: sync lint mypy
	# run all tests with including all extras to the environment
	# Tests marked with 'recommended_extras' excluded because they will fail
	@uv run --all-extras pytest -s -vv
	# run specific tests with [recommended] extras in isolated environment (so the actual .venv is not used)
	@uv run --isolated --extra recommended pytest -s -vv tests/recommended_extras --run-recommended-extras

.PHONY: lint
lint:
	@uv run ruff check
	@uv run ruff format --check

.PHONY: mypy
mypy:
	@uv run mypy . --exclude dist --exclude docs

.PHONY: sync
sync:
	@uv sync --locked --all-extras --dev