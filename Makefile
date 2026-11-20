APP := $(shell basename "$$PWD")

clean:
	@rm -rf *.egg-info build dist .ruff_cache .pytest_cache
	@find . -type d -name "__pycache__" -exec rm -rf {} +

hooks:
	@uv run pre-commit install

install:
	@rm -rf .venv
	@uv sync --extra pyspark35
	@uv pip install -e .

install-dev:
	@rm -rf .venv
	@uv sync --extra pyspark35
	@uv pip install -e .
	@uv run pre-commit install
	@rm -rf *.egg-info

format:
	@uv run ruff format
	@uv run ruff check . --fix --show-fixes

check:
	@uv run ruff check .
	@uv run ruff format --check

test: test35 test40

test35: clean
	@uv run --extra pyspark35 pytest -vvv .

test40: clean
	@uv run --extra pyspark40 pytest -vvv .

build:
	@uv build

deploy:
	@uv publish
