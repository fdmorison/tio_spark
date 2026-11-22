# tio_spark

## What This Repo Is

tio_spark is the Spark family for Tiozin. It ships Tiozin plugins (runners, inputs,
transforms, outputs) that run data jobs on Apache Spark.

Tiozin is a runtime framework for defining and executing modular data jobs. It supports plugin-based
development, plugin sharing, and declarative or programmatic job definitions. Tiozin core lives at
https://github.com/fdmorison/tiozin and is consumed here as a dependency.

The family is registered through the `tiozin.family` entry point in `pyproject.toml`.

## Commands

```bash
make install      # Install everything needed to develop locally
make format       # Format and fix code
make check        # Verify code style — fails if violations are found
make test         # Run full test suite
make build        # Build a deployment-ready package
make clean        # Remove unnecessary files such as builds and caches
```

## Directory Structure

```
tio_spark/       # Family package (the public API is tio_spark/__init__.py)
docs/            # User guides and plugin reference pages
tests/           # Unit and integration tests
```

# Additional Context

- Tech stack: `@.claude/knowledge/tech-stack.md`
- Concepts and terminology: `@.claude/knowledge/glossary.md`
