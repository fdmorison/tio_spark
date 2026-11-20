# Installation

`tio_spark` is installed as a Python package. It requires Python 3.11 or later and installs [Tiozin](https://github.com/fdmorison/tiozin) 4.1.0 or later, below 5.0, as a dependency.

```bash
pip install "tio_spark[pyspark]"
```

Tiozin discovers the family automatically, so its plugins are available by class name in any job right after installation.

## The `pyspark` Extra

The package provides one extra, `pyspark`. It installs [PySpark](https://spark.apache.org/docs/latest/api/python/) 3.5 or later, below 4.0, with its [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) dependencies.

Without the extra, PySpark is not installed. If another package already provides PySpark, install `tio_spark` without the extra:

```bash
pip install tio_spark
```

## Adding It to a Project

Add the package to the project's dependencies, such as in the `pyproject.toml` file:

```toml
[project]
dependencies = [
  "tio_spark[pyspark]",
]
```

## Next Steps

- [Getting Started](getting-started.md): set up the settings files to run the example jobs.
- [Examples](examples.md): the example jobs in this repository and how to run them.
- [tio_spark](index.md): how to reference the plugins in a job and how data flows between steps.
