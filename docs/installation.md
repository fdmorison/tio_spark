# Installation

`tio_spark` is installed as a Python package. It requires Python 3.11 or later and installs [Tiozin](https://github.com/fdmorison/tiozin) 5.0.0 or later as a dependency.

```bash
pip install "tio_spark[pyspark]"
```

Tiozin discovers the family automatically, so its plugins are available by class name in any job right after installation.

## Choosing a PySpark Version

The `pyspark` extra installs [PySpark](https://spark.apache.org/docs/latest/api/python/) 3.5 or later, with no upper bound, together with its [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) dependencies.

To pin a Spark line, use one of the versioned extras instead:

- `pyspark35`: PySpark 3.5.x.
- `pyspark40`: PySpark 4.0.x.

```bash
pip install "tio_spark[pyspark40]"
```

Both versioned extras also install the Spark Connect dependencies. They pin different Spark lines, so `pyspark35` and `pyspark40` cannot be installed together.

## Running on a Cluster That Already Provides Spark

Some clusters ship their own Spark installation. On those, install the package without an extra:

```bash
pip install tio_spark
```

Without an extra, only the plugins are installed and PySpark is not. This keeps a PySpark from PyPI from shadowing the cluster's own Spark.

## When the Shell Already Sets `SPARK_HOME`

Tiozin loads the project `.env` file, the first one found from the working directory upward. Its variables win over the ones already set in the shell, and the variables it leaves out keep their shell values.

If the shell profile exports a `SPARK_HOME` for another Spark installation, the one in the `.env` file replaces it:

```bash
# Shell profile
export SPARK_HOME=/opt/spark/3.5.4

# The .env file
SPARK_HOME=.venv/lib/python3.11/site-packages/pyspark

# → the job runs with SPARK_HOME=.venv/lib/python3.11/site-packages/pyspark
```

Setting `SPARK_HOME` in the project `.env` file is meant for local development. For local runs, point it at a Spark installation that matches the installed [PySpark](https://spark.apache.org/docs/latest/api/python/getting_started/install.html) version.

On a cluster that already provides Spark, leave `SPARK_HOME` out of the `.env` file. A `SPARK_HOME` there replaces the cluster's own.

## Installing Through Tiozin

The Tiozin package exposes matching extras that install this family:

- `tiozin[tio_spark]`: the plugins only, without PySpark.
- `tiozin[tio_spark35]`: the plugins with PySpark 3.5.x.
- `tiozin[tio_spark40]`: the plugins with PySpark 4.0.x.

```bash
pip install "tiozin[tio_spark40]"
```

As with the family extras, `tio_spark35` and `tio_spark40` cannot be installed together.

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
