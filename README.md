# tio_spark

`tio_spark` is the Spark family for [Tiozin](https://github.com/fdmorison/tiozin). It provides Tiozin plugins that run data jobs on [Apache Spark](https://spark.apache.org/docs/latest/), in batch or streaming mode.

A family is an installable package of Tiozin plugins that share the same runtime. See [The Family Model](https://github.com/fdmorison/tiozin/blob/main/docs/concepts/family.md) for how families fit into Tiozin.

## Usage

Tiozin discovers the family automatically through the `tiozin.family` entry point. The family is registered as `tio_spark`, and its Python package is `tio_spark`.

After installation, its plugins are available by class name in any job:

```yaml
runner:
  kind: SparkRunner
```

If another installed family defines a class with the same name, qualify the kind with the family name:

```yaml
runner:
  kind: tio_spark:SparkRunner
```

Neither `runner:` block sets more than the `kind`. Arguments that a job leaves out, such as the Spark `log_level`, can come from `tiozin.yaml`, the [Tiozin settings file](https://github.com/fdmorison/tiozin/blob/main/docs/settings/index.md).

## Installation

`tio_spark` is installed as a Python dependency. Once it is installed, its plugins are available in any job.

The package provides one extra, `pyspark`.

The `pyspark` extra installs [PySpark](https://spark.apache.org/docs/latest/api/python/) 3.5 or later, below 4.0, with its [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) dependencies.

```bash
pip install "tio_spark[pyspark]"
```

If another package already provides PySpark, install `tio_spark` without the extra:

```bash
pip install tio_spark
```

Add it to the project's dependencies, such as in `pyproject.toml`:

```toml
[project]
dependencies = [
  "tio_spark[pyspark]",
]
```

## Setting Up tiozin.yaml

`tiozin.yaml` is the [Tiozin settings file](https://github.com/fdmorison/tiozin/blob/main/docs/settings/index.md). It configures [registries](https://github.com/fdmorison/tiozin/blob/main/docs/settings/index.md#registries), such as the job and schema registries, and default plugin arguments under [`runtime_defaults`](https://github.com/fdmorison/tiozin/blob/main/docs/settings/index.md#runtime-defaults).

The example settings use `runtime_defaults` to set the Spark log level.

1. Copy the `.env.example` file to `.env` at the repository root, then review the variables in the `.env` file. The `.env` file is gitignored.

   ```bash
   cp .env.example .env
   ```

2. Copy the `tiozin.example.yaml` file to `tiozin.yaml` at the repository root. The `tiozin.yaml` file is gitignored.

   ```bash
   cp tiozin.example.yaml tiozin.yaml
   ```

3. Find the log level under `runtime_defaults`. [SparkRunner](https://github.com/fdmorison/tio_spark/blob/main/docs/runner.md) reads it from an environment variable:

   ```yaml
   runtime_defaults:

     - kind: SparkRunner
       log_level: "{{ ENV.LOG_LEVEL | default('WARN') }}"
   ```

   A job that leaves `log_level` out takes it from here. The value comes from the `LOG_LEVEL` variable in the `.env` file, and falls back to `WARN` when the variable is not set.

   Spark accepts levels such as `WARN`, `INFO`, and `ERROR`, but not `WARNING`. See [`SparkContext.setLogLevel`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkContext.setLogLevel.html) for the full list.

## Plugins

- [SparkRunner](https://github.com/fdmorison/tio_spark/blob/main/docs/runner.md): runs a job on a `SparkSession` that it creates during setup and stops during teardown.
- [SparkIcebergRunner](https://github.com/fdmorison/tio_spark/blob/main/docs/iceberg-runner.md): a SparkRunner pre-configured for [Apache Iceberg](https://iceberg.apache.org/docs/latest/).
- [SparkFileInput](https://github.com/fdmorison/tio_spark/blob/main/docs/file-input.md): reads files into a Spark `DataFrame`.
- [SparkTableInput](https://github.com/fdmorison/tio_spark/blob/main/docs/table-input.md): reads a catalog table, or the result of a SQL query, into a Spark `DataFrame`.
- [SparkDeduplicateTransform](https://github.com/fdmorison/tio_spark/blob/main/docs/deduplicate-transform.md): removes duplicate rows, keeping one row per key.
- [SparkSchemaInferenceTransform](https://github.com/fdmorison/tio_spark/blob/main/docs/schema-inference-transform.md): infers the schema of fields that hold JSON strings and converts them to Spark types.
- [SparkSqlTransform](https://github.com/fdmorison/tio_spark/blob/main/docs/sql-transform.md): runs a Spark SQL query against the views registered by previous steps.
- [SparkWordCountTransform](https://github.com/fdmorison/tio_spark/blob/main/docs/examples.md#shakespeare): counts word occurrences in a text column. The Shakespeare example jobs use it.
- [SparkFileOutput](https://github.com/fdmorison/tio_spark/blob/main/docs/file-output.md): writes a Spark `DataFrame` to files.
- [SparkTableOutput](https://github.com/fdmorison/tio_spark/blob/main/docs/table-output.md): writes a Spark `DataFrame` to an Apache Iceberg or [Delta Lake](https://docs.delta.io/) table registered in a catalog.

The [family documentation](https://github.com/fdmorison/tio_spark/blob/main/docs/index.md) lists every plugin page and explains how data flows between steps.

## Learn More

- [Examples](https://github.com/fdmorison/tio_spark/blob/main/docs/examples.md)
- [Creating Pluggable Tiozins](https://github.com/fdmorison/tiozin/blob/main/docs/extending/tiozins.md)
- [Tiozin](https://github.com/fdmorison/tiozin)
- [Apache Spark](https://spark.apache.org/docs/latest/)
