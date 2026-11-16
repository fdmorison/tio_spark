# tio_spark

`tio_spark` is the Spark family for [Tiozin](https://github.com/fdmorison/tiozin). It provides Tiozin plugins that run data jobs on [Apache Spark](https://spark.apache.org/docs/latest/), in batch or streaming mode.

A family is an installable package of Tiozin plugins that share the same runtime. See [The Family Model](https://github.com/fdmorison/tiozin/blob/main/docs/concepts/family.md) for how families fit into Tiozin.

## Installation

The package requires Python 3.11 or later and installs Tiozin 4.x (4.1.0 or later) as a dependency.

To install [PySpark](https://spark.apache.org/docs/latest/api/python/) along with the family, use the `pyspark` extra. It installs PySpark 3.5 or later, below 4.0, with its [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) dependencies:

```bash
pip install "tio_spark[pyspark]"
```

Without the extra, PySpark is not installed. If another package already provides PySpark, install the package on its own:

```bash
pip install tio_spark
```

## How Tiozin Finds It

Tiozin discovers the family automatically through the `tiozin.family` entry point. The family is registered as `tio_spark`, and its Python package is `tio_spark`.

After installation, its plugins are available by class name in any job:

```yaml
runner:
  kind: SparkRunner
  master: local[*]
  log_level: WARN
```

If another installed family defines a class with the same name, qualify the kind with the family name:

```yaml
runner:
  kind: tio_spark:SparkRunner
  master: local[*]
  log_level: WARN
```

## Plugins

- [SparkRunner](https://github.com/fdmorison/tio_spark/blob/main/docs/runner.md): runs a job on a `SparkSession` that it creates during setup and stops during teardown.
- [SparkIcebergRunner](https://github.com/fdmorison/tio_spark/blob/main/docs/iceberg-runner.md): a SparkRunner pre-configured for Apache Iceberg.
- [SparkFileInput](https://github.com/fdmorison/tio_spark/blob/main/docs/file-input.md): reads files into a Spark `DataFrame`.
- [SparkTableInput](https://github.com/fdmorison/tio_spark/blob/main/docs/table-input.md): reads a catalog table, or the result of a SQL query, into a Spark `DataFrame`.
- [SparkDeduplicateTransform](https://github.com/fdmorison/tio_spark/blob/main/docs/deduplicate-transform.md): removes duplicate rows, keeping one row per key.
- [SparkSchemaInferenceTransform](https://github.com/fdmorison/tio_spark/blob/main/docs/schema-inference-transform.md): infers the schema of fields that hold JSON strings and converts them to Spark types.
- [SparkSqlTransform](https://github.com/fdmorison/tio_spark/blob/main/docs/sql-transform.md): runs a Spark SQL query against the views registered by previous steps.
- [SparkWordCountTransform](https://github.com/fdmorison/tio_spark/blob/main/docs/examples.md#shakespeare): counts word occurrences in a text column. The Shakespeare example jobs use it.
- [SparkFileOutput](https://github.com/fdmorison/tio_spark/blob/main/docs/file-output.md): writes a Spark `DataFrame` to files.
- [SparkTableOutput](https://github.com/fdmorison/tio_spark/blob/main/docs/table-output.md): writes a Spark `DataFrame` to an Apache Iceberg or Delta Lake table registered in a catalog.

The [family documentation](https://github.com/fdmorison/tio_spark/blob/main/docs/index.md) lists every plugin page and explains how data flows between steps.

## Learn More

- [Example jobs](https://github.com/fdmorison/tio_spark/blob/main/docs/examples.md)
- [Creating Pluggable Tiozins](https://github.com/fdmorison/tiozin/blob/main/docs/extending/tiozins.md)
- [Tiozin](https://github.com/fdmorison/tiozin)
- [Apache Spark](https://spark.apache.org/docs/latest/)
