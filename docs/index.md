# tio_spark

`tio_spark` is the Tiozin Family for [Apache Spark](https://spark.apache.org/docs/latest/). It runs jobs on a managed `SparkSession` and supports both batch and streaming execution. A family is an installable package of Tiozin plugins that share the same runtime. See [The Family Model](https://github.com/fdmorison/tiozin/blob/main/docs/concepts/family.md) for how families fit into Tiozin.

## Installation

The package requires Python 3.11 or later and installs Tiozin 4.x (4.1.0 or later) as a dependency. Tiozin discovers the family automatically through the `tiozin.family` entry point, so its plugins are available by class name in job YAML right after installation.

To install [PySpark](https://spark.apache.org/docs/latest/api/python/) along with the family, use the `pyspark` extra. It installs PySpark 3.5 or later, below 4.0, with its [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) dependencies:

```bash
pip install "tio_spark[pyspark]"
```

Without the extra, PySpark is not installed. If another package already provides PySpark, install the package on its own:

```bash
pip install tio_spark
```

## How data flows between steps

Every step in `tio_spark` produces a Spark `DataFrame`. After each step runs, the framework registers that DataFrame as a named temporary view using the step's `name`.

```yaml
inputs:
  - kind: SparkFileInput
    name: raw customers
    # → registered as the view raw_customers
    path: data/customers

transforms:
  - kind: SparkSqlTransform
    name: filtered
    query: SELECT * FROM raw_customers WHERE status = 'active'
    # raw_customers is available because the input was registered under that name
```

This view registration happens at the framework level for all inputs and transforms.

## Components

- [SparkRunner](runner.md)
- [SparkIcebergRunner](iceberg-runner.md)
- [SparkFileInput](file-input.md)
- [SparkTableInput](table-input.md)
- [SparkDeduplicateTransform](deduplicate-transform.md)
- [SparkSchemaInferenceTransform](schema-inference-transform.md)
- [SparkSqlTransform](sql-transform.md)
- [SparkFileOutput](file-output.md)
- [SparkTableOutput](table-output.md)
- [SparkWordCountTransform](examples.md#shakespeare)

To implement a custom Input, Transform, or Output from scratch, see [Creating Pluggable Tiozins](https://github.com/fdmorison/tiozin/blob/main/docs/extending/tiozins.md).

## Examples

[Examples](examples.md) lists the example jobs in this repository and how to run them. They read CSV and text files and write [Apache Parquet](https://parquet.apache.org/docs/) files and [Apache Iceberg](https://iceberg.apache.org/docs/latest/) tables.

## Accessing the Spark session

Use `self.spark` to access the active `SparkSession` from any custom step that extends `SparkInput`, `SparkTransform`, `SparkCoTransform`, or `SparkOutput`.

```python
def transform(self, data: DataFrame) -> DataFrame:
    return self.spark.sql("SELECT * FROM some_view")
```

This session is created and managed by the runner. It is shared across all steps in the job, so every step reads from and writes to the same session. Do not call `SparkSession.builder.getOrCreate()` inside a step. That can interfere with the runner-managed session and bypass the temporary views registered by other steps.

`self.spark` is shorthand for:

```python
session = self.context.runner.session  # SparkSession
```

Both are equivalent. `self.spark` is the idiomatic form.
