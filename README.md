# tio_spark

`tio_spark` is the Spark family for [Tiozin](https://github.com/fdmorison/tiozin): a set of Tiozin plugins that run data jobs on [Apache Spark](https://spark.apache.org/docs/latest/), in batch or streaming mode. See [The Family Model](https://github.com/fdmorison/tiozin/blob/main/docs/concepts/family.md) for how families fit into Tiozin.

## One-Minute Example

```bash
pip install "tio_spark[pyspark]"
```

This job loads the text of Hamlet into a data lake. [SparkRunner](https://github.com/fdmorison/tio_spark/blob/main/docs/runner.md) runs it, [SparkFileInput](https://github.com/fdmorison/tio_spark/blob/main/docs/file-input.md) reads the text, and [SparkFileOutput](https://github.com/fdmorison/tio_spark/blob/main/docs/file-output.md) writes it.

```yaml
kind: LinearJob
name: ingestion_hamlet_job
owner: literature@tiozin.com
maintainer: tiozin
cost_center: tio_scrooge

org: tiozin
region: latam
domain: literature
subdomain: classics
product: shakespeare
model: hamlet
layer: raw

runner:
  kind: SparkRunner

inputs:
  - kind: SparkFileInput
    name: load_hamlet
    path: examples/data/{{product}}/{{model}}.txt
    format: text

outputs:
  - kind: SparkFileOutput
    name: save_hamlet
    path: .output/lake-{{domain}}-{{layer}}/{{product}}/{{model}}/date={{today}}
```

Run it:

```bash
tiozin run examples/jobs/oneminute.yaml
```

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

## Documentation

### Getting Started
- [Installation](https://github.com/fdmorison/tio_spark/blob/main/docs/installation.md)
- [Getting Started](https://github.com/fdmorison/tio_spark/blob/main/docs/getting-started.md)

### Plugins
- [Family Guide](https://github.com/fdmorison/tio_spark/blob/main/docs/index.md)
- [How Data Flows Between Steps](https://github.com/fdmorison/tio_spark/blob/main/docs/index.md#how-data-flows-between-steps)

### Examples
- [Example Jobs](https://github.com/fdmorison/tio_spark/blob/main/docs/examples.md)

### Writing Custom Steps
- [Accessing the Spark Session](https://github.com/fdmorison/tio_spark/blob/main/docs/index.md#accessing-the-spark-session)
- [Creating Pluggable Tiozins](https://github.com/fdmorison/tiozin/blob/main/docs/extending/tiozins.md)

### Learn More
- [Tiozin](https://github.com/fdmorison/tiozin)
- [Apache Spark](https://spark.apache.org/docs/latest/)

## License

This project is licensed under the [Mozilla Public License 2.0](https://github.com/fdmorison/tio_spark/blob/main/LICENSE).
