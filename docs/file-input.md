# SparkFileInput

Reads files into a Spark `DataFrame`. Supports all formats Spark supports natively and any format added by a loaded Spark package.

```yaml
inputs:
  - kind: SparkFileInput
    name: customers
    path: data/customers
    format: parquet
```

## All available options

| Property | Description | Default |
|---|---|---|
| `path` | Path or list of paths to read from | |
| `format` | File format | `parquet` |
| `explode_filepath` | Enrich rows with file path columns | `false` |
| `**options` | Spark reader options passed directly to Spark | |

## Supported formats

The `format` field is passed directly to Spark's `DataFrameReader.format()`.

| Format | Notes |
|---|---|
| `parquet` | Default |
| `csv` | |
| `json` | Newline-delimited JSON |
| `orc` | |
| `avro` | Requires the `spark-avro` package |
| `text` | Reads each line as a single `value` string column |
| `binaryFile` | Reads each file as a binary blob with metadata columns |
| `xml` | Requires the `spark-xml` package |
| `sequenceFile` | Hadoop SequenceFile format |

## Adding file path columns

When reading from multiple files, identifying which file each row came from is often necessary. Set `explode_filepath: true` to add file-level columns to every row:

| Column | Value |
|---|---|
| `filesize` | File size in bytes |
| `dirpath` | Full path to the directory containing the file |
| `dirname` | Name of the directory (last path component before the filename) |
| `filepath` | Full path to the file |
| `filename` | Name of the file including extension |
| `filestem` | Name of the file without extension |
| `filetype` | File extension without the dot |

```yaml
inputs:
  - kind: SparkFileInput
    name: logs
    path: /data/logs/
    format: text
    explode_filepath: true
```

## Passing extra reader options

Any property not listed in the options table is forwarded directly to Spark's `DataFrameReader.options()`.

```yaml
inputs:
  - kind: SparkFileInput
    name: raw
    path: data/raw.csv
    format: csv
    header: true
    inferSchema: true
    delimiter: ";"
```

## Column Comments

If the step sets [`schema_subject`](https://github.com/fdmorison/tiozin/blob/main/docs/how-to/schemas.md#attach-a-schema-to-a-step), the DataFrame it reads carries each description from that schema as the column comment, and the next steps receive those columns. Columns without a description in the schema get no comment from it.

Spark keeps column comments on columns passed along unchanged, such as with `SELECT *`, so a description set on an input can reach the written data even when the output sets no `schema_subject`.

```yaml
inputs:
  - kind: SparkFileInput
    name: load_customers
    path: examples/data/ecommerce/customers.csv
    format: csv
    header: true
    schema_subject: auto

transforms:
  - kind: SparkSqlTransform
    name: add_metadata
    query: SELECT *, '{{job.run_id}}' AS _{{layer}}_run_id FROM @data

outputs:
  - kind: SparkFileOutput
    name: save_customers
    path: .output/lake-{{domain}}-{{layer}}/{{product}}/{{model}}
    format: parquet
    mode: overwrite
# → id: comment "Unique customer identifier"
# → _raw_run_id: no comment
```

## Lineage

Each path is reported as a separate input dataset following the [OpenLineage naming spec](https://openlineage.io/docs/spec/naming/).

The step also reports the [schema](https://openlineage.io/docs/spec/facets/dataset-facets/schema) of the DataFrame it reads, so the columns and types always match the data. If the step sets [`schema_subject`](https://github.com/fdmorison/tiozin/blob/main/docs/how-to/schemas.md#attach-a-schema-to-a-step), the field descriptions from the registry schema are added to the columns with the same name. Columns that are not in the registry schema keep any comment the data already carries as their description, and the rest are reported without one.

## Streaming mode

When the runner has `streaming: true`, `SparkFileInput` uses `spark.readStream` instead of `spark.read`. In streaming mode, `path` must be a single directory path. Providing a list of paths raises an error.

```yaml
runner:
  kind: SparkRunner
  master: local[*]
  streaming: true

inputs:
  - kind: SparkFileInput
    name: events
    path: /data/incoming/events/   # single directory required in streaming mode
    format: json
```
