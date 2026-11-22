# SparkFileOutput

Writes a Spark `DataFrame` to files.

```yaml
outputs:
  - kind: SparkFileOutput
    name: customers
    path: .output/lake/customers
    format: parquet
    mode: overwrite
```

## All available options

| Property | Description | Default |
|---|---|---|
| `path` | Target path to write to | |
| `format` | Output format | `parquet` |
| `mode` | Write mode | `append` |
| `partition_by` | Columns to use for Hive-style partitioning | `[]` |
| `**options` | Spark writer options passed directly to Spark | |

## Write modes

| Mode | Description |
|---|---|
| `append` | Adds new files to the path. Does not remove existing files |
| `overwrite` | Removes all existing files at the path before writing |
| `ignore` | Writes only if no data exists at the path. Skips silently if it does |
| `error` | Raises an error if any data already exists at the path |
| `errorifexists` | Alias for `error` |

## Hive-style partitioning

Use `partition_by` to split the output into subdirectories by column values:

```yaml
outputs:
  - kind: SparkFileOutput
    name: partitioned
    path: .output/lake/orders
    format: parquet
    mode: overwrite
    partition_by:
      - country
      - date
```

This produces directories like `.output/lake/orders/country=BR/date=2024-01-01/`.

## Passing extra writer options

Any property not listed in the options table is forwarded directly to Spark's `DataFrameWriter.option()`.

```yaml
outputs:
  - kind: SparkFileOutput
    name: compressed
    path: .output/lake/events
    format: json
    mode: overwrite
    compression: gzip
```

## Lineage

The output path is reported as the dataset written by this step following the [OpenLineage naming spec](https://openlineage.io/docs/spec/naming/).

The dataset also carries the [schema](https://openlineage.io/docs/spec/facets/dataset-facets/schema) of the DataFrame, so its columns and types always match the data. If the step sets [`schema_subject`](https://github.com/fdmorison/tiozin/blob/main/docs/how-to/schemas.md#attach-a-schema-to-a-step), the field descriptions from the registry schema are added to the columns with the same name. Columns that are not in the registry schema, such as ones added by a transform, are reported without a description.

```yaml
outputs:
  - kind: SparkFileOutput
    name: save_customers
    path: .output/lake-{{domain}}-{{layer}}/{{product}}/{{model}}
    format: parquet
    mode: overwrite
    schema_subject: auto
# → id: Unique customer identifier
# → _raw_run_id: no description
```
