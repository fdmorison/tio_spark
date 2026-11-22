# SparkTableInput

Reads a table registered in a catalog, or the result of a SQL query, into a Spark `DataFrame`.

The table is addressed by name, so the catalog decides where the data is read from.

```yaml
inputs:
  - kind: SparkTableInput
    name: sales
    table: my_catalog.default.sales
```

This reads every row and column of `my_catalog.default.sales`.

## Parameters

| Property | Required | Default | Description |
| -------- | -------- | ------- | ----------- |
| `table` | one of `table` or `query` | | Table name, qualified as `catalog.schema.table` |
| `query` | one of `table` or `query` | | SQL query to read with |
| `columns` | no | all columns | Column or list of columns to read |
| `where` | no | | SQL filter applied to the rows, such as `day >= '2026-01-01'` |
| `version` | no | | Table version to read. Applies only to `table` |
| `timestamp` | no | | Point in time to read the table at. Applies only to `table` |
| `**options` | no | | Reader options passed directly to Spark |

The properties shared by every Input are described in [Inputs, Transforms & Outputs](https://github.com/fdmorison/tiozin/blob/main/docs/concepts/steps.md#input).

Exactly one of `table` or `query` must be set. If neither or both are provided, a `TiozinInputError` is raised when the job is built.

## query

A query reads data that a single table cannot provide, such as a join across tables.

```yaml
inputs:
  - kind: SparkTableInput
    name: sales by segment
    query: |
      SELECT s.order_id, s.amount, c.segment
      FROM my_catalog.default.sales s
      JOIN my_catalog.crm.customers c USING (customer_id)
```

The query result becomes the step output. `columns` and `where` also apply to that result.

## columns

A single column name or a list is accepted. Only the listed columns are kept in the `DataFrame`.

```yaml
inputs:
  - kind: SparkTableInput
    name: sales
    table: my_catalog.default.sales
    columns: [order_id, amount]
```

## where

The filter is a SQL condition that keeps only the matching rows. It can reference a column that is not listed in `columns`, or not selected by the `query`.

```yaml
inputs:
  - kind: SparkTableInput
    name: recent sales
    table: my_catalog.default.sales
    columns: [order_id, amount]
    where: day >= '2026-01-01'
```

This reads `order_id` and `amount` for the rows where `day` is on or after `2026-01-01`, even though `day` is not part of the result.

The filter can also use [template](https://github.com/fdmorison/tiozin/blob/main/docs/templates.md#date-variables) expressions, such as a date relative to the day the job runs for. This example reads the rows of D-1:

```yaml
inputs:
  - kind: SparkTableInput
    name: yesterday sales
    table: my_catalog.default.sales
    where: day = '{{ DAY[-1] }}'
    # → day = '2026-01-16' when the job runs for 2026-01-17
```

## Time Travel

Historical reads use `version` or `timestamp`. They work when the table format supports time travel, such as [Apache Iceberg](https://iceberg.apache.org/docs/latest/spark-queries/#time-travel) or [Delta Lake](https://docs.delta.io/latest/delta-batch.html#query-an-older-snapshot-of-a-table-time-travel).

Both apply only to `table` and cannot be used together. If they are combined with each other, or with `query`, a `TiozinInputError` is raised when the job is built.

### version

The table is read as it was at the given version. `version: 0` is a valid value and reads the first version of the table.

```yaml
inputs:
  - kind: SparkTableInput
    name: sales
    table: my_catalog.default.sales
    version: 1
```

### timestamp

The table is read as it was at the given point in time. A quoted string or an unquoted YAML date or datetime is accepted.

```yaml
inputs:
  - kind: SparkTableInput
    name: sales
    table: my_catalog.default.sales
    timestamp: 2026-01-15 12:00:00
```

If the value has no time zone, it is interpreted in the [Spark session time zone](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration) (`spark.sql.session.timeZone`). An offset such as `2026-01-15 12:00:00+00:00` is respected.

## Passing Extra Reader Options

Any property not listed in the parameters table is passed to Spark as a reader option. Which options take effect depends on the table format. See the [Iceberg read options](https://iceberg.apache.org/docs/latest/spark-configuration/#read-options) or the [Delta Lake batch reads](https://docs.delta.io/latest/delta-batch.html) documentation for the options each format accepts.

Reader options are not applied when reading with `query`, `version`, or `timestamp`.

## Column Comments

If the step sets [`schema_subject`](https://github.com/fdmorison/tiozin/blob/main/docs/how-to/schemas.md#attach-a-schema-to-a-step), the DataFrame it reads carries each description from that schema as the column comment, and the next steps receive those columns. Columns without a description in the schema get no comment from it.

Spark keeps column comments on columns passed along unchanged, such as with `SELECT *`, so a description set on an input can reach the written data even when the output sets no `schema_subject`.

```yaml
inputs:
  - kind: SparkTableInput
    name: load_customers
    layer: raw
    model: customers
    table: example.{{domain}}_{{layer}}.{{product}}_{{model}}
    schema_subject: auto
# → id: comment "Unique customer identifier"
```

## Lineage

The tables read are reported as the input datasets of this step. With `table`, that is the table itself. With `query`, it is every table the query references, including joined tables.

For each table, the catalog becomes the dataset namespace, and `schema.table` becomes the dataset name.

```yaml
table: my_catalog.default.sales
# → namespace: my_catalog
# → name: default.sales
```

The step also reports the [schema](https://openlineage.io/docs/spec/facets/dataset-facets/schema) of the DataFrame it reads, so the columns and types always match the data. If the step sets [`schema_subject`](https://github.com/fdmorison/tiozin/blob/main/docs/how-to/schemas.md#attach-a-schema-to-a-step), the field descriptions from the registry schema are added to the columns with the same name. Columns that are not in the registry schema keep any comment the data already carries as their description, and the rest are reported without one.

See [How to Configure OpenLineage](https://github.com/fdmorison/tiozin/blob/main/docs/how-to/openlineage.md) for how datasets are emitted, and the [OpenLineage naming spec](https://openlineage.io/docs/spec/naming/) for the naming conventions.

## Related

- [SparkTableOutput](table-output.md)
- [SparkFileInput](file-input.md)
- [Inputs, Transforms & Outputs](https://github.com/fdmorison/tiozin/blob/main/docs/concepts/steps.md)
