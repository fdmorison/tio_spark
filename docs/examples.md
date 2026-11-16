# Examples

The example jobs live under [`examples/jobs/`](../examples/jobs/), and their source data lives under [`examples/data/`](../examples/data/). Each example is a self-contained job manifest that runs on a local [Apache Spark](https://spark.apache.org/docs/latest/) session and writes its results under `.output/` in the repository root.

## How the Folder Structure Works

Jobs are grouped by scenario, and each job gets its own YAML file named after its layer or purpose:

```text
examples/
  data/
    ecommerce/
      customers.csv
      orders.csv
    shakespeare/
      hamlet.txt
      kinglear.txt
      loverscomplaint.txt
      sonnets.txt
  jobs/
    ecommerce/
      ingestion_customers_job.yaml
      ingestion_orders_job.yaml
      refined_orders_job.yaml
    iceberg/
      ingestion_customers_job.yaml
      ingestion_orders_job.yaml
      refined_orders_job.yaml
    shakespeare/
      ingestion_job.yaml
      refined_hamlet_job.yaml
      refined_kinglear_job.yaml
      refined_loverscomplaint_job.yaml
      refined_sonnets_job.yaml
  schemas/
    tiozin.eu.ecommerce.sales.raw.storefront.customers.yaml
    tiozin.eu.ecommerce.sales.raw.storefront.orders.yaml
    tiozin.latam.literature.classics.raw.shakespeare.shakespeare.yaml
```

One folder per scenario. One YAML per job. The `iceberg/` folder runs the ecommerce scenario against Iceberg tables instead of Parquet files.

## The Settings File

The jobs run with `tiozin.yaml`, a copy of [`tiozin.example.yaml`](../tiozin.example.yaml) at the repository root. Tiozin reads its registries and runtime defaults from this file. See [Settings](https://github.com/fdmorison/tiozin/blob/main/docs/settings/index.md) for the full format.

The example settings configure the following:

- The job registry reads job files from `examples/jobs`.
- The schema registry reads schema files from `examples/schemas`.
- The runtime defaults fill in arguments that a job does not set. Job arguments always take precedence.

The runtime defaults matter for the examples, because several steps leave arguments out:

- [SparkRunner](runner.md) takes its `log_level` from the `LOG_LEVEL` environment variable. If the variable is not set, `log_level` defaults to `WARN`.
- [SparkFileInput](file-input.md) defaults to `format: parquet`.
- [SparkFileOutput](file-output.md) defaults to `format: parquet` and `mode: append`.

Steps that declare `schema_subject: auto` look up their schema under a subject built from the `subject_template` of the schema registry. For the customer ingestion job, the subject matches the file `examples/schemas/tiozin.eu.ecommerce.sales.raw.storefront.customers.yaml`:

```yaml
subject_template: "{{org}}.{{region}}.{{domain}}.{{subdomain}}.{{layer}}.{{product}}.{{model}}"
# → tiozin.eu.ecommerce.sales.raw.storefront.customers
```

## Before Running

1. Install the family with the `pyspark` extra. See [Installation](index.md#installation).
2. Optionally, set the `LOG_LEVEL` environment variable to a level that Spark accepts, such as `WARN`, `INFO`, or `ERROR`. Spark does not accept `WARNING`. See [`SparkContext.setLogLevel`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkContext.setLogLevel.html) for the full list. If the variable is not set, the settings file falls back to `WARN`.
3. Open a shell at the repository root. The input paths, such as `examples/data/ecommerce/customers.csv`, and the `.output/` paths are relative to it.
4. Copy the example settings into place. Tiozin reads its settings from `tiozin.yaml` in the current directory, and that file is gitignored. Without it, the example jobs fail because their schemas are not found.

   ```bash
   cp tiozin.example.yaml tiozin.yaml
   ```

## Running a Job

From the repository root, name the job to run:

```bash
tiozin run examples/jobs/ecommerce/ingestion_customers_job.yaml
```

If copying the file is not an option, pass the example settings directly with `--settings-path tiozin.example.yaml`.

Every scenario has ingestion jobs and refined jobs. The refined jobs read what the ingestion jobs write, so run the ingestion jobs of a scenario first.

## Two Transform Styles

The examples show two approaches to business logic:

- **SQL transforms** ([SparkSqlTransform](sql-transform.md)): logic lives in the query, and the plugin is generic. The ecommerce and Iceberg examples follow this pattern.
- **Plugin transforms** (`SparkWordCountTransform`): logic lives in the plugin class, and the YAML only declares parameters. The Shakespeare refined jobs follow this pattern.

## Ecommerce

A two-step pipeline split across three jobs: raw ingestion for customers and orders, followed by a refined join. The ingestion jobs read CSV files with [SparkFileInput](file-input.md), add ingestion metadata columns with [SparkSqlTransform](sql-transform.md), and write [Apache Parquet](https://parquet.apache.org/docs/) files with [SparkFileOutput](file-output.md).

Output paths are [templates](https://github.com/fdmorison/tiozin/blob/main/docs/templates.md) filled from the job's fields:

```yaml
path: .output/lake-{{domain}}-{{layer}}/{{product}}/{{model}}
# → .output/lake-ecommerce-raw/storefront/customers
```

| File | What it shows |
|---|---|
| [`ecommerce/ingestion_customers_job.yaml`](../examples/jobs/ecommerce/ingestion_customers_job.yaml) | CSV ingestion from `examples/data/ecommerce/customers.csv` with `header: true`, the metadata columns `_raw_run_id`, `_raw_nominal_date`, `_raw_created_at`, and `_source_created_at` added in SQL, and output to `.output/lake-ecommerce-raw/storefront/customers` with `mode: overwrite` |
| [`ecommerce/ingestion_orders_job.yaml`](../examples/jobs/ecommerce/ingestion_orders_job.yaml) | Same ingestion pattern for orders, with `inferSchema: true`, written to `.output/lake-ecommerce-raw/storefront/orders` |
| [`ecommerce/refined_orders_job.yaml`](../examples/jobs/ecommerce/refined_orders_job.yaml) | Both raw outputs read back with `SparkFileInput`, using `layer: raw` and `model` on each input to render the raw paths, joined in SQL by step name, chained transforms with `@data`, a parameterized query with `args:`, and output to `.output/lake-ecommerce-refined/storefront/high_value_orders` with `mode: overwrite`, partitioned by `country` |

The refined job joins its inputs by name because every step's output is registered as a view named after the step. See [How data flows between steps](index.md#how-data-flows-between-steps).

Run both ingestion jobs before `refined_orders_job.yaml`:

```bash
tiozin run examples/jobs/ecommerce/ingestion_customers_job.yaml
tiozin run examples/jobs/ecommerce/ingestion_orders_job.yaml
tiozin run examples/jobs/ecommerce/refined_orders_job.yaml
```

## Shakespeare

A multi-job pipeline using works of Shakespeare as the source dataset. An ingestion job loads the raw texts from `examples/data/shakespeare`. Each refined job then counts the words of one work independently, so a collection of independent jobs composes into a larger pipeline.

The ingestion job reads the texts with [SparkFileInput](file-input.md), using `format: text` and `explode_filepath: true`. The second option adds file columns to every row, including `filestem`, the file name without its extension. See [Adding file path columns](file-input.md#adding-file-path-columns). [SparkFileOutput](file-output.md) then writes one partition per `filestem`:

```yaml
path: .output/lake-{{domain}}-{{layer}}/{{product}}/{{model}}/date={{today}}
# → .output/lake-literature-raw/shakespeare/shakespeare/date=2026-11-16
```

Each refined job reads the partition of its own work, counts the words with `SparkWordCountTransform`, and writes the counts to its own path. The refined steps declare no `format`, so they read and write Parquet through the runtime defaults of the settings file.

`SparkWordCountTransform` counts word occurrences in the column named by `content_field`, which defaults to `value`. With `lowercase: true`, words are normalized to lowercase before counting. The output has the columns `word STRING` and `count BIGINT`. If `count_by` names one or more columns, the counts are scoped by them, and those columns come before `word` and `count` in the output.

| File | What it shows |
|---|---|
| [`shakespeare/ingestion_job.yaml`](../examples/jobs/shakespeare/ingestion_job.yaml) | Text ingestion with `SparkFileInput`, `format: text`, and `explode_filepath: true`, a runner with `master: local[*]`, `enable_hive_support: true`, and `log_level: WARN`, and output to `.output/lake-literature-raw/shakespeare/shakespeare/date=<today>`, partitioned by `filestem` |
| [`shakespeare/refined_hamlet_job.yaml`](../examples/jobs/shakespeare/refined_hamlet_job.yaml) | The `filestem=hamlet` partition read with `SparkFileInput`, word count with `SparkWordCountTransform`, and output to `.output/lake-literature-refined/shakespeare/hamlet/date=<today>` |
| [`shakespeare/refined_sonnets_job.yaml`](../examples/jobs/shakespeare/refined_sonnets_job.yaml) | Word count of the Sonnets |
| [`shakespeare/refined_kinglear_job.yaml`](../examples/jobs/shakespeare/refined_kinglear_job.yaml) | Word count of King Lear |
| [`shakespeare/refined_loverscomplaint_job.yaml`](../examples/jobs/shakespeare/refined_loverscomplaint_job.yaml) | Word count of A Lover's Complaint |

The ingestion output path and the refined input paths both include `date={{today}}`, so a refined job reads only the texts ingested for the same date.

Run the ingestion job before any refined job:

```bash
tiozin run examples/jobs/shakespeare/ingestion_job.yaml
tiozin run examples/jobs/shakespeare/refined_hamlet_job.yaml
```

## Iceberg

The `iceberg/` jobs run the ecommerce scenario on [Apache Iceberg](https://iceberg.apache.org/docs/latest/) tables instead of Parquet files. All three use [SparkIcebergRunner](iceberg-runner.md) with the same runner block:

```yaml
runner:
  kind: SparkIcebergRunner
  catalog_name: example
  catalog_type: hadoop
  catalog_warehouse: .output/iceberg
  jars_packages:
    - org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1
```

With `catalog_type: hadoop`, the catalog lives on the filesystem, and its warehouse sits under `.output/iceberg`. The `jars_packages` entry adds the Iceberg Spark runtime to the Spark classpath. See [Adding JAR packages](runner.md#adding-jar-packages).

Table names are templates too, prefixed by the catalog name:

```yaml
table: example.{{domain}}_{{layer}}.{{product}}_{{model}}
# → example.ecommerce_raw.storefront_customers
```

| File | What it shows |
|---|---|
| [`iceberg/ingestion_customers_job.yaml`](../examples/jobs/iceberg/ingestion_customers_job.yaml) | CSV ingestion with `SparkFileInput`, written to `example.ecommerce_raw.storefront_customers` with `SparkTableOutput`, `mode: merge`, `merge_keys: id`, and `merge_exclude_fields: [created_at, _raw_created_at]`, partitioned by `days(created_at)` |
| [`iceberg/ingestion_orders_job.yaml`](../examples/jobs/iceberg/ingestion_orders_job.yaml) | Same ingestion pattern for orders, written to `example.ecommerce_raw.storefront_orders` with `mode: merge`, `merge_keys: id`, and `merge_exclude_fields: [created_at, _raw_created_at]`, partitioned by `days(created_at)` |
| [`iceberg/refined_orders_job.yaml`](../examples/jobs/iceberg/refined_orders_job.yaml) | Both raw tables read with `SparkTableInput`, joined and filtered in SQL, and written to `example.ecommerce_refined.storefront_high_value_orders` with `mode: truncate`, partitioned by `days(created_at)` |

The ingestion jobs write with [SparkTableOutput](table-output.md) in `merge` mode. On a later run, rows that match on `id` are updated and the others are inserted. The columns in `merge_exclude_fields` keep the values already in the table, so a row keeps its original `created_at` and `_raw_created_at`. The refined job reads the raw tables with [SparkTableInput](table-input.md).

Run both ingestion jobs before `refined_orders_job.yaml`:

```bash
tiozin run examples/jobs/iceberg/ingestion_customers_job.yaml
tiozin run examples/jobs/iceberg/ingestion_orders_job.yaml
tiozin run examples/jobs/iceberg/refined_orders_job.yaml
```
