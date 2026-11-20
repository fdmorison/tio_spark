# Getting Started

The [example jobs](examples.md) run from the root of a clone of this repository. They need two files there: the `.env` file and the `tiozin.yaml` file. Install the family first, as described in [Installation](installation.md).

## Setting Up the Files

1. Copy the `.env.example` file to `.env` at the repository root, then review the variables in the `.env` file. The `.env` file is gitignored.

   ```bash
   cp .env.example .env
   ```

2. Copy the `tiozin.example.yaml` file to `tiozin.yaml` at the repository root. The `tiozin.yaml` file is gitignored.

   ```bash
   cp tiozin.example.yaml tiozin.yaml
   ```

Tiozin reads both files from the directory where the job runs, so run every job from the repository root.

## What the `tiozin.yaml` File Configures

The `tiozin.yaml` file is the [Tiozin settings file](https://github.com/fdmorison/tiozin/blob/main/docs/settings/index.md). It configures [registries](https://github.com/fdmorison/tiozin/blob/main/docs/settings/index.md#registries), such as the job and schema registries, and default plugin arguments under [`runtime_defaults`](https://github.com/fdmorison/tiozin/blob/main/docs/settings/index.md#runtime-defaults).

```yaml
registries:

  schema:
    kind: FileSchemaRegistry
    location: examples/schemas
```

## Setting the Spark Log Level

The example settings use `runtime_defaults` to set the Spark log level. [SparkRunner](runner.md) reads it from an environment variable:

```yaml
runtime_defaults:

  - kind: SparkRunner
    log_level: "{{ ENV.LOG_LEVEL | default('WARN') }}"
```

A job that leaves `log_level` out takes it from here. The value comes from the `LOG_LEVEL` variable in the `.env` file, and falls back to `WARN` when the variable is not set.

Spark accepts levels such as `WARN`, `INFO`, and `ERROR`, but not `WARNING`. See [`SparkContext.setLogLevel`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkContext.setLogLevel.html) for the full list.

## Next Steps

- [Examples](examples.md): run the example jobs.
- [tio_spark](index.md): how to reference the plugins in a job and how data flows between steps.
- [SparkRunner](runner.md): every runner option.
