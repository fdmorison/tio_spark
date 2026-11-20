# Examples

The example jobs live in [`examples/jobs/`](../examples/jobs/), and their source data lives in [`examples/data/`](../examples/data/). Each scenario is split into two layers: ingestion jobs load the source data, and refined jobs build new datasets from it.

Before running, complete the [setup in Getting Started](getting-started.md), then run every job from the repository root.

## Ecommerce

Ecommerce demonstrates tabular data: customers and orders from CSV files.

Ingestion jobs:

- [`ecommerce/ingestion_customers_job.yaml`](../examples/jobs/ecommerce/ingestion_customers_job.yaml): loads the customers.
- [`ecommerce/ingestion_orders_job.yaml`](../examples/jobs/ecommerce/ingestion_orders_job.yaml): loads the orders.

Refined jobs:

- [`ecommerce/refined_orders_job.yaml`](../examples/jobs/ecommerce/refined_orders_job.yaml): combines customers and orders into high-value orders.

Run the ingestion jobs first:

```bash
tiozin run examples/jobs/ecommerce/ingestion_customers_job.yaml
tiozin run examples/jobs/ecommerce/ingestion_orders_job.yaml
tiozin run examples/jobs/ecommerce/refined_orders_job.yaml
```

## Shakespeare

Shakespeare demonstrates textual data: four works of Shakespeare from text files.

Ingestion jobs:

- [`shakespeare/ingestion_job.yaml`](../examples/jobs/shakespeare/ingestion_job.yaml): loads all four works.

Refined jobs, one per work:

- [`shakespeare/refined_hamlet_job.yaml`](../examples/jobs/shakespeare/refined_hamlet_job.yaml): Hamlet.
- [`shakespeare/refined_kinglear_job.yaml`](../examples/jobs/shakespeare/refined_kinglear_job.yaml): King Lear.
- [`shakespeare/refined_loverscomplaint_job.yaml`](../examples/jobs/shakespeare/refined_loverscomplaint_job.yaml): A Lover's Complaint.
- [`shakespeare/refined_sonnets_job.yaml`](../examples/jobs/shakespeare/refined_sonnets_job.yaml): the Sonnets.

Run the ingestion job first:

```bash
tiozin run examples/jobs/shakespeare/ingestion_job.yaml
tiozin run examples/jobs/shakespeare/refined_hamlet_job.yaml
```

## Iceberg

Iceberg demonstrates the ecommerce data on [Apache Iceberg](https://iceberg.apache.org/docs/latest/) tables instead of files.

These jobs add the Iceberg runtime for Spark 3.5 through `jars_packages`, so they run as they are on PySpark 3.5, installed with the `pyspark35` extra described in [Choosing a PySpark Version](installation.md#choosing-a-pyspark-version). On Spark 4.0, replace that package with the Spark 4.0 build, as described in [Adding the Iceberg Runtime](iceberg-runner.md#adding-the-iceberg-runtime).

Ingestion jobs:

- [`iceberg/ingestion_customers_job.yaml`](../examples/jobs/iceberg/ingestion_customers_job.yaml): loads the customers into an Iceberg table.
- [`iceberg/ingestion_orders_job.yaml`](../examples/jobs/iceberg/ingestion_orders_job.yaml): loads the orders into an Iceberg table.

Refined jobs:

- [`iceberg/refined_orders_job.yaml`](../examples/jobs/iceberg/refined_orders_job.yaml): combines customers and orders into a high-value orders table.

Run the ingestion jobs first:

```bash
tiozin run examples/jobs/iceberg/ingestion_customers_job.yaml
tiozin run examples/jobs/iceberg/ingestion_orders_job.yaml
tiozin run examples/jobs/iceberg/refined_orders_job.yaml
```
