import uuid
from collections.abc import Generator
from typing import Any

import pyspark
import pytest
from packaging.version import Version
from pyspark.sql import SparkSession
from tiozin import Context

from tests.stubs import JobStub, RunnerStub, TransformStub

SPARK_PROFILES = {
    "3.5": {
        "scala": "2.12",
        "delta": "3.2.0",
        "iceberg": "iceberg-spark-runtime-3.5_2.12:1.7.1",
    },
    "4.0": {
        "scala": "2.13",
        "delta": "4.0.1",
        "iceberg": "iceberg-spark-runtime-4.0_2.13:1.10.1",
    },
}

PYSPARK = Version(pyspark.__version__)
SPARK = SPARK_PROFILES[f"{PYSPARK.major}.{PYSPARK.minor}"]


@pytest.fixture(scope="session", autouse=True)
def spark(tmp_path_factory: pytest.TempPathFactory) -> Generator[SparkSession, Any, None]:
    iceberg_warehouse = tmp_path_factory.mktemp("iceberg")
    with pytest.MonkeyPatch.context() as mp:
        mp.delenv("SPARK_HOME", raising=False)
        builder: SparkSession.Builder = SparkSession.builder
        spark = (
            builder.appName("test")
            # Session
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.warehouse.dir", tmp_path_factory.mktemp(str(uuid.uuid4())))
            # Packages
            .config(
                "spark.jars.packages",
                f"org.apache.iceberg:{SPARK['iceberg']},"
                f"io.delta:delta-spark_{SPARK['scala']}:{SPARK['delta']},"
                "org.xerial:sqlite-jdbc:3.46.1.3",
            )
            # Extensions
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            # Delta catalog (session catalog)
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            # Iceberg catalog (JDBC on SQLite)
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg.type", "jdbc")
            .config("spark.sql.catalog.iceberg.uri", f"jdbc:sqlite:{iceberg_warehouse}/catalog.db")
            .config("spark.sql.catalog.iceberg.jdbc.schema-version", "V1")
            .config("spark.sql.catalog.iceberg.warehouse", str(iceberg_warehouse))
            .getOrCreate()
        )
    yield spark
    spark.stop()


@pytest.fixture
def skip_delta_replace_bug(spark: SparkSession, table_format: str) -> None:
    """Skips Delta cases while the running versions cannot `replace()` an existing table."""
    has_bug = (
        table_format == "delta"
        and Version(spark.version) >= Version("3.5.6")
        and Version(SPARK["delta"]) <= Version("3.3.2")
    )
    if has_bug:
        pytest.skip(f"delta-io/delta#4671: Spark {spark.version} with Delta {SPARK['delta']}")


@pytest.fixture(autouse=True)
def spark_job(spark: SparkSession) -> JobStub:
    runner = RunnerStub()
    runner._session = spark
    return JobStub(
        name="test_job",
        org="acme",
        region="latam",
        domain="ecommerce",
        subdomain="checkout",
        layer="raw",
        product="sales",
        model="orders",
        runner=runner,
        inputs=[TransformStub(name="test_input")],
    )


@pytest.fixture(autouse=True)
def spark_job_context(spark_job: JobStub) -> Generator[Context, Any, None]:
    with Context.for_job(spark_job) as context:
        yield context
