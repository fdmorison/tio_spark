import uuid
from collections.abc import Generator
from typing import Any

import pytest
from pyspark.sql import SparkSession
from tiozin import Context

from tests.stubs import JobStub, RunnerStub, TransformStub
from tio_spark import SparkRunner


@pytest.fixture(scope="session", autouse=True)
def spark(tmp_path_factory: pytest.TempPathFactory) -> Generator[SparkSession, Any, None]:
    builder: SparkSession.Builder = SparkSession.builder
    spark = (
        builder.appName("test")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.warehouse.dir", tmp_path_factory.mktemp(str(uuid.uuid4())))
        .getOrCreate()
    )
    yield spark
    spark.stop()


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
def spark_session(spark_job: JobStub) -> Generator[Any, Any, None]:
    with Context.for_job(spark_job) as context:
        yield context.runner.session


@pytest.fixture
def spark_runner() -> Generator[SparkRunner, Any, None]:
    """Returns a raw SparkRunner already set up."""
    runner: SparkRunner = SparkRunner().__wrapped__
    runner.setup()
    yield runner
    runner.teardown()
