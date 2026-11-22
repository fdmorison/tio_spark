from pathlib import Path
from unittest.mock import MagicMock

from pyspark.sql import SparkSession
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
from pyspark.sql.types import StringType, StructField, StructType
from tiozin import Context, Schema
from tiozin.api.metadata.bundle import Registries
from tiozin.family.tio_kernel import FileSchemaRegistry
from tiozin.utils.runtime import tio_alias

from tests.stubs import JobStub, OutputStub, TransformStub
from tio_spark import SparkFileInput, SparkFileOutput, SparkWordCountTransform
from tio_spark.compose.step_proxy import SparkStepProxy

# =============================================================================
# Testing SparkStepProxy._register_view — name as view name
# =============================================================================


def test_proxy_should_register_view_using_step_name(spark: SparkSession):
    """Temp view is registered under the normalized step name."""
    # Arrange
    step = SparkWordCountTransform(name="my word count step")
    df = spark.createDataFrame([("hello world",)], schema="`value` STRING")

    # Act
    result = step.transform(df)

    # Assert
    actual = tio_alias(result)
    expected = "my_word_count_step"
    assert actual == expected


def test_proxy_should_make_view_queryable_by_name(spark: SparkSession):
    """After registration, the temp view is accessible by its name in Spark SQL."""
    # Arrange
    step = SparkWordCountTransform(name="my word count step")
    df = spark.createDataFrame([("hello world",)], schema="`value` STRING")

    # Act
    step.transform(df)

    # Assert — name is a valid Spark SQL identifier and the view is queryable
    actual = spark.sql("SELECT * FROM my_word_count_step").collect()
    assert len(actual) > 0


def test_proxy_should_normalize_view_name_when_name_has_special_characters(
    spark: SparkSession,
):
    """The view name handles hyphens, mixed case, and extra spaces in the step name."""
    # Arrange
    step = SparkWordCountTransform(name="Word Count - 2024!")
    df = spark.createDataFrame([("hello world",)], schema="`value` STRING")

    # Act
    result = step.transform(df)

    # Assert
    actual = tio_alias(result)
    expected = "word_count_2024"
    assert actual == expected


def test_proxy_should_register_view_using_step_name_when_dataframe_is_spark_connect():
    """Spark Connect DataFrames do not inherit from the classic DataFrame but are registered too."""
    # Arrange
    step = SparkStepProxy(TransformStub(name="connect step"))
    df = MagicMock(spec=ConnectDataFrame)
    df.schema = StructType([StructField("value", StringType())])
    df.to.return_value = df

    # Act
    step.transform(df)

    # Assert
    df.createOrReplaceTempView.assert_called_once_with("connect_step")


# =============================================================================
# Testing SparkStepProxy.write — schema captured from input data
# =============================================================================


def test_write_should_capture_schema_from_input_when_output_is_not_dataframe(
    spark: SparkSession, tmp_path: Path
):
    """
    Regression test: write() must capture the schema from the input DataFrame
    even when the step outputs a non-DataFrame (e.g. DataFrameWriter).
    Without this, context.schema stays None and the schema is absent from lineage.
    """
    # Arrange
    step = SparkFileOutput(name="orders_output", path=str(tmp_path / "out"))
    df = spark.createDataFrame(
        [(1, "alice")],
        schema="`id` INT, `name` STRING",
    )

    # Act
    step.write(df)

    # Assert — schema is recorded on the catalog output dataset
    outputs = Context.current().catalog.get_outputs(step)
    actual = isinstance(outputs[0].tiozin_schema, Schema)
    expected = True
    assert actual == expected


def test_write_should_record_dataframe_schema_when_step_has_no_contract(
    spark: SparkSession, tmp_path: Path
):
    # Arrange
    df = spark.createDataFrame(
        [(1, "alice")],
        schema="`id` INT, `name` STRING",
    )

    # Act
    step = SparkFileOutput(name="orders_output", path=str(tmp_path / "out"))
    step.write(df)

    # Assert
    outputs = Context.current().catalog.get_outputs(step)
    actual = outputs[0].tiozin_schema.to_spark()
    expected = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "integer", "nullable": True, "metadata": {}},
            {"name": "name", "type": "string", "nullable": True, "metadata": {}},
        ],
        "metadata": {},
    }
    assert actual == expected


# =============================================================================
# Testing SparkStepProxy — output schema documented from the registry contract
# =============================================================================


def test_write_should_document_output_schema_from_contract(
    spark: SparkSession, spark_job: JobStub, tmp_path: Path
):
    # Arrange
    registry = FileSchemaRegistry(location=str(tmp_path / "schemas"))
    registry.register(
        "orders",
        Schema(name="orders")
        .integer("id", description="Order id.")
        .string("name", description="Customer name."),
    )
    df = spark.createDataFrame(
        [(1, "alice")],
        schema="`id` INT, `name` STRING",
    )

    # Act
    with Context.for_job(spark_job, registries=Registries(schema=registry)) as context:
        step = SparkFileOutput(
            name="orders_output",
            path=str(tmp_path / "out"),
            schema_subject="orders",
        )
        step.write(df)

    # Assert
    outputs = context.catalog.get_outputs(step)
    actual = outputs[0].tiozin_schema.to_spark()
    expected = {
        "type": "struct",
        "fields": [
            {
                "name": "id",
                "type": "integer",
                "nullable": True,
                "metadata": {"comment": "Order id."},
            },
            {
                "name": "name",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "Customer name."},
            },
        ],
        "metadata": {},
    }
    assert actual == expected


def test_write_should_leave_column_undocumented_when_contract_does_not_declare_it(
    spark: SparkSession, spark_job: JobStub, tmp_path: Path
):
    # Arrange
    registry = FileSchemaRegistry(location=str(tmp_path / "schemas"))
    registry.register(
        "orders",
        Schema(name="orders").integer("id", description="Order id."),
    )
    df = spark.createDataFrame(
        [(1, "run-1")],
        schema="`id` INT, `_raw_run_id` STRING",
    )

    # Act
    with Context.for_job(spark_job, registries=Registries(schema=registry)) as context:
        step = SparkFileOutput(
            name="orders_output",
            path=str(tmp_path / "out"),
            schema_subject="orders",
        )
        step.write(df)

    # Assert
    outputs = context.catalog.get_outputs(step)
    actual = outputs[0].tiozin_schema.to_spark()
    expected = {
        "type": "struct",
        "fields": [
            {
                "name": "id",
                "type": "integer",
                "nullable": True,
                "metadata": {"comment": "Order id."},
            },
            {"name": "_raw_run_id", "type": "string", "nullable": True, "metadata": {}},
        ],
        "metadata": {},
    }
    assert actual == expected


def test_write_should_keep_dataframe_structure_when_contract_differs(
    spark: SparkSession, spark_job: JobStub, tmp_path: Path
):
    # Arrange
    registry = FileSchemaRegistry(location=str(tmp_path / "schemas"))
    registry.register(
        "orders",
        Schema(name="orders")
        .string("id", description="Order id.")
        .string("created_at", description="Creation time."),
    )
    df = spark.createDataFrame(
        [(1,)],
        schema="`id` INT",
    )

    # Act
    with Context.for_job(spark_job, registries=Registries(schema=registry)) as context:
        step = SparkFileOutput(
            name="orders_output",
            path=str(tmp_path / "out"),
            schema_subject="orders",
        )
        step.write(df)

    # Assert
    outputs = context.catalog.get_outputs(step)
    actual = outputs[0].tiozin_schema.to_spark()
    expected = {
        "type": "struct",
        "fields": [
            {
                "name": "id",
                "type": "integer",
                "nullable": True,
                "metadata": {"comment": "Order id."},
            },
        ],
        "metadata": {},
    }
    assert actual == expected


# =============================================================================
# Testing SparkStepProxy.write — contract descriptions as column comments
# =============================================================================


def test_write_should_give_output_step_contract_descriptions_as_column_comments(
    spark: SparkSession, spark_job: JobStub, tmp_path: Path
):
    # Arrange
    registry = FileSchemaRegistry(location=str(tmp_path / "schemas"))
    registry.register(
        "orders",
        Schema(name="orders")
        .integer("id", description="Order id.")
        .string("name", description="Customer name."),
    )
    df = spark.createDataFrame(
        [(1, "alice")],
        schema="`id` INT, `name` STRING",
    )

    # Act
    with Context.for_job(spark_job, registries=Registries(schema=registry)):
        step = OutputStub(name="orders_output", schema_subject="orders")
        result = step.write(df)

    # Assert
    actual = result.schema.jsonValue()
    expected = {
        "type": "struct",
        "fields": [
            {
                "name": "id",
                "type": "integer",
                "nullable": True,
                "metadata": {"comment": "Order id."},
            },
            {
                "name": "name",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "Customer name."},
            },
        ],
    }
    assert actual == expected


def test_write_should_give_output_step_no_column_comment_when_contract_has_no_description(
    spark: SparkSession, spark_job: JobStub, tmp_path: Path
):
    # Arrange
    registry = FileSchemaRegistry(location=str(tmp_path / "schemas"))
    registry.register(
        "orders",
        Schema(name="orders").integer("id", description="Order id.").string("name"),
    )
    df = spark.createDataFrame(
        [(1, "alice")],
        schema="`id` INT, `name` STRING",
    )

    # Act
    with Context.for_job(spark_job, registries=Registries(schema=registry)):
        step = OutputStub(name="orders_output", schema_subject="orders")
        result = step.write(df)

    # Assert
    actual = result.schema.jsonValue()
    expected = {
        "type": "struct",
        "fields": [
            {
                "name": "id",
                "type": "integer",
                "nullable": True,
                "metadata": {"comment": "Order id."},
            },
            {"name": "name", "type": "string", "nullable": True, "metadata": {}},
        ],
    }
    assert actual == expected


def test_read_should_document_output_schema_from_contract(spark_job: JobStub, tmp_path: Path):
    # Arrange
    registry = FileSchemaRegistry(location=str(tmp_path / "schemas"))
    registry.register(
        "orders",
        Schema(name="orders").integer("id", description="Order id."),
    )
    source = tmp_path / "orders.json"
    source.write_text('{"id": 1, "name": "alice"}')

    # Act
    with Context.for_job(spark_job, registries=Registries(schema=registry)) as context:
        step = SparkFileInput(
            name="orders_input",
            path=str(source),
            format="json",
            schema_subject="orders",
        )
        step.read()

    # Assert
    outputs = context.catalog.get_outputs(step)
    actual = outputs[0].tiozin_schema.to_spark()
    expected = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "long", "nullable": True, "metadata": {"comment": "Order id."}},
            {"name": "name", "type": "string", "nullable": True, "metadata": {}},
        ],
        "metadata": {},
    }
    assert actual == expected


def test_transform_should_document_output_schema_from_contract(
    spark: SparkSession, spark_job: JobStub, tmp_path: Path
):
    # Arrange
    registry = FileSchemaRegistry(location=str(tmp_path / "schemas"))
    registry.register(
        "word_counts",
        Schema(name="word_counts").string("word", description="A word from the text."),
    )
    df = spark.createDataFrame(
        [("hello world",)],
        schema="`value` STRING",
    )

    # Act
    with Context.for_job(spark_job, registries=Registries(schema=registry)) as context:
        step = SparkWordCountTransform(name="word_count", schema_subject="word_counts")
        step.transform(df)

    # Assert
    outputs = context.catalog.get_outputs(step)
    actual = outputs[0].tiozin_schema.to_spark()
    expected = {
        "type": "struct",
        "fields": [
            {
                "name": "word",
                "type": "string",
                "nullable": False,
                "metadata": {"comment": "A word from the text."},
            },
            {"name": "count", "type": "long", "nullable": False, "metadata": {}},
        ],
        "metadata": {},
    }
    assert actual == expected


# =============================================================================
# Testing SparkStepProxy.read and transform — contract descriptions as column comments
# =============================================================================


def test_read_should_give_input_step_contract_descriptions_as_column_comments(
    spark_job: JobStub, tmp_path: Path
):
    # Arrange
    registry = FileSchemaRegistry(location=str(tmp_path / "schemas"))
    registry.register(
        "orders",
        Schema(name="orders")
        .integer("id", description="Order id.")
        .string("name", description="Customer name."),
    )
    source = tmp_path / "orders.json"
    source.write_text('{"id": 1, "name": "alice"}')

    # Act
    with Context.for_job(spark_job, registries=Registries(schema=registry)):
        step = SparkFileInput(
            name="orders_input",
            path=str(source),
            format="json",
            schema_subject="orders",
        )
        result = step.read()

    # Assert
    actual = result.schema.jsonValue()
    expected = {
        "type": "struct",
        "fields": [
            {
                "name": "id",
                "type": "long",
                "nullable": True,
                "metadata": {"comment": "Order id."},
            },
            {
                "name": "name",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "Customer name."},
            },
        ],
    }
    assert actual == expected


def test_transform_should_give_transform_step_contract_descriptions_as_column_comments(
    spark: SparkSession, spark_job: JobStub, tmp_path: Path
):
    # Arrange
    registry = FileSchemaRegistry(location=str(tmp_path / "schemas"))
    registry.register(
        "word_counts",
        Schema(name="word_counts")
        .string("word", description="A word from the text.")
        .integer("count", description="Occurrences of the word."),
    )
    df = spark.createDataFrame(
        [("hello world",)],
        schema="`value` STRING",
    )

    # Act
    with Context.for_job(spark_job, registries=Registries(schema=registry)):
        step = SparkWordCountTransform(name="word_count", schema_subject="word_counts")
        result = step.transform(df)

    # Assert
    actual = result.schema.jsonValue()
    expected = {
        "type": "struct",
        "fields": [
            {
                "name": "word",
                "type": "string",
                "nullable": False,
                "metadata": {"comment": "A word from the text."},
            },
            {
                "name": "count",
                "type": "long",
                "nullable": False,
                "metadata": {"comment": "Occurrences of the word."},
            },
        ],
    }
    assert actual == expected
