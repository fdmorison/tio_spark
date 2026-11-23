from collections.abc import Generator
from datetime import UTC, date, datetime
from typing import Any

import pytest
from py4j.protocol import Py4JJavaError
from pyspark.errors import AnalysisException, NumberFormatException
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DateType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.testing import assertDataFrameEqual
from tiozin import Schema
from tiozin.exceptions import RequiredArgumentError

from tio_spark.dataframe_utils import (
    as_spark_schema,
    as_tiozin_schema,
    enforce_schema,
    rename_schema_by_position,
    with_schema_metadata,
)

# ============================================================================
# as_spark_schema
# ============================================================================


def test_as_spark_schema_should_return_struct_type_as_is() -> None:
    # Arrange
    schema = StructType(
        [
            StructField("name", StringType()),
            StructField("id", LongType()),
        ]
    )

    # Act
    result = as_spark_schema(schema)

    # Assert
    assert result is schema


def test_as_spark_schema_should_convert_dataframe(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [("Ana", 1)],
        schema="name STRING, id LONG",
    )

    # Act
    result = as_spark_schema(df)

    # Assert
    actual = result
    expected = StructType(
        [
            StructField("name", StringType()),
            StructField("id", LongType()),
        ]
    )
    assert actual == expected


def test_as_spark_schema_should_parse_ddl_schema(spark: SparkSession) -> None:
    # Arrange
    schema = "name STRING, id LONG"

    # Act
    result = as_spark_schema(schema)

    # Assert
    actual = result
    expected = StructType(
        [
            StructField("name", StringType()),
            StructField("id", LongType()),
        ]
    )
    assert actual == expected


def test_as_spark_schema_should_parse_json_schema() -> None:
    # Arrange
    schema = {
        "type": "struct",
        "fields": [
            {"name": "name", "type": "string", "nullable": True, "metadata": {}},
            {"name": "id", "type": "long", "nullable": True, "metadata": {}},
        ],
    }

    # Act
    result = as_spark_schema(schema)

    # Assert
    actual = result
    expected = StructType(
        [
            StructField("name", StringType()),
            StructField("id", LongType()),
        ]
    )
    assert actual == expected


def test_as_spark_schema_should_convert_tiozin_schema() -> None:
    # Arrange
    schema = Schema(name="users").string("name").integer("id")

    # Act
    result = as_spark_schema(schema)

    # Assert
    actual = result
    expected = StructType(
        [
            StructField("name", StringType()),
            StructField("id", LongType()),
        ]
    )
    assert actual == expected


# ============================================================================
# as_tiozin_schema
# ============================================================================


def test_as_tiozin_schema_should_return_tiozin_schema_as_is() -> None:
    # Arrange
    schema = Schema(name="users").string("name").integer("id")

    # Act
    result = as_tiozin_schema(schema)

    # Assert
    assert result is schema


def test_as_tiozin_schema_should_convert_dataframe(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [("Ana", 1)],
        schema="name STRING, id LONG",
    )

    # Act
    result = as_tiozin_schema(df)

    # Assert
    actual = result
    expected = (
        Schema(physicalType="table")
        .string("name", physicalType="string")
        .integer("id", physicalType="long", logicalTypeOptions={"format": "i64"})
    )
    assert actual == expected


def test_as_tiozin_schema_should_convert_struct_type() -> None:
    # Arrange
    schema = StructType(
        [
            StructField("name", StringType()),
            StructField("id", LongType()),
        ]
    )

    # Act
    result = as_tiozin_schema(schema)

    # Assert
    actual = result
    expected = (
        Schema(physicalType="table")
        .string("name", physicalType="string")
        .integer("id", physicalType="long", logicalTypeOptions={"format": "i64"})
    )
    assert actual == expected


def test_as_tiozin_schema_should_parse_ddl_schema(spark: SparkSession) -> None:
    # Arrange
    schema = "name STRING, id LONG"

    # Act
    result = as_tiozin_schema(schema)

    # Assert
    actual = result
    expected = (
        Schema(physicalType="table")
        .string("name", physicalType="string")
        .integer("id", physicalType="long", logicalTypeOptions={"format": "i64"})
    )
    assert actual == expected


def test_as_tiozin_schema_should_parse_json_schema() -> None:
    # Arrange
    schema = {
        "type": "struct",
        "fields": [
            {"name": "name", "type": "string", "nullable": True, "metadata": {}},
            {"name": "id", "type": "long", "nullable": True, "metadata": {}},
        ],
    }

    # Act
    result = as_tiozin_schema(schema)

    # Assert
    actual = result
    expected = (
        Schema(physicalType="table")
        .string("name", physicalType="string")
        .integer("id", physicalType="long", logicalTypeOptions={"format": "i64"})
    )
    assert actual == expected


# ============================================================================
# with_schema_metadata
# ============================================================================


def test_with_schema_metadata_should_return_dataframe_as_is_when_schema_is_none(
    spark: SparkSession,
) -> None:
    # Arrange
    df = spark.createDataFrame(
        [("Ana",)],
        schema="name STRING",
    )

    # Act
    result = with_schema_metadata(df, None)

    # Assert
    assert result is df


def test_with_schema_metadata_should_raise_when_dataframe_is_missing() -> None:
    # Arrange
    schema = "id INT"

    # Act
    with pytest.raises(RequiredArgumentError) as exc_info:
        with_schema_metadata(None, schema)

    # Assert
    actual = str(exc_info.value)
    expected = "Missing required fields: 'df'"
    assert actual == expected


def test_with_schema_metadata_should_keep_data(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [("Ana",)],
        schema="name STRING",
    )
    schema = StructType(
        [
            StructField("name", StringType(), metadata={"comment": "Full name"}),
        ]
    )

    # Act
    result = with_schema_metadata(df, schema)

    # Assert
    actual = result
    expected = spark.createDataFrame(
        [("Ana",)],
        schema="name STRING",
    )
    assertDataFrameEqual(actual, expected)


def test_with_schema_metadata_should_apply_field_metadata(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [("Ana",)],
        schema="name STRING",
    )
    schema = StructType(
        [
            StructField("name", StringType(), metadata={"comment": "Full name"}),
        ]
    )

    # Act
    result = with_schema_metadata(df, schema)

    # Assert
    actual = result.schema
    expected = StructType(
        [
            StructField("name", StringType(), metadata={"comment": "Full name"}),
        ]
    )
    assert actual == expected


def test_with_schema_metadata_should_apply_struct_field_metadata(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [("Ana", ("Rio",))],
        schema="name STRING, addr STRUCT<city STRING>",
    )
    schema = StructType(
        [
            StructField("name", StringType()),
            StructField(
                "addr",
                StructType(
                    [
                        StructField("city", StringType(), metadata={"comment": "City name"}),
                    ]
                ),
            ),
        ]
    )

    # Act
    result = with_schema_metadata(df, schema)

    # Assert
    actual = result.schema
    expected = StructType(
        [
            StructField("name", StringType()),
            StructField(
                "addr",
                StructType(
                    [
                        StructField("city", StringType(), metadata={"comment": "City name"}),
                    ]
                ),
            ),
        ]
    )
    assert actual == expected


def test_with_schema_metadata_should_keep_columns_missing_from_schema(
    spark: SparkSession,
) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1, "Ana")],
        schema="id INT, name STRING",
    )
    schema = Schema(name="users").string("name", description="Full name")

    # Act
    result = with_schema_metadata(df, schema)

    # Assert
    actual = result
    expected = spark.createDataFrame(
        [(1, "Ana")],
        schema="id INT, name STRING",
    )
    assertDataFrameEqual(actual, expected)


def test_with_schema_metadata_should_keep_own_metadata_of_columns_missing_from_schema(
    spark: SparkSession,
) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1, "Ana")],
        schema="id INT, name STRING",
    ).withMetadata("id", {"comment": "Own id doc"})
    schema = Schema(name="users").string("name", description="Full name")

    # Act
    result = with_schema_metadata(df, schema)

    # Assert
    actual = result.schema
    expected = StructType(
        [
            StructField("id", IntegerType(), metadata={"comment": "Own id doc"}),
            StructField("name", StringType(), metadata={"comment": "Full name"}),
        ]
    )
    assert actual == expected


def test_with_schema_metadata_should_keep_dataframe_field_order(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1, "Ana")],
        schema="id INT, name STRING",
    )
    schema = (
        Schema(name="users")
        .string("name", description="Full name")
        .int32("id", description="User id")
    )

    # Act
    result = with_schema_metadata(df, schema)

    # Assert
    actual = result.schema
    expected = StructType(
        [
            StructField("id", IntegerType(), metadata={"comment": "User id"}),
            StructField("name", StringType(), metadata={"comment": "Full name"}),
        ]
    )
    assert actual == expected


def test_with_schema_metadata_should_keep_dataframe_field_types(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1,)],
        schema="id INT",
    )
    schema = Schema(name="users").string("id", description="User id")

    # Act
    result = with_schema_metadata(df, schema)

    # Assert
    actual = result.schema
    expected = StructType(
        [
            StructField("id", IntegerType(), metadata={"comment": "User id"}),
        ]
    )
    assert actual == expected


def test_with_schema_metadata_should_ignore_schema_fields_missing_from_dataframe(
    spark: SparkSession,
) -> None:
    # Arrange
    df = spark.createDataFrame(
        [("Ana",)],
        schema="name STRING",
    )
    schema = (
        Schema(name="users")
        .string("name", description="Full name")
        .string("ghost", description="Not in the DataFrame")
    )

    # Act
    result = with_schema_metadata(df, schema)

    # Assert
    actual = result.schema
    expected = StructType(
        [
            StructField("name", StringType(), metadata={"comment": "Full name"}),
        ]
    )
    assert actual == expected


def test_with_schema_metadata_should_apply_contract_descriptions(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [("Ana", ("Rio",))],
        schema="name STRING, addr STRUCT<city STRING>",
    )
    contract = (
        Schema(name="users")
        .string("name", description="Full name")
        .object("addr")
        .string("addr.city", description="City name")
    )
    schema = as_tiozin_schema(df).document_from(contract)

    # Act
    result = with_schema_metadata(df, schema)

    # Assert
    actual = result.schema
    expected = StructType(
        [
            StructField("name", StringType(), metadata={"comment": "Full name"}),
            StructField(
                "addr",
                StructType(
                    [
                        StructField("city", StringType(), metadata={"comment": "City name"}),
                    ]
                ),
            ),
        ]
    )
    assert actual == expected


# ============================================================================
# enforce_schema
# ============================================================================


@pytest.fixture
def ansi_enabled(spark: SparkSession, request: pytest.FixtureRequest) -> Generator[None, Any, None]:
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", request.param)
    yield
    spark.conf.set("spark.sql.ansi.enabled", previous)


def test_enforce_schema_should_match_fields_by_name(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [("Ana", "Bia")],
        schema="last STRING, first STRING",
    )
    schema = StructType(
        [
            StructField("first", StringType()),
            StructField("last", StringType()),
        ]
    )

    # Act
    result = enforce_schema(df, schema)

    # Assert
    actual = result
    expected = spark.createDataFrame(
        [("Bia", "Ana")],
        schema="first STRING, last STRING",
    )
    assertDataFrameEqual(actual, expected)


def test_enforce_schema_should_cast_fields_to_schema_types(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [("1", "1982-10-24")],
        schema="id STRING, data STRING",
    )
    schema = StructType(
        [
            StructField("id", LongType()),
            StructField("data", TimestampType()),
        ]
    )

    # Act
    result = enforce_schema(df, schema)

    # Assert
    actual = result
    expected = spark.createDataFrame(
        [(1, datetime(1982, 10, 24, tzinfo=UTC))],
        schema="id LONG, data TIMESTAMP",
    )
    assertDataFrameEqual(actual, expected)


def test_enforce_schema_should_fill_fields_missing_from_dataframe_with_null(
    spark: SparkSession,
) -> None:
    # Arrange
    df = spark.createDataFrame(
        [("1",)],
        schema="id STRING",
    )
    schema = StructType(
        [
            StructField("name", StringType()),
            StructField("id", StringType()),
        ]
    )

    # Act
    result = enforce_schema(df, schema)

    # Assert
    actual = result
    expected = spark.createDataFrame(
        [(None, "1")],
        schema="name STRING, id STRING",
    )
    assertDataFrameEqual(actual, expected)


def test_enforce_schema_should_drop_fields_missing_from_schema(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [("1", "extra")],
        schema="id STRING, extra STRING",
    )
    schema = StructType(
        [
            StructField("id", StringType()),
        ]
    )

    # Act
    result = enforce_schema(df, schema)

    # Assert
    actual = result
    expected = spark.createDataFrame(
        [("1",)],
        schema="id STRING",
    )
    assertDataFrameEqual(actual, expected)


def test_enforce_schema_should_accept_ddl_schema(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [{"id": "1", "s": {"b": "2", "a": "A"}}],
        schema="id STRING, s STRUCT<b STRING, a STRING>",
    )
    schema = "s STRUCT<a STRING, b INT>, id LONG"

    # Act
    result = enforce_schema(df, schema)

    # Assert
    actual = result
    expected = spark.createDataFrame(
        [{"s": {"a": "A", "b": 2}, "id": 1}],
        schema="s STRUCT<a STRING, b INT>, id LONG",
    )
    assertDataFrameEqual(actual, expected)


def test_enforce_schema_should_accept_json_schema(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [{"id": "1", "s": {"b": "2", "a": "A"}}],
        schema="id STRING, s STRUCT<b STRING, a STRING>",
    )
    schema = {
        "type": "struct",
        "fields": [
            {
                "name": "s",
                "type": {
                    "type": "struct",
                    "fields": [
                        {"name": "a", "type": "string", "nullable": True, "metadata": {}},
                        {"name": "b", "type": "integer", "nullable": True, "metadata": {}},
                    ],
                },
                "nullable": True,
                "metadata": {},
            },
            {"name": "id", "type": "long", "nullable": True, "metadata": {}},
        ],
    }

    # Act
    result = enforce_schema(df, schema)

    # Assert
    actual = result
    expected = spark.createDataFrame(
        [{"s": {"a": "A", "b": 2}, "id": 1}],
        schema="s STRUCT<a STRING, b INT>, id LONG",
    )
    assertDataFrameEqual(actual, expected)


def test_enforce_schema_should_accept_tiozin_schema(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [{"id": "1", "s": {"b": "2", "a": "A"}}],
        schema="id STRING, s STRUCT<b STRING, a STRING>",
    )
    schema = Schema(name="users").object("s").string("s.a").integer("s.b").integer("id")

    # Act
    result = enforce_schema(df, schema)

    # Assert
    actual = result
    expected = spark.createDataFrame(
        [{"s": {"a": "A", "b": 2}, "id": 1}],
        schema="s STRUCT<a STRING, b LONG>, id LONG",
    )
    assertDataFrameEqual(actual, expected)


def test_enforce_schema_should_conform_struct_fields(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [{"s": {"b": "2", "a": "2026-01-02"}}],
        schema="s STRUCT<b STRING, a STRING>",
    )
    schema = StructType(
        [
            StructField(
                "s",
                StructType(
                    [
                        StructField("a", DateType()),
                        StructField("b", IntegerType()),
                        StructField("c", StringType()),
                    ]
                ),
            ),
        ]
    )

    # Act
    result = enforce_schema(df, schema)

    # Assert
    actual = result
    expected = spark.createDataFrame(
        [{"s": {"a": date(2026, 1, 2), "b": 2, "c": None}}],
        schema="s STRUCT<a DATE, b INT, c STRING>",
    )
    assertDataFrameEqual(actual, expected)


def test_enforce_schema_should_conform_array_of_struct_fields(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [{"arr": [{"y": "7", "x": "X"}]}],
        schema="arr ARRAY<STRUCT<y STRING, x STRING>>",
    )
    schema = StructType(
        [
            StructField(
                "arr",
                ArrayType(
                    StructType(
                        [
                            StructField("x", StringType()),
                            StructField("y", LongType()),
                        ]
                    )
                ),
            ),
        ]
    )

    # Act
    result = enforce_schema(df, schema)

    # Assert
    actual = result
    expected = spark.createDataFrame(
        [{"arr": [{"x": "X", "y": 7}]}],
        schema="arr ARRAY<STRUCT<x STRING, y LONG>>",
    )
    assertDataFrameEqual(actual, expected)


def test_enforce_schema_should_conform_map_of_struct_values(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [{"m": {"k": {"b": "B", "a": "A"}}}],
        schema="m MAP<STRING, STRUCT<b STRING, a STRING>>",
    )
    schema = StructType(
        [
            StructField(
                "m",
                MapType(
                    StringType(),
                    StructType(
                        [
                            StructField("a", StringType()),
                            StructField("b", StringType()),
                        ]
                    ),
                ),
            ),
        ]
    )

    # Act
    result = enforce_schema(df, schema)

    # Assert
    actual = result
    expected = spark.createDataFrame(
        [{"m": {"k": {"a": "A", "b": "B"}}}],
        schema="m MAP<STRING, STRUCT<a STRING, b STRING>>",
    )
    assertDataFrameEqual(actual, expected)


def test_enforce_schema_should_drop_nested_fields_missing_from_schema(
    spark: SparkSession,
) -> None:
    # Arrange
    df = spark.createDataFrame(
        [{"s": {"a": "A", "extra": "extra"}}],
        schema="s STRUCT<a STRING, extra STRING>",
    )
    schema = StructType(
        [
            StructField(
                "s",
                StructType(
                    [
                        StructField("a", StringType()),
                    ]
                ),
            ),
        ]
    )

    # Act
    result = enforce_schema(df, schema)

    # Assert
    actual = result
    expected = spark.createDataFrame(
        [{"s": {"a": "A"}}],
        schema="s STRUCT<a STRING>",
    )
    assertDataFrameEqual(actual, expected)


@pytest.mark.parametrize("ansi_enabled", ["false"], indirect=True)
@pytest.mark.usefixtures("ansi_enabled")
def test_enforce_schema_should_return_null_when_value_cannot_be_cast(
    spark: SparkSession,
) -> None:
    # Arrange
    df = spark.createDataFrame(
        [("abc",)],
        schema="id STRING",
    )
    schema = StructType(
        [
            StructField("id", LongType()),
        ]
    )

    # Act
    result = enforce_schema(df, schema)

    # Assert
    actual = result
    expected = spark.createDataFrame(
        [(None,)],
        schema="id LONG",
    )
    assertDataFrameEqual(actual, expected)


@pytest.mark.parametrize("ansi_enabled", ["true"], indirect=True)
@pytest.mark.usefixtures("ansi_enabled")
def test_enforce_schema_should_raise_cast_error_when_ansi_enabled(
    spark: SparkSession,
) -> None:
    # Arrange
    df = spark.createDataFrame(
        [("abc",)],
        schema="id STRING",
    )
    schema = StructType(
        [
            StructField("id", LongType()),
        ]
    )

    # Act
    result = enforce_schema(df, schema)

    # Assert
    with pytest.raises((NumberFormatException, Py4JJavaError), match="CAST_INVALID_INPUT"):
        result.collect()


# ============================================================================
# rename_schema_by_position
# ============================================================================


def test_rename_schema_by_position_should_rename_fields_by_position(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1, "one")],
        schema="key LONG, content STRING",
    )
    schema = StructType(
        [
            StructField("id", LongType()),
            StructField("body", StringType()),
        ]
    )

    # Act
    result = rename_schema_by_position(df, schema)

    # Assert
    actual = result
    expected = spark.createDataFrame(
        [(1, "one")],
        schema="id LONG, body STRING",
    )
    assertDataFrameEqual(actual, expected)


def test_rename_schema_by_position_should_cast_fields_to_schema_types(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1, "2")],
        schema="key LONG, size STRING",
    )
    schema = StructType(
        [
            StructField("id", LongType()),
            StructField("length", IntegerType()),
        ]
    )

    # Act
    result = rename_schema_by_position(df, schema)

    # Assert
    actual = result
    expected = spark.createDataFrame(
        [(1, 2)],
        schema="id LONG, length INT",
    )
    assertDataFrameEqual(actual, expected)


def test_rename_schema_by_position_should_accept_ddl_schema(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1, "one")],
        schema="key LONG, content STRING",
    )
    schema = "id LONG, body STRING"

    # Act
    result = rename_schema_by_position(df, schema)

    # Assert
    actual = result
    expected = spark.createDataFrame(
        [(1, "one")],
        schema="id LONG, body STRING",
    )
    assertDataFrameEqual(actual, expected)


def test_rename_schema_by_position_should_accept_json_schema(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1, "one")],
        schema="key LONG, content STRING",
    )
    schema = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "long", "nullable": True, "metadata": {}},
            {"name": "body", "type": "string", "nullable": True, "metadata": {}},
        ],
    }

    # Act
    result = rename_schema_by_position(df, schema)

    # Assert
    actual = result
    expected = spark.createDataFrame(
        [(1, "one")],
        schema="id LONG, body STRING",
    )
    assertDataFrameEqual(actual, expected)


def test_rename_schema_by_position_should_accept_tiozin_schema(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1, "one")],
        schema="key LONG, content STRING",
    )
    schema = Schema(name="users").integer("id").string("body")

    # Act
    result = rename_schema_by_position(df, schema)

    # Assert
    actual = result
    expected = spark.createDataFrame(
        [(1, "one")],
        schema="id LONG, body STRING",
    )
    assertDataFrameEqual(actual, expected)


def test_rename_schema_by_position_should_rename_struct_fields(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1, ("one", 2))],
        schema="key LONG, content STRUCT<text STRING, size INT>",
    )
    schema = StructType(
        [
            StructField("id", LongType()),
            StructField(
                "body",
                StructType(
                    [
                        StructField("value", StringType()),
                        StructField("length", IntegerType()),
                    ]
                ),
            ),
        ]
    )

    # Act
    result = rename_schema_by_position(df, schema)

    # Assert
    actual = result
    expected = spark.createDataFrame(
        [(1, ("one", 2))],
        schema="id LONG, body STRUCT<value STRING, length INT>",
    )
    assertDataFrameEqual(actual, expected)


def test_rename_schema_by_position_should_rename_array_of_struct_fields(
    spark: SparkSession,
) -> None:
    # Arrange
    df = spark.createDataFrame(
        [{"arr": [{"y": "7", "x": "X"}]}],
        schema="arr ARRAY<STRUCT<y STRING, x STRING>>",
    )
    schema = StructType(
        [
            StructField(
                "items",
                ArrayType(
                    StructType(
                        [
                            StructField("a", StringType()),
                            StructField("b", StringType()),
                        ]
                    )
                ),
            ),
        ]
    )

    # Act
    result = rename_schema_by_position(df, schema)

    # Assert
    actual = result
    expected = spark.createDataFrame(
        [{"items": [{"a": "7", "b": "X"}]}],
        schema="items ARRAY<STRUCT<a STRING, b STRING>>",
    )
    assertDataFrameEqual(actual, expected)


def test_rename_schema_by_position_should_rename_map_of_struct_values(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [{"m": {"k": {"b": "B", "a": "A"}}}],
        schema="m MAP<STRING, STRUCT<b STRING, a STRING>>",
    )
    schema = StructType(
        [
            StructField(
                "entries",
                MapType(
                    StringType(),
                    StructType(
                        [
                            StructField("x", StringType()),
                            StructField("y", StringType()),
                        ]
                    ),
                ),
            ),
        ]
    )

    # Act
    result = rename_schema_by_position(df, schema)

    # Assert
    actual = result
    expected = spark.createDataFrame(
        [{"entries": {"k": {"x": "B", "y": "A"}}}],
        schema="entries MAP<STRING, STRUCT<x STRING, y STRING>>",
    )
    assertDataFrameEqual(actual, expected)


def test_rename_schema_by_position_should_raise_analysis_error_when_field_count_differs(
    spark: SparkSession,
) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1, "one")],
        schema="key LONG, content STRING",
    )
    schema = StructType(
        [
            StructField("id", LongType()),
        ]
    )

    # Act
    with pytest.raises(AnalysisException) as exc_info:
        rename_schema_by_position(df, schema)

    # Assert
    assert "DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION" in str(exc_info.value)
