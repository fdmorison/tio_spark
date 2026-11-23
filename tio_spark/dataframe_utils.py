"""
PySpark DataFrame utilities.

Functions that receive and return PySpark DataFrames.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
from pyspark.sql.types import ArrayType, DataType, MapType, StructField, StructType
from tiozin import Schema


def as_spark_schema(
    schema_or_df: DataFrame | Schema | StructType | str | dict[str, Any],
) -> StructType:
    """
    Converts a schema into a Spark schema.

    A DDL string is parsed with the active Spark session, through
    `createDataFrame` instead of `_parse_datatype_string`, which does not
    support Spark Connect on PySpark 3.5.

    Args:
        schema_or_df:
            A DataFrame, a Tiozin Schema, a StructType, a Spark DDL string or a
            Spark JSON schema dict. A DataFrame gives its own schema.

    Returns:
        The schema as a StructType.

    Example:
        ```python
        as_spark_schema("name STRING, id LONG")
        # StructType([StructField("name", StringType()), StructField("id", LongType())])
        ```
    """
    match schema_or_df:
        case DataFrame() | ConnectDataFrame():
            return schema_or_df.schema
        case Schema():
            return StructType.fromJson(schema_or_df.to_spark())
        case str():
            return SparkSession.active().createDataFrame([], schema_or_df).schema
        case dict():
            return StructType.fromJson(schema_or_df)
        case _:
            return schema_or_df


def as_tiozin_schema(
    schema_or_df: DataFrame | Schema | StructType | str | dict[str, Any],
) -> Schema:
    """
    Converts a schema into a Tiozin Schema.

    Args:
        schema_or_df:
            A DataFrame, a Tiozin Schema, a StructType, a Spark DDL string or a
            Spark JSON schema dict. A DataFrame gives its own schema.

    Returns:
        The schema as a Tiozin Schema.

    Example:
        ```python
        as_tiozin_schema(df)
        # Schema with the fields of the DataFrame
        ```
    """
    match schema_or_df:
        case Schema():
            return schema_or_df
        case _:
            return Schema.from_spark(as_spark_schema(schema_or_df).jsonValue())


def with_schema_metadata(
    df: DataFrame,
    schema: Schema | StructType | str | dict[str, Any],
) -> DataFrame:
    """
    Applies the metadata of a schema, such as field descriptions, to the
    fields of a DataFrame.

    The structure of the DataFrame does not change: its fields keep their
    order and types, even when the schema declares other types. Fields
    missing from the schema keep their own metadata, and fields missing from
    the DataFrame are ignored.

    Args:
        df:
            The DataFrame that receives the metadata.
        schema:
            The schema that holds the metadata, as a Tiozin Schema, a
            StructType, a Spark DDL string or a Spark JSON schema dict.

    Returns:
        A new DataFrame with the metadata of the schema in its fields.

    Example:
        ```python
        contract = Schema(name="users").string("name", description="Full name")

        with_schema_metadata(df, contract)
        # the name field of df now carries the description "Full name"
        ```
    """
    observed_schema = as_tiozin_schema(df)
    contract_schema = as_tiozin_schema(schema)
    documented = observed_schema.document_from(contract_schema)
    enriched = df.to(as_spark_schema(documented))
    return enriched


def enforce_schema(
    df: DataFrame,
    schema: Schema | StructType | str | dict[str, Any],
) -> DataFrame:
    """
    Conforms a DataFrame to a schema, matching fields by name at every level.

    Fields are matched by name, including inside structs, arrays and maps, then
    reordered and cast to the types of the schema. Fields missing from the
    DataFrame become null, and fields missing from the schema are dropped.

    Enable `spark.sql.ansi.enabled` to fail on values that cannot be cast,
    instead of turning them into null.

    Notes:
        Useful for applying an evolved schema, such as the one returned by
        `Schema.evolve`, to the original DataFrame.

    Args:
        df:
            The DataFrame to conform.
        schema:
            The schema the DataFrame must follow, as a Tiozin Schema, a
            StructType, a Spark DDL string or a Spark JSON schema dict.

    Returns:
        A new DataFrame with the fields, order and types of the schema.

    Example:
        ```python
        df = spark.createDataFrame(
            [("1", "1982-10-24")],
            "id STRING, data STRING",
        )

        enforce_schema(
            df,
            "name STRING, data TIMESTAMP, id LONG",
        )
        # name: string = null, data: timestamp = 1982-10-24 00:00:00, id: long = 1
        ```
    """

    def layout(source: DataType, target: DataType) -> DataType:
        match source, target:
            case StructType(), StructType():
                fields = {field.name: field.dataType for field in source.fields}
                return StructType(
                    [
                        StructField(
                            field.name,
                            layout(fields[field.name], field.dataType)
                            if field.name in fields
                            else field.dataType,
                        )
                        for field in target.fields
                    ]
                )
            case ArrayType(), ArrayType():
                return ArrayType(layout(source.elementType, target.elementType))
            case MapType(), MapType():
                return MapType(
                    layout(source.keyType, target.keyType),
                    layout(source.valueType, target.valueType),
                )
            case _:
                return source

    schema = as_spark_schema(schema)
    reordered = df.to(layout(df.schema, schema))
    casted = reordered.select(sf.struct("*").cast(schema).alias("root")).select("root.*")

    return casted


def rename_schema_by_position(
    df: DataFrame,
    schema: Schema | StructType | str | dict[str, Any],
) -> DataFrame:
    """
    Renames the fields of a DataFrame by position, at every level.

    Each field takes the name of the field in the same position of the schema,
    including inside structs, arrays and maps, and is cast to its type. The
    DataFrame and the schema must have the same structure.

    Enable `spark.sql.ansi.enabled` to fail on values that cannot be cast,
    instead of turning them into null.

    Args:
        df:
            The DataFrame to rename.
        schema:
            The schema with the new names, as a Tiozin Schema, a StructType, a
            Spark DDL string or a Spark JSON schema dict.

    Returns:
        A new DataFrame with the names and types of the schema.

    Example:
        ```python
        df = spark.createDataFrame(
            [(1, ("one", 2))],
            "key LONG, content STRUCT<text: STRING, size: INT>",
        )

        rename_schema_by_position(
            df,
            "id LONG, body STRUCT<value: STRING, length: INT>",
        )
        # id: long = 1, body: struct<value: string, length: int> = {one, 2}
        ```
    """
    schema = as_spark_schema(schema)
    renamed = df.select(sf.struct("*").cast(schema).alias("root")).select("root.*")
    return renamed
