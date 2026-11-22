from pyspark.sql import DataFrame

from tio_spark import SparkOutput


class OutputStub(SparkOutput):
    def write(self, data: DataFrame) -> DataFrame:
        return data
