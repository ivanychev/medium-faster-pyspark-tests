from logic.colsum import sum_by
from tests.conftest import SparkTestingContext
from chispa import assert_df_equality

import pyspark.sql.types as T

SCHEMA = T.StructType([
    T.StructField("key_col", T.StringType()),
    T.StructField("value_col", T.LongType()),
])


def test_sum_by(spark_testing_context: SparkTestingContext):
    spark = spark_testing_context.spark

    df = spark.createDataFrame([
        ("foo", 1),
        ("foo", 2),
        ("bar", 42)
    ], schema=SCHEMA)

    actual_df = sum_by(df, "key_col", "value_col")

    expected_df = spark.createDataFrame([
        ("foo", 3),
        ("bar", 42)
    ], schema=SCHEMA)
    assert_df_equality(expected_df, actual_df, ignore_row_order=True)
