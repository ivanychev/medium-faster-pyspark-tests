from pyspark.sql import DataFrame
import pyspark.sql.functions as f


def sum_by(df: DataFrame, key: str, value: str) -> DataFrame:
    return (df.groupBy(key)
            .agg(f.sum(value).alias(value)))
