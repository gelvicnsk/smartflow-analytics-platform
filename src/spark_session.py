from pyspark.sql import SparkSession
from src.config import SPARK_APP_NAME

def get_spark():
    return (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
