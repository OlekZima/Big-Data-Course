from pyspark.sql import SparkSession


def get_spark():
    return (
        SparkSession.builder.appName("BGD")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
