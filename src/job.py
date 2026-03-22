from pathlib import Path

from pyspark.sql.functions import col, count, to_timestamp

from .utils.logger import get_logger
from .utils.spark import get_spark

logger = get_logger(__name__)


class Job:
    def __init__(self, data_path: Path, spark_session):
        """
        Initializes the Job class.

        Args:
            data_path: The path to the data directory.
            spark_session: The SparkSession object.
        """
        self.data_path = data_path
        self.spark_session = spark_session

    def write_gold(self):
        """
        Writes the aggregated data to the gold layer.

        Reads data from the silver layer, groups it by channel, and writes the
        count of transactions per channel to the gold layer in Parquet format.
        """
        df = self.spark_session.read.parquet(str(self.data_path / "silver"))

        gold = df.groupBy("channel").agg(count("*").alias("cnt"))
        gold.write.mode("overwrite").parquet(str(self.data_path / "gold"))

    def write_silver(self):
        """
        Writes the cleaned and processed data to the silver layer.

        Reads data from the raw layer, removes duplicates, filters out rows with
        null transaction IDs, converts the transaction timestamp, and writes the
        processed data to the silver layer in Parquet format.
        """
        df = self.spark_session.read.parquet(str(self.data_path / "raw"))

        df_clean = (
            df.dropDuplicates()
            .filter(col("transaction_id").isNotNull())
            .withColumn(
                "transaction_timestamp", to_timestamp(col("transaction_timestamp"))
            )
        )

        df_clean.write.mode("overwrite").parquet(str(self.data_path / "silver"))


def main():
    logger.info("[JOB] Starting Spark medallion parquet job")
    spark = get_spark()
    job = Job(Path("data"), spark)
    logger.info("[JOB] Writing Silver parquet layer")
    job.write_silver()
    logger.info("[JOB] Writing Gold parquet layer")
    job.write_gold()
    logger.info("[JOB] Spark job completed")


if __name__ == "__main__":
    main()
