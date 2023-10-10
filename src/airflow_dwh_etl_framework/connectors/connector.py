from pyspark.sql import SparkSession
from airflow.models import Variable


class SparkConnector:
    def __init__(self, app_name, tag='main'):
        assert tag in ('main', )
        assert app_name is not None and app_name != ""

        self.tag = tag
        self.app_name = app_name

    def read_jdbc(self, url, query, driver):
        jdbc_df = self.spark.read.format("jdbc") \
            .option("url", url) \
            .option("batchsize", Variable.get(f"{self.tag.upper()}_SPARK_BATCH_SIZE")) \
            .option("fetchsize", Variable.get(f"{self.tag.upper()}_SPARK_FETCH_SIZE")) \
            .option("query", query) \
            .option("driver", driver) \
            .load()
        return jdbc_df

    def __enter__(self):
        self.spark = SparkSession.builder.appName(self.app_name).getOrCreate()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.spark.stop()
