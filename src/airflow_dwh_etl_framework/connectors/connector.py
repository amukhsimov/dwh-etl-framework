from pyspark.sql import SparkSession
from airflow.models import Variable
import yaml


class Connectors:
    @classmethod
    def get_flexcube_conn(cls, tag='main'):
        return yaml.safe_load(Variable.get(f'{tag.upper()}_FLEXCUBE_CONN'))

    @classmethod
    def get_greenplum_conn(cls, tag='main'):
        return yaml.safe_load(Variable.get(f'{tag.upper()}_GREENPLUM_CONN'))

    @classmethod
    def get_conn(cls, system_name, tag='main'):
        return yaml.safe_load(Variable.get(f'{tag.upper()}_{system_name.upper()}_CONN'))

class SparkConnector:
    def __init__(self, app_name, tag='main'):
        assert tag in ('main', )
        assert app_name is not None and app_name != ""

        self.tag = tag
        self.app_name = app_name

    def read_jdbc(self, conn_info, query=None, dbtable=None):
        if not query and not dbtable:
            raise ValueError("read_jdbc(): Either query or dbtable should be specified")
        jdbc_df = self.spark.read.format("jdbc") \
            .option("url", conn_info["spark_url"]) \
            .option("user", conn_info["username"]) \
            .option("password", conn_info.get("password") or '') \
            .option("batchsize", Variable.get(f"{self.tag.upper()}_SPARK_BATCH_SIZE")) \
            .option("fetchsize", Variable.get(f"{self.tag.upper()}_SPARK_FETCH_SIZE")) \
            .option("driver", conn_info["driver"])
        if query:
            jdbc_df = jdbc_df.option("query", query)
        else:
            jdbc_df = jdbc_df.option("dbtable", dbtable)
        return jdbc_df.load()

    def write_jdbc(self, spark_df, conn_info, target_schema, target_table, mode='append'):
        spark_df.write.format("jdbc") \
            .option("url", conn_info["spark_url"]) \
            .option("user", conn_info["username"]) \
            .option("password", conn_info.get("password") or '') \
            .option("batchsize", Variable.get(f"{self.tag.upper()}_SPARK_BATCH_SIZE")) \
            .option("fetchsize", Variable.get(f"{self.tag.upper()}_SPARK_FETCH_SIZE")) \
            .option("driver", conn_info["driver"]) \
            .option("mode", mode) \
            .option("dbtable", f"{target_schema}.{target_table}") \
            .save()

    def __enter__(self):
        self.spark = SparkSession.builder.appName(self.app_name).getOrCreate()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.spark.stop()
