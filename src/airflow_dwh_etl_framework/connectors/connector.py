from pyspark.sql import SparkSession
from airflow.models import Variable
import yaml


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

    def read_greenplum_jdbc_query(self, query):
        greenplum_conn = yaml.safe_load(Variable.get('MAIN_GREENPLUM_CONN'))
        host = greenplum_conn['host']
        port = greenplum_conn['port']
        dbname = greenplum_conn['dbname']
        username = greenplum_conn['username']
        password = greenplum_conn.get('password', '')
        url = f'jdbc:postgresql://{host}:{port}/{dbname}'
        driver = 'org.postgresql.Driver'

        spark_df = self.spark.read.format('jdbc') \
            .option("url", url) \
            .option("query", query) \
            .option("user", username) \
            .option("password", password) \
            .option("driver", driver) \
            .load()

        return spark_df

    def read_greenplum_jdbc_table(self, schema, table):
        greenplum_conn = yaml.safe_load(Variable.get('MAIN_GREENPLUM_CONN'))
        host = greenplum_conn['host']
        port = greenplum_conn['port']
        dbname = greenplum_conn['dbname']
        username = greenplum_conn['username']
        password = greenplum_conn.get('password') or ''
        url = f'jdbc:postgresql://{host}:{port}/{dbname}'
        driver = 'org.postgresql.Driver'

        spark_df = self.spark.read.format('jdbc') \
            .option("url", url) \
            .option("dbtable", f"{schema}.{table}") \
            .option("user", username) \
            .option("password", password) \
            .option("driver", driver) \
            .load()

        return spark_df

    def __enter__(self):
        self.spark = SparkSession.builder.appName(self.app_name).getOrCreate()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.spark.stop()
