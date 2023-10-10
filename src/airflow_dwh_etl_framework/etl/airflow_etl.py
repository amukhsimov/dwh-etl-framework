from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BaseOperator
import os
from airflow.models import Variable
from .. import SparkConnector


class AirflowETL:
    def __init__(self, dag: DAG):
        self.dag: DAG = dag

    @classmethod
    def _extract_db_full(cls, spark_connector, url, driver, query, datalake_target_path):
        jdbc_df = spark_connector.read_jdbc(url=url, driver=driver, query=query)
        jdbc_df.write.orc(datalake_target_path, mode='overwrite')

    @classmethod
    def _extract_db(cls, source_system_name, source_system_tag, scheme, table_name, mode):
        """
        :param source_system_name: Source system (e.g.: flexcube)
        :param source_system_tag: Source system tag (e.g.: main, test, prod)
        :param scheme: Scheme name in source database
        :param table_name: Table
        :param mode: full/delta
        :return:
        """
        if mode == 'delta':
            raise NotImplementedError('Delta load mode is not implemented yet')
        assert mode in ('full',)  # 'delta' is not implemented yet

        with SparkConnector(
                app_name=f"extract_{source_system_name.lower()}_{source_system_tag.lower()}_{table_name.lower()}"
        ) as spark_conn:
            with open(os.path.join(Variable.get("AIRFLOW_SQL_FOLDER"),
                                   "extract", source_system_name.lower(), source_system_tag.lower(),
                                   scheme.lower(), f"{table_name.lower()}_{mode.lower()}")) as fp:
                query = fp.read()

            var_pref = f"{source_system_tag.upper()}_{source_system_name.upper()}"
            url = Variable.get(f"{var_pref}_URL")
            driver = Variable.get(f"{var_pref}_DRIVER")
            datalake_target_path = os.path.join(
                "s3a://datalake",
                source_system_name.lower(),
                source_system_tag.lower(),
                scheme.lower(),
                f"{table_name.lower()}.orc"
            )

            if mode == 'full':
                AirflowETL._extract_db_full(spark_connector=spark_conn,
                                            url=url, driver=driver, query=query,
                                            datalake_target_path=datalake_target_path)

    def extract_db(self, source_system_name, source_system_tag, scheme, table_name, mode) -> BaseOperator:
        """
        :param source_system_name: Source system (e.g.: flexcube)
        :param source_system_tag: Source system tag (e.g.: main, test, prod)
        :param scheme: Scheme name in source database
        :param table_name: Table
        :param mode: full/delta
        :return:
        """
        return PythonOperator(python_callable=AirflowETL.extract_db,
                              op_kwargs={
                                  "source_system_name": source_system_name,
                                  "source_system_tag": source_system_tag,
                                  "scheme": scheme,
                                  "table_name": table_name,
                                  "mode": mode,
                              },
                              dag=self.dag)

    def transform(self, **kwargs) -> BaseOperator:
        raise NotImplementedError("AirflowETL.transform() is not implemented yet")
