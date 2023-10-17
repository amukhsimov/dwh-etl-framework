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
    def _extract_db_full(cls, task_id, source_system_name, source_system_tag, scheme, table_name, params=None):
        """
        :param source_system_name: Source system (e.g.: flexcube)
        :param source_system_tag: Source system tag (e.g.: main, test, prod)
        :param scheme: Scheme name in source database
        :param table_name: Table
        :param params: load parameters
        :return:
        """
        if params and 'mode' in params:
            assert params['mode'] in ('overwrite', 'append'), f'Invalid spark write mode: {params["mode"]}'
            mode = params['mode']
        else:
            mode = 'overwrite'

        with SparkConnector(
                app_name=task_id
        ) as spark_conn:
            fname = os.path.join(Variable.get("AIRFLOW_SQL_FOLDER"),
                                 "extract", source_system_name.lower(), source_system_tag.lower(),
                                 scheme.lower(), f"{table_name.lower()}-full.sql")
            if not os.path.isfile(fname):
                raise FileNotFoundError(fname)

            with open(fname) as fp:
                query = fp.read()

            var_pref = f"{source_system_tag.upper()}_{source_system_name.upper()}"
            url = Variable.get(f"{var_pref}_URL")
            driver = Variable.get(f"{var_pref}_DRIVER")
            datalake_target_path = os.path.join(
                "s3a://datalake",
                source_system_name.lower(),
                source_system_tag.lower(),
                scheme.lower(),
                f"{table_name.lower()}"
            )

            jdbc_df = spark_conn.read_jdbc(url=url, driver=driver, query=query)
            jdbc_df.write.orc(datalake_target_path, mode=mode)

    @classmethod
    def _extract_db_delta(cls, task_id, source_system_name, source_system_tag, scheme, table_name, params):
        """
        :param source_system_name: Source system (e.g.: flexcube)
        :param source_system_tag: Source system tag (e.g.: main, test, prod)
        :param scheme: Scheme name in source database
        :param table_name: Table
        :param params: parameters for delta load
        :return:
        """
        # TODO
        raise NotImplementedError()
        # with SparkConnector(
        #         app_name=task_id
        # ) as spark_conn:
        #     with open(os.path.join(Variable.get("AIRFLOW_SQL_FOLDER"),
        #                            "extract", source_system_name.lower(), source_system_tag.lower(),
        #                            scheme.lower(), f"{table_name.lower()}-full.sql")) as fp:
        #         query = fp.read()
        #
        #     var_pref = f"{source_system_tag.upper()}_{source_system_name.upper()}"
        #     url = Variable.get(f"{var_pref}_URL")
        #     driver = Variable.get(f"{var_pref}_DRIVER")
        #     datalake_target_path = os.path.join(
        #         "s3a://datalake",
        #         source_system_name.lower(),
        #         source_system_tag.lower(),
        #         scheme.lower(),
        #         f"{table_name.lower()}"
        #     )
        #
        #     jdbc_df = spark_conn.read_jdbc(url=url, driver=driver, query=query)
        #     jdbc_df.write.orc(datalake_target_path, mode='overwrite')

    @classmethod
    def _extract_db_full1(cls, task_id, source_system_name, source_system_tag, scheme, table_name, params=None):
        print(123123123)

    def extract_db(self, source_system_name, source_system_tag, scheme, table_name,
                   mode, params=None) -> BaseOperator:
        """
        :param source_system_name: Source system (e.g.: flexcube)
        :param source_system_tag: Source system tag (e.g.: main, test, prod)
        :param scheme: Scheme name in source database
        :param table_name: Table
        :param mode: full/delta/manual
        :param params: Parameters
        :return:
        """
        source_system_name = source_system_name.lower()
        source_system_tag = source_system_tag.lower()
        scheme = scheme.lower()
        table_name = table_name.lower()
        mode = mode.lower()

        task_id = f"task_extract_" \
                  f"{source_system_name}_{source_system_tag}_{table_name}_full"
        return PythonOperator(python_callable=AirflowETL._extract_db_full1,
                              task_id=task_id,
                              op_kwargs={
                                  "task_id": task_id,
                                  "source_system_name": source_system_name,
                                  "source_system_tag": source_system_tag,
                                  "scheme": scheme,
                                  "table_name": table_name,
                                  "params": params,
                              },
                              dag=self.dag)

        if mode == 'full':
            task_id = f"task_extract_" \
                      f"{source_system_name}_{source_system_tag}_{table_name}_full"
            return PythonOperator(python_callable=AirflowETL._extract_db_full,
                                  task_id=task_id,
                                  op_kwargs={
                                      "task_id": task_id,
                                      "source_system_name": source_system_name,
                                      "source_system_tag": source_system_tag,
                                      "scheme": scheme,
                                      "table_name": table_name,
                                      "params": params,
                                  },
                                  dag=self.dag)
        elif mode == 'delta':
            task_id = f"extract_{source_system_name.lower()}_{source_system_tag.lower()}_{table_name.lower()}_delta"
            return PythonOperator(python_callable=AirflowETL._extract_db_delta,
                                  task_id=task_id,
                                  op_kwargs={
                                      "task_id": task_id,
                                      "source_system_name": source_system_name,
                                      "source_system_tag": source_system_tag,
                                      "scheme": scheme,
                                      "table_name": table_name,
                                      "params": params,
                                  },
                                  dag=self.dag)

    def transform(self, **kwargs) -> BaseOperator:
        raise NotImplementedError("AirflowETL.transform() is not implemented yet")
