from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BaseOperator
import os
import pandas as pd
from airflow.models import Variable
import psycopg2
import yaml
from .. import SparkConnector


class ETLUtils:
    class Postgres:
        @classmethod
        def execute_script(cls, conn_info, sql_script):
            """
            Executes and commits script into a postgres database.
            :param conn_info: connection info in to following format:
                {'url': 'postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]',
                 'host': '',
                 'port': ,
                 'dbname': '',
                 'username': '',
                 'password': ,
                 '...': '....'}
            :param sql_script: sql script text
            :return:
            """
            conn = psycopg2.connect(database=conn_info['dbname'],
                                    user=conn_info['username'],
                                    password=conn_info['password'],
                                    host=conn_info['host'],
                                    port=conn_info['port'])
            cur = conn.cursor()
            cur.execute(sql_script)
            conn.commit()
            conn.close()
            cur.close()

        @classmethod
        def load_sql_script_spark(cls, greenplum_conn, sql_script, spark):
            host = greenplum_conn['host']
            port = greenplum_conn['port']
            dbname = greenplum_conn['dbname']

            return spark.read.format('jdbc') \
                .option("url", f"jdbc:postgresql://{host}:{port}/{dbname}") \
                .option("query", sql_script) \
                .option("user", "spark") \
                .option("password", "") \
                .option("driver", "org.postgresql.Driver") \
                .load()

        @classmethod
        def load_sql_script(cls, conn_info, query):
            conn = psycopg2.connect(database=conn_info['dbname'],
                                    user=conn_info['username'],
                                    password=conn_info['password'],
                                    host=conn_info['host'],
                                    port=conn_info['port'])
            cur = conn.cursor()
            cur.execute(query)
            data = cur.fetchall()
            columns = [x.name for x in cur.description]
            conn.close()
            cur.close()
            return pd.DataFrame(data, columns=columns)

        @classmethod
        def write_df_to_db(cls, spark_df, postgres_conn_info, target_schema, target_table_name, write_mode):
            """
            :param spark_df: spark dataframe to write
            :param postgres_conn_info: connection info in to following format:
                {'url': 'postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]',
                 'host': '',
                 'port': ,
                 'dbname': '',
                 'username': '',
                 'password': ,
                 '...': '....'}
            :param target_schema: schema name in postgres database
            :param target_table_name: table name in postgres database
            :param write_mode: if 'overwrite' - truncates the table and writes the data (overwrite/append)
            :return:
            """
            if write_mode not in ('overwrite', 'append'):
                raise ValueError(f"Invalid write_df_to_postgers write_mode: '{write_mode}'")

            properties = {"user": postgres_conn_info['username'],
                          "password": postgres_conn_info['password'] or '',
                          "driver": "org.postgresql.Driver"}

            if write_mode == 'overwrite':
                ETLUtils.Postgres.execute_script(
                    conn_info=postgres_conn_info,
                    sql_script=f"truncate table {target_schema}.{target_table_name};"
                )

            spark_df.write.jdbc(
                url=f"jdbc:postgresql://{postgres_conn_info['host']}:"
                    f"{postgres_conn_info['port']}/{postgres_conn_info['dbname']}",
                table=f"{target_schema}.{target_table_name}",
                mode='append',
                properties=properties)

        @classmethod
        def get_table_cols(cls, conn_info, table_schema, table_name):
            query = f"""
            select t1.column_name, 
                case when t3.column_name is not null then 'Y' else 'N' end as is_primary
            from information_schema.columns t1
            left join information_schema.table_constraints t2
                on t2.table_schema = t1.table_schema 
                    and t2.table_name = t1.table_name 
                    and t2.constraint_type = 'PRIMARY KEY'
            left join information_schema.key_column_usage t3
                on t3.constraint_name = t2.constraint_name
                    and t3.table_schema = t2.table_schema
                    and t3.table_name = t2.table_name
                    and t3.column_name = t1.column_name 
            where t1.table_schema = '{table_schema}'
                and t1.table_name = '{table_name}'
            order by t1.ordinal_position;
            """
            return ETLUtils.Postgres.load_sql_script(conn_info=conn_info, query=query)

        @classmethod
        def merge_target_table(cls, conn_info, table_schema, table_name, merge_mode):
            """
            Merges *__journal table into master table
            :param conn_info: postgres connection info
            :param table_schema:
            :param table_name:
            :param merge_mode: full/delta
            :return:
            """
            df_cols = ETLUtils.Postgres.get_table_cols(conn_info=conn_info,
                                                       table_schema=table_schema,
                                                       table_name=table_name)

            partition_columns = ', '.join([
                    f'"{col_name}"'
                    for col_name in df_cols.loc[df_cols['is_primary'] == 'Y']['column_name'].values
                ])
            all_columns = ', '.join([
                    f'"{col_name}"'
                    for col_name in df_cols['column_name'].values
                ])

            if merge_mode == 'full':
                # truncate master table
                ETLUtils.Postgres.execute_script(
                    conn_info=conn_info,
                    sql_script=f'''
                    truncate table {table_schema}."{table_name}"
                    '''
                )
                # insert records into master table from journal
                ETLUtils.Postgres.execute_script(
                    conn_info=conn_info,
                    sql_script=f'''
                    INSERT INTO {table_schema}."{table_name}" AS t1 
                    (
                        SELECT {all_columns}
                        FROM (
                            SELECT {all_columns},
                                row_number() over (partition BY {partition_columns}
                                                   ORDER BY __transform_dt DESC, __load_dt DESC, __seqno ASC) AS rnk
                            FROM {table_schema}."{table_name}__journal"
                        ) AS t1
                        WHERE rnk = 1
                    )
                    '''
                )
            elif merge_mode == 'delta':
                # clear master table given corresponding records
                conditions = ' and '.join([
                    f't1."{col_name}" = t2."{col_name}"'
                    for col_name in df_cols.loc[df_cols['is_primary'] == 'Y']['column_name'].values
                ])
                ETLUtils.Postgres.execute_script(
                    conn_info=conn_info,
                    sql_script=f'''
                    DELETE FROM {table_schema}."{table_name}" AS t1
                    USING (
                        SELECT DISTINCT {partition_columns}
                        FROM {table_schema}."{table_name}__journal"
                        WHERE __record_state = 'A'
                    ) AS t2
                    WHERE ({conditions})
                    '''
                )
                # insert records into master table from journal
                ETLUtils.Postgres.execute_script(
                    conn_info=conn_info,
                    sql_script=f'''
                    INSERT INTO {table_schema}."{table_name}" AS t1 
                    (
                        SELECT {all_columns}
                        FROM (
                            SELECT {all_columns},
                                row_number() over (partition by {partition_columns}
                                                   order by __transform_dt desc, __load_dt desc, __seqno asc) as rnk
                            FROM {table_schema}."{table_name}__journal"
                            WHERE __record_state = 'A'
                        ) AS t1
                        where rnk = 1
                    )
                    '''
                )

            ETLUtils.Postgres.execute_script(
                conn_info=conn_info,
                sql_script=f'''
                UPDATE {table_schema}."{table_name}__journal" 
                SET __record_state = 'H'
                WHERE __record_state = 'A'
                '''
            )

    class Datalake:
        @classmethod
        def get_dump_dir(cls):
            return 's3a://dump/etl'

        @classmethod
        def upload_file_to_s3(cls, source, target):
            pass

    class Spark:
        @classmethod
        def execute_sql_script(cls, spark, sql_script, cache_dir=None,
                               alias=None, engine='spark', **kwargs):
            """
            Runs sql script in a given spark session, caches result and loads it as a view if
                cache_dir and alias is provided
            :param spark: spark session
            :param sql_script: sql script to load
            :param cache_dir: directory in datalake to cache data
            :param alias: alias to create temp view in spark catalog
            :param engine: greenplum/spark
            :param **kwargs: {'greenplum_conn': 'optional conn info'}
            :return:
            """
            if engine == 'spark':
                # run sql script via spark session
                df = spark.sql(sql_script)
            elif engine == 'greenplum':
                if 'greenplum_conn' in kwargs:
                    greenplum_conn = kwargs['greenplum_conn']
                else:
                    greenplum_conn = yaml.safe_load(Variable.get('MAIN_GREENPLUM_CONN'))

                df = ETLUtils.Postgres.load_sql_script_spark(
                    greenplum_conn=greenplum_conn, sql_script=sql_script, spark=spark,
                )
            else:
                raise ValueError(f"Invalid engine: '{engine}'")
            # if cache_dir is specified - cache it
            if cache_dir:
                df.write.orc(cache_dir, mode='overwrite')
                df = spark.read.orc(cache_dir)
            # if alias is specified - create a view
            if alias:
                df.createOrReplaceTempView(alias)
            return df


        @classmethod
        def execute_sql_file(cls, spark, filename, cache_dir=None,
                               alias=None, engine='spark', **kwargs):
            """
            Runs sql file in a given spark session, caches result and loads it as a view if
                cache_dir and alias is provided
            :param spark:
            :param filename:
            :param cache_dir:
            :param alias:
            :return:
            """
            # check if file exists
            if not os.path.isfile(filename):
                raise FileNotFoundError(f"SQL file not exists: {filename}")
            # read sql script from file
            with open(filename, 'rt') as fp:
                sql_script = fp.read()

            return ETLUtils.Spark.execute_sql_script(spark=spark,
                                                     sql_script=sql_script,
                                                     cache_dir=cache_dir,
                                                     alias=alias,
                                                     engine=engine,
                                                     **kwargs)

        @classmethod
        def load_dependencies(cls, dependencies, spark_connector: SparkConnector):
            """
            Creates temporal views in spark catalog.
            :param dependencies: Tables in the following format:
                [{  'source': 'datalake',
                    'source_system_name': 'flexcube',
                    'source_system_tag': 'main',
                    'schema': 'ociuz',
                    'table_name': 'gltb_rpt_vd_bal_custom',
                    'alias': 'fc_saldo'},
                   {'source': 'datalake',
                    'source_system_name': 'flexcube',
                    'source_system_tag': 'main',
                    'schema': 'ociuz',
                    'table_name': 'sttm_customer',
                    'alias': 'fc_saldo'  }]
            :return:
            """
            for dependency in dependencies:
                source = dependency.get('source')
                path = dependency.get('path')
                source_system_name = dependency.get('source_system_name')
                source_system_tag = dependency.get('source_system_tag')
                schema = dependency.get('schema')
                source_table_name = dependency.get('table_name')
                format = dependency.get('format')
                alias = dependency.get('alias')

                if not source:
                    raise ValueError(f"load_dependencies_into_spark(): Invalid source: '{source}'.")
                if not (source == 'datalake' and path is not None) and \
                        (not source_system_name or not source_system_tag
                         or not schema or not source_table_name):
                    raise ValueError(f"load_dependencies_into_spark(): Path or source info has to be specified.")
                if format not in ('jdbc', 'hudi', 'csv'):
                    raise ValueError(f"load_dependencies_into_spark(): Invalid format: '{format}'")
                if format == 'jdbc' and source != 'greenplum':
                    raise ValueError(f"'jdbc' dependency format is only supported for source type 'greenplum'.")
                if source == 'greenplum' and format != 'jdbc':
                    raise ValueError(f"'{format}' format is not supported for '{source}' source type.")
                if not alias:
                    raise ValueError(f"load_dependencies_into_spark(): Invalid alias: '{alias}'.")

                if source == 'datalake':
                    if path:
                        datalake_path = os.path.join('s3a://', path)
                    else:
                        datalake_path = os.path.join(
                            f"s3a://", source.lower(), source_system_name.lower(),
                            source_system_tag.lower(), schema.lower(), source_table_name.lower()
                        )
                    if format == 'csv':
                        source_df = spark_connector.spark.read.option('header', 'true') \
                                .format(format).load(datalake_path)
                    else:
                        source_df = spark_connector.spark.read.format(format).load(datalake_path)
                elif source == 'greenplum':
                    # greenplum_conn = yaml.safe_load(Variable.get('MAIN_GREENPLUM_CONN'))
                    # host = greenplum_conn['host']
                    # port = greenplum_conn['port']
                    # dbname = greenplum_conn['dbname']

                    source_df = spark_connector.read_greenplum_jdbc_table(schema=schema, table=source_table_name)
                    # source_df = spark_connector.spark.read.format(format) \
                    #     .option("url", f"jdbc:postgresql://{host}:{port}/{dbname}") \
                    #     .option("dbtable", f"{schema}.{source_table_name}") \
                    #     .option("user", "spark") \
                    #     .option("password", "") \
                    #     .option("driver", "org.postgresql.Driver") \
                    #     .load()
                else:
                    raise ValueError(f"Invalid source type: '{source}'")

                source_df.createOrReplaceTempView(alias)


    @classmethod
    def fill_sql_parameters(cls, sql_script, parameters):
        """
        Fills given sql script with parameters specified.
        :param sql_script: sql script text
        :param parameters: parameters to fill into sql script. Has the following format:
            [ {'name': 'param name', 'type': 'param type', 'value': 'only for constant param type'}, ... ]
            Where type: report_date, constant
        :return: str
        """
        for param in parameters:
            name = param['name']
            _type = param['type']

            if _type == 'report_date':
                report_date = Variable.get('REPORT_DATE')
                sql_script = sql_script.format(**{name: report_date})
            elif _type == 'constant':
                value = param['value']
                sql_script = sql_script.format(**{name: value})

        return sql_script


class AirflowETL:
    def __init__(self, dag: DAG):
        self.dag: DAG = dag

    @classmethod
    def _extract_db_full(cls, task_id, source_system_name, source_system_tag, scheme, table_name, write_mode):
        """
        :param source_system_name: Source system (e.g.: flexcube)
        :param source_system_tag: Source system tag (e.g.: main, test, prod)
        :param scheme: Scheme name in source database
        :param table_name: Table
        :param write_mode: 'overwrite' - truncates target table and writes the data.
                           'append' - appends data to the target table without deleting data.
        :return:
        """
        if write_mode not in ('overwrite', 'append'):
            raise ValueError(f'Invalid spark write mode: {write_mode}')

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
            url = yaml.safe_load(Variable.get(f"{var_pref}_CONN"))['url']
            driver = Variable.get(f"{var_pref}_DRIVER")
            datalake_target_path = os.path.join(
                "s3a://datalake",
                source_system_name.lower(),
                source_system_tag.lower(),
                scheme.lower(),
                f"{table_name.lower()}"
            )

            jdbc_df = spark_conn.read_jdbc(url=url, driver=driver, query=query)
            hudi_options = {
                'hoodie.table.name': table_name.lower()
            }
            jdbc_df.write \
                .format('hudi') \
                .options(**hudi_options) \
                .mode(write_mode) \
                .save(datalake_target_path)

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

    def extract_db(self, source_system_name, source_system_tag, scheme, table_name,
                   read_mode, write_mode=None) -> BaseOperator:
        """
        :param source_system_name: Source system (e.g.: flexcube)
        :param source_system_tag: Source system tag (e.g.: main, test, prod)
        :param scheme: Scheme name in source database
        :param table_name: Table
        :param read_mode: full/delta/manual
        :param write_mode: overwrite/append
        :return:
        """
        if not write_mode:
            if read_mode == 'full':
                write_mode = 'overwrite'
            elif read_mode == 'delta':
                write_mode = 'append'
            else:
                write_mode = 'append'

        source_system_name = source_system_name.lower()
        source_system_tag = source_system_tag.lower()
        scheme = scheme.lower()
        table_name = table_name.lower()
        read_mode = read_mode.lower()
        write_mode = write_mode.lower()

        if read_mode == 'full':
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
                                      "write_mode": write_mode,
                                  },
                                  dag=self.dag)
        elif read_mode == 'delta':
            task_id = f"extract_{source_system_name.lower()}_{source_system_tag.lower()}_{table_name.lower()}_delta"
            return PythonOperator(python_callable=AirflowETL._extract_db_delta,
                                  task_id=task_id,
                                  op_kwargs={
                                      "task_id": task_id,
                                      "source_system_name": source_system_name,
                                      "source_system_tag": source_system_tag,
                                      "scheme": scheme,
                                      "table_name": table_name,
                                      "write_mode": write_mode,
                                  },
                                  dag=self.dag)

    @classmethod
    def _run_select_step(cls, task_id, transform_step, table_folder, spark):
        """
        Runs a specific sql step. If the step is final - returns a dataframe
        :param task_id: Task id for datalake dump path
        :param transform_step: transform step details
        :param spark: spark instance
        :return:
        """
        # check step type
        engine = transform_step.get('engine', 'spark')

        assert engine in ('spark', 'greenplum')
        if 'sql' not in transform_step:
            raise ValueError(f"'sql' should be specified for 'select' step type")

        # fetch sql file
        with open(os.path.join(table_folder, transform_step['sql']), 'rt') as fp:
            sql_script = fp.read()
        # if parameters are specified
        if 'parameters' in transform_step:
            params = transform_step['parameters']
            # format sql script with given parameters
            sql_script = ETLUtils.fill_sql_parameters(sql_script=sql_script, parameters=params)

        # if step needs to be cached
        if 'cache' in transform_step:
            if 'alias' not in transform_step['cache']:
                raise ValueError(f"No alias provided for 'cached' select step")
            alias = transform_step.get('cache').get('alias')
            if not alias or len(alias) <= 1:
                raise ValueError(f"Too short alias: '{alias}'")

            cache_dir = transform_step.get('cache').get(
                'cache_dir', os.path.join(ETLUtils.Datalake.get_dump_dir(), f"{task_id}", alias)
            )

            return ETLUtils.Spark.execute_sql_script(
                spark=spark,
                sql_script=sql_script,
                cache_dir=cache_dir,
                alias=alias,
                engine=engine,
            )
        # if it doesn't need to be cached
        else:
            df = ETLUtils.Spark.execute_sql_script(
                spark=spark,
                sql_script=sql_script,
                engine=engine,
            )
            return df

    @classmethod
    def _run_sql_script_step(cls, transform_step, table_folder):
        """
        Runs a specific sql step. If the step is final - returns a dataframe
        :param task_id: Task id for datalake dump path
        :param transform_step: transform step details
        :param spark: spark instance
        :return:
        """
        # check step type
        engine = transform_step.get('engine', 'greenplum')

        if engine != 'greenplum':
            raise ValueError(f"Currently only 'greenplum' sql script engine is supported")

        assert engine in ('spark', 'greenplum')

        if 'sql' not in transform_step:
            raise ValueError(f"'sql' should be specified for 'select' step type")

        # fetch sql file
        with open(os.path.join(table_folder, transform_step['sql']), 'rt') as fp:
            sql_script = fp.read()
        # if parameters are specified
        if 'parameters' in transform_step:
            params = transform_step['parameters']
            # format sql script with given parameters
            sql_script = ETLUtils.fill_sql_parameters(sql_script=sql_script, parameters=params)

        ETLUtils.Postgres.execute_script(
            conn_info=yaml.safe_load(Variable.get('MAIN_GREENPLUM_CONN')),
            sql_script=sql_script,
        )
        return None

    @classmethod
    def _run_python_file_step(cls, transform_step, table_folder):
        """
        Runs a specific sql step. If the step is final - returns a dataframe
        :param transform_step: transform step details
        :return:
        """
        if 'python_file' not in transform_step:
            raise ValueError(f"'python_file' should be specified for 'python' step type")

        python_file = transform_step.get('python_file')

        with open(python_file, "rb") as source_file:
            code = compile(source_file.read(), python_file, "exec")
        exec(code, globals(), locals())

    @classmethod
    def _run_transform_steps(cls, task_id, transform_steps, table_folder, spark):
        """
        Iterates through sql scripts
        :param task_id:
        :param transform_steps:
        :param spark:
        :return:
        """
        df = None
        for transform_step in transform_steps:
            # if step is and sql-file
            step_type = transform_step.get('type')

            if not step_type: raise ValueError(f"step 'type' has to be specified in yaml file")

            if step_type == 'select':
                df = AirflowETL._run_select_step(task_id=task_id, transform_step=transform_step,
                                                 table_folder=table_folder, spark=spark)
            elif step_type == 'sql script':
                AirflowETL._run_sql_script_step(transform_step=transform_step, table_folder=table_folder)
            elif 'python' in transform_step:
                AirflowETL._run_python_file_step(transform_step=transform_step, table_folder=table_folder)

        return df

    @classmethod
    def _transform_full(cls, task_id, table_folder, read_mode, write_mode, merge_mode):
        """
        This function
        :param task_id: task id to create spark application
        :param target_schema: target schema name in greenplum
        :param target_table_name: target table name in greenplum
        :param read_mode: full/delta/manual
        :param write_mode: append/overwrite
        :param merge_mode: full/delta
        :return:
        """
        # load yaml file
        yaml_file = os.path.join(table_folder, 'config.yaml')
        if not os.path.isfile(yaml_file):
            raise RuntimeError(f"Couldn't find yaml file: '{yaml_file}'")
        with open(yaml_file, 'rt') as fp:
            conf = yaml.safe_load(fp.read())

        for table_conf in conf:
            target_schema = table_conf.get('target', {}).get('target_schema')
            target_table_name = table_conf.get('target', {}).get('target_table_name')
            greenplum_conn = yaml.safe_load(Variable.get('MAIN_GREENPLUM_CONN'))

            # read migration script
            # if it exists
            migration_file = os.path.join(table_folder, table_conf['migration'])
            if os.path.isfile(migration_file):
                # read
                with open(migration_file, 'rt') as fp:
                    migration_sql = fp.read()
                # and run
                ETLUtils.Postgres.execute_script(greenplum_conn, migration_sql)

            #
            with SparkConnector(app_name=task_id) as spark_connector:
                # load dependencies
                dependencies = table_conf.get('dependencies')
                if dependencies:
                    ETLUtils.Spark.load_dependencies(dependencies=table_conf['dependencies'],
                                                     spark_connector=spark_connector)

                # run transform steps
                transform_steps = table_conf.get('transform', {}).get(read_mode)
                if transform_steps:
                    # iterate through SQL steps
                    final_df = AirflowETL._run_transform_steps(task_id=task_id,
                                                               transform_steps=transform_steps,
                                                               table_folder=table_folder,
                                                               spark=spark_connector.spark)

                    if final_df and target_schema and target_table_name:
                        # write final df to greenplum
                        ETLUtils.Postgres.write_df_to_db(spark_df=final_df,
                                                         postgres_conn_info=greenplum_conn,
                                                         target_schema=target_schema,
                                                         target_table_name=f"{target_table_name}__journal",
                                                         write_mode=write_mode)
            if target_schema and target_table_name:
                # merge journal table into master table
                ETLUtils.Postgres.merge_target_table(
                    conn_info=greenplum_conn,
                    table_schema=target_schema,
                    table_name=target_table_name,
                    merge_mode=merge_mode,
                )

    def transform_db(self, table_folder, read_mode='full', write_mode='overwrite', merge_mode='full') -> BaseOperator:
        """
        :param table_folder: Target table folder in repository.
        :param read_mode: Data read mode (full/delta/manual).
            If 'full' mode specified - reads full source data tables.
            If 'delta' mode specified - reads only incremental data.
            If 'manual' mode specified - reads data for only specified dates.
        :param write_mode: Transform mode - whether to transform and overwrite full table
                     or to load only incremental data ('overwrite', 'append').
                     If 'overwrite' mode specified - truncates *__journal table,
                     if 'append' mode specified - appends new data to *__journal table.
        :param merge_mode: Merge mode to use when merging journal table into master table.
            If 'full' mode specified - truncates master table and merges full *__journal table into master table.
            If 'delta' mode specified - only merges those entries which where appended to *__journal table.
        :return:
        """
        if read_mode not in ('full', 'delta', 'manual'):
            raise ValueError(f"transform_db(): Invalid read_mode: '{read_mode}'")
        if write_mode not in ('overwrite', 'append'):
            raise ValueError(f"transform_db(): Invalid write_mode: '{write_mode}'")
        if merge_mode not in ('full', 'delta'):
            raise ValueError(f"transform_db(): Invalid merge_mode: '{merge_mode}'")

        if read_mode == 'full':
            task_id = f"task_transform_{table_folder.replace('/', '_')}_{read_mode}"
            return PythonOperator(python_callable=AirflowETL._transform_full,
                                  task_id=task_id,
                                  op_kwargs={
                                      "task_id": task_id,
                                      "table_folder": table_folder,
                                      "read_mode": read_mode,
                                      "write_mode": write_mode,
                                      "merge_mode": merge_mode,
                                  },
                                  dag=self.dag)
        elif read_mode == 'delta':
            raise NotImplementedError('Delta transform mode is not implemented yet')
        elif read_mode == 'manual':
            raise NotImplementedError('Manual transform mode is not implemented yet')
