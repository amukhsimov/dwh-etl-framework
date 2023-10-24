# ETL Framework

## `AirflowETL` class

### - `extract_db()`
*Parameters:*
 - `source_system_name`
 - `source_system_tag`
 - `scheme`
 - `table_name`
 - `read_mode`
 - `write_mode`

*This function returns an airflow task which extracts data from source system
and writes it to datalake (S3) in format of `hudi` in the following path: <br>
`s3a//datalake/<source_system_name>/<source_system_tag>/<scheme>/<table_name>"`*

**Connection details** must be specified in airflow variables.
_See connection details description below._

SQL file for extraction should be stored in the following file: <br>
`<AIRFLOW_SQL_FOLDER>/extract/<source_system_name>/<source_system_tag>/<schema>/<table_name>-full.sql`

`read_mode` - specifies whether to load full table or to load only incremental data. <br>
Possible values:
 - `full` - reads full table.
 - `delta` - reads only incremental data *(not implemented yet)*.

`write_mode` - specifies whether to truncate target table before appending data. <br>
Possible values:
 - `overwrite` - truncates target table and writes the data.
 - `append` - appends data to the target table without deleting data.

### - `transform_db()`
_Parameters:_
self, table_folder, read_mode, write_mode, merge_mode='full'
*Parameters:*
 - `table_folder` - path to folder where transformation files are contained.
                    `config.yaml`, `sql/python` files should be stored here.
 - `read_mode` - specifies read mode (`full/delta/manual`).
 - `write_mode` - specifies write mode (`append/overwrite`)
 - `merge_mode` - specifies merge mode (`full/delta`)

*This function returns an airflow task which transforms data from given sources
and writes it to the target specified.*

**Connection details** must be specified in airflow variables.
_See connection details description below._

`read_mode` - specifies whether to load full table or to load only incremental data. <br>
Possible values:
 - `full` - reads full table.
 - `delta` - reads only incremental data *(not implemented yet)*.
 - `manual` - reads only data with specified date in airflow variable *(not implemented yet)*.

`write_mode` - specifies whether to truncate journal table before appending data. <br>
Possible values:
 - `overwrite` - truncates target table and writes the data.
 - `append` - appends data to the target table without deleting data.

`merge` - specifies whether to truncate target table before appending data. <br>
Possible values:
 - `full` - truncates target table and writes full journal.
 - `delta` - appends data from journal to target table without deleting data.

**Transformation `config.yaml` file:**
_See details below._

## Connection details
_All connections must be specified in airflow variables._ <br>

Variable name must have the following name:
`<source_system_tag.upper()>_<source_system_name.upper()>_CONN` <br>

Value of this variable should have the following yaml structure:
```
url: jdbc...
host: ip address
port: port
sid: sid for oracle
dbname: dbname for postgres
username: database username
password: user password
```

## Transformation `config.yaml` file
Transformation `config.yaml` file has the following structure:
```
- dependencies:                     # optional, specifies all needed dependencies
    - source: datalake              # source type. datalake/greenplum
    
      # source, source_system_name, source_system_tag, 
      #                                        schema and table_name
      # are used to make path to source table in datalake 
      #                                         (S3 storage).
      # Path of the table to read from S3 storage:
      # s3a://{source}/{source_system_name}/{source_system_tag}/{schema}/{table_name}
      source_system_name: flexcube
      source_system_tag: main  # main/prod/dev/test
      schema: ociuz
      table_name: gltb_rpt_vd_bal_custom
      
      # format:
      # possible values: jdbc/hudi/csv
      # jdbc - loads a table from database (only 'greenplum' source 
      #                                     type supported)
      # hudi - reads hudi file from s3 storage
      # csv - reads csv file from s3 storage
      format: hudi
      
      # path:
      # if specified - source, source_system_name, source_system_tag,  
      # schema and table_name are ignored
      # should be specified without s3a:// prefix
      path: datalake/mapping/reporting/crs055L_Balance/mapping.csv
      
      # alias:
      # creates a temp view in spark catalog for this dependency 
      # using 'alias'
      alias: fc_saldo
  
  # migration sql file. Executed in greenplum (optional).
  migration: migration.sql
  
  # transformation steps. Executed sequentially.
  # If the last step returns some data - the framework will 
  # write this data into target if target is specified 
  #                                     (see 'target' below)
  transform:
    # transform section keys - are read modes.
    # full, delta and manual - are read modes.
    full:
    
    # type:
    #  - select - executes an sql script specified in 'sql' 
    #             subsection. sql-file should be a select query.
    #  - sql script - executes an sql script specified in 'sql'.
    #                 This file doesn't have to return data.
    #  - python - executes a python script specified in
    #             'python_file' section.
    - type: select  # select, sql script, python
    
      # sql script for 'type' in ('select', 'sql script')
      sql: transform1.sql
      
      # engine:
      # specifies on which system to run the script.
      # Possible values: greenplum/spark.
      # Only used for 'type' in ('select', 'sql script')
      engine: greenplum
      
      # parameters used within sql-file
      # Say, we have the following sql file:
      #
      #  SELECT * FROM some_table WHERE date >= {report_date}
      #
      # Specified 'parameters' section - we can dynamically fill
      # this parameter in sql-file.
      #
      # Python script fills parameter using 'format()' function:
      #
      #  if param['type'] == 'report_date':
      #      report_date = Variable.get('REPORT_DATE')
      #      sql_script = sql_script.format(**{name: report_date})
      #  elif param['type'] == 'constant':
      #      value = param['value']
      #      sql_script = sql_script.format(**{name: value})
      #
      parameters:
        - name: report_date
          type: report_date
      
      # cache:
      # if specified - the framework caches transformation 
      # step results into the datalake.
      # if cache_dir is not specified - the framework caches 
      # data into the following directory:
      # s3a://dump/etl/<task_id>/alias
      cache:
        # creates a temp view inside the spark catalog with name
        # <alias>
        alias: table_1
        cache_dir: s3a://temp_file
    - type: python
      python_file: transform3.py
    
    delta:
    - type: ...
      # ...
  target:  # target table in greenplum (only greenplum supported 
           #                                            for now)
    target_schema: dwh
    target_table_name: saldo

```
