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

**Connection details** must be specified in airflow variables with the following naming:
`<source_system_tag.upper()>_<source_system_name.upper()>_CONN` <br>
*Value of this variable should have the following yaml structure:*
```
url: jdbc...
host: ip address
port: port
sid: sid for oracle
dbname: dbname for postgres
username: database username
password: user password
```

`read_mode` - *specifies whether to load full table or to load only incremental data.* <br>
Possible values:
 - `full` - _reads full table._
 - `delta` - _reads only incremental data._

`write_mode` - *specifies whether to truncate target table before appending data.* <br>
Possible values:
 - `overwrite` - _truncates target table and writes the data._
 - `append` - _appends data to the target table without deleting data._

