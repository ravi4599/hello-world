create TABLE IF NOT EXISTS PIPELINE_CONFIG (
	PIPELINE_ID VARCHAR(16777216),
	DESCRIPTION VARIANT,
	PIPELINE_TYPE VARCHAR(16777216),
	LOAD_TYPE VARCHAR(16777216),
	DATA_LOAD_FREQUENCY VARCHAR(16777216),
	DATA_OWNER VARCHAR(16777216),
	CONFIG_TYPE VARCHAR(16777216),
	CONFIG_TEMPLATE VARCHAR(16777216),
	SCHEDULE_TYPE VARCHAR(16777216),
	SCHEDULE_VALUE VARCHAR(16777216),
	START_DATE TIMESTAMP_NTZ(9),
	CREATED_TIMESTAMP TIMESTAMP_NTZ(9),
	CREATED_BY VARCHAR(16777216),
	LAST_MODIFIED_TIMESTAMP TIMESTAMP_NTZ(9),
	LAST_MODIFIED_BY VARCHAR(16777216),
	VERSION VARCHAR(16777216)
);
create TABLE IF NOT EXISTS TASK_CONFIG (
	PIPELINE_ID VARCHAR(16777216),
	TASK_ID VARCHAR(16777216),
	TASK_TYPE VARCHAR(16777216),
	SOURCE_CONN_ID VARCHAR(16777216),
	SOURCE_PATH VARCHAR(16777216),
	SOURCE_TYPE VARCHAR(16777216),
	SOURCE_NAME VARCHAR(16777216),
	SOURCE_FILE_DELIMITER VARCHAR(16777216),
	SOURCE_DATA_SCHEMA VARCHAR(16777216),
	DEST_CONN_ID VARCHAR(16777216),
	DEST_PATH VARCHAR(16777216),
	DEST_TYPE VARCHAR(16777216),
	DEST_NAME VARCHAR(16777216),
	DEST_FILE_DELIMITER VARCHAR(16777216),
	DEST_DATA_SCHEMA VARCHAR(16777216),
	BUSINESS_KEY_COLUMNS VARCHAR(16777216),
	ATTRIBUTE_COLUMNS VARCHAR(16777216),
	EXTRA_PARAMS VARIANT,
	EMAIL VARCHAR(16777216),
	RETRIES NUMBER(38,0),
	DEPENDS_ON_PAST BOOLEAN,
	TASK_GROUP VARCHAR(16777216),
	UPSTREAM_TASKS VARCHAR(16777216),
	CREATED_TIMESTAMP TIMESTAMP_NTZ(9),
	CREATED_BY VARCHAR(16777216),
	LAST_MODIFIED_TIMESTAMP TIMESTAMP_NTZ(9),
	LAST_MODIFIED_BY VARCHAR(16777216)
);
create or replace view PIPELINE_CONFIG_BV(
	PIPELINE_ID,
	CONFIG_TYPE,
	CONFIG_VALUE,
	CONFIG_VALUE_CHECKSUM
) as (
  SELECT
    PC.PIPELINE_ID AS PIPELINE_ID,
    PC.CONFIG_TYPE AS CONFIG_TYPE,
    TO_VARIANT(
      OBJECT_INSERT(
        GET(
          TO_VARIANT(
            SELECT
              ARRAY_AGG(
                OBJECT_CONSTRUCT_KEEP_NULL(
                  'pipeline',
                  OBJECT_CONSTRUCT_KEEP_NULL(
                    'id', IPC.PIPELINE_ID,
                    'description', IPC.DESCRIPTION:description,
                    'tags', IPC.DESCRIPTION:tags,
                    'type', IPC.PIPELINE_TYPE,
                    'load_type', IPC.LOAD_TYPE,
                    'data_load_frequency', IPC.DATA_LOAD_FREQUENCY,
                    'data_owner',
                    IFF(ARRAY_SIZE(SPLIT(IPC.DATA_OWNER, ',')) = 1, SPLIT(IPC.DATA_OWNER, ',')[0], SPLIT(IPC.DATA_OWNER, ','))
                    ,
                    'config_type', IPC.CONFIG_TYPE,
                    'schedule_type', IPC.SCHEDULE_TYPE,
                    'schedule_value', IPC.SCHEDULE_VALUE,
                    'start_date', TO_VARCHAR(IPC.START_DATE, 'YYYY-MM-DD HH:MI:SS'),
                    'created_timestamp', TO_VARCHAR(IPC.CREATED_TIMESTAMP, 'YYYY-MM-DD HH:MI:SS'),
                    'created_by', IPC.CREATED_BY,
                    'last_modified_timestamp', TO_VARCHAR(IPC.LAST_MODIFIED_TIMESTAMP, 'YYYY-MM-DD HH:MI:SS'),
                    'last_modified_by', IPC.LAST_MODIFIED_BY,
                    'version', IPC.VERSION
                  )
                )
              )
            FROM
              ITPRCS_DEV.PRCSCONFIG.PIPELINE_CONFIG IPC
            WHERE
              IPC.PIPELINE_ID = PC.PIPELINE_ID
          ),
          0
        ),
        'tasks',
        COALESCE(
          TO_VARIANT(
            SELECT
              ARRAY_AGG(
                OBJECT_CONSTRUCT_KEEP_NULL(
                  ITC.TASK_ID,
                  OBJECT_CONSTRUCT_KEEP_NULL(
                    'task_config',
                    OBJECT_CONSTRUCT_KEEP_NULL(
                      'source_conn_id', ITC.SOURCE_CONN_ID,
                      'source_path', ITC.SOURCE_PATH,
                      'source_type', ITC.SOURCE_TYPE,
                      'source_name', ITC.SOURCE_NAME,
                      'source_file_delimiter', ITC.SOURCE_FILE_DELIMITER,
                      'source_data_schema', ITC.SOURCE_DATA_SCHEMA,
                      'dest_conn_id', ITC.DEST_CONN_ID,
                      'dest_path', ITC.DEST_PATH,
                      'dest_type', ITC.DEST_TYPE,
                      'dest_name', ITC.DEST_NAME,
                      'dest_file_delimiter', ITC.DEST_FILE_DELIMITER,
                      'dest_data_schema', ITC.DEST_DATA_SCHEMA,
                      'business_key_columns',
                      IFF(ARRAY_SIZE(SPLIT(ITC.BUSINESS_KEY_COLUMNS, ',')) = 1, SPLIT(ITC.BUSINESS_KEY_COLUMNS, ',')[0], SPLIT(ITC.BUSINESS_KEY_COLUMNS, ','))
                      ,
                      'attribute_columns',
                      IFF(ARRAY_SIZE(SPLIT(ITC.ATTRIBUTE_COLUMNS, ',')) = 1, SPLIT(ITC.ATTRIBUTE_COLUMNS, ',')[0], SPLIT(ITC.ATTRIBUTE_COLUMNS, ','))
                      ,
                      'extra_params', ITC.EXTRA_PARAMS,
                      'task_group', ITC.TASK_GROUP,
                      'email',
                      IFF(ARRAY_SIZE(SPLIT(ITC.EMAIL, ',')) = 1, SPLIT(ITC.EMAIL, ',')[0], SPLIT(ITC.EMAIL, ','))
                      ,
                      'retries', ITC.RETRIES,
                      'depends_on_past', ITC.DEPENDS_ON_PAST
                    ),
                    'task_type', ITC.TASK_TYPE,
                    'upstream_tasks', COALESCE(SPLIT(ITC.UPSTREAM_TASKS, ','), [])
                  )
                )
              )
            FROM
              ITPRCS_DEV.PRCSCONFIG.TASK_CONFIG ITC
            WHERE
              ITC.PIPELINE_ID = PC.PIPELINE_ID
          ),
          []
        )
      )
    ) AS CONFIG_VALUE,
    SHA2(CONFIG_VALUE, 256) AS CONFIG_VALUE_CHECKSUM
  FROM
    ITPRCS_DEV.PRCSCONFIG.PIPELINE_CONFIG PC
);