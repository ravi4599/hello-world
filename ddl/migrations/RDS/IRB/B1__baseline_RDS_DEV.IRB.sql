create TABLE IF NOT EXISTS DAG_RUN_LOG (
	DAG_NAME VARCHAR(16777216) NOT NULL,
	DAG_ID VARCHAR(16777216) NOT NULL,
	START_DATETIME TIMESTAMP_NTZ(9),
	END_DATETIME TIMESTAMP_NTZ(9),
	EXECUTION_PARAMS VARCHAR(16777216),
	STATUS VARCHAR(16777216),
	FAILURE_MESSAGE VARCHAR(16777216)
);
create TABLE IF NOT EXISTS DATAVENGERS_CONTROL_TABLE (
	DAG_NAME VARCHAR(16777216),
	TASK_ID VARCHAR(16777216),
	PARAMS VARIANT
);
create TABLE IF NOT EXISTS FLYWAY_SCHEMA_HISTORY (
	INSTALLED_RANK NUMBER(38,0) NOT NULL,
	VERSION VARCHAR(50),
	DESCRIPTION VARCHAR(200),
	TYPE VARCHAR(20) NOT NULL,
	SCRIPT VARCHAR(1000) NOT NULL,
	CHECKSUM NUMBER(38,0),
	INSTALLED_BY VARCHAR(100) NOT NULL,
	INSTALLED_ON TIMESTAMP_LTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP(),
	EXECUTION_TIME NUMBER(38,0) NOT NULL,
	SUCCESS BOOLEAN NOT NULL,
	primary key (INSTALLED_RANK)
);
create TABLE IF NOT EXISTS MO_NONBRAND_INTERNAL (
	WEEK VARCHAR(16777216),
	ADVERTISER VARCHAR(16777216),
	BRAND VARCHAR(16777216),
	PRODUCT VARCHAR(16777216),
	MEDIA VARCHAR(16777216),
	MARKET VARCHAR(16777216),
	DOLS VARCHAR(16777216),
	LOADDATETIME VARCHAR(16777216),
	LOADID VARCHAR(16777216),
	LOADTYPE VARCHAR(16777216),
	FILENAME VARCHAR(16777216)
);
create TABLE IF NOT EXISTS STAGE_MONITORING_LOG (
	DATABASE VARCHAR(16777216),
	SCHEMA VARCHAR(16777216),
	STAGE_NAME VARCHAR(16777216),
	FILE_COUNT NUMBER(38,0),
	LOG_TIMESTAMP TIMESTAMP_NTZ(9)
);
create TABLE IF NOT EXISTS STAGE_MONITORING_TEMP (
	DATABASE VARCHAR(16777216),
	SCHEMA VARCHAR(16777216),
	STAGE_NAME VARCHAR(16777216),
	FILE_COUNT NUMBER(38,0),
	STAGE_SIZE NUMBER(38,0),
	LOG_TIMESTAMP TIMESTAMP_NTZ(9)
);
create TABLE IF NOT EXISTS TASK_RUN_LOG (
	DAG_NAME VARCHAR(16777216) NOT NULL,
	DAG_ID VARCHAR(16777216) NOT NULL,
	TASK_ID VARCHAR(16777216) NOT NULL,
	START_DATETIME TIMESTAMP_NTZ(9),
	END_DATETIME TIMESTAMP_NTZ(9),
	STATUS VARCHAR(16777216),
	FAILURE_MESSAGE VARCHAR(16777216)
);
CREATE FILE FORMAT IF NOT EXISTS MO_IRB_CSV_FORMAT
	SKIP_HEADER = 1
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	ENCODING = 'iso-8859-1'
;
CREATE PROCEDURE IF NOT EXISTS LOCATION_HIERARCHY_PLR_STP_PROC_CDM_TO_SNOWFLAKE_TABLES_HISTORICAL("SRC_DB_PARAM" VARCHAR(16777216), "SRC_SCHEMA_PARAM" VARCHAR(16777216), "DST_DB_PARAM" VARCHAR(16777216), "DST_SCHEMA_PARAM" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '

    var select_max_checkpoint_log_stmt = snowflake.createStatement(
        {
            sqlText: `SELECT max(filename) FROM `+ SRC_DB_PARAM +`.`+SRC_SCHEMA_PARAM+`.location_hierarchy_plr_delta_log_parquet`
        });
    var select_min_json_log_stmt = snowflake.createStatement(
        {
            sqlText: `SELECT min(filename) FROM `+ SRC_DB_PARAM +`.`+SRC_SCHEMA_PARAM+`.location_hierarchy_plr_delta_log`
        });
    var insert_checkpoint_data = `INSERT INTO `+ DST_DB_PARAM +`.`+DST_SCHEMA_PARAM+`.location_hierarchy_plr(
LOCATIONID, 
L1ID, 
L1NAME, 
L1MANAGERID, 
L1MANAGER, 
L2ID, 
L2NAME, 
L2MANAGERID, 
L2MANAGER, 
L3ID, 
L3NAME, 
L3MANAGERID, 
L3MANAGER, 
L4ID, 
L4NAME, 
L4MANAGERID, 
L4MANAGER, 
L5ID, 
L5NAME, 
L5MANAGERID, 
L5MANAGER, 
BRANDID, 
SOURCE, 
CDMLOADDATE, 
LOADTYPE, 
FILENAME

                              )
                          SELECT
                                delta_table.LOCATIONID, 
delta_table.L1ID, 
delta_table.L1NAME, 
delta_table.L1MANAGERID, 
delta_table.L1MANAGER, 
delta_table.L2ID, 
delta_table.L2NAME, 
delta_table.L2MANAGERID, 
delta_table.L2MANAGER, 
delta_table.L3ID, 
delta_table.L3NAME, 
delta_table.L3MANAGERID, 
delta_table.L3MANAGER, 
delta_table.L4ID, 
delta_table.L4NAME, 
delta_table.L4MANAGERID, 
delta_table.L4MANAGER, 
delta_table.L5ID, 
delta_table.L5NAME, 
delta_table.L5MANAGERID, 
delta_table.L5MANAGER, 
delta_table.BRANDID, 
delta_table.SOURCE, 
delta_table.CDMLOADDATE, 
delta_table.LOADTYPE, 
delta_table.FILENAME
                          FROM `+ SRC_DB_PARAM +`.`+SRC_SCHEMA_PARAM+`.location_hierarchy_plr_delta delta_table
                          WHERE EXISTS(
                                       select
                                          metadata.add_file
                                       from
                                          (
                                             select
                                                metadata_parquet.add_file
                                             from
                                                (
                                                   select
                                                      add_file
                                                   from `+ SRC_DB_PARAM +`.`+SRC_SCHEMA_PARAM+`.location_hierarchy_plr_delta_log_parquet
                                                   where
                                                      filename = :1
                                                      and add_file is not null
                                                )
                                                metadata_parquet
                                             where
                                                not exists
                                                (
                                                   select
                                                      target_table.filename
                                                   from `+ DST_DB_PARAM +`.`+DST_SCHEMA_PARAM+`.location_hierarchy_plr target_table
                                                   where
                                                      target_table.filename = metadata_parquet.add_file
                                                )
                                          )
                                          metadata
                                       where
                                          metadata.add_file = delta_table.filename )`;

    var delete_checkpoint_data = `DELETE FROM `+ DST_DB_PARAM +`.`+DST_SCHEMA_PARAM+`.location_hierarchy_plr target
                                    WHERE filename in (
                                                SELECT
                                                    remove_file
                                                FROM `+ SRC_DB_PARAM +`.`+SRC_SCHEMA_PARAM+`.location_hierarchy_plr_delta_log_parquet delta_log_parquet
                                                WHERE
                                                    delta_log_parquet.remove_file IS NOT NULL and delta_log_parquet.filename = :1
                                    )`;

    var insert_json_data = `INSERT INTO `+ DST_DB_PARAM +`.`+DST_SCHEMA_PARAM+`.location_hierarchy_plr(
LOCATIONID, 
L1ID, 
L1NAME, 
L1MANAGERID, 
L1MANAGER, 
L2ID, 
L2NAME, 
L2MANAGERID, 
L2MANAGER, 
L3ID, 
L3NAME, 
L3MANAGERID, 
L3MANAGER, 
L4ID, 
L4NAME, 
L4MANAGERID, 
L4MANAGER, 
L5ID, 
L5NAME, 
L5MANAGERID, 
L5MANAGER, 
BRANDID, 
SOURCE, 
CDMLOADDATE, 
LOADTYPE, 
FILENAME
                              )
                          SELECT
                                delta_table.LOCATIONID, 
delta_table.L1ID, 
delta_table.L1NAME, 
delta_table.L1MANAGERID, 
delta_table.L1MANAGER, 
delta_table.L2ID, 
delta_table.L2NAME, 
delta_table.L2MANAGERID, 
delta_table.L2MANAGER, 
delta_table.L3ID, 
delta_table.L3NAME, 
delta_table.L3MANAGERID, 
delta_table.L3MANAGER, 
delta_table.L4ID, 
delta_table.L4NAME, 
delta_table.L4MANAGERID, 
delta_table.L4MANAGER, 
delta_table.L5ID, 
delta_table.L5NAME, 
delta_table.L5MANAGERID, 
delta_table.L5MANAGER, 
delta_table.BRANDID, 
delta_table.SOURCE, 
delta_table.CDMLOADDATE, 
delta_table.LOADTYPE, 
delta_table.FILENAME
                          FROM `+ SRC_DB_PARAM +`.`+SRC_SCHEMA_PARAM+`.location_hierarchy_plr_delta delta_table
                          WHERE EXISTS(
                                      select
                                          metadata.add_file
                                       from
                                          (
                                             select
                                                metadata_json.add_file
                                             from
                                                (
                                                   select
                                                      add_file
                                                   from `+ SRC_DB_PARAM +`.`+SRC_SCHEMA_PARAM+`.location_hierarchy_plr_delta_log
                                                   where
                                                      filename > :1
                                                      and add_file is not null
                                                )
                                                metadata_json
                                             where
                                                not exists
                                                (
                                                   select
                                                      target_table.filename
                                                   from `+ DST_DB_PARAM +`.`+DST_SCHEMA_PARAM+`.location_hierarchy_plr target_table
                                                   where
                                                      target_table.filename = metadata_json.add_file
                                                )
                                          )
                                          metadata
                                       where
                                          metadata.add_file = delta_table.filename )`;

    var delete_json_data = `DELETE FROM `+ DST_DB_PARAM +`.`+DST_SCHEMA_PARAM+`.location_hierarchy_plr target
                              WHERE filename in (
                                        SELECT
                                            remove_file
                                        FROM `+ SRC_DB_PARAM +`.`+SRC_SCHEMA_PARAM+`.location_hierarchy_plr_delta_log
                                        WHERE
                                            remove_file IS NOT NULL and filename > :1
                            )`;
    var max_checkpoint_log = select_max_checkpoint_log_stmt.execute();
    max_checkpoint_log.next();
    var max_checkpoint_log_value = max_checkpoint_log.getColumnValue(1);

    var min_json_log_value;
    var max_checkpoint_log_to_json_value;

    if (max_checkpoint_log_value === null) {
        var min_json_log = select_min_json_log_stmt.execute();
        min_json_log.next();
        min_json_log_value = min_json_log.getColumnValue(1);
    } else {
        max_checkpoint_log_to_json_value = max_checkpoint_log_value.replace(''.checkpoint.parquet'', ''.json'');
    }

    var json_search = max_checkpoint_log_value !== null ? max_checkpoint_log_to_json_value: min_json_log_value;
    try {
        snowflake.execute (
        {sqlText: "begin transaction"}
        );

        var insert_checkpoint_stmt = snowflake.createStatement(
                {
                sqlText: insert_checkpoint_data,
                binds: [max_checkpoint_log_value]
                }
        );
        insert_checkpoint_stmt.execute();
       var delete_checkpoint_data_stmt = snowflake.createStatement(
                {
                sqlText: delete_checkpoint_data,
                binds: [max_checkpoint_log_value]
                }
        );
        delete_checkpoint_data_stmt.execute();

        var insert_json_data_stmt = snowflake.createStatement(
                {
                sqlText: insert_json_data,
                binds: [json_search]
                }
        );
        insert_json_data_stmt.execute();
        var delete_json_data_stmt = snowflake.createStatement(
                {
                sqlText: delete_json_data,
                binds: [json_search]
                }
        );
        delete_json_data_stmt.execute();
        snowflake.execute (
            {sqlText: "commit"}
        );
        return "Succeeded.";
        }
    catch (err)  {
        snowflake.execute (
            {sqlText: "rollback"}
        );
        throw err;
        }
    ';