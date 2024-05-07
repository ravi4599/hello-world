## Instructions

Create Hive table:

Important: Because of the current implementation of the `datamart-scd` library, for accumulative facts at the same time of creating the target table in Hive, you need to create also a table with same structure, using as name and path the original with the `_err` suffix. For the `hvtb_mart_fact_incident`, the error table will be `hvtb_mart_fact_incident_err` with backing files stored at `s3://vv-dev-emr-cluster/data/mart/hvtb_mart_fact_incident_err`


```
spark.sql(s"""
CREATE EXTERNAL TABLE shipdw.hvtb_mart_fact_incident(
    voyage_id string,
    ship_code string,
    voyage_skey int,
    incident_detail_skey int,
    reported_by_person_skey int,
    assigned_to_person_skey int,
    due_date_utc_skey int,
    due_time_utc_skey int,
    due_date_skey int,
    due_time_skey int,
    cabin_skey int,
    venue_skey int,
    duty_manager_person_skey int,
    impacted_guest_person_skey int,
    incident_created_datetime_utc timestamp,
    incident_created_datetime timestamp,
    incident_updated_datetime_utc timestamp,
    incident_updated_datetime timestamp,
    incident_category_code_skey int,
    incident_category_parent_code_skey int,
    resolution_time int,
    incident_status string,
    load_dt timestamp, 	
    upd_dt timestamp,
    primaryhash string, 
    md5_hash string
) PARTITIONED BY (part_dt date)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
STORED AS
  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://vv-dev-emr-cluster/data/mart/hvtb_mart_fact_incident'
""")
```

Create Redshift table:

```
CREATE EXTERNAL TABLE hive_schema_stg.incident_fact
(
    voyage_id text,
    ship_code text,
    voyage_skey int,
    incident_detail_skey int,
    reported_by_person_skey int,
    assigned_to_person_skey int,
    due_date_utc_skey int,
    due_time_utc_skey int,
    due_date_skey int,
    due_time_skey int,
    cabin_skey int,
    venue_skey int,
    duty_manager_person_skey int,
    impacted_guest_person_skey int,
    incident_created_datetime_utc timestamp,
    incident_created_datetime timestamp,
    incident_updated_datetime_utc timestamp,
    incident_updated_datetime timestamp,
    incident_category_code_skey int,
    incident_category_parent_code_skey int,
    resolution_time int,
    incident_status text,
    load_dt timestamp, 	
    upd_dt timestamp,
    primaryhash text, 
    md5_hash text 
) PARTITIONED BY (part_dt date)
STORED AS PARQUET 
LOCATION 's3://vv-dev-emr-cluster/data/mart/hvtb_mart_fact_incident';
```

Before the first run you need to create the initial record in the metadata table in HBase (to connect use the CLI utility phoenix-sqlline):

```
UPSERT INTO SHIP.HBTB_INGESTION_METADATA (BATCH_INSTANCE_ID,BATCH_ID,PARENTBATCH,SRCCONFIGID,TGTCONFIGID,BATCHSTARTTIME,BATCHENDTIME,BATCH_EXECUTION_STARTTIME,BATCH_EXECUTION_ENDTIME,STATUS,ENVIRONMENT,VOYAGEID,TYPE) VALUES ('SrcVxpIncident-TgtIncidentFact','SrcVxpIncident-TgtIncidentFact','SrcVxpDxpIncident-TgtVxpDxpIncident','SrcVxpIncident','TgtIncidentFact','1900-01-01 00:00:00.0','1900-01-01 00:00:00.0','1900-01-01 00:00:00.0','1900-01-01 00:00:00.0','Successful','DEV','SC2002269NCR','Fact');
```

Spark program:

```
spark-submit --conf spark.yarn.maxAppAttempts=1 --name IncidentFact --class com.virginvoyages.shore.facts.IncidentFactLoad --master yarn --deploy-mode cluster --properties-file /home/ecastillo/SrcVxpIncident_TgtIncidentFact --num-executors 4 --driver-memory 4g --executor-memory 4g --executor-cores 4 --jars /data/apps/talend/shared/scripts/lib/RedshiftJDBC42-no-awssdk-1.2.37.1061.jar,/data/apps/talend/shared/scripts/nbx/SCD/shore-datamart-scd-0.0.1-SNAPSHOT-jar-with-dependencies.jar,/data/apps/talend/shared/scripts/nbx/Metadata/virginvoyages_shore_metadata-0.0.1-SNAPSHOT-jar-with-dependencies.jar /home/ecastillo/vv-nbx-datamart-incident-fact-0.0.1-SNAPSHOT-jar-with-dependencies.jar
``` 

Once the Postgres table is filled by the Spark application, you can use the following statement to generate the reporting view:

```
CREATE OR REPLACE VIEW shipdw.incident_fact
AS 
SELECT * FROM hive_schema_stg.incident_fact;
```

SQL QUERY

```
spark.sql("""
SELECT DISTINCT
  dim_voyage.voyage_id AS voyage_id,
  NVL(ShipCode, -1) AS ship_code,
  NVL(dim_voyage.voyage_skey,-1) AS voyage_skey,
  NVL(dim_incdet.incident_detail_skey, -1) AS incident_detail_skey,
  COALESCE (
    CASE WHEN incident.reportedbypersontypecode in ('G', 'RG') THEN dim_person_reported.person_skey 
    WHEN incident.reportedbypersontypecode = 'C' THEN dim_person_crew_reported.person_skey 
    ELSE NULL END, -1
  ) as reported_by_person_skey,
  NVL(dim_person_crew_assigned.person_skey, -1) AS assigned_to_person_skey,
  NVL(dim_date_utc.date_id, -1) AS due_date_utc_skey,
  NVL(dim_time_utc.s_key, -1) AS due_time_utc_skey,
  NVL(dim_date.date_id, -1) AS due_date_skey,
  NVL(dim_time.s_key, -1) AS due_time_skey,
  NVL(cabin_skey, -1) AS cabin_skey,
  NVL(venue_skey, -1) AS venue_skey,
  NVL(dim_person_duttymanager.person_skey, -1) AS duty_manager_person_skey,
  NVL(dim_person_impactedguest.person_skey, -1) AS impacted_guest_person_skey,
  addeddate AS incident_created_datetime_utc,
  CAST(unix_timestamp(addeddate) + addeddateoffset * 60 AS TIMESTAMP) AS incident_created_datetime,
  lastmodifieddate AS incident_updated_datetime_utc,
  CAST(unix_timestamp(lastmodifieddate) + lastmodifieddateoffset * 60 AS TIMESTAMP) AS incident_updated_datetime,
  NVL(dim_inc_cat.incident_category_skey, -1) AS incident_category_code_skey,
  NVL(dim_inc_cat_parent.incident_category_skey, -1) AS incident_category_parent_code_skey,
  resolutiontime AS resolution_time,
  status AS incident_status,
  incident.part_date,
  incident.batchtime
FROM (SELECT * FROM (
    SELECT voyagenumber, incidentid, ShipCode, reportedbypersonid, reportedbypersontypecode, assignedtoteammemberid, duedate, duedateoffset, stateroom, globalvenueid, dutymanagerid, impactedguest, addeddate, addeddateoffset, lastmodifieddate, lastmodifieddateoffset, incidentcategorycode, parentincidentcategorycode, resolutiontime, status, batchtime, Part_Date, row_number() over (partition by incidentid order by lastmodifieddate desc) as rn FROM shipdw.hvtb_parse_vxp_dxp_incident
) WHERE rn = 1) incident
LEFT JOIN shipdw.hvtb_mart_dim_voyage dim_voyage ON src_deleted_flag = false AND src_active_flag = true AND incident.voyagenumber = dim_voyage.voyage_number
LEFT JOIN (SELECT * FROM (SELECT incident_id, incident_detail_skey, row_number() over (partition by incident_id order by upd_dt desc) as rn FROM shipdw.hvtb_mart_dim_incident_detail) WHERE rn = 1) dim_incdet ON dim_incdet.incident_id = incident.incidentid

LEFT JOIN (SELECT * FROM (SELECT reservationguest_guid, person_skey, row_number() over (partition by person_id order by upd_dt desc) as rn FROM shipdw.hvtb_mart_dim_person) WHERE rn = 1) dim_person_reported ON incident.reportedbypersonid = dim_person_reported.reservationguest_guid
LEFT JOIN (select teammemberid,teammembernumber from (select teammemberid,teammembernumber,row_number() over(partition by teammemberid order by lastmodifieddate desc) as rn  from shipdw.hvtb_parse_vxp_dxpcore_teammember) team_member where team_member.rn=1) teammember_reported ON teammember_reported.teammemberid = incident.reportedbypersonid
LEFT JOIN (SELECT * FROM (SELECT person_external_id, person_skey, row_number() over (partition by person_id order by upd_dt desc) as rn FROM shipdw.hvtb_mart_dim_person) WHERE rn = 1) dim_person_crew_reported ON dim_person_crew_reported.person_external_id=teammember_reported.teammembernumber

LEFT JOIN (select teammemberid,teammembernumber from (select teammemberid,teammembernumber,row_number() over(partition by teammemberid order by lastmodifieddate desc) as rn  from shipdw.hvtb_parse_vxp_dxpcore_teammember) team_member where team_member.rn=1) teammember_assigned ON teammember_assigned.teammemberid = incident.assignedtoteammemberid
LEFT JOIN (SELECT * FROM (SELECT person_external_id, person_skey, row_number() over (partition by person_id order by upd_dt desc) as rn FROM shipdw.hvtb_mart_dim_person) WHERE rn = 1) dim_person_crew_assigned ON dim_person_crew_assigned.person_external_id=teammember_assigned.teammembernumber

LEFT JOIN (SELECT * FROM (SELECT teammemberid, teammembernumber, row_number() over (partition by teammemberid order by lastmodifieddate desc) as rn FROM shipdw.hvtb_parse_vxp_dxpcore_teammember) WHERE rn=1) teammember_duttymanager ON dutymanagerid = teammember_duttymanager.teammemberid
LEFT JOIN (SELECT * FROM (SELECT person_external_id, booking_cruise_number, person_skey, row_number() over (partition by person_id order by upd_dt desc) as rn FROM shipdw.hvtb_mart_dim_person) WHERE rn=1) dim_person_duttymanager ON dim_person_duttymanager.person_external_id=teammember_duttymanager.teammembernumber AND dim_person_duttymanager.booking_cruise_number=dim_voyage.voyage_number

LEFT JOIN (SELECT * FROM (SELECT person_external_id, reservationguest_guid, person_skey, row_number() over (partition by person_id order by upd_dt desc) as rn FROM shipdw.hvtb_mart_dim_person) WHERE rn=1) dim_person_impactedguest ON dim_person_impactedguest.reservationguest_guid=impactedguest

LEFT JOIN shipdw.hvtb_mart_dim_date dim_date_utc ON to_date(incident.duedate) = dim_date_utc.`date`
LEFT JOIN shipdw.hvtb_mart_dim_time dim_time_utc ON (dim_time_utc.second_of_day==hour(duedate)*3600+minute(duedate)*60+second(duedate)) 

LEFT JOIN shipdw.hvtb_mart_dim_date dim_date ON to_date(CAST(unix_timestamp(incident.duedate) + incident.duedateoffset * 60 AS TIMESTAMP)) = dim_date.`date`
LEFT JOIN shipdw.hvtb_mart_dim_time dim_time ON (dim_time.second_of_day==hour(CAST(unix_timestamp(incident.duedate) + incident.duedateoffset * 60 AS TIMESTAMP)) * 3600+minute(CAST(unix_timestamp(incident.duedate) + incident.duedateoffset * 60 AS TIMESTAMP)) * 60+second(CAST(unix_timestamp(incident.duedate) + incident.duedateoffset * 60 AS TIMESTAMP)))

LEFT JOIN (SELECT * FROM (SELECT cabin_number, cabin_skey, row_number() over (partition by cabin_number order by upd_dt desc) as rn FROM shipdw.hvtb_mart_dim_cabin) WHERE rn = 1) dim_cabin ON incident.stateroom = dim_cabin.cabin_number
LEFT JOIN (SELECT * FROM (SELECT venue_id, venue_skey, row_number() over (partition by venue_id order by upd_dt desc) as rn FROM shipdw.hvtb_mart_dim_venue) WHERE rn = 1) dim_venue ON incident.globalvenueid = dim_venue.venue_id

LEFT JOIN shipdw.hvtb_mart_dim_incident_category dim_inc_cat ON incident.incidentcategorycode = dim_inc_cat.incident_category_code AND dim_inc_cat.src_deleted_flg = false
LEFT JOIN shipdw.hvtb_mart_dim_incident_category dim_inc_cat_parent ON incident.parentincidentcategorycode = dim_inc_cat_parent.incident_category_code AND dim_inc_cat_parent.src_deleted_flg = false
""").show
```
