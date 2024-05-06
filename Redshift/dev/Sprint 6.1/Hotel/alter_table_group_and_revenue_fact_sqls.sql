alter table hive_schema_stg.seaware_group_fact rename column hotel_id to hotel_group_req_id;

alter table hive_schema_stg.seaware_revenue_fact rename column hotel_id to hotel_res_req_id;