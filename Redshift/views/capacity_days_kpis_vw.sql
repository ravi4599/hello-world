create or replace view seaware.capacity_days_kpis_vw AS 
select 
sail_id,
sail_days,
I1_Built_Cabins,
Non_I1_Built_Cabins,
V1_Built_Cabins,
Non_V1_Built_Cabins,
I1_Booked_Cabins,
Non_I1_Booked_Cabins,
V1_Booked_Cabins,
Non_V1_Booked_Cabins,
Solo_Built_Cabins,
Non_Solo_Built_Cabins,
Solo_Booked_Cabins,
Non_Solo_Booked_Cabins,
total_cabins,
((2*Non_Solo_Built_Cabins)+Solo_Built_Cabins)*sail_days as Actual_Capacity_Days,
((2*Non_Solo_Booked_Cabins)+Solo_Booked_Cabins)*sail_days as Booked_Capacity_Days,
guest_count*sail_days as Actual_Sailor_Days,
(ok_abs+gty_abs+ok_gtry_rstr_abs)/total_cabins::float as "Cabin_Occupancy_%"
FROM 
(
select 
scf.snapshot_date,
scf.sail_id,
sd.sail_days,
sum(scf.I1_Built_Cabins) I1_Built_Cabins,
sum(scf.Non_I1_Built_Cabins) Non_I1_Built_Cabins,
sum(scf.V1_Built_Cabins) V1_Built_Cabins,
sum(scf.Non_V1_Built_Cabins) Non_V1_Built_Cabins,
sum(scf.I1_Booked_Cabins) I1_Booked_Cabins,
sum(scf.Non_I1_Booked_Cabins) Non_I1_Booked_Cabins,
sum(scf.V1_Booked_Cabins) V1_Booked_Cabins,
sum(scf.Non_V1_Booked_Cabins) Non_V1_Booked_Cabins,
sum(scf.Solo_Built_Cabins) Solo_Built_Cabins,
sum(scf.Non_Solo_Built_Cabins) Non_Solo_Built_Cabins,
sum(scf.Solo_Booked_Cabins) Solo_Booked_Cabins,
sum(scf.Non_Solo_Booked_Cabins) Non_Solo_Booked_Cabins,
sum(scf.total_cabins) total_cabins,
sum(scf.ooo_cabins) ooo_cabins,
sum(scf.ok_abs) as ok_abs,
sum(scf.gty_abs) as gty_abs,
sum(scf.ok_gtry_rstr_abs) as ok_gtry_rstr_abs,
sum(rf.guest_count) guest_count,
sum(rf.bk_cabin_count) bk_cabin_count     
from (select scf.snapshot_date,
scf.sail_id, 
sum(CASE WHEN scf.cabin_category = 'I1' THEN nvl(scf.num_of_cabins,0) ELSE 0 END) as I1_Built_Cabins,
sum(CASE WHEN scf.cabin_category != 'I1' THEN nvl(scf.num_of_cabins,0) ELSE 0 END) as Non_I1_Built_Cabins,
sum(CASE WHEN scf.cabin_category = 'V1' THEN nvl(scf.num_of_cabins,0) ELSE 0 END) as V1_Built_Cabins,
sum(CASE WHEN scf.cabin_category != 'V1' THEN nvl(scf.num_of_cabins,0) ELSE 0 END) as Non_V1_Built_Cabins,
sum(CASE WHEN scf.cabin_category = 'I1' THEN (nvl(scf.ok_abs,0) + nvl(scf.gty_abs,0) + nvl(ok_gtry_rstr_abs,0)) ELSE 0 END) as I1_Booked_Cabins,
sum(CASE WHEN scf.cabin_category != 'I1' THEN (nvl(scf.ok_abs,0) + nvl(scf.gty_abs,0) + nvl(ok_gtry_rstr_abs,0)) ELSE 0 END) as Non_I1_Booked_Cabins,
sum(CASE WHEN scf.cabin_category = 'V1' THEN (nvl(scf.ok_abs,0) + nvl(scf.gty_abs,0) + nvl(ok_gtry_rstr_abs,0)) ELSE 0 END) as V1_Booked_Cabins,
sum(CASE WHEN scf.cabin_category != 'V1' THEN (nvl(scf.ok_abs,0) + nvl(scf.gty_abs,0) + nvl(ok_gtry_rstr_abs,0)) ELSE 0 END) as Non_V1_Booked_Cabins,
sum(CASE WHEN (scf.cabin_category = 'I1' OR scf.cabin_category = 'V1') THEN nvl(scf.num_of_cabins,0) ELSE 0 END) as Solo_Built_Cabins,
sum(CASE WHEN (scf.cabin_category != 'I1' AND scf.cabin_category != 'V1') THEN nvl(scf.num_of_cabins,0) ELSE 0 END) as Non_Solo_Built_Cabins,
sum(CASE WHEN (scf.cabin_category = 'I1' OR scf.cabin_category = 'V1') THEN (nvl(scf.ok_abs,0) + nvl(scf.gty_abs,0) + nvl(ok_gtry_rstr_abs,0)) ELSE 0 END) as Solo_Booked_Cabins,
sum(CASE WHEN (scf.cabin_category != 'I1' AND scf.cabin_category != 'V1') THEN (nvl(scf.ok_abs,0) + nvl(scf.gty_abs,0) + nvl(ok_gtry_rstr_abs,0)) ELSE 0 END) as Non_Solo_Booked_Cabins,
sum(nvl(scf.num_of_cabins,0)) as total_cabins,
sum(nvl(scf.na_abs,0)) as ooo_cabins,
sum(nvl(scf.ok_abs,0)) ok_abs,
sum(nvl(scf.gty_abs,0)) gty_abs,
sum(nvl(scf.ok_gtry_rstr_abs,0)) ok_gtry_rstr_abs      
from hive_schema_stg.seaware_sail_cabin_fact scf 
where scf.snapshot_date=(select max(snapshot_date) from hive_schema_stg.seaware_sail_cabin_fact) 
group by scf.snapshot_date,scf.sail_id 
) scf   
inner join hive_schema_stg.seaware_sail_dim sd on scf.sail_id = sd.sail_id  
inner join (select rf.sail_id as sail_id, 
count(distinct crd.guest_id) guest_count,
count(distinct crd.cabin_id) bk_cabin_count  
from hive_schema_stg.seaware_revenue_fact rf 
left join hive_schema_stg.seaware_booked_cabin_reservation_dim crd on rf.res_id=crd.res_id and rf.guest_id=crd.guest_id and crd.rec_end_dttm='9999-12-31 00:00:00' 
join hive_schema_stg.seaware_reservation_dim rd on rf.res_id=rd.res_id and rd.res_status='BK' 
where rf.snapshot_date=(select max(snapshot_date) from hive_schema_stg.seaware_revenue_fact rf) 
group by rf.sail_id
) rf on rf.sail_id = sd.sail_id 
group by 
scf.snapshot_date,
scf.sail_id,
sd.sail_days   
) main  
with no schema binding;

GRANT USAGE ON SCHEMA seaware TO  group Tableau_poweruser_group;

grant select on all tables in schema seaware to group Tableau_poweruser_group ;