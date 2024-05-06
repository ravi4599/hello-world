create or replace view seaware.capacity_days_kpis_vw AS 
select 
sail_id,
sail_days,
cabin_category,
Built_Cabins,
Booked_Cabins,
Solo_Built_Cabins,
Non_Solo_Built_Cabins,
Solo_Booked_Cabins,
Non_Solo_Booked_Cabins,
((2*Non_Solo_Built_Cabins)+Solo_Built_Cabins)*sail_days as Actual_Capacity_Days,
((2*Non_Solo_Booked_Cabins)+Solo_Booked_Cabins)*sail_days as Booked_Capacity_Days,
guest_count*sail_days as Actual_Sailor_Days,
case when (Built_Cabins<>0) then ((ok_abs+gty_abs+ok_gtry_rstr_abs)/Built_Cabins::float) else 0 end as "Cabin_Occupancy_%",
guest_count
FROM 
(
select 
scf.snapshot_date,
scf.sail_id,
sd.sail_days,
scf.cabin_category, 
sum(scf.Built_Cabins) Built_Cabins,
sum(scf.Booked_Cabins) Booked_Cabins,
sum(scf.Solo_Built_Cabins) Solo_Built_Cabins,
sum(scf.Non_Solo_Built_Cabins) Non_Solo_Built_Cabins,
sum(scf.Solo_Booked_Cabins) Solo_Booked_Cabins,
sum(scf.Non_Solo_Booked_Cabins) Non_Solo_Booked_Cabins,
sum(scf.ooo_cabins) ooo_cabins,
sum(scf.ok_abs) as ok_abs,
sum(scf.gty_abs) as gty_abs,
sum(scf.ok_gtry_rstr_abs) as ok_gtry_rstr_abs,
sum(rf.guest_count) guest_count,
sum(rf.bk_cabin_count) bk_cabin_count     
from (select scf.snapshot_date,
scf.sail_id, 
scf.cabin_category,
sum(nvl(scf.num_of_cabins,0)) as Built_Cabins,
sum(nvl(scf.ok_abs,0) + nvl(scf.gty_abs,0) + nvl(ok_gtry_rstr_abs,0)) as Booked_Cabins,
sum(CASE WHEN (scf.cabin_category = 'I1' OR scf.cabin_category = 'V1') THEN nvl(scf.num_of_cabins,0) ELSE 0 END) as Solo_Built_Cabins,
sum(CASE WHEN (scf.cabin_category != 'I1' AND scf.cabin_category != 'V1') THEN nvl(scf.num_of_cabins,0) ELSE 0 END) as Non_Solo_Built_Cabins,
sum(CASE WHEN (scf.cabin_category = 'I1' OR scf.cabin_category = 'V1') THEN (nvl(scf.ok_abs,0) + nvl(scf.gty_abs,0) + nvl(ok_gtry_rstr_abs,0)) ELSE 0 END) as Solo_Booked_Cabins,
sum(CASE WHEN (scf.cabin_category != 'I1' AND scf.cabin_category != 'V1') THEN (nvl(scf.ok_abs,0) + nvl(scf.gty_abs,0) + nvl(ok_gtry_rstr_abs,0)) ELSE 0 END) as Non_Solo_Booked_Cabins,
sum(nvl(scf.na_abs,0)) as ooo_cabins,
sum(nvl(scf.ok_abs,0)) ok_abs,
sum(nvl(scf.gty_abs,0)) gty_abs,
sum(nvl(scf.ok_gtry_rstr_abs,0)) ok_gtry_rstr_abs      
from hive_schema_stg.seaware_sail_cabin_fact scf 
where scf.snapshot_date=(select max(snapshot_date) from hive_schema_stg.seaware_sail_cabin_fact) 
group by scf.snapshot_date,scf.sail_id, scf.cabin_category  
) scf   
inner join hive_schema_stg.seaware_sail_dim sd on scf.sail_id = sd.sail_id  
left join (select rf.sail_id as sail_id, crd.price_category as cabin_category,  
count(distinct crd.guest_id) guest_count,
count(distinct crd.cabin_id) bk_cabin_count  
from hive_schema_stg.seaware_revenue_fact rf 
left join hive_schema_stg.seaware_booked_cabin_reservation_dim crd on rf.res_id=crd.res_id and rf.guest_id=crd.guest_id and crd.rec_end_dttm='9999-12-31 00:00:00' 
join hive_schema_stg.seaware_reservation_dim rd on rf.res_id=rd.res_id and rd.res_status in ('BK','OF')  
where rf.snapshot_date=(select max(snapshot_date) from hive_schema_stg.seaware_revenue_fact rf) 
group by rf.sail_id, crd.price_category 
) rf on rf.sail_id = sd.sail_id and scf.cabin_category = rf.cabin_category 
group by 
scf.snapshot_date,
scf.sail_id,
sd.sail_days,
scf.cabin_category    
) main  
with no schema binding;