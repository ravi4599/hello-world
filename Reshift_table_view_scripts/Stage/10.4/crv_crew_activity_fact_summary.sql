create or replace view shipdw.crew_activity_fact_summary as 
select case when activity_hours.org_unit_skey is null then case when work_hours.org_unit_skey is null then planned_hours.org_unit_skey else work_hours.org_unit_skey end else activity_hours.org_unit_skey end as org_unit_skey,
case when activity_hours.crew_activity_detail_skey is null then case when work_hours.crew_activity_detail_skey is null then planned_hours.crew_activity_detail_skey else work_hours.crew_activity_detail_skey end else activity_hours.crew_activity_detail_skey end as crew_activity_detail_skey,
case when activity_hours.person_skey is null then case when work_hours.person_skey is null then planned_hours.person_skey else work_hours.person_skey end else activity_hours.person_skey end as person_skey,
case when activity_hours.is_actuals is null then case when work_hours.is_actuals is null then planned_hours.is_actuals else work_hours.is_actuals end else activity_hours.is_actuals end as is_actuals,
case when activity_hours.activity_skey is null then work_hours.activity_skey else activity_hours.activity_skey end as activity_skey,
activity_hours.schedule_activity_day, 
activity_hours.schedule_activity_datetime_from,
activity_hours.schedule_activity_datetime_to, 
activity_hours.schedule_activity_duration_hours,
planned_hours.schedule_activity_datetime_from as schedule_activity_datetime_from_planned,
planned_hours.schedule_activity_datetime_to as schedule_activity_datetime_to_planned, 
planned_hours.schedule_activity_duration_hours_planned, 
work_hours.schedule_activity_datetime_from as schedule_activity_datetime_from_work,
work_hours.schedule_activity_datetime_to as schedule_activity_datetime_to_work,
work_hours.schedule_activity_duration_hours_work,
case when activity_hours.is_actual_activity_confirmed is null then work_hours.is_actual_activity_confirmed else activity_hours.is_actual_activity_confirmed end as is_actual_activity_confirmed,
case when activity_hours.is_actual_activity_approved is null then work_hours.is_actual_activity_approved else activity_hours.is_actual_activity_approved end as is_actual_activity_approved    
  from 
(
select 
org_unit_skey,
crew_activity_detail_skey,
person_skey,
is_actuals,
activity_skey,
schedule_activity_day,
schedule_activity_datetime_from,
schedule_activity_datetime_to,
schedule_activity_duration_hours,
is_actual_activity_confirmed,
is_actual_activity_approved
from shipdw.crew_activity_fact af 
join shipdw.crew_activity_dim ad on af.activity_skey = ad.crew_activity_skey 
where af.is_deleted = false and af.is_actuals = true and upper(ad.activity_name)<>'WORK' 
) activity_hours 
full join 
(
select 
org_unit_skey,
crew_activity_detail_skey,
person_skey,
is_actuals,
schedule_activity_day,
schedule_activity_datetime_from, /*Assuming there will be a single record for planned hours per person per day*/
schedule_activity_datetime_to,
sum(schedule_activity_duration_hours) as schedule_activity_duration_hours_planned 
from shipdw.crew_activity_fact 
where is_deleted = false and is_actuals = false 
group by 
org_unit_skey,
crew_activity_detail_skey,
person_skey,
is_actuals,
schedule_activity_day,
schedule_activity_datetime_from, 
schedule_activity_datetime_to
) planned_hours on planned_hours.person_skey = activity_hours.person_skey and activity_hours.schedule_activity_datetime_from between planned_hours.schedule_activity_datetime_from and planned_hours.schedule_activity_datetime_to--planned_hours.schedule_activity_day = activity_hours.schedule_activity_day 
full join 
(
select 
org_unit_skey,
crew_activity_detail_skey,
person_skey,
is_actuals,
activity_skey,
schedule_activity_day,
schedule_activity_datetime_from, /*Assuming there will be a single record for planned hours per person per day*/
schedule_activity_datetime_to,
sum(schedule_activity_duration_hours) as schedule_activity_duration_hours_work,
is_actual_activity_confirmed,
is_actual_activity_approved
from shipdw.crew_activity_fact af 
join shipdw.crew_activity_dim ad on af.activity_skey = ad.crew_activity_skey 
where af.is_deleted = false and af.is_actuals = true and upper(ad.activity_name)='WORK' 
group by 
org_unit_skey,
crew_activity_detail_skey,
person_skey,
is_actuals,
activity_skey,
schedule_activity_day,
schedule_activity_datetime_from, 
schedule_activity_datetime_to,
is_actual_activity_confirmed,
is_actual_activity_approved  
) work_hours on work_hours.person_skey = activity_hours.person_skey and work_hours.schedule_activity_datetime_from between planned_hours.schedule_activity_datetime_from and planned_hours.schedule_activity_datetime_to--work_hours.schedule_activity_day = activity_hours.schedule_activity_day 
;