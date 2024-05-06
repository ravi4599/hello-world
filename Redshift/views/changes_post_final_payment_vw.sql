create or replace view seaware.changes_post_final_payemnt as 
select  
guest_original.net_due net_due_original,
guest_now.net_due net_due_now, 
rd_original.res_id res_id_original,
rd_original.src_res_id src_res_id,
rd_original.res_init_date res_init_date,
rd_original.res_guest_count res_guest_count_original,
rd_original.res_status res_status_original,
rd_original.stage stage_original,
rd_now.res_id res_id_now,
rd_now.res_guest_count res_guest_count_now,
rd_now.res_status res_status_now,
rd_now.stage stage_now,
gd_original.guest_id guest_id_original,
gd_original.src_guest_id src_guest_id_original,
gd_original.guest_seqn guest_seqn_original,
gd_original.client_id client_id_original,
gd_original.last_name last_name_original,
gd_original.first_name first_name_original,
gd_original.middle_name middle_name_original,
gd_now.guest_id guest_id_now,
gd_now.src_guest_id src_guest_id_now,
gd_now.guest_seqn guest_seqn_now,
gd_now.client_id client_id_now,
gd_now.last_name last_name_now,
gd_now.first_name first_name_now,
gd_now.middle_name middle_name_now,
shd.ship_id ship_id,
shd.ship ship,
ship_name ship_name,
sd.sail_id sail_id,
sd.src_sail_id src_sail_id,
sd.sail_date_from sail_date_from,
sd.sail_date_to sail_date_to,
am.agency_id agency_id,
am.seaware_agency_id__c seaware_agency_id__c,
am.name agency_name,
am.lastname agency_lastname,
am.firstname agency_firstname,
am.billingcity billingcity,
am.billingcountry billingcountry,
am.billingstate billingstate,
agt.agent_id agent_id,
agt.seaware_agent_id__c seaware_agent_id__c,
agt.name agent_name,
agt.lastname agent_lastname,
agt.firstname agent_firstname,
cm.cabin_number cabin_number,
cm.cabin_name cabin_name 
from 
(select distinct rd.src_res_id, fact.res_id,fact.guest_id,gd.guest_seqn,fact.sail_id, fact.ship_id, fact.agent_id, 
fact.agency_id, main.snapshot_date,sum(net_due) net_due from 
"seaware"."seaware_revenue_fact" fact 
join "seaware"."seaware_reservation_dim" rd on (fact.res_id=rd.res_id) 
join 
(select distinct rd.src_res_id, min(snapshot_date) snapshot_date from "seaware"."seaware_revenue_fact" fact  
join "seaware"."seaware_reservation_dim" rd on fact.res_id=rd.res_id 
where net_due = 0 
group by rd.src_res_id) main on (main.src_res_id=rd.src_res_id and main.snapshot_date=fact.snapshot_date) 
join "seaware"."seaware_guest_dim" gd on fact.guest_id=gd.guest_id 
group by rd.src_res_id, fact.res_id,fact.guest_id,gd.guest_seqn,fact.sail_id, fact.ship_id, fact.agent_id, 
fact.agency_id, main.snapshot_date) guest_original 
full join (select rd.src_res_id, fact.res_id, gd.guest_id,gd.guest_seqn, sum(net_due) net_due, 
snapshot_date from "seaware"."seaware_revenue_fact" fact  
join "seaware"."seaware_guest_dim" gd on fact.guest_id=gd.guest_id 
join "seaware"."seaware_reservation_dim" rd on fact.res_id=rd.res_id 
where snapshot_date = (select max(snapshot_date) from  "hive_schema_stg"."seaware_revenue_fact") 
group by 
rd.src_res_id, fact.res_id, gd.guest_id,gd.guest_seqn, snapshot_date) guest_now on (guest_original.src_res_id=guest_now.src_res_id 
and guest_original.guest_seqn=guest_now.guest_seqn) 
left join seaware.seaware_reservation_dim rd_original on (guest_original.res_id = rd_original.res_id) 
left join seaware.seaware_reservation_dim rd_now on (guest_now.res_id = rd_now.res_id) 
left join seaware.seaware_guest_dim gd_original on (guest_original.guest_id = gd_original.guest_id) 
left join seaware.seaware_guest_dim gd_now on (guest_now.guest_id = gd_now.guest_id) 
left join seaware.seaware_ship_dim shd on (guest_original.ship_id = shd.ship_id) 
left join seaware.seaware_sail_dim sd on (guest_original.sail_id = sd.sail_id) 
left join seaware.seaware_agency_dim ad on (guest_original.agency_id = ad.agency_id) 
left join crm.crm_agency_master am ON (ad.src_agency_id = am.seaware_agency_id__c)
left join crm.crm_agent_master agt ON (guest_original.agent_id = agt.agent_id) 
left join seaware.seaware_booked_cabin_reservation_dim bcrd on (rd_original.res_id=bcrd.res_id and gd_original.guest_id=bcrd.guest_id)
left join seaware.seaware_cabin_master cm on (cm.cabin_id = bcrd.cabin_id) 
with no schema binding
--where guest_original.res_id is not null --and guest_original.guest_id<>guest_now.guest_id 
;