CREATE OR REPLACE PROCEDURE seaware.commission_fact_rpt_sp()
	LANGUAGE plpgsql
AS $$ 	 	                                                                    

BEGIN
delete from  seaware.commission_fact_rpt;
INSERT
INTO
seaware.commission_fact_rpt
SELECT agency.src_agency_id seaware_agency_id,
	   crm_agency.id crm_agency_id,	
       agency.agency_name,
	   crm_agency.billingcity,
	   crm_agency.Currency_Type__c,
       agent.seaware_agent_id__c seaware_agent_id,
	   agent.id crm_agent_id,
	   agent.name agent_name,
	   agent.agent_status__c,
       rd.src_res_id,
       rd.res_status,
	   sh.ship_name ship_name,
       sd.sail_date_from,
       pd.package_name product_name,
       ct.commission_code,
       inv.invoice_item_type,
       rf.currency booking_currency,
       rf.amount item_amount,
       cf.currency_code commission_currency,
       cf.commission_amount,
       cf.commission_payout_date,
       /*sd.sail_date_from,
	   datediff(day,cf.snapshot_date,sd.sail_date_from),
	   rf.net_due,
	   tran.trans_time_stamp,
	   datediff(day,tran.trans_time_stamp,next_day(CURRENT_DATE,'Friday')),
	   date_part(w,tran.trans_time_stamp),
	   case when mod(date_part(w,tran.trans_time_stamp)::INT,2)=0 then next_day(tran.trans_time_stamp,'Friday') else dateadd(day,7,next_day(tran.trans_time_stamp,'Friday')) end pd,*/   
	   case when (rf.net_due=0 and datediff(day,cf.snapshot_date,sd.sail_date_from)<=120 and datediff(day,tran.trans_time_stamp,next_day(CURRENT_DATE,'Friday'))>2) then (case when mod(date_part(w,tran.trans_time_stamp)::INT,2)=0 then next_day(tran.trans_time_stamp,'Friday') else dateadd(day,7,next_day(tran.trans_time_stamp,'Friday')) end ) end expected_payout_date  
FROM hive_schema_stg.seaware_commission_fact cf
LEFT JOIN hive_schema_stg.seaware_agency_dim agency ON cf.agency_id = agency.agency_id 
LEFT JOIN hive_schema_stg.crm_agency_master crm_agency ON agency.src_agency_id = crm_agency.seaware_agency_id__c 
LEFT JOIN hive_schema_stg.crm_agent_master agent ON cf.agent_id = agent.agent_id
LEFT JOIN hive_schema_stg.seaware_reservation_dim rd ON cf.res_id = rd.res_id
LEFT JOIN hive_schema_stg.seaware_sail_dim sd ON cf.sail_id = sd.sail_id
LEFT JOIN hive_schema_stg.seaware_commission_type_lkp ct ON cf.commission_type_skey = commission_type_id
LEFT JOIN hive_schema_stg.seaware_invoice_item_type_lkp inv ON cf.invoice_item_type_id = inv.invoice_item_type_id
LEFT JOIN hive_schema_stg.seaware_package_dim pd ON pd.package_id = cf.package_id 
LEFT JOIN seaware.seaware_ship_dim sh ON sd.ship_id = sh.ship_id 
LEFT JOIN hive_schema_stg.seaware_revenue_fact rf ON rf.res_id = cf.res_id
AND rf.guest_id = cf.guest_id
AND rf.package_id = cf.package_id
AND rf.invoice_item_type_id = inv.invoice_item_type_id 
LEFT JOIN (
select res_id, trans_time_stamp from hive_schema_stg.seaware_transaction_evt a
where trans_id = (select max(trans_id) from hive_schema_stg.seaware_transaction_evt b 
where a.res_id=b.res_id and b.trans_type='PMNT' and trans_status='OK')) tran ON tran.res_id = rd.res_id 
WHERE  cf.snapshot_date = (SELECT Max(snapshot_date) 
                           FROM   hive_schema_stg.seaware_commission_fact) 
       AND rf.snapshot_date = (SELECT Max(snapshot_date) 
                               FROM   hive_schema_stg.seaware_revenue_fact) ;
     
END;

 

       $$
;
