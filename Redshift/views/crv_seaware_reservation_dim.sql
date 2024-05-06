create or replace view seaware.seaware_reservation_dim
AS
SELECT 
   res_id,             
   rec_start_dttm,     
   rec_end_dttm,       
   src_res_id,         
   res_init_date,      
   res_guest_count,    
   res_mode,           
   cancellation_date,  
   res_status,         
   operator,           
   res_type,           
   is_internal,        
   source_code,        
   effective_date,
   BOOKING_SOURCE,
   SALES_CHANNEL, 
   SUB_SALES_CHANNEL, 
   CASE 
	 WHEN SUB_SALES_CHANNEL='VV SALES CREW' THEN 'VV SALES CREW' 
	 WHEN SUB_SALES_CHANNEL='SAILOR SERVICES' and source_code in ('IN HOUSE','INT-ASSC','INT-CON') THEN 'SS VIA CALL/CHAT'
     WHEN SUB_SALES_CHANNEL='SAILOR SERVICES' and source_code='INT-AGENT' THEN 'SS VIA FMDC'
     WHEN SUB_SALES_CHANNEL='VV.COM' THEN 'VV.COM'
     WHEN SUB_SALES_CHANNEL='FIRST MATES' and source_code='INT-AGENT' THEN 'FM VIA FMDC'
     WHEN SUB_SALES_CHANNEL='FIRST MATES' and source_code='OPENTRAVEL' THEN 'FM VIA VV.COM'
     WHEN SUB_SALES_CHANNEL='FIRST MATES' and source_code in ('IN HOUSE','INT-ASSC','INT-CON') THEN 'FM VIA SAILOR SERVICES' 
	 WHEN SUB_SALES_CHANNEL='FIRST MATES' and source_code in ('ODYSSEUS','REVELEX','AMADEUS','TRAVELTEK','TRAVTECH') THEN 'FM VIA API'
     END AS SUB_SUB_SALES_CHANNEL ,
   vip_status,
   stage,
   booking_source_erp,
   last_modifiedby_id,
   hotel_flag,
   src_sail_id,
   seaware_agency_id,
   seaware_agent_id,
   agency_id,
agent_id,
sec_agency_id,
sec_agent_id,
   opted_fvc,
    CANCELLATION_CASE,
  SRC_GROUP_ID ,
   CURRENCY,
   CURRENCY_RATE,
   etl_ld_status,
   etl_ld_dt,          
   etl_upd_dt  
FROM 
(SELECT DISTINCT 
   rd.res_id,             
   rd.rec_start_dttm,     
   rd.rec_end_dttm,       
   rd.src_res_id,         
   rd.res_init_date,      
   rd.res_guest_count,    
   rd.res_mode,           
   rd.cancellation_date,  
   rd.res_status,         
   rd.operator,           
   rd.res_type,           
   rd.is_internal,        
   rd.source_code,        
   rd.effective_date,
   CASE WHEN rd.source_code = 'INT-AGENT' THEN 'FMDC' 
	 WHEN rd.source_code in ('INT-ASSC','IN HOUSE','INT-CON') THEN 'SS' 
	 WHEN rd.source_code in ('OPENTRAVEL','SPLASH') THEN 'VV.COM' 
	 WHEN rd.source_code in ('ODYSSEUS','REVELEX','AMADEUS','TRAVELTEK','TRAVTECH')  THEN 'API' 
	 ELSE 'NO CHANNEL FOUND' END AS BOOKING_SOURCE, 
   CASE WHEN /*ad.src_agency_id = 7193*/ rd.seaware_agency_id in (7193,6204,11920) THEN 'FM and VV SALES CREW'
		WHEN rd.is_internal ='Y' THEN 'SAILOR' 
		ELSE 'FM and VV SALES CREW' END AS SALES_CHANNEL, 
   CASE 
		WHEN /*ad.src_agency_id = 7193*/ rd.seaware_agency_id in (7193,6204,11920) THEN 'VV SALES CREW'
		WHEN rd.is_internal='N' THEN 'FIRST MATES'
        WHEN rd.is_internal='Y' AND rd.source_code in ('OPENTRAVEL','SPLASH') THEN 'VV.COM'
        WHEN rd.is_internal='Y' AND rd.source_code in ('IN HOUSE','INT-ASSC','INT-CON','INT-AGENT') THEN 'SAILOR SERVICES' 
        END  AS SUB_SALES_CHANNEL,
   rd.vip_status,
   rd.stage,
   rd.booking_source booking_source_erp,
   rd.last_modifiedby_id,
   rd.hotel_flag,
   rd.src_sail_id,
   rd.seaware_agency_id,
   rd.seaware_agent_id,
   rd.agency_id,
rd.agent_id,
rd.sec_agency_id,
rd.sec_agent_id,
   rd.opted_fvc,
    CANCELLATION_CASE,
  SRC_GROUP_ID ,
   CURRENCY,
   CURRENCY_RATE,
   rd.etl_ld_status,
   rd.etl_ld_dt,          
   rd.etl_upd_dt
 from hive_schema_stg.seaware_reservation_dim rd 
/*left join hive_schema_stg.seaware_revenue_fact rf on rf.res_id = rd.res_id and rf.snapshot_date = (select max(snapshot_date) from seaware.seaware_revenue_fact) 
left join hive_schema_stg.seaware_agency_dim ad on rf.agency_id = ad.agency_id*/--1 Oct 2020 Since we are now populating ageny_id in reservation_dim, no need to take join with revenue_fact. However note that the agency_id in reservation_dim is from CRM. 
) RESERVATION_DIM
with no schema binding;
