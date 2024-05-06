create or replace view seaware.seaware_group_dim
as 
select 
   group_id,           
   src_group_id,       
   group_init_date,    
   group_status,       
   group_type,         
   group_name,         
   group_mode,         
   is_internal,        
   source_code,        
   office_code ,       
   operator,           
   n_of_guests,        
   cancellation_date,  
   rec_start_dttm,     
   rec_end_dttm,
   BOOKING_SOURCE,
   SALES_CHANNEL, 
   SUB_SALES_CHANNEL,
   CASE 
     WHEN SUB_SALES_CHANNEL='VV SALES CREW' THEN 'VV SALES CREW' 
	 WHEN SUB_SALES_CHANNEL='SAILOR SERVICES' and (source_code='IN HOUSE' or source_code='INT-ASSC'or source_code='INT-CON')THEN 'SS VIA CALL/CHAT'
     WHEN SUB_SALES_CHANNEL='SAILOR SERVICES' and source_code='INT-AGENT' THEN 'SS VIA FMDC'
     WHEN SUB_SALES_CHANNEL='VV.COM' THEN 'VV.COM'
     WHEN SUB_SALES_CHANNEL='FIRST MATES' and source_code='INT-AGENT' THEN 'FM VIA FMDC'
     WHEN SUB_SALES_CHANNEL='FIRST MATES' and source_code='OPENTRAVEL' THEN 'FM VIA VV.COM'
     WHEN SUB_SALES_CHANNEL='FIRST MATES' and (source_code='IN HOUSE' or source_code='INT-ASSC'or source_code='INT-CON') THEN 'FM VIA SAILOR SERVICES'
     END AS SUB_SUB_SALES_CHANNEL
from          
(
select DISTINCT 
   gd.group_id,           
   gd.src_group_id,       
   gd.group_init_date,    
   gd.group_status,       
   gd.group_type,         
   gd.group_name,         
   gd.group_mode,         
   gd.is_internal,        
   gd.source_code,        
   gd.office_code ,       
   gd.operator,           
   gd.n_of_guests,        
   gd.cancellation_date,  
   gd.rec_start_dttm,     
   gd.rec_end_dttm,
   CASE 
	 WHEN gd.source_code = 'INT-AGENT' THEN 'FMDC' 
	 WHEN gd.source_code = 'INT-ASSC' THEN 'SS' 
	 WHEN gd.source_code = 'IN HOUSE' THEN 'SS'
	 WHEN gd.source_Code = 'INT-CON' THEN 'SS'
	 WHEN gd.source_code = 'OPENTRAVEL' THEN 'VV.COM' 
	 ELSE 'NO CHANNEL FOUND' END AS BOOKING_SOURCE, 
    CASE WHEN grd.res_id is NOT NULL THEN
      CASE 
			WHEN rf_ad.src_agency_id = 7193 THEN 'FM and VV SALES CREW' 
			WHEN gd.is_internal ='Y' THEN 'SAILOR' 
			ELSE 'FM' 
	  END
	ELSE
	  CASE 
			WHEN gf_ad.src_agency_id = 7193 THEN 'FM and VV SALES CREW' 
			WHEN gd.is_internal ='Y' THEN 'SAILOR' 
			ELSE 'FM' 
	  END	
	END	AS SALES_CHANNEL,
	CASE WHEN grd.res_id is NOT NULL THEN
	  CASE
			WHEN rf_ad.src_agency_id = 7193 THEN 'VV SALES CREW' 
			WHEN gd.is_internal='N' THEN 'FIRST MATES'
			WHEN gd.is_internal='Y' AND gd.source_code ='OPENTRAVEL' THEN 'VV.COM'
			WHEN gd.is_internal='Y' AND (gd.source_code ='IN HOUSE' OR gd.source_code ='INT-ASSC' OR gd.source_code='INT-CON') THEN 'SAILOR SERVICES' 
			WHEN gd.is_internal='Y' AND gd.source_code ='INT-AGENT' THEN 'SAILOR SERVICES'
      END
	ELSE
	  CASE
			WHEN gf_ad.src_agency_id = 7193 THEN 'VV SALES CREW' 
			WHEN gd.is_internal='N' THEN 'FIRST MATES'
			WHEN gd.is_internal='Y' AND gd.source_code ='OPENTRAVEL' THEN 'VV.COM'
			WHEN gd.is_internal='Y' AND (gd.source_code ='IN HOUSE' OR gd.source_code ='INT-ASSC' OR gd.source_code='INT-CON') THEN 'SAILOR SERVICES' 
			WHEN gd.is_internal='Y' AND gd.source_code ='INT-AGENT' THEN 'SAILOR SERVICES'
      END	
	END AS SUB_SALES_CHANNEL
from hive_schema_stg.seaware_group_dim  gd 
--left join (select group_id, max(res_id) res_id from hive_schema_stg.seaware_group_res_dim group by group_id) grd on grd.group_id = gd.group_id 
left join (select src_group_id, max(res_id) res_id from hive_schema_stg.seaware_reservation_dim rd group by src_group_id) grd on grd.src_group_id = gd.src_group_id 
left join hive_schema_stg.seaware_group_fact gf on gf.group_id = gd.group_id and gf.snapshot_date = (select max(snapshot_date) from seaware.seaware_group_fact)
left join hive_schema_stg.seaware_revenue_fact rf on rf.res_id = grd.res_id and rf.snapshot_date = (select max(snapshot_date) from seaware.seaware_revenue_fact) 
left join hive_schema_stg.seaware_agency_dim rf_ad on rf.agency_id = rf_ad.agency_id 
left join hive_schema_stg.seaware_agency_dim gf_ad on gf.agency_id = gf_ad.agency_id 
 ) GROUP_DIM
with no schema binding;