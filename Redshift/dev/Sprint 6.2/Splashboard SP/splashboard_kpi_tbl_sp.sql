CREATE OR REPLACE PROCEDURE crm.splashboard_kpi_tbl()
	LANGUAGE plpgsql
AS $$ 	 	                                                    

BEGIN
  delete from crm.splashboard_kpi_tbl;
INSERT
      INTO
      crm.splashboard_kpi_tbl
SELECT
      seaware_cabin_master.cabin_number AS cabin_number,
      seaware_reservation_dim.res_init_date AS res_init_date,
      seaware_package_dim.package_code AS package_code,
      seaware_package_dim.package_name AS package_name,
      seaware_booked_cabin_reservation_dim.price_category AS price_category,
      crm_agency_master.seaware_agency_id__c AS seaware_agency_id__c,
	  crm_agency_master.id AS crm_agency_id,
      crm_agency_master.name AS agency_name,
	  crm_agency_master.billingcity AS agency_city,
	  crm_agency_master.Currency_Type__c AS agency_currency,
      crm_agent_master.seaware_agent_id__c AS seaware_agent_id__c,
      crm_agent_master.id AS agent_contact_id,
      crm_agent_master.name AS agent_name,
	  crm_agent_master.agent_status__c AS agent_status,
	  seaware_ship_dim.ship_name AS ship_name,
      seaware_group_dim.src_group_id AS src_group_id,
      seaware_reservation_dim.src_res_id AS src_res_id,
	  seaware_reservation_dim.res_status AS res_status, 
      seaware_reservation_dim.res_guest_count AS res_guest_count,
      SUM(seaware_revenue_fact.actual_gross_ticket_revenue) AS sum_actual_gross_ticket_revenue,
  SUM(seaware_revenue_fact.actual_gross_ticket_revenue_usd) AS sum_actual_gross_ticket_revenue_usd,
      currency,
      currency_rate,
      seaware_sail_dim.sail_date_from AS sail_date_from,
      seaware_sail_dim.sail_date_to AS sail_date_to,
      CURRENT_TIMESTAMP as etl_ld_dt
FROM
      seaware.seaware_revenue_fact seaware_revenue_fact
INNER JOIN seaware.seaware_reservation_dim seaware_reservation_dim ON
      (seaware_revenue_fact.res_id = seaware_reservation_dim.res_id)
INNER JOIN seaware.seaware_sail_dim seaware_sail_dim ON
      (seaware_revenue_fact.sail_id = seaware_sail_dim.sail_id)
INNER JOIN seaware.seaware_ship_dim seaware_ship_dim ON
      (seaware_revenue_fact.ship_id = seaware_ship_dim.ship_id)
INNER JOIN seaware.seaware_package_dim seaware_package_dim ON
      (seaware_revenue_fact.package_id = seaware_package_dim.package_id)
INNER JOIN seaware.seaware_booked_cabin_reservation_dim seaware_booked_cabin_reservation_dim ON
      (seaware_reservation_dim.res_id = seaware_booked_cabin_reservation_dim.res_id)
INNER JOIN seaware.seaware_guest_dim seaware_guest_dim ON
      (seaware_revenue_fact.guest_id = seaware_guest_dim.guest_id)
INNER JOIN crm.crm_agent_master crm_agent_master ON
      (seaware_revenue_fact.agent_id = crm_agent_master.agent_id) 
INNER JOIN seaware.seaware_agency_dim seaware_agency_dim ON 
		(seaware_revenue_fact.agency_id = seaware_agency_dim.agency_id)
LEFT JOIN crm.crm_agency_master crm_agency_master ON
      (seaware_agency_dim.src_agency_id = crm_agency_master.seaware_agency_id__c and crm_agency_master.rec_end_dttm='9999-12-31 00:00:00')
LEFT JOIN seaware.seaware_group_res_dim seaware_group_res_dim ON
      (seaware_reservation_dim.res_id = seaware_group_res_dim.res_id)
LEFT JOIN seaware.seaware_group_dim seaware_group_dim ON
      (seaware_group_res_dim.group_id = seaware_group_dim.group_id)
LEFT JOIN seaware.seaware_cabin_master seaware_cabin_master ON
      (seaware_booked_cabin_reservation_dim.cabin_id = seaware_cabin_master.cabin_id)
WHERE
      (seaware_revenue_fact.snapshot_date = (select max(snapshot_date) FROM   seaware.seaware_revenue_fact))
GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
	  14,
	  15,
	  16,
	  17,
      18,
      19,
	  22,
      23,
      24,
	  25;
END;

      $$
;
