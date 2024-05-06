DROP TABLE IF EXISTS crm.splashboard_kpi_tbl;

CREATE TABLE IF NOT EXISTS crm.splashboard_kpi_tbl
(
	cabin_number VARCHAR(10)   ENCODE lzo
	,res_init_date TIMESTAMP WITHOUT TIME ZONE   ENCODE lzo
	,package_code VARCHAR(30)   ENCODE lzo
	,package_name VARCHAR(100)   ENCODE lzo
	,price_category VARCHAR(4)   ENCODE lzo
	,seaware_agency_id__c VARCHAR(100)   ENCODE lzo
	,crm_agency_id VARCHAR(100)   ENCODE lzo 
	,agency_name VARCHAR(100)   ENCODE lzo 
	,agency_city VARCHAR(100)	ENCODE lzo
	,agency_currency VARCHAR(100)	ENCODE lzo 
	,seaware_agent_id__c VARCHAR(100)   ENCODE lzo
	,agent_contact_id VARCHAR(500)   ENCODE lzo
	,agent_name VARCHAR(100)   ENCODE lzo 
	,agent_status VARCHAR(100)	ENCODE lzo 
	,ship_name VARCHAR(50)	ENCODE lzo
	,src_group_id INTEGER   ENCODE lzo
	,src_res_id INTEGER   ENCODE lzo
	,res_status CHAR(2)   ENCODE lzo 
	,res_guest_count INTEGER   ENCODE lzo
	,sum_actual_gross_ticket_revenue DOUBLE PRECISION   
	,sum_actual_gross_ticket_revenue_usd DOUBLE PRECISION   
	,currency VARCHAR(5)   ENCODE lzo
	,currency_rate NUMERIC(10,5)   ENCODE lzo
	,sail_date_from TIMESTAMP WITHOUT TIME ZONE   ENCODE lzo
	,sail_date_to TIMESTAMP WITHOUT TIME ZONE   ENCODE lzo
	,etl_ld_dt TIMESTAMP WITHOUT TIME ZONE   ENCODE lzo
)
;

GRANT USAGE ON SCHEMA crm TO  group Tableau_poweruser_group;
GRANT USAGE ON SCHEMA crm TO  group integration_user_group;

grant select on all tables in schema crm to group Tableau_poweruser_group ;
grant select on all tables in schema crm to group integration_user_group ;


