DROP TABLE IF EXISTS seaware.commission_fact_rpt;

CREATE TABLE IF NOT EXISTS seaware.commission_fact_rpt
(
	seaware_agency_id INTEGER   ENCODE lzo
	,crm_agency_id VARCHAR(100)   ENCODE lzo
	,agency_name VARCHAR(100)   ENCODE lzo
	,agency_city VARCHAR(100)	ENCODE lzo
	,agency_currency VARCHAR(100)	ENCODE lzo 
	,seaware_agent_id VARCHAR(100)   ENCODE lzo
	,crm_agent_id VARCHAR(100)   ENCODE lzo
	,agent_name VARCHAR(100)   ENCODE lzo
	,agent_status VARCHAR(100)	ENCODE lzo 
	,src_res_id INTEGER   ENCODE lzo
	,res_status CHAR(2)   ENCODE lzo
	,ship_name VARCHAR(50)	ENCODE lzo 
	,sail_date_from TIMESTAMP WITHOUT TIME ZONE   ENCODE lzo
	,product_name VARCHAR(100) ENCODE lzo
	,commission_code VARCHAR(15)   ENCODE lzo
	,invoice_item_type VARCHAR(50)   ENCODE lzo
	,booking_currency VARCHAR(5)   ENCODE lzo
	,item_amount DOUBLE PRECISION   
	,commission_currency VARCHAR(5)   ENCODE lzo
	,commission_amount DOUBLE PRECISION   
	,commission_payout_date TIMESTAMP WITHOUT TIME ZONE   ENCODE lzo 
	,expected_payout_date TIMESTAMP WITHOUT TIME ZONE   ENCODE lzo 
)
DISTSTYLE EVEN
;

GRANT USAGE ON SCHEMA seaware TO  group Tableau_poweruser_group;
GRANT USAGE ON SCHEMA seaware TO  group integration_user_group;

grant select on all tables in schema seaware to group Tableau_poweruser_group ;
grant select on all tables in schema seaware to group integration_user_group ;




