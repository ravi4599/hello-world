CREATE OR REPLACE VIEW crm.crm_userrole_hierarchy_rel_vw
AS select
*
from crm.crm_userrole_hierarchy_rel 
with no schema binding;

GRANT SELECT ON crm.crm.crm_userrole_hierarchy_rel_vw TO GROUP tableau_poweruser_group;
GRANT SELECT ON crm.crm.crm_userrole_hierarchy_rel_vw TO GROUP integration_user_group;