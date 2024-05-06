CREATE OR REPLACE VIEW crm.agency_parent_vw AS  
SELECT agency.id   agency_id, 
       agency.NAME agency_name, 
       CASE 
         WHEN ( agency_parent.id IS NULL 
                 OR Trim(agency_parent.id) = '' ) THEN agency.id 
         ELSE agency_parent.id 
       END         AS parent_agency_id, 
       CASE 
         WHEN ( agency_parent.NAME IS NULL 
                 OR Trim(agency_parent.NAME) = '' ) THEN agency.NAME 
         ELSE agency_parent.NAME 
       END         AS parent_agency_name 
FROM   crm.core_crm_account_vw agency 
       LEFT JOIN crm.core_crm_account_vw agency_parent 
              ON agency.parentid = agency_parent.id 
                 AND Trim(agency_parent.id) <> '' 
WHERE  Trim(agency.id) <> '' and upper(agency.accountrecordtype) = 'AGENCY'
       AND parent_agency_id IS NOT NULL 
with no schema binding;