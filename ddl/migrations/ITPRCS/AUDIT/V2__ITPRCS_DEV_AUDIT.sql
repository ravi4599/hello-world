
create or replace view V_UDP_AUDIT(
	RDS_TABLE,
	SRC_SYSTEM,
	LOADID,
	SRC_COUNT,
	TGT_COUNT,
	AUDIT_SUMMARY
) as

select *, case
when (SRC_COUNT - TGT_COUNT) <> 0 then 'Not Matching'
when (SRC_COUNT is null) or (TGT_COUNT is null) then 'Record not complete'
else 'Matching'
end as AUDIT_SUMMARY
from(
select UPPER(TABLE_NAME) as INPUT_TABLE, UPPER(SRC_SYSTEM) as SRC_SYSTEM, LOADID, 
        to_number(sum(ADLS_COUNT)) as SRC_COUNT,
        to_number(sum(RDS_COUNT)) as TGT_COUNT
        from ITPRCS_DEV.AUDIT.UDP_AUDIT_TRANS
        group by 1,2,3
        order by 3 desc
)a;
CREATE PROCEDURE IF NOT EXISTS "SRC_TGT_COUNT"("IDS_TABLE_NAME" VARCHAR(16777216), "BRANDID" VARCHAR(16777216), "LOADID" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '

    var ids_count_Statement = "SELECT COUNT(*) FROM " + IDS_TABLE_NAME + " WHERE LOAD_ID = ''" + LOADID + "''";

    var get_rds_table_Statement = "SELECT RDS_TABLE FROM ITPRCS_DEV.AUDIT.UDP_AUDIT_MAPPING WHERE IDS_TABLE = ''" + IDS_TABLE_NAME + "'' AND BRAND_ID = ''" + BRANDID + "''";

       
	try 
        {	
            var ids_count_query = snowflake.createStatement({sqlText: ids_count_Statement});
            var result_ids = ids_count_query.execute();
    
            result_ids.next();
            var  resultset_ids = result_ids.getColumnValue(1);
			
            var insert_statement = "INSERT INTO ITPRCS_DEV.AUDIT.UDP_AUDIT_RDS_IDS (IDS_TABLE, IDS_COUNT,BRAND_ID,LOADID) VALUES (''" + IDS_TABLE_NAME + "'',''" + resultset_ids + "'',''" + BRANDID + "'',''" + LOADID + "'')" ;

            var insert_query = snowflake.createStatement({sqlText: insert_statement});
            insert_query.execute();
            
            var get_rds_table = snowflake.createStatement({sqlText: get_rds_table_Statement});
			result_get_rds_table = get_rds_table.execute();

            var result_arr = [];
			
			while (result_get_rds_table.next())
			{
                var results = result_get_rds_table.getColumnValue(1);
                result_arr.push(results);
            }

            for (var i=0; i < result_arr.length ; i++)
            {   
				                
                var get_rds_count_Statement = "SELECT RDS_COUNT FROM ITPRCS_DEV.AUDIT.UDP_AUDIT_TRANS WHERE table_name = ''" + result_arr[i] + "'' AND LOADID = ''" + LOADID + "''" ;
				
			    var rds_count_query = snowflake.createStatement({sqlText: get_rds_count_Statement});
				result_rds_count = rds_count_query.execute();
                
                result_rds_count.next();
                var resultset_rds_count = result_rds_count.getColumnValue(1);
                return resultset_rds_count;
                
                if (resultset_rds_count > 0)
                {
                    var rds1_update_statement = "UPDATE ITPRCS_DEV.AUDIT.UDP_AUDIT_RDS_IDS SET RDS_TABLE = ''" + result_arr[i] + "'', RDS_COUNT = " + resultset_rds_count + " WHERE IDS_TABLE = ''" + IDS_TABLE_NAME + "'' AND BRAND_ID = ''" + BRANDID + "'' AND LOADID = ''" + LOADID + "''";
                    var rds1_update_query = snowflake.createStatement({sqlText: rds1_update_statement});
				    update_rds1 = rds1_update_query.execute();
                    
                }
                else
                {
                   var rds_select_statement = "SELECT COUNT(*) FROM " + result_arr[i]  + "WHERE loadid = ''" + LOADID + "''";
                   var rds_select_query = snowflake.createStatement({sqlText: rds_select_statement});
				   select_rds_count = rds_select_query.execute();

                   select_rds_count.next();
                   var select_rds_count_result = select_rds_count.getColumnValue(1);

                   var rds2_update_statement = "UPDATE ITPRCS_DEV.AUDIT.UDP_AUDIT_RDS_IDS SET RDS_TABLE = ''" + result_arr[i] + "'', RDS_COUNT = " + select_rds_count_result + " WHERE IDS_TABLE = ''" + IDS_TABLE_NAME + "'' AND BRAND_ID = ''" + BRANDID + "'' AND LOADID = ''" + LOADID + "''";
                    var rds2_update_query = snowflake.createStatement({sqlText: rds2_update_statement});
				    update_rds2 = rds2_update_query.execute();
                }
             }
                
		}
	    catch (err) 
	    {
            return "Error: " + err;
        }

';
CREATE PROCEDURE IF NOT EXISTS "UDP_AUDIT_SP"("TABLE_NAME_IP" VARCHAR(16777216), "SRC_SYSTEM_IP" VARCHAR(16777216), "SOURCE_NAME_IP" VARCHAR(16777216), "LOADID_IP" VARCHAR(16777216), "DYNAMIC_VALUES" VARIANT)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '

	var baseStatement = "SELECT COUNT(*) FROM ITPRCS_DEV.AUDIT.UDP_AUDIT_TRANS WHERE TABLE_NAME = ''" + TABLE_NAME_IP + "'' AND SRC_SYSTEM = ''" + SRC_SYSTEM_IP + "'' AND SOURCE_NAME = ''" + SOURCE_NAME_IP + "'' AND LOADID = " + LOADID_IP;
    
    var updateStatement = "UPDATE ITPRCS_DEV.AUDIT.UDP_AUDIT_TRANS SET ";
    var updateParams = [];
    
    var insertStatement = "INSERT INTO ITPRCS_DEV.AUDIT.UDP_AUDIT_TRANS (TABLE_NAME, SRC_SYSTEM, SOURCE_NAME, LOADID";
    var insertValues = "VALUES (''" + TABLE_NAME_IP + "'',''" + SRC_SYSTEM_IP + "'', ''" + SOURCE_NAME_IP + "'', " + LOADID_IP ;
	
	var where_clause = " WHERE TABLE_NAME =''" + TABLE_NAME_IP + "'' AND SRC_SYSTEM = ''" + SRC_SYSTEM_IP + "'' AND SOURCE_NAME = ''" + SOURCE_NAME_IP + "'' AND LOADID = " + LOADID_IP ;

    len_var=Object.keys(DYNAMIC_VALUES).length

    if(len_var > 0)
    {
        for (var key in DYNAMIC_VALUES) 
	    {
            if (DYNAMIC_VALUES.hasOwnProperty(key))
		    {
                updateStatement += key + " =" + DYNAMIC_VALUES[key] +", ";            
                insertStatement += ", " + key;
                insertValues += ", " + DYNAMIC_VALUES[key];
            }    
       }
    
        updateStatement = updateStatement.slice(0, -2);
        
        insertStatement += ")";
        insertValues += ")";
    
        var countStatement = baseStatement;
        var updateQuery = updateStatement + where_clause;
        var insertQuery = insertStatement + " " + insertValues;
    
        try 
        {
            var countStatement = snowflake.createStatement({sqlText: countStatement});
            var countResult = countStatement.execute();
        
            if (countResult.next()) 
		    {
                var count = countResult.getColumnValue(1);
            
                if (count > 0)
			    {
                    var updateStatement = snowflake.createStatement({sqlText: updateQuery});
                    updateStatement.execute();
                    return "Updated";
                } 
			    else 
			    {
                    var insertStatement = snowflake.createStatement({sqlText: insertQuery});
                    insertStatement.execute();
                    return "Inserted";
                }
            } 
		    else
		    {
                return "Error counting records";
            }
        }
	    catch (err) 
	    {
            return "Error: " + err;
        }
    }
    else
    {
        return "No. of parameters passed is : " + len_var + ". Pass atleast one input"
    }
    ';