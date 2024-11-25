---Alter table add columns Domain and UDP Version
create or replace view UDP_AUDIT_BV_TEST(
	RDS_TABLE,
	SRC_SYSTEM,
	LOADID,
	SRC_COUNT,
	TGT_COUNT,
	UDP_VERSION,
	AUDIT_SUMMARY
) as

select *, case
when (SRC_COUNT - TGT_COUNT) <> 0 then 'Not Matching'
when (TGT_COUNT is null) AND (UDP_VERSION = '1.0') then 'NA'
when (SRC_COUNT is null) or (TGT_COUNT is null) then 'Record not complete'
else 'Matching'
end as AUDIT_SUMMARY
from(
select UPPER(TABLE_NAME) as INPUT_TABLE, UPPER(SRC_SYSTEM) as SRC_SYSTEM, LOADID,
        to_number(sum(ADLS_COUNT)) as SRC_COUNT,
        to_number(sum(RDS_COUNT)) as TGT_COUNT,
        case 
        WHEN UDP_VERSION = '1.0' THEN '1.0'
        ELSE '2.0' 
        END AS UDP_VERSION
        from ITPRCS_DEV.AUDIT.UDP_AUDIT_TRANS_BKP
        group by 1,2,3,6
        order by 3 desc
)a;

CREATE OR REPLACE PROCEDURE UDP_AUDIT_SP_TEST("TABLE_NAME_IP" VARCHAR(16777216), "SRC_SYSTEM_IP" VARCHAR(16777216), "SOURCE_NAME_IP" VARCHAR(16777216), "LOADID_IP" VARCHAR(16777216), "DYNAMIC_VALUES" VARIANT)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '

              var baseStatement = "SELECT COUNT(*) FROM ITPRCS_DEV.AUDIT.UDP_AUDIT_TRANS_BKP WHERE TABLE_NAME = ''" + TABLE_NAME_IP + "'' AND SRC_SYSTEM = ''" + SRC_SYSTEM_IP + "'' AND SOURCE_NAME = ''" + SOURCE_NAME_IP + "'' AND LOADID = " + LOADID_IP;
    
    var updateStatement = "UPDATE ITPRCS_DEV.AUDIT.UDP_AUDIT_TRANS_BKP SET ";
    var updateParams = [];
    
    var insertStatement = "INSERT INTO ITPRCS_DEV.AUDIT.UDP_AUDIT_TRANS_BKP (TABLE_NAME, SRC_SYSTEM, SOURCE_NAME, LOADID";
    var insertValues = "VALUES (''" + TABLE_NAME_IP + "'',''" + SRC_SYSTEM_IP + "'', ''" + SOURCE_NAME_IP + "'', " + LOADID_IP ;
              
              var where_clause = " WHERE TABLE_NAME =''" + TABLE_NAME_IP + "'' AND SRC_SYSTEM = ''" + SRC_SYSTEM_IP + "'' AND SOURCE_NAME = ''" + SOURCE_NAME_IP + "'' AND LOADID = " + LOADID_IP ;

    len_var=Object.keys(DYNAMIC_VALUES).length

    if(len_var > 0)
    {
        for (var key in DYNAMIC_VALUES) 
                  {
            if (DYNAMIC_VALUES.hasOwnProperty(key))
                                 {

                updateStatement += key + " = " + (typeof DYNAMIC_VALUES[key] === "string" ? "''" + DYNAMIC_VALUES[key] + "''" : DYNAMIC_VALUES[key]) + ", ";
                insertStatement += ", " + key;
                insertValues += ", " + (typeof DYNAMIC_VALUES[key] === "string" ? "''" + DYNAMIC_VALUES[key] + "''" : DYNAMIC_VALUES[key]);
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


