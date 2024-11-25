create or replace TABLE UDP_AUDIT_TRANS (
	TABLE_NAME VARCHAR(255),
	SRC_SYSTEM VARCHAR(255),
	SOURCE_NAME VARCHAR(255),
	RDS_COUNT VARCHAR(255),
	ADLS_COUNT VARCHAR(255),
	LOADID VARCHAR(255),
	LOAD_DTTM TIMESTAMP default current_timestamp(),
    UPDATE_DTTM TIMESTAMP default current_timestamp()
);

CREATE OR REPLACE PROCEDURE UDP_AUDIT_SP(
  TABLE_NAME_IP STRING,
  SRC_SYSTEM_IP STRING,
  SOURCE_NAME_IP STRING,
  LOADID_IP STRING,
  DYNAMIC_VALUES VARIANT)
RETURNS STRING
LANGUAGE JAVASCRIPT

AS $$

	var baseStatement = "SELECT COUNT(*) FROM ITPRCS_DEV.AUDIT.UDP_AUDIT_TRANS WHERE TABLE_NAME = '" + TABLE_NAME_IP + "' AND SRC_SYSTEM = '" + SRC_SYSTEM_IP + "' AND SOURCE_NAME = '" + SOURCE_NAME_IP + "' AND LOADID = " + LOADID_IP;
    
    var updateStatement = "UPDATE ITPRCS_DEV.AUDIT.UDP_AUDIT_TRANS SET ";
    var updateParams = [];
    
    var insertStatement = "INSERT INTO ITPRCS_DEV.AUDIT.UDP_AUDIT_TRANS (TABLE_NAME, SRC_SYSTEM, SOURCE_NAME, LOADID";
    var insertValues = "VALUES ('" + TABLE_NAME_IP + "','" + SRC_SYSTEM_IP + "', '" + SOURCE_NAME_IP + "', " + LOADID_IP ;
	
	var where_clause = " WHERE TABLE_NAME ='" + TABLE_NAME_IP + "' AND SRC_SYSTEM = '" + SRC_SYSTEM_IP + "' AND SOURCE_NAME = '" + SOURCE_NAME_IP + "' AND LOADID = " + LOADID_IP ;

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
    $$;