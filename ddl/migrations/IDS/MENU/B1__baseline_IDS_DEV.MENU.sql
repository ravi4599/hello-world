
CREATE PROCEDURE IF NOT EXISTS BASE_VIEW_CREATION("SRC_DB_PARAM" VARCHAR(16777216), "SRC_SCHEMA_PARAM" VARCHAR(16777216), "DST_DB_PARAM" VARCHAR(16777216), "DST_SCHEMA_PARAM" VARCHAR(16777216), "SRC_TB_PREFIXES_PARAM" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    function prepareSelectTableNames(src_tb_prefixes) {
      
      var where_table_name_stmt = "";

      if (src_tb_prefixes) {
         var src_db_prefixes_array = src_tb_prefixes.split('','')

         if (src_db_prefixes_array.length > 1){
             where_table_name_stmt += ` AND TABLE_NAME LIKE ''${src_db_prefixes_array.shift()}%''`
             for (const prefix of src_db_prefixes_array) {
                 where_table_name_stmt += ` OR table_name LIKE ''${prefix}%''`;
             }
         } else {
             where_table_name_stmt += ` AND TABLE_NAME LIKE ''${SRC_TB_PREFIXES_PARAM}%''`;
         }
      }
      return `SELECT table_name FROM ` + SRC_DB_PARAM + `.INFORMATION_SCHEMA.TABLES where TABLE_CATALOG = :1 AND Table_schema = :2 and TABLE_TYPE = ''BASE TABLE'' ${where_table_name_stmt};`;
    }    

    var select_table_names = prepareSelectTableNames(SRC_TB_PREFIXES_PARAM);
            
    var select_column_names = `SELECT column_name FROM ` + SRC_DB_PARAM + `.INFORMATION_SCHEMA.COLUMNS where TABLE_CATALOG = :1 AND Table_schema = :2 and TABLE_NAME = :3;`
           
    var select_table_names_stmt = snowflake.createStatement(
                {
                sqlText: select_table_names,
                binds: [SRC_DB_PARAM, SRC_SCHEMA_PARAM]
                }
        );
 
    var table_names = select_table_names_stmt.execute();
        
    var table_name_array=[];
    while(table_names.next())
    {
        table_name_array.push(table_names.getColumnValue(1));
    }
    
    for (const table_name of table_name_array) {
    
        var select_column_names_stmt = snowflake.createStatement(
                {
                sqlText: select_column_names,
                binds: [SRC_DB_PARAM, SRC_SCHEMA_PARAM, table_name]
                }
        );
        
        var column_names = select_column_names_stmt.execute();
        var column_name_array=[];
        while(column_names.next())
        {
            column_name_array.push(column_names.getColumnValue(1));
        }
        
        var source_tb_columns = column_name_array.join('', '');
        
        var view_template = `CREATE VIEW IF NOT EXISTS ` + DST_DB_PARAM + `.` + DST_SCHEMA_PARAM + `.${table_name}_BV COPY GRANTS
                            AS SELECT
                               ${source_tb_columns}
                            FROM ` + SRC_DB_PARAM + `.` + SRC_SCHEMA_PARAM + `."${table_name}";`
        var execute_base_view_stmt = snowflake.createStatement(
        {
            sqlText: view_template
        });
        execute_base_view_stmt.execute();
    }
    try {

        return `Base view are created in the database: ${DST_DB_PARAM} and schema: ${DST_SCHEMA_PARAM}`;
        }
    catch (err)  {
        throw err;
        }
    ';