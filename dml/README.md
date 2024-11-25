
# Introduction 
[Snowflake's](https://www.snowflake.com/) DML exeuctions on _UAT_ databases using [SnowSQL CLI Client](https://docs.snowflake.com/en/user-guide/snowsql).

# IMORTANT
1. __This automation doesn't support versioning like flyway. That means all SQL file will be executed during every run and because of that SQL files shouled be cleaned and/or updated as part of _every_ Pull Request__
2. __This automation doesn't have any syntax validation__
3. __This automation doesn't allo switching between roles and/or warehouses using `USE ROLE` or `USE WAREHOUSE`__

# Requirements
1. [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/)
2. [SnowSQL CLI Client](https://docs.snowflake.com/en/user-guide/snowsql)

# Content
    .
    ├── migration                       # Root folder for DML
    │   ├── IRB                         # Folder for IRB databases. DMLs under this fodler will be exeuted using IRB_PRCSSA_UAT_ROLE
    │   │   ├── file1.sql
    │   │   ├── file2.sql
    │   │   ├── ...
    │   │   └── fileN.sql
    │   └── RA                          # Folder for RA databases. DMLs under this fodler will be exeuted using RA_PRCSSA_UAT_ROLE
    │       ├── file1.sql
    │       ├── file2.sql
    │       ├── ...
    │       └── fileN.sql
    └── azure-pipelines.yml             # Pipeline definition

# Pipeline
1. On pull Request validates that sql files don't have `USE ROLE` or `USE WAREHOUSE`
2. After merge to `master` branch pipeline will run sql files in IRB and RA folders in alphabetical order
