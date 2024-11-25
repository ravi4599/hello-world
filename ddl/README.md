# Introduction 
[Snowflake's](https://www.snowflake.com/) schema migration using [Flyway](https://flywaydb.org/).

# Requirements
1. [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/)
2. [Terraform](https://www.terraform.io/)
3. [Flyway](https://flywaydb.org/)

# Content
    .
    ├── migration                       # Folder containing migrations for target database
    │   ├── V1__baseline_migration.sql
    │   ├── V2__test_schema.sql
    │   └── V3__update_test_schema.sql
    ├── terraform                       # Folder containg terraform configuration for creating a clone database
    │   └── main.tf
    ├── azure-pipelines.yml             # Pipeline definition
    ├── flyway.yml                      # Templated stage for applying migration on target database
    └── test-flyway.yml                 # Templated stage for creating clone of target database and applying migration on it


# Pipeline
The main idea of the pipeline is to test test migration prior to actual migration. In order to that was created to templated stages [flyway.yml](./flyway.yml) and [test-flyway.yml](./test-flyway.yml). _test-flyway_ stages should be performed automatically while _flyway_ stage might require an additional approval

## Steps of flyway.yml
1. Sign in [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/) using `az login` command 
2. Get information about the snowflake account from `snowflake-<SNOWFLAKE_ACCOUNT>-usr-pwd` and `snowflake-<SNOWFLAKE_ACCOUNT>-region` secrets that should be stored in Azure KeyVault using `az keyvault secret show` command
3. Prepare Comman variables for following steps
4. Debug Workspace
5. Execute [flyway repair](https://flywaydb.org/documentation/command/repair) for the target database
6. Execute [flyway info](https://flywaydb.org/documentation/command/info) for the target database
7. Execute [flyway migrate](https://flywaydb.org/documentation/command/migrate) for the target database

## Steps of test-flyway.yml
1. Sign in [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/) using `az login` command 
2. Get information about the snowflake account from `snowflake-<SNOWFLAKE_ACCOUNT>-usr-pwd` and `snowflake-<SNOWFLAKE_ACCOUNT>-region` secrets that should be stored in Azure KeyVault using `az keyvault secret show` command
3. Prepare Comman variables for following steps
4. Debug Workspace
5. Create a clone of target target database using prepared configuration file [main.tf](./terraform/main.tf)
6. Execute [flyway repair](https://flywaydb.org/documentation/command/repair) for the clone of the target database
7. Execute [flyway info](https://flywaydb.org/documentation/command/info) for the clone of the target database
8. Execute [flyway migrate](https://flywaydb.org/documentation/command/migrate) for the clone of the target database
9. Decommission the clone of the target database


# Baseline DDL
As baseline DDL can be used a DDl that changes existing database or a DDL that describes current state of the database. DDl with current state of the database can be generated using [GET_DDL](https://docs.snowflake.com/en/sql-reference/functions/get_ddl.html) function(eg. `select get_ddl('DATABASE', '<DATABASE_NAME>', TRUE);`). In the generated DDL will be good to make a couple changes
1. Replace `CREATE OR REPLACE` with `CREATE IF NOT EXISTS`
2. Remove references to databse as it will be controlled by flyway. Eg. `CREATE VIEW IF NOT EXISTS CDM_FLYWAY.PUBLIC.CAMPAIGN_PERFORMANCE_VIEW` replace with `CREATE VIEW IF NOT EXISTS PUBLIC.CAMPAIGN_PERFORMANCE_VIEW` where `CDM_FLYWAY` is the name of database.
3. Remoce `CREATE DATABASE` statement if the same DDL will be used in multiple database (eg. dev/qa/prod)

# Flyway logging
Flyway might complain about the missing class `SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder`. It isn't critical and won't fail an execution but can be fixed by adding `slf4j-api` and `slf4j-simple` jar files into the flyway's lib folder.
