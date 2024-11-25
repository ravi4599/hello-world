terraform {
  required_version = "1.3.7"
  
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "0.55.1"
    }
    snowsql = {
      source  = "aidanmelen/snowsql"
      version = "0.2.1"
    }
  }
}

# Provider Configuration
# account  - Defined as SNOWFLAKE_ACCOUNT environment variable
# region   - Defined as SNOWFLAKE_REGION environment variable
# username - Defined as SNOWFLAKE_USER environment variable
# password - Defined as SNOWFLAKE_PASSWORD environment variable
provider snowflake {
  alias = "sysadmin"
  role  = "SYSADMIN"
}

provider snowsql {
  role  = "ACCOUNTADMIN"
}

variable "NAME" {
  type        = string
  description = "(Required) Snowflake Database to create"
  default     = ""
}

variable "FROM_DATABASE" {
  type        = string
  description = "(Required) Snowflake Database to create from"
  default     = ""
}

variable "OBJOWNR_ROLE" {
  type        = string
  description = "(Required) Name of the Object Owner Role"
  default     = ""
}

variable "COMMENT" {
  type        = string
  description = "(Optional) Comment to assign to Database"
  default     = "Temprorary Database for Flyway Testing"
}

variable "ROLES" {
  type        = string
  description = "(Optional) Grant usage to this roles"
  default     = "[]"
}


variable "SNOWFLAKE_WAREHOUSE" {
  type        = string
  description = "(Required) Snowflake warehouse name"
  default     = ""
}

resource "snowflake_database" "main" {
  provider      = snowflake.sysadmin
  name          = var.NAME
  from_database = var.FROM_DATABASE
  comment       = var.COMMENT
}

resource "snowsql_exec" "main" {
  name = var.OBJOWNR_ROLE

  create {
    statements = <<-EOT
    GRANT USAGE ON DATABASE ${var.NAME} to role ${var.OBJOWNR_ROLE};
    EOT
  }
  
  # Below two parameters should be kept as listed to proper work with a custom warehouse
  delete_on_create = true
  delete {
    statements = <<-EOT
    USE WAREHOUSE ${var.SNOWFLAKE_WAREHOUSE};
    EOT
  }

  depends_on = [
    snowflake_database.main
  ]
}

resource "snowsql_exec" "roles" {
  for_each = toset(jsondecode(var.ROLES))
  
  name = each.key

  create {
    statements = <<-EOT
    GRANT USAGE ON DATABASE ${var.NAME} to role ${var.OBJOWNR_ROLE};
    EOT
  }
  
  # Below two parameters should be kept as listed to proper work with a custom warehouse
  delete_on_create = true
  delete {
    statements = <<-EOT
    USE WAREHOUSE ${var.SNOWFLAKE_WAREHOUSE};
    EOT
  }

  depends_on = [
    snowflake_database.main
  ]
}
