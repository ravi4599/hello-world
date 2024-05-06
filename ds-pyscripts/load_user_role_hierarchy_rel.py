from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext

## Functions
def initSparkSession(appName):
    """
    Initializes the SparkSession

    :param appName  : name for the SparkSession
    :return         : initialized SparkSession
    """

    try:
        spark = SparkSession \
            .builder \
            .appName(appName) \
            .enableHiveSupport() \
            .getOrCreate()
        spark.conf.set("hive.exec.dynamic.partition", "true")
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
		
    except Exception as e:
        traceback.print_exc()
        raise TypeError(
            "Error Code 6: Error initializing SparkSession. Please review SparkSession configurations. ")

    return spark


def stopSparkSession(spark):
    """ Shuts down the current SparkSession

    
    :param spark    : current SparkSession
    :return         : N/A
    """
    spark.stop()

if __name__ == "__main__":
    spark = initSparkSession("Insert SeaQ Fact Data")
	
    df = spark.sql("insert overwrite table vv_db.hvtb_nbx_core_CRM_USERROLE_HIERARCHY_REL select usr.id UserID, usr.id UserChildID, usr.contactid ContactID, usr.contactid UserChildcontactID,userrole.portalrole UsrRole, userrole.portalrole UserChildRole, 0 lvl, current_timestamp  from vv_db.hvtb_nbx_core_crm_user usr, vv_db.userrole userrole   where usr.userroleid = userrole.id and userrole.portalrole != '' limit 0")

    df = spark.sql("insert into vv_db.hvtb_nbx_core_CRM_USERROLE_HIERARCHY_REL select usr.id UserID, usr.id UserChildID, usr.contactid ContactID, usr.contactid UserChildcontactID,userrole.portalrole UsrRole, userrole.portalrole UserChildRole, 0 lvl,current_timestamp  from vv_db.hvtb_nbx_core_crm_user usr, vv_db.userrole userrole   where usr.userroleid = userrole.id and userrole.portalrole != ''")

    df = spark.sql("with child as (select usr.id UserChildID, usr.contactid UserChildcontactID, userrole.portalrole UserChildRole, userrole.parentroleid  parentroleid    from vv_db.hvtb_nbx_core_crm_user usr,  vv_db.userrole userrole    where usr.userroleid = userrole.id ), parent as (select usr.id UserID, usr.contactid ContactID,  userrole.portalrole UsrRole, usr.userroleid      from vv_db.hvtb_nbx_core_crm_user usr,  vv_db.userrole userrole    where usr.userroleid = userrole.id)  Insert into vv_db.hvtb_nbx_core_CRM_USERROLE_HIERARCHY_REL select parent.UserID UserID, child.UserChildID UserChildID, parent.ContactID ContactID,  child.UserChildcontactID UserChildcontactID, parent.UsrRole UsrRole,  child.UserChildRole UserChildRole, 1 lvl,current_timestamp  from parent, child  where child.parentroleid = parent.userroleid  and parent.UserID in (select usr.id UserID       from vv_db.hvtb_nbx_core_crm_user usr,  vv_db.userrole userrole    where usr.userroleid = userrole.id  and userrole.portalrole != '')  and parent.UsrRole != ''")

    df = spark.sql("with child as (select usr.id UserChildID, usr.contactid UserChildcontactID, userrole.portalrole UserChildRole, userrole.parentroleid  parentroleid    from vv_db.hvtb_nbx_core_crm_user usr,  vv_db.userrole userrole    where usr.userroleid = userrole.id ), parent as (select usr.id UserID, usr.contactid ContactID,  userrole.portalrole UsrRole, userrole.parentroleid  parentroleid, usr.userroleid      from vv_db.hvtb_nbx_core_crm_user usr,  vv_db.userrole userrole    where usr.userroleid = userrole.id),  super_parent as (select usr.id UserSuperParentID, usr.contactid UserSuperParentContactID,  userrole.portalrole UsrSuperParentRole, usr.userroleid      from vv_db.hvtb_nbx_core_crm_user usr,  vv_db.userrole userrole    where usr.userroleid = userrole.id)   Insert into vv_db.hvtb_nbx_core_CRM_USERROLE_HIERARCHY_REL select DISTINCT super_parent.UserSuperParentID UserID, child.UserChildID UserChildID, super_parent.UserSuperParentContactID ContactID,  child.UserChildcontactID UserChildcontactID, super_parent.UsrSuperParentRole UsrRole,  child.UserChildRole UserChildRole, 2 lvl,current_timestamp  from parent, child, super_parent   where child.parentroleid = parent.userroleid  and parent.parentroleid = super_parent.userroleid  and super_parent.userroleid in (select usr.UserRoleID      from vv_db.hvtb_nbx_core_crm_user usr,  vv_db.userrole userrole    where usr.userroleid = userrole.id  and userrole.portalrole != '')  and parent.UsrRole != ''")
    
    stopSparkSession(spark)

