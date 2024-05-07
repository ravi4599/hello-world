package com.virginvoyages.dimension
import java.util.Properties
import java.sql.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ StructType, StructField, TimestampType, StringType, IntegerType, DateType, LongType };
import org.apache.spark.sql.expressions.Window
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

//import io.github.edersoncorbari.connect.sparkWrapper
//import com.virginvoyages.dimension.HierarchyEmployee
import com.virginvoyages.dimension.HeirarchyRel

object CRMUserHierarchyRelLoad {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)

    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    try {

      println("Starting ")
      val iddf = spark.sql("""select id from ( select id, parentroleid, portalrole, row_number() over(partition by id order by lastmodifieddate desc) as rn from vv_db.hvtb_parse_sfdc_UserRole) where rn = 1 and portalrole!='null'""")
      iddf.createOrReplaceTempView("iddt")

      val empDF = spark.sql("""select a.id,if(i.id is not null,parentroleid,a.id) as parentroleid,portalrole from 
        (select * from ( select id, parentroleid, portalrole, row_number() over(partition by id order by lastmodifieddate desc) 
        as rn from vv_db.hvtb_parse_sfdc_UserRole) where rn = 1 and portalrole!='null') a left
         join iddt i on a.parentroleid=i.id """)

      empDF.createOrReplaceTempView("emptemp")
      spark.sql("select * from emptemp where id in ('00E3s000000UaTaEAK','00E3s000000UaSlEAK','00E3s000000UaTbEAK')")show(false)
      empDF.printSchema
      //  empDF1.printSchema

      import spark.implicits._
      val graphDF = HeirarchyRel(spark).compute(empDF).sort($"parentroleid".asc)
      graphDF.show(false)
      val splitDf = graphDF.withColumn("genre", explode(split($"path", "[/]"))) //.show
      splitDf.show(100, false)
      splitDf.createOrReplaceTempView("splitt")

      val hrchDf = spark.sql("""select *,case when UserRole=UserChildRole then 0 
        when UserRole!=UserChildRole and UserRole='Manager' and UserChildRole='Worker' then 1
        when  UserRole!=UserChildRole and UserRole='Executive' and UserChildRole='Worker' then 2
        when  UserRole!=UserChildRole and UserRole='Executive' and UserChildRole='Manager' then 1
        else slevel end  as level
            from (select e.id as UserRoleID,s.id as UserChildRoleID,e.portalrole as UserRole,
        s.portalrole as UserChildRole,s.parentroleid,level as slevel,isleaf  from emptemp e left join splitt s on e.id=s.genre ) a 
        order by UserRoleID """)


      hrchDf.createOrReplaceTempView("hrchytbl")
             
      val cachedJoin = spark.sql(s"""
select crmuser.id as UserId, crmuser.contactid as UserContactId, account.id as UserAgencyId, RecordType.name as AccountRecordType
,userroleid from ( select * from ( select id, contactid, userroleid, accountid,
 row_number() over(partition by id order by lastmodifieddate desc) as rn from vv_db.hvtb_parse_sfdc_User) 
 where rn = 1) crmuser  left join ( select * from ( select id, recordtypeid, row_number() over(partition by id 
 order by lastmodifieddate desc) as rn from vv_db.hvtb_parse_sfdc_account) where rn = 1 ) account on
  crmuser.accountid = account.id left join ( select * from ( select id, Name, row_number() over(partition by id 
  order by LastModifiedDate desc) as rn from vv_db.hvtb_lnd_sfdc_recordtype) p where p.rn = 1)RecordType 
  on ACCOUNT.recordtypeid = RecordType.id 
""").cache()

      cachedJoin.createOrReplaceTempView("SrcData")

      val userDf = spark.sql(s"""select UserId ,UserContactId,UserAgencyId,AccountRecordType,src.UserRoleID,UserChildRoleID,
        UserRole,UserChildRole,parentroleid,level from SrcData src inner join hrchytbl hr on  
        src.userroleid =hr.UserRoleID 
         order by src.UserId""")


      userDf.createOrReplaceTempView("usertbl")

      val data = spark.sql("""select user1.*,src.UserId as UserChildID,src.UserContactId as UserChildContactID,src.UserAgencyID as 
        UserChildAgencyID from usertbl user1, usertbl src where user1.UserChildRoleID =src.UserRoleID and
         user1.UserRole!=user1.UserChildRole 
         order by src.UserChildRoleID""")

      val data1 = spark.sql(s"""select user1.*,user1.UserId as UserChildID,user1.UserContactId as 
        UserChildContactID,user1.UserAgencyID as UserChildAgencyID from usertbl user1 where 
        user1.UserRole=user1.UserChildRole  order by user1.UserChildRoleID""")

      val dfs = Seq(data, data1)
      val interdf = dfs.reduce(_ union _)


      interdf.createOrReplaceTempView("finaltbl")
    
      val df1 = spark.sql("""select UserId as user_id,UserChildID as user_child_id,UserContactId as user_contact_id,
         UserChildContactID as user_child_contact_id,UserRole as user_role,UserChildRole as user_child_role,UserRoleID as user_role_id,UserChildRoleID as user_child_role_id,
parentroleid as immediate_parent_role_id,UserAgencyId as user_agency_id,UserChildAgencyID as user_child_agency_id, level 
from finaltbl""") //spark.sql("select count(1),UserId,UserRoleID,UserChildRoleID from finaltbl group by UserId,UserRoleID,UserChildRoleID having count(1)>1").show(false)
      df1.createOrReplaceTempView("finaltbl2")
      
      //   spark.sql("select * from finaltbl2 where user_role_id in ('00E3s000000UaTaEAK','00E3s000000UaSlEAK','00E3s000000UaTbEAK')")show(false)
     df1.printSchema
      val agencyDf = spark.sql("select * from vv_db.hvtb_mart_crm_agency_hierarchy_rel where level !=0")
      agencyDf.createOrReplaceTempView("agncy")
      //val seedUsers = cachedJoin.where("UserRole = 'Agency'").collect()
      val parentagdf = spark.sql("select distinct user_id,user_contact_id,user_role,user_role_id,immediate_parent_role_id,user_agency_id from finaltbl2 where user_role='Executive'")
      parentagdf.createOrReplaceTempView("tbl3")
      val parentagdf1 = spark.sql("select * from tbl3 t join agncy a on t.user_agency_id=a.agency_id ")
      parentagdf1.createOrReplaceTempView("tbl4")

      val parentagdf2 = spark.sql("""select user_id,user_child_id,user_contact_id,user_child_contact_id,user_role,user_child_role,user_role_id,user_child_role_id,immediate_parent_role_id,user_agency_id,user_child_agency_id,level from (select t4.user_id as user_id,t4.user_contact_id as user_contact_id,t4.user_role as user_role,t4.user_role_id as user_role_id,t4.user_agency_id as user_agency_id,
  t2.immediate_parent_role_id as immediate_parent_role_id,t2.user_id as user_child_id,t2.user_contact_id as user_child_contact_id,t2.user_role as user_child_role,t2.user_role_id as user_child_role_id,t2.user_agency_id as user_child_agency_id,111 as level
 from tbl4 t4 join finaltbl2 t2 on t4.child_agency_id=t2.user_agency_id where t2.level=0) s order by user_agency_id,user_child_agency_id""")
      
 println("**********5")
 parentagdf2.createOrReplaceTempView("tbl5")
 interdf.printSchema
 parentagdf2.printSchema
       val dfs1 = Seq(df1, parentagdf2)
      val finaldf1 = dfs1.reduce(_ union _)
      finaldf1.createOrReplaceTempView("tbl5")

   val finaldf =     spark.sql("""select distinct user_id as userid,user_child_id as userchildid,user_contact_id as usercontactid,
         user_child_contact_id as userchildcontactid,user_role as userrole,user_child_role as userchildrole,user_role_id as userroleid,user_child_role_id as userchildroleid,
immediate_parent_role_id as immediate_parentroleid,user_agency_id as useragencyid,user_child_agency_id as userchildagencyid, cast(level as  
integer ) as level,current_timestamp as load_dt ,current_timestamp as upd_dt from tbl5""")
finaldf.printSchema

      val pond_table = spark.sparkContext.getConf.get("spark.pond.database").trim() + "." + spark.sparkContext.getConf.get("spark.pond.table").trim()
      spark.sql("REFRESH TABLE " + pond_table)

       val targetFinal = spark.sparkContext.getConf.get("spark.target.location").trim()
       finaldf.repartition(15).write.mode("Overwrite").parquet(targetFinal)
      spark.sql("REFRESH TABLE " + pond_table)
      
      spark.sql("insert overwrite table vv_db.hvtb_mart_crm_userrole_hierarchy_rel select * from vv_db.hvtb_mart_crm_userrole_hierarchy_rel_temp")
      
    } finally {
      println("Test")
    }
  }
}

