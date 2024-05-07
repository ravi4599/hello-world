package com.virginvoyages.mxp.sailorexperiences

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType, DateType, LongType };
import java.text.SimpleDateFormat
import java.sql.Date
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import org.apache.hadoop.fs._;
import scala.util.{Try,Success,Failure}

import com.virginvoyages.metadataframework.ManageMetadata
import org.apache.spark.sql._
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.util.Properties



object CreateSurveyFile {

  /**
   * Initialize the logger
   */
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]) {

val sparkSession = SparkSession
			.builder()
			.enableHiveSupport()
			.getOrCreate()
val sc = sparkSession.sparkContext

val sparkConfiguration = sparkSession.sparkContext.broadcast(sparkSession.sparkContext.getConf.getAll.toMap)

val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, sparkSession)
    metadata.productIterator.foreach(println)
    val batch_id1 = metadata._1
    val batch_instance_id1 = metadata._2
    val batch_start_tme = metadata._3
    val batch_end_tme = metadata._4
    val part_read_start = metadata._5
    val part_read_end = metadata._6
    val start_execution_time = metadata._7
    val part_write_date = metadata._8

    
      try {
      /**
       * Read Reservation data
       */	  
import sparkSession.sqlContext.implicits._

println("""inside the code""")


//Check Condition 
def AssignFlag(FlagVal: => String):(String,Integer) = {
var vygFlag = "N"
try {vygFlag = FlagVal} catch {case e: NoSuchElementException => {vygFlag = "N"} } 
val vygFlagChk = if (vygFlag == "Y") 1 else 0
return(vygFlag,vygFlagChk)
}

def AssignConfigVal(ConfigVal: => String):(String) = {
var RetVal = "".toString
try {RetVal = ConfigVal} catch {case e: NoSuchElementException => {RetVal = ""} }
return(RetVal)
}
val debugFlag = AssignConfigVal(sparkSession.sqlContext.sparkContext.getConf.get("spark.sailorsurvey.debugFlag"))
//val promoCode = AssignConfigVal(sparkSession.sparkContext.getConf.get("spark.sailorsurvey.promocode"))

val (postvygFlag,postvygFlagChk) = AssignFlag(sparkSession.sqlContext.sparkContext.getConf.get("spark.sailorsurvey.PostVygFlag")) 
val (firstmtFlag,firstmtFlagChk) = AssignFlag(sparkSession.sqlContext.sparkContext.getConf.get("spark.sailorsurvey.FirstMtFlag"))
val (remiFlag,remiFlagChK)       = AssignFlag(sparkSession.sqlContext.sparkContext.getConf.get("spark.sailorsurvey.RemiFlag")) 

//Only one flag is allowed to be true
val ChkOneFlagYes = postvygFlagChk + firstmtFlagChk + remiFlagChK
if (ChkOneFlagYes == 1) 
{
//Column used for filtering Dataframe
val ColToBeUsed = if (postvygFlagChk == 1) "IsPostVygFlag" else if (firstmtFlagChk == 1) "IsFirstMtFlag" else "IsRemiFlag"
val ColToBeUsedToCol = col(ColToBeUsed)
log.info("ColToBeUsed ==> "+ColToBeUsed)


//by default var VoyageCode = sparkSession.sqlContext.sparkContext.getConf.get("spark.sailorsurvey.VoyageCode")
//this is mandatory val ShipCode = sparkSession.sqlContext.sparkContext.getConf.get("spark.sailorsurvey.ShipCode")
//var VoyageCode = "SC2011085NCM"
//val ShipCode = "SC"
	
val convStr = udf((arr: Seq[String]) => arr.mkString(","))

var VoyageCode = sparkSession.sparkContext.getConf.get("spark.sailorsurvey.VoyageCode")
//val ShipCode = sparkSession.sqlContext.sparkContext.getConf.get("spark.sailorsurvey.ShipCode")
var ShipCode = sparkSession.sqlContext.sparkContext.getConf.get("spark.sailorsurvey.ShipCode")
val subDatedf = sparkSession.sparkContext.getConf.get("spark.sailorsurvey.daysDelay")
val template = sparkSession.sparkContext.getConf.get("spark.sailorsurvey.template")
//val csvpath = sparkSession.sparkContext.getConf.get("spark.sailorsurvey.csvpath")




//val ShipCode = "SC"
var subDate = 2
	
// optional flag by default make it 'N' val manualFlag = sparkSession.sqlContext.sparkContext.getConf.get("spark.sailorsurvey.ManualFlag") 
//assigning manual flag
val manualFlag = sparkSession.sqlContext.sparkContext.getConf.get("spark.sailorsurvey.ManualFlag")
if (!manualFlag.isEmpty) {
        val manualFlag = sparkSession.sqlContext.sparkContext.getConf.get("spark.sailorsurvey.ManualFlag")
        }
		else{
          val manualFlag = "N"
        }
if (!subDatedf.isEmpty) {
        subDate = sparkSession.sparkContext.getConf.get("spark.sailorsurvey.daysDelay").toInt
      }
else{
    subDate = 2
  }
       val pguser=sparkSession.sparkContext.getConf.get("spark.target.user").trim()
       val pgpassword=sparkSession.sparkContext.getConf.get("spark.target.password").trim()
       val jdbcUrl = sparkSession.sparkContext.getConf.get("spark.target.con.url").trim() 
       val tableName =  sparkSession.sparkContext.getConf.get("spark.target.ops.table").trim()
       val jdbcDriver = sparkSession.sparkContext.getConf.get("spark.target.con.driver").trim()
       val con_format = sparkSession.sparkContext.getConf.get("spark.target.con.format").trim()
       val op_mode = sparkSession.sparkContext.getConf.get("spark.target.ops.table.mode").trim() 

  
if (manualFlag == "N") {  
//val subDatedf = sparkSession.sparkContext.getConf.get("spark.sailorsurvey.daysDelay")
     
//val subDate = 2 


/*val voyageSailorDf = sparkSession.sqlContext.read.format("org.apache.phoenix.spark")
.option("inferSchema", "true")
.option("table", sparkSession.sqlContext.sparkContext.getConf.get("spark.sailorsurvey.table.name"))
.option("header", "true")
.option("zkUrl", sparkSession.sqlContext.sparkContext.getConf.get("spark.zkurl")).load().select("SELECTEDSAILINGGROUP_VOYAGEID","SELECTEDSAILINGGROUP_END","IsPostVygFlag","IsFirstMtFlag","IsRemiFlag").withColumn("newDT",to_date($"SELECTEDSAILINGGROUP_END","yyyy-MM-dd")).filter(($"newDT" <= date_sub(current_date(),subDate)) && (ColToBeUsedToCol === "N")  && ($"CRUISELINE_SHIPCODE" === ShipCode))*/

val voyageSailorDf = sparkSession.read.format(con_format).option("driver",jdbcDriver).option("dbtable",tableName).option("url",jdbcUrl)
        .option("user",pguser).option("password",pgpassword).load().select("SELECTEDSAILINGGROUP_VOYAGEID","SELECTEDSAILINGGROUP_END","IsPostVygFlag","IsFirstMtFlag","IsRemiFlag","LOADDATETIME").withColumn("newDT",to_date($"SELECTEDSAILINGGROUP_END","yyyy-MM-dd")).filter(($"newDT" === date_sub(current_date(),subDate)) && (ColToBeUsedToCol === "N")  && ($"CRUISELINE_SHIPCODE" === ShipCode))

//if primary key is shipcode, start & end date         
/* val voyageSailorDf = sparkSession.read.format(con_format).option("driver",jdbcDriver).option("dbtable",tableName).option("url",jdbcUrl)
        .option("user",pguser).option("password",pgpassword).load().select("SELECTEDSAILINGGROUP_VOYAGEID","CRUISELINE_SHIPCODE","SELECTEDSAILINGGROUP_START","SELECTEDSAILINGGROUP_END","IsPostVygFlag","IsFirstMtFlag","IsRemiFlag").withColumn("newDT",to_date($"SELECTEDSAILINGGROUP_END","yyyy-MM-dd")).filter(($"newDT" <= date_sub(current_date(),subDate)) && (ColToBeUsedToCol === "N")  && ($"CRUISELINE_SHIPCODE" === ShipCode))*/
        
      print("voyage Sailor DF ...")  
voyageSailorDf.show(false)

voyageSailorDf.createOrReplaceTempView("voyageSailorDfTbl")

val VoyageCodefromDF = sparkSession.sql("""
select SELECTEDSAILINGGROUP_VOYAGEID from 
(select *,row_number() over (order by SELECTEDSAILINGGROUP_END,LOADDATETIME desc) as rownum 
from voyageSailorDfTbl) OuterQry
where rownum = 1""")

print("Dedup voyage Sailor DF ...")  
VoyageCodefromDF.show(1,false)
VoyageCode = VoyageCodefromDF.collect().map(_.getString(0)).mkString(",")

//if (length(VoyageCode)= 0) then sparkSession.stop();

}
/*end of manual flag*/

if (!VoyageCode.isEmpty)
{
log.info("---------------------------------------------------------  dataframe call  --------------------------------------------------")


/*val ReservationDF = sparkSession.sqlContext.read.format(sparkSession.sqlContext.sparkContext.getConf.get("spark.voyage.format")).option("inferSchema", "true").option("table", sparkSession.sqlContext.sparkContext.getConf.get("spark.reservation.table.name")).option("header", "true").option("zkUrl", sparkSession.sqlContext.sparkContext.getConf.get("spark.zkurl")).load().select("RESERVATIONID","SELECTEDSAILINGGROUP_VOYAGEID","LOYALTYMEMBERSHIPID","EMAIL","CRUISELINE_SHIPCODE","CRUISELINE_SHIPNAME","SELECTEDSAILINGGROUP_VOYAGEID","SELECTEDSAILINGGROUP_START","SELECTEDSAILINGGROUP_END").filter($"SELECTEDSAILINGGROUP_VOYAGEID" === VoyageCode && $"CRUISELINE_SHIPCODE" === ShipCode)*/

//change end for firstmates


//val promoCode = sparkSession.sparkContext.getConf.get("spark.sailorsurvey.promocode")
//val promoCode = "SCDMYVTIERNA|VPFREEVF2020|NYSWTPAUSE2020|SCDMYVTIERINTL|SWTDOVFUP2020|MIASWTPAUSE2020" 
//Changed from promotion_dim table to crm_oppurtunity table
//val promoCodeStr = promoCode.split(",").map(x=>"\""+x+"\"").mkString(",")
/*val SqlStrFirmates = """
select res_id, guest_id from (
select pdim.res_id,pdim.guest_id, row_number() over (partition by pdim.res_id,pdim.guest_id order by pdim.rec_end_dttm desc, pdim.etl_ld_dt desc) rownum
from vv_db.hvtb_nbx_core_sw_promotion_dim pdim 
join vv_db.hvtb_nbx_core_sw_promotion_lkp plkp on pdim.promotion_id  = plkp.promotion_id
where plkp.promo_code in (""" + promoCodeStr + """)
) outerQry where rownum = 1
"""*/


val promoCode = sparkSession.sparkContext.getConf.get("spark.sailorsurvey.promocode")
val SqlStrFirmates = """select cast (reservation_number__c as INT) as reservation_number__c,promo_code__c from vv_db.hvtb_nbx_core_crm_opportunity where upper(promo_code__c) rlike ("""" + promoCode+"""")"""

val PromoCodeDF = sparkSession.sql(SqlStrFirmates)
PromoCodeDF.createOrReplaceTempView("PromoCodeDFbl")

//change end for firstmates
//val addFilter = if (firstmtFlagChk == 1) " join PromoCodeDFbl prom on prom.res_id = res.res_id and prom.guest_id = guest.guest_id " else ""


val addFilter = if (firstmtFlagChk == 1) " join PromoCodeDFbl prom on prom.reservation_number__c = res.src_res_id " else ""
val addFilterwhere = if (firstmtFlagChk == 1) " and guest.guest_seqn = 1 " else ""
  
val ResSqlStrTmp = """
select cast (guest.client_id as INT) as LOYALTYMEMBERSHIPID, guest.email as EMAIL, ship.ship as CRUISELINE_SHIPCODE, ship.ship_name as CRUISELINE_SHIPNAME , pkgdim.package_code as SELECTEDSAILINGGROUP_VOYAGEID, to_date(sail.sail_date_from) as SELECTEDSAILINGGROUP_START, to_date(sail.sail_date_to) as SELECTEDSAILINGGROUP_END, res.src_res_id as RESERVATIONID,sail.sail_port_from as EMBARK_PORT,guest.guest_id as GUEST_ID
from vv_db.hvtb_nbx_core_sw_package_dim pkgdim
join vv_db.hvtb_nbx_core_sw_sail_dim sail on pkgdim.src_sail_id  = sail.src_sail_id
join vv_db.hvtb_nbx_core_sw_reservation_dim res on res.src_sail_id  = sail.src_sail_id and res.res_status in ('BK','CL','TM')
join vv_db.hvtb_nbx_core_sw_res_guest_rel rel on res.res_id = rel.res_id
join vv_db.hvtb_nbx_core_sw_guest_dim guest on guest.guest_id = rel.guest_id
join vv_db.hvtb_nbx_core_sw_ship_dim ship on sail.ship_id = ship.ship_id and ship.ship = '"""+ShipCode+"""'"""+addFilter+"""
where pkgdim.rec_end_dttm = '9999-12-31 00:00:00' and pkgdim.package_class = 'VOYAGE' and pkgdim.package_code = '"""+VoyageCode + """'
and sail.rec_end_dttm = '9999-12-31 00:00:00' 
and (res.rec_end_dttm = '9999-12-31 00:00:00' or res.rec_end_dttm > sail.sail_date_from)
and (guest.rec_end_dttm = '9999-12-31 00:00:00' or guest.rec_end_dttm > sail.sail_date_from)
and guest.client_id is not null
""" + addFilterwhere

 
val ReservationDF = sparkSession.sql(ResSqlStrTmp)
ReservationDF.printSchema
ReservationDF.createOrReplaceTempView("ReservationDFtbl")

val SeawareDF = sparkSession.sql("""select src_res_id,prom.guest_id,promo_lkp.promo_code from vv_db.hvtb_nbx_core_sw_reservation_dim res left join vv_db.hvtb_nbx_core_sw_promotion_dim prom
on res.res_id = prom.res_id
left join vv_db.hvtb_nbx_core_sw_promotion_lkp promo_lkp on prom.promotion_id = promo_lkp.promotion_id
where date(prom.rec_end_dttm) = '9999-12-31' and date(res.rec_end_dttm) = '9999-12-31'
and promo_lkp.promo_code = 'DEEP BLUE PERK'
group by src_res_id,prom.guest_id,promo_lkp.promo_code""")

SeawareDF.createOrReplaceTempView("SeawareDFtbl")


val PersonAccDF = sparkSession.sql("""select * from (select person.person_id, person.booking_reference, person.booking_cruise_number, person.mega_rockstar_flag, person_acc_items.debit_amount, person_acc_items.org_unit_quick_code, person_acc_items.period_id, person.ship_code from ( select * from ( select *, ROW_NUMBER() over(PARTITION by PERSON_ACCOUNT_ID, PERIOD_ID, account_record_description order by last_changed desc) as rn from shipdw.hvtb_parse_mxp_person_account_items where org_unit_quick_code = '98501' ) person_items where rn = 1) person_acc_items join ( select * from ( select *, ROW_NUMBER() over(PARTITION by person_account_id, org_unit_id order by last_changed desc) as rn from shipdw.hvtb_parse_mxp_person_account where status_id = 4 and ROUTE_TO_ACCOUNT_ID is null) acc where rn = 1) person_acc on person_acc_items.person_account_id = person_acc.person_account_id join ( select CRUISE_ID, cruise_number, start_date, end_date, org_unit_id from ( select CRUISE_ID, cruise_number, org_unit_id, cast(start_date as date) as start_date, cast(end_date as date) as end_date, cruise_type, row_number() over (partition by CRUISE_ID, org_unit_id order by last_changed desc) as rownum from shipdw.hvtb_parse_mxp_cruise where cruise_type = 'C' and active = true) c where rownum = 1) cruise on person_acc_items.period_id = cruise.cruise_id and person_acc.org_unit_id = cruise.org_unit_id and cast(person_acc_items.business_day as date) between cruise.start_date and cruise.end_date join ( select * from ( select *, ROW_NUMBER() over(PARTITION by ORG_UNIT_ID order by last_changed desc) as rn from shipdw.hvtb_parse_mxp_org_units where ORG_UNIT_ABBREVIATION in ('VL', 'RS') )org_unit where rn = 1) org on cruise.ORG_UNIT_ID = org.ORG_UNIT_ID left join ( select * from ( select booking_reference, seaware_id, booking_cruise_number, mega_rockstar_flag, person_id, voyage_skey, substring(booking_cruise_number, 1, 2) as ship_code, row_number() over (partition by person_id, booking_reference, booking_cruise_number order by upd_dt desc) as rn from shipdw.hvtb_mart_dim_person where person_type <> '9' and booking_status <> '3') p where rn = 1) person on person_acc.person_id = cast(person.person_id as int) and cruise.cruise_number = person.booking_cruise_number) wifi """)

PersonAccDF.createOrReplaceTempView("PersonAccDFtbl")

val WiFiDF = sparkSession.sql(""" select SeawareDFtbl.src_res_id,SeawareDFtbl.guest_id,
case when  SeawareDFtbl.promo_code='DEEP BLUE PERK' or PersonAccDFtbl.mega_rockstar_flag = true or PersonAccDFtbl.debit_amount <> 0 
then 'PremiumWiFi'
else 'WiFi' end as WIFI_STATUS
from SeawareDFtbl SeawareDFtbl left join PersonAccDFtbl PersonAccDFtbl on SeawareDFtbl.src_res_id=
PersonAccDFtbl.booking_reference""")

WiFiDF.createOrReplaceTempView("WifiDFtbl")


val CombinedDF= sparkSession.sql("""select LOYALTYMEMBERSHIPID, EMAIL,CRUISELINE_SHIPCODE, CRUISELINE_SHIPNAME , SELECTEDSAILINGGROUP_VOYAGEID, SELECTEDSAILINGGROUP_START,SELECTEDSAILINGGROUP_END,RESERVATIONID,EMBARK_PORT,ReservationDFtbl.GUEST_ID,WifiDFtbl.WIFI_STATUS from ReservationDFtbl  left join WifiDFtbl on ReservationDFtbl.RESERVATIONID=WifiDFtbl.src_res_id AND ReservationDFtbl.GUEST_ID = WifiDFtbl.guest_id """)

val resSeawareDF = CombinedDF.withColumn("SEAWARE_ID",$"LOYALTYMEMBERSHIPID").withColumn("EMAIL_ADDRESS",$"EMAIL").withColumn("FILE_GENERATION_DATE", current_date()).withColumn("SHIPCODE",$"CRUISELINE_SHIPCODE").withColumn("SHIPNAME",$"CRUISELINE_SHIPNAME").withColumn("VOYAGEID",$"SELECTEDSAILINGGROUP_VOYAGEID").withColumn("VOYAGE_START",$"SELECTEDSAILINGGROUP_START").withColumn("VOYAGE_END",$"SELECTEDSAILINGGROUP_END").select("RESERVATIONID","SEAWARE_ID","EMAIL_ADDRESS","FILE_GENERATION_DATE","SHIPCODE","SHIPNAME","VOYAGEID","VOYAGE_START","VOYAGE_END","EMBARK_PORT","WIFI_STATUS")


resSeawareDF.createOrReplaceTempView("resSeawareDFTbl")

if (debugFlag == "Y")
{
val resSeawareDFcnt = resSeawareDF.count()
log.info("resSeawareDFcnt cnt ==> " + resSeawareDFcnt)
}

/* unique list of reservations to reduce the records in  the join*/
val UniqueSeawareResDF = sparkSession.sql("""select RESERVATIONID from resSeawareDFTbl group by RESERVATIONID """)

/* cache the data so this can be used - do not expect more than few thousand records per Voyage*/
UniqueSeawareResDF.persist(StorageLevel.MEMORY_AND_DISK)

UniqueSeawareResDF.createOrReplaceTempView("UniqueSeawareResDFTbl")

/* Get itinerary ports */

val cityDF = sparkSession.sql("""select city_id, city_name as ITINERARY_PORTS,city_abbreviation from 
(select city_id, city_name,city_abbreviation, 
row_number() over (partition by city_id order by last_changed desc) as rownum 
from shipdw.hvtb_parse_mxp_cities  cit where active = true) outerQry
where rownum = 1 """)
cityDF.createOrReplaceTempView("cityDFTbl")

val itineraryDF = sparkSession.sql(""" select CRUISE_ID, PORT_ID from 
(select ci.CRUISE_ID, ci.PORT_ID, row_number() over (partition by ci.CRUISE_ITINERARY_ID order by last_changed desc) as rownum  
from shipdw.hvtb_parse_mxp_cruise_itinerary  ci) outerQry
where rownum = 1 """)
itineraryDF.createOrReplaceTempView("itineraryDFtbl")


/*val cruiseDF = sparkSession.sql(""" select CRUISE_ID, cruise_number from
(select c.CRUISE_ID, c.cruise_number, row_number() over (partition by c.CRUISE_ID order by row_counter desc, batchtime desc) as rownum  
from shipdw.hvtb_parse_mxp_cruise c where c.DELETED = 0 and c.cruise_number = '"""+VoyageCode+"""') outerQry
where rownum = 1 """)*/

val cruiseDF = sparkSession.sql(""" select CRUISE_ID, cruise_number from 
(select c.CRUISE_ID, c.cruise_number, row_number() over (partition by c.CRUISE_ID order by last_changed desc) as rownum  
from shipdw.hvtb_parse_mxp_cruise c where c.cruise_type = 'C' and c.active = true and c.cruise_number = '"""+VoyageCode+"""') outerQry
where rownum = 1 """)

cruiseDF.createOrReplaceTempView("cruiseDFtbl")


val IniteraryPortDF = sparkSession.sql("""Select c.cruise_number,cit.city_abbreviation, concat("\"",cit.ITINERARY_PORTS,"\"") as port_code
FROM itineraryDFtbl ci join
cruiseDFtbl c on ci.CRUISE_ID = c.CRUISE_ID
join cityDFTbl cit on ci.PORT_ID  = cit.CITY_ID
group by c.cruise_number, cit.ITINERARY_PORTS, cit.city_abbreviation
""")

val IniteraryPortByCruiseDF = IniteraryPortDF.withColumn("ITINERARY_PORTS_TEMP", collect_set($"port_code").over(Window.partitionBy($"cruise_number"))).withColumn("ITINERARY_PORTS", convStr($"ITINERARY_PORTS_TEMP")).select("cruise_number","city_abbreviation","ITINERARY_PORTS").dropDuplicates


/* expect data for only one voyage so should be very small */
IniteraryPortByCruiseDF.persist(StorageLevel.MEMORY_AND_DISK)
IniteraryPortByCruiseDF.createOrReplaceTempView("IniteraryPortByCruiseDFtbl")

/* Get itinerary ports done */

/* check if the reservation is boarded */


val resDF = sparkSession.sql("""
select VXPRes.reservationnumber, VXPRes.reservationid from
(select res.reservationnumber, res.reservationid, row_number() over (partition by res.reservationnumber order by res.lastmodifieddate desc, res.batchtime desc) as rownum from shipdw.hvtb_parse_vxp_reservation res) VXPRes 
join UniqueSeawareResDFTbl SeawareRes on SeawareRes.RESERVATIONID = VXPRes.reservationnumber 
where rownum = 1
""")
resDF.createOrReplaceTempView("reservationDFtbl")



val guestDF = sparkSession.sql("""select reservationguestid, reservationid from
(Select guest.reservationid, guest.reservationguestid,  row_number() over (partition by guest.reservationid, guest.reservationguestid order by guest.lastmodifieddate desc, guest.batchtime desc) as rownum from shipdw.hvtb_parse_vxp_reservationguest guest) OuterQry
where rownum = 1
""")

guestDF.createOrReplaceTempView("guestDFtbl")



/* status ASHORE */


val statusDF = sparkSession.sql("""
select personid from 
(select sts.personid, row_number() over (partition by sts.personid order by sts.lastmodifieddate desc, sts.batchtime desc) as rownum  
from shipdw.hvtb_parse_vxp_personstatus sts
where sts.statustypecode = 'BS' and sts.isdeleted = 0) OuterQry
where rownum = 1
""")
statusDF.createOrReplaceTempView("statusDFtbl")


val BoardingDF = sparkSession.sql("""
select res.reservationnumber, res.reservationid, guest.reservationguestid 
from reservationDFtbl res 
join guestDFtbl guest on res.reservationid = guest.reservationid
join statusDFtbl sts on guest.reservationguestid = sts.personid
group by res.reservationnumber, res.reservationid, guest.reservationguestid """)


/* cache the data so this can be used - do not expect more than few thousand records per Voyage*/


BoardingDF.persist(StorageLevel.MEMORY_AND_DISK)
UniqueSeawareResDF.unpersist()

BoardingDF.createOrReplaceTempView("BoardingDFTbl")

if (debugFlag == "Y")
{
val BoardingDFcnt = BoardingDF.count()
log.info("BoardingDFcnt cnt ==> " + BoardingDFcnt)
}
      
val personbkgDF = sparkSession.sql(""" 
select activitybookingid, personid from
(select bkg.activitybookingid, bkg.personid , row_number() over (partition by bkg.personid,bkg.activitybookingid order by bkg.lastmodifieddate desc, bkg.batchtime desc) as rownum  
from shipdw.hvtb_parse_vxp_activitypersonbooking bkg
where bkg.isdeleted = 0) OuterQry
where rownum = 1
""")
personbkgDF.createOrReplaceTempView("personbkgDFTbl")


val actbkgDF = sparkSession.sql(""" 
select activitybookingid, activitycode,reservationnumber,status,activitygroupcode from 
(select actbkg.activitybookingid, actbkg.activitycode,reservationnumber,status,activitygroupcode, row_number() over (partition by actbkg.activitybookingid order by actbkg.lastmodifieddate desc) as rownum from shipdw.hvtb_parse_vxp_ars_activitybooking actbkg
) OuterQry
where rownum = 1 and upper(status) = 'CONFIRMED' and OuterQry.activitygroupcode= 'PA'
""")
actbkgDF.createOrReplaceTempView("actbkgDFDFTbl")

/* get unique shorex code and name */


val ShorexCodeNameDF = sparkSession.sql("""
select code ,name,portcode from
(select code ,name,portcode, 
row_number() over (partition by code order by lastmodifieddate  desc) as rownum 
from shipdw.hvtb_parse_vxp_ars_activity 
where  activitygroupcode = 'PA'
) outerQry
where rownum = 1
""")
ShorexCodeNameDF.createOrReplaceTempView("ShorexCodeNameDFTbl")

log.info("---------------------------------------------------------  entertaintment csvload --------------------------------------------------")

/*adding events from entertaintment csv*/
//val lkpfetchdf = sparkSession.read.parquet("s3://vv-prod-emr-cluster/data/temp/csv_output/*")

val lkpfetchdf = sparkSession.sql("""select * from shipdw.hvtb_nbx_lkp_entertainment""")

lkpfetchdf.printSchema
lkpfetchdf.createOrReplaceTempView("tmp")

val lookupDF = sparkSession.sql("""select  TO_DATE(Start_Date) as start_date ,TO_DATE(End_date) as end_date,case when Ship = 'SCL' then 'SC' when Ship = 'VAL' then 'VL' when Ship = 'RES' then 'RS' WHEN  Ship = 'BR' then 'BR' end as Ship ,Event,EventCode from tmp""")


//val lookupDF = sparkSession.sql("""select  TO_DATE(CAST(UNIX_TIMESTAMP(start_date, 'yyyy/MM/dd') AS TIMESTAMP)) as start_date ,TO_DATE(CAST(UNIX_TIMESTAMP(end_date, 'yyyy/MM/dd') AS TIMESTAMP)) as end_date,case when Ship = 'SCL' then 'SC' when Ship = 'VAL' then 'VL' when Ship = 'RES' then 'RS' WHEN  Ship = 'BR' then 'BR' end as Ship ,Event,EventCode from tmp where start_date <> 'TBD' and end_date <> 'TBD'""")

//val lkpfinal = lookupDF.withColumn("EventCode", collect_set($"EventCode").over(Window.partitionBy($"Ship",$"end_date"))).withColumn("EventCode",convStr($"EventCode")).select("start_date","end_date","Ship","Event","EventCode").dropDuplicates()

lookupDF.createOrReplaceTempView("entertainmentlkp")
lookupDF.show(1,false)

val ShorexSqlStr = ("""
select resSeaware.SEAWARE_ID, resSeaware.EMAIL_ADDRESS, resSeaware.FILE_GENERATION_DATE, resSeaware.SHIPCODE, resSeaware.SHIPNAME, resSeaware.VOYAGEID ,resSeaware.VOYAGE_START, resSeaware.VOYAGE_END, concat("\"",shrx.name,"\"") as ShorexName, nvl((concat("[",ItnryPort.ITINERARY_PORTS,"]")),'[]') as ITINERARY_PORTS,EMBARK_PORT,WIFI_STATUS,lkp.EventCode as EVENTCODE
from resSeawareDFTbl resSeaware
join BoardingDFTbl brdg  on resSeaware.RESERVATIONID = brdg.reservationnumber
left join IniteraryPortByCruiseDFtbl ItnryPort on resSeaware.VOYAGEID = ItnryPort.cruise_number
left join personbkgDFTbl personbkg on  brdg.reservationguestid = personbkg.personid
left join actbkgDFDFTbl actbkg on  personbkg.activitybookingid = actbkg.activitybookingid and actbkg.reservationnumber = brdg.reservationnumber
left join ShorexCodeNameDFTbl shrx on actbkg.activitycode = shrx.code and ItnryPort.city_abbreviation = shrx.portcode
left join entertainmentlkp lkp on resSeaware.shipcode = lkp.ship and to_date(lkp.end_date) >= resSeaware.VOYAGE_END and to_date(lkp.start_date) <= resSeaware.VOYAGE_START
""")


val ShorexResDF = sparkSession.sql(ShorexSqlStr)


val eventfetchdf = ShorexResDF.withColumn("EVENTCODE", collect_set($"EVENTCODE").over(Window.partitionBy($"SEAWARE_ID",$"SHIPCODE"))).withColumn("EVENTCODE",convStr($"EVENTCODE")).select("SEAWARE_ID","EMAIL_ADDRESS","FILE_GENERATION_DATE","SHIPCODE","SHIPNAME","VOYAGEID","VOYAGE_START","VOYAGE_END","ITINERARY_PORTS","ShorexName","EMBARK_PORT","WIFI_STATUS","EVENTCODE").dropDuplicates()

val ShorexNameDF = eventfetchdf.withColumn("ShorexTmp", collect_set($"ShorexName").over(Window.partitionBy($"SEAWARE_ID"))).withColumn("SHOREX_NAME",convStr($"ShorexTmp")).select("SEAWARE_ID","EMAIL_ADDRESS","FILE_GENERATION_DATE","SHIPCODE","SHIPNAME","VOYAGEID","VOYAGE_START","VOYAGE_END","ITINERARY_PORTS","SHOREX_NAME","EMBARK_PORT","WIFI_STATUS","EVENTCODE").dropDuplicates()

ShorexNameDF.createOrReplaceTempView("ShorexNameDFTbl")

val finalTbl = sparkSession.sql("""Select SEAWARE_ID,EMAIL_ADDRESS,FILE_GENERATION_DATE,SHIPCODE,SHIPNAME,VOYAGEID,VOYAGE_START,VOYAGE_END,ITINERARY_PORTS, case when trim(SHOREX_NAME) is null or length(SHOREX_NAME) = 0 then "[]" else concat("[",SHOREX_NAME,"]") end as SHOREX_NAME,EMBARK_PORT,case when trim(WIFI_STATUS) is null then "WiFi" else WIFI_STATUS end as WIFI_STATUS,case when trim(EVENTCODE) is null or length(EVENTCODE) = 0 then "[]" else concat("[",EVENTCODE,"]") end as EVENTCODE, """+ template +""" as template from ShorexNameDFTbl""")
val finalTbltmp= finalTbl.dropDuplicates()
finalTbltmp.createOrReplaceTempView("finalTbltmp")
val Rem_clientid =sparkSession.sparkContext.getConf.get("spark.sailorsurvey.removeclientid")
val finaldataquery = """select * from finalTbltmp where SEAWARE_ID not in ("""" + Rem_clientid+"""")"""
val finalTblUpd = sparkSession.sql(finaldataquery)

finalTblUpd.show(4,false)

if (debugFlag == "Y")
{
val finalTblDFcnt = finalTbltmp.count()
log.info("finalTblDFcnt cnt ==> " + finalTblDFcnt)
}
 

 
 if(!finalTblUpd.take(1).isEmpty) {
       
/* write the data to the target location */
 //val filename1 = "Virgin_sample"
 
 
val fileName = sparkSession.sparkContext.getConf.get("spark.sailorsurvey.fileNamePrefix").concat("_").concat(VoyageCode).concat("_").concat(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").format(LocalDateTime.now)).concat(".txt")

print("File Name ..."+ fileName)

//val fileName = VoyageCode.concat(sparkSession.sparkContext.getConf.get("spark.sailorsurvey.fileNamePrefix")).concat("__").concat(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").format(LocalDateTime.now)).concat(".txt")

//val hdfsWritePath= "hdfs:///data/core/SailorSurvey/FTPFile/"


print("Writing following records to HDFS path ...")
finalTblUpd.show(10,false)

val cnt = finalTblUpd.count
print("record count   "+ cnt)

/*write to hdfs path */

val hdfsWritePath = sparkSession.sqlContext.sparkContext.getConf.get("spark.sailorsurvey.tgtLocation")
finalTblUpd.repartition(1).write.format("csv").option("delimiter", "|").option("header", "true").mode("overwrite").option("quote", "\u0000").save(s"$hdfsWritePath")

/* write to gcs path */
//val gcsWritePath = sparkSession.sqlContext.sparkContext.getConf.get("spark.sailorsurvey.tgtLocation")
//finalTblUpd.repartition(1).write.format("csv").option("delimiter", "|").option("header", "true").mode("overwrite").option("quote", "\u0000").save(s"$gcsWritePath") 

/* rename filename */

val fs = FileSystem.get(sc.hadoopConfiguration);

//val file = fs.globStatus(new Path(s"$gcsWritePath/part*"))(0).getPath().getName();
val file = fs.globStatus(new Path(s"$hdfsWritePath/part*"))(0).getPath().getName();

print("globStatus file ..."+file)
//fs.rename(new Path(s"$gcsWritePath" + file), new Path(s"$gcsWritePath" + fileName))
fs.rename(new Path(s"$hdfsWritePath" + file), new Path(s"$hdfsWritePath" + fileName))
print("file rename success ")

                      }
					  
					  else {
					  log.info("..............Generated file is empty...................");
					  }


val updStr = "select '"+VoyageCode +"' as SELECTEDSAILINGGROUP_VOYAGEID, 'Y' as "+ ColToBeUsed

//val updStr = "select '"+VoyageCode +"' as SELECTEDSAILINGGROUP_VOYAGEID, '"+ShipCode+"' as  CRUISELINE_SHIPCODE, 'Y' as "+ ColToBeUsed

val VoyageTblUpdateDF= sparkSession.sql(updStr)

/*VoyageTblUpdateDF.write
  .format(sparkSession.sqlContext.sparkContext.getConf.get("spark.voyage.format")) 
  .mode(sparkSession.sqlContext.sparkContext.getConf.get("spark.voyage.mode"))
  .option("table", sparkSession.sqlContext.sparkContext.getConf.get("spark.voyage.table")) 
  .option("zkUrl", sparkSession.sqlContext.sparkContext.getConf.get("spark.zkurl"))
  .save()*/
  
  
  print("updating following records to postgre Voyage table ")
  
 var upd_qry=""
   
 log.info(" voyage code in where clause is "+ VoyageCode);
 log.info(" ColToBeUsed " + ColToBeUsed);
 try{
      log.info( "start reading from in try block ")
      Class.forName(jdbcDriver);
      val connObj = DriverManager.getConnection(jdbcUrl, pguser, pgpassword);
  
      if(ColToBeUsed== "IsPostVygFlag")
           upd_qry="UPDATE shipdw.HBTB_NBX_VOYAGE_SAILORSURVEY  SET  ISPOSTVYGFLAG = 'Y' WHERE  SELECTEDSAILINGGROUP_VOYAGEID = ?"
      else if(ColToBeUsed == "IsFirstMtFlag")
           upd_qry="UPDATE shipdw.HBTB_NBX_VOYAGE_SAILORSURVEY  SET  ISFIRSTMTFLAG = 'Y' WHERE  SELECTEDSAILINGGROUP_VOYAGEID = ?"
      else if(ColToBeUsed == "IsRemiFlag")
           upd_qry="UPDATE shipdw.HBTB_NBX_VOYAGE_SAILORSURVEY  SET  ISREMIFLAG = 'Y' WHERE  SELECTEDSAILINGGROUP_VOYAGEID = ?"
      log.info("update query  "+ upd_qry)
      val statement = connObj.prepareStatement(upd_qry)
      try{
          statement.setString(1,VoyageCode);
          val number_of_rows_updated = statement.executeUpdate();
          log.info("number_of_rows_updated  " +number_of_rows_updated)
      }
      finally{
          statement.close();
          }
      connObj.close();
  }
  catch {
      case e:SQLException => e.printStackTrace();
  }
  
   
/*
VoyageTblUpdateDF.write.format("org.apache.phoenix.spark").mode("overwrite").option("table","HBTB_NBX_VOYAGE_SAILORSURVEY") .option("zkUrl","10.15.2.206:2181/hbase-unsecure").save()
*/
}
else
{
  log.info("VoyageCode not fetched from HBTB_NBX_VOYAGE_SAILORSURVEY");
}
ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", sparkSession)
}
else {
	throw new Exception("Multiple Flags set to Y or no Flag set to Y")
	}
} catch {
case e: SQLException => {
e.printStackTrace(); log.info("HBase connectioin issue..please check HBase service");

}
case e: Exception => { log.info("******************in the catch of CreateSurveyFile ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace") }
case e: Exception => {
        ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", sparkSession);
        e.printStackTrace();
        throw e
      }
}


sparkSession.stop()
}
}