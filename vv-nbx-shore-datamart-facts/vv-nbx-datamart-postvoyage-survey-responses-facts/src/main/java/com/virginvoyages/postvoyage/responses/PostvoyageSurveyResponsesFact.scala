package com.virginvoyages.postvoyage.responses

import com.virginvoyages.scd.ChangeDataCapture.loadDimFact

import com.virginvoyages.metadataframework.ManageMetadata
import java.util.Date
import java.util.Properties
import java.sql.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ StructType, StructField, TimestampType, StringType, IntegerType, DateType, LongType, DoubleType, DecimalType };
import org.apache.spark.sql.expressions.Window
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import scala.collection.JavaConversions._
import org.apache.spark.sql.functions._
import scala.util.Try
import org.apache.spark.sql.DataFrame
import java.time.{ ZonedDateTime, ZoneId }
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{ Column, DataFrame }
import scala.collection.mutable.ArrayBuffer
import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.sql.SaveMode
import java.time.{LocalDate, ZoneId}

object PostvoyageSurveyResponsesFact {
 val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
  def main(args: Array[String]): Unit = {

    val currentDate = java.time.LocalDate.now
  	println("Current Date = " + currentDate)
  	val batchStartDate = currentDate.minusDays(1)
	  println("Batch Start Time = "+ batchStartDate + " " + "00:00:00")
	  val batchStartTime = batchStartDate + " " + "00:00:00"
    val batchEndTime = batchStartDate + " " + "23:59:59"


    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)

    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    
    val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
  metadata.productIterator.foreach(println)
  val batch_id1 = metadata._1
  val batch_instance_id1 = metadata._2
  val batch_start_tme = metadata._3
  val batch_end_tme = metadata._4
  val part_read_start = metadata._5
  val part_read_end = metadata._6
  val start_execution_time = metadata._7
  val part_write_date = metadata._8

    spark.conf.set("spark.sql.broadcastTimeout", 36000)
    spark.conf.set("spark.sql.hive.convertMetastoreParquet", "false")
	val processfilepath = spark.sparkContext.getConf.get("spark.target.processedfilepath").trim()

    import spark.implicits._

    try {
      val whereClause = s"""where batchtime >= from_unixtime(unix_timestamp('$batchStartTime', "yyyy-MM-dd' 'HH:mm:ss")) and batchtime <= from_unixtime(unix_timestamp('$batchEndTime', "yyyy-MM-dd' 'HH:mm:ss")) and part_dt>=to_date('$batchStartTime') and part_dt<=to_date('$batchEndTime')"""
      log.info("select * from %s %s".format(sc.getConf.get("spark.parse.dynamic.table"), whereClause))
	  
      val postvoyagedynamicdf = spark.sql("select * from %s %s".format(sc.getConf.get("spark.parse.dynamic.table"), whereClause)).withColumn("Qprefixlower", lower(trim(col("Qprefix")))).withColumn("Qprefixupper", upper(trim(col("Qprefix"))))
	  
	  postvoyagedynamicdf.createOrReplaceTempView("postvoyagedynamicdfTmpTblflg")
	  
	  val postvoyagedynamicdfFlag = spark.sql("""select *,case when (regexp_extract(lower(file_name), '(virginpostcruise)',0) = "virginpostcruise" or regexp_extract(lower(file_name), '(postcruise)',0) = "postcruise" ) then "virginpostcruise" when regexp_extract(lower(file_name),'(virginreminiscence)',0) = "virginreminiscence" then "virginreminiscence" else "NA" end as file_type from postvoyagedynamicdfTmpTblflg""")
	  
	  	  
      postvoyagedynamicdfFlag.createOrReplaceTempView("postvoyagedynamicdfTmpTbl")
		  
      val whereClause1 = s"""where batchtime >= from_unixtime(unix_timestamp('$batchStartTime', "yyyy-MM-dd' 'HH:mm:ss")) and batchtime <= from_unixtime(unix_timestamp('$batchEndTime', "yyyy-MM-dd' 'HH:mm:ss")) and part_date>=to_date('$batchStartTime') and part_date<=to_date('$batchEndTime')"""
	  log.info("select responseid as static_responseid, interview_start as static_interview_start, interview_end as static_interview_end, clientid as static_clientid, voyageid as static_voyageid, to_date(voyage_start) as static_voyage_start, to_date(voyage_end) as static_voyage_end, shipcode as static_shipcode, file_name as static_file_name from %s %s".format(sc.getConf.get("spark.parse.static.table"), whereClause1))
       
	  val postvoyagestaticdf = spark.sql("select responseid as static_responseid, interview_start as static_interview_start, interview_end as static_interview_end, clientid as static_clientid, voyageid as static_voyageid, to_date(voyage_start) as static_voyage_start, to_date(voyage_end) as static_voyage_end, shipcode as static_shipcode, file_name as static_file_name,emailaddress as static_emailaddress from %s %s".format(sc.getConf.get("spark.parse.static.table"), whereClause1)).withColumn("static_voyage_startdate",col("static_voyage_start")).withColumn("static_voyage_enddate",col("static_voyage_end"))
	  
	  postvoyagestaticdf.createOrReplaceTempView("postvoyagestaticdfTmpTblflg")
	  
	  //val postvoyagestaticdflag = spark.sql("""select static_responseid,static_interview_start,static_interview_end,static_clientid,static_voyageid,static_voyage_start,static_voyage_end,static_shipcode,static_file_name,to_date(static_voyage_startdate,"yyyy-MM-dd"),to_date(static_voyage_enddate,"yyyy-MM-dd"),case when regexp_extract(lower(static_file_name), '(virginpostcruise)',0) = "virginpostcruise" then "virginpostcruise" when regexp_extract(lower(static_file_name),'(virginreminiscence)',0) = "virginreminiscence" then "virginreminiscence" else null end as file_type from postvoyagestaticdfTmpTblflg""")
	  val postvoyagestaticdflag = spark.sql("""select static_responseid,static_interview_start,static_interview_end,static_clientid,static_voyageid,static_emailaddress,static_voyage_start,static_voyage_end,static_shipcode,static_file_name,to_date(static_voyage_startdate,"yyyy-MM-dd") as static_voyage_startdate,to_date(static_voyage_enddate,"yyyy-MM-dd") as static_voyage_enddate,case when (regexp_extract(lower(static_file_name), '(virginpostcruise)',0) = "virginpostcruise" or regexp_extract(lower(static_file_name), '(postcruise)',0) = "postcruise") then "virginpostcruise" when regexp_extract(lower(static_file_name),'(virginreminiscence)',0) = "virginreminiscence" then "virginreminiscence" else "NA" end as file_type from postvoyagestaticdfTmpTblflg""")
	  
postvoyagestaticdflag.createOrReplaceTempView("postvoyagestaticdfTmpTbl")
	  
	  
	  /* same response can be sent in multiple files - get the latest response_id based on filename */
val postvoyagestaticdfUnique = spark.sql(" select static_responseid, static_interview_start, static_interview_end, static_clientid,static_emailaddress, static_voyageid, static_voyage_start, static_voyage_end, static_shipcode, static_file_name, static_voyage_startdate, static_voyage_enddate,file_type from (select static_responseid, static_interview_start, static_interview_end, static_clientid,static_emailaddress, static_voyageid, static_voyage_start, static_voyage_end, static_shipcode, static_file_name, static_voyage_startdate, static_voyage_enddate,file_type,row_number() over(partition by static_responseid,file_type order by (CAST(regexp_extract(static_file_name, '([0-9]+)',1) AS INT)) desc) as rn from postvoyagestaticdfTmpTbl) where rn = 1 ") 
	  
postvoyagestaticdfUnique.createOrReplaceTempView("postvoyagestaticdfTbl")	

val saildimdf = spark.sql("select sail_id, src_sail_id, sail_date_from, sail_date_to, ship_id,sail_dim_sailfromdate,sail_dim_sailtodate from (Select sail_id, src_sail_id, sail_date_from, sail_date_to, ship_id, to_date(sail_date_from) as sail_dim_sailfromdate, to_date(sail_date_to) as sail_dim_sailtodate from vv_db.hvtb_nbx_core_sw_sail_dim where to_date(rec_end_dttm) = '9999-12-31' and is_active = 'Y')")
saildimdf.createOrReplaceTempView("saildimdfTbl")  

val postvoyagestatic1dfTbl = spark.sql(" select gd1.static_responseid, gd1.static_interview_start, gd1.static_interview_end, gd2.client_id,static_emailaddress, gd1.static_voyageid, gd1.static_voyage_start, gd1.static_voyage_end, gd1.static_shipcode, gd1.static_file_name, gd1.static_voyage_startdate, gd1.static_voyage_enddate,gd1.file_type,gd2.guest_id, rd.src_res_id,rd.src_sail_id from (select ps.*,gd.src_guest_id from postvoyagestaticdfTbl ps left join (select distinct client_id,src_guest_id from vv_db.hvtb_nbx_core_sw_guest_dim) gd on ps.static_clientid = gd.client_id) gd1 left join (select client_id,guest_id,src_guest_id from vv_db.hvtb_nbx_core_sw_guest_dim where to_date(rec_end_dttm) = '9999-12-31') gd2 on gd1.src_guest_id = gd2.src_guest_id left join vv_db.hvtb_nbx_core_sw_res_guest_rel rel on rel.guest_id = gd2.guest_id LEFT JOIN (select res_id,src_res_id,src_sail_id from vv_db.hvtb_nbx_core_sw_reservation_dim where res_status in ('BK','TM','CL') and to_date(rec_end_dttm) = '9999-12-31') rd on rd.res_id = rel.res_id inner join saildimdfTbl sail_dim on (sail_dim.src_sail_id = rd.src_sail_id and to_date(gd1.static_voyage_start) = to_date(sail_dim.sail_date_from) and to_date(gd1.static_voyage_end) = to_date(sail_dim.sail_date_to) )")
postvoyagestatic1dfTbl.createOrReplaceTempView("postvoyagestatic2dfTbl") 
	  
val postvoyagedynamicdfUnique = spark.sql("select responseid, qkey, qvalue, qprefix, qindex, Qprefixlower, Qprefixupper, file_name,file_type from (select responseid, qkey, qvalue, qprefix, qindex, Qprefixlower, Qprefixupper, file_name,file_type,row_number() over(partition by responseid, qkey,file_type order by (CAST(regexp_extract(file_name, '([0-9]+)',1) AS INT)) desc) as rn from postvoyagedynamicdfTmpTbl) where rn = 1")	  
postvoyagedynamicdfUnique.createOrReplaceTempView("postvoyagedynamicdfTbl")
	  
val postvoyagedynamicQ11b = spark.sql("Select q11b.responseid, q11b.file_name, q11b.qkey as q11bqkey, q11b.qvalue as q11bqvalue , hqpcity.qkey as hqpcityqkey,  hqpcity.qvalue as hqpcityqvalue from postvoyagedynamicdfTbl q11b join postvoyagedynamicdfTbl hqpcity on q11b.responseid = hqpcity.responseid and q11b.file_name = hqpcity.file_name  and q11b.Qindex = hqpcity.Qindex and q11b.Qprefixlower = 'q11b' and hqpcity.Qprefixupper = 'HQPCITY' ")
postvoyagedynamicQ11b.createOrReplaceTempView("postvoyagedynamicQ11bTbl")

val postvoyagedynamicQ11c = spark.sql("Select q11c.responseid, q11c.file_name, q11c.qkey as q11cqkey, q11c.qvalue as q11cqvalue , hqshore.qkey as hqshoreqkey,  trim(lower(hqshore.qvalue)) as hqshoreqvalue from postvoyagedynamicdfTbl q11c join postvoyagedynamicdfTbl hqshore on q11c.responseid = hqshore.responseid and q11c.file_name = hqshore.file_name and q11c.Qindex = hqshore.Qindex and q11c.Qprefixlower = 'q11c' and hqshore.Qprefixupper = 'HQSHORE' and hqshore.qvalue is not null  ")
postvoyagedynamicQ11c.createOrReplaceTempView("postvoyagedynamicQ11cTbl")


val activitydimdf1 = spark.sql("select activity_skey,activity_id,activity_name,activity_code,activity_port_code,activity_group_code,ship_code, src_deleted_flg, voyage_id from (select activity_skey,activity_id,trim(lower(activity_name)) as activity_name,activity_code,activity_port_code,activity_group_code,ship_code,voyage_id,src_deleted_flg from shipdw.hvtb_mart_dim_activity) act where act.src_deleted_flg = false and act.activity_group_code= 'PA'")
activitydimdf1.createOrReplaceTempView("activitydimdfTbl1")

val actbkgDF = spark.sql(""" 
select activitycode,reservationnumber,status,activitygroupcode,voyageid,voyagenumber from 
(select activitybookingid, activitycode,reservationnumber,activitygroupcode,status,voyageid,voyagenumber,row_number() over (partition by activitybookingid order by lastmodifieddate desc) as rownum from shipdw.hvtb_parse_vxp_ars_activitybooking) OuterQry
where rownum = 1 and upper(status) = 'CONFIRMED' and activitygroupcode = 'PA'
group by activitycode,reservationnumber,status,activitygroupcode,voyageid,voyagenumber
""")
actbkgDF.createOrReplaceTempView("actbkgDFDFTbl")


val activitydimdfinal = spark.sql("""select distinct activity_skey,activity_name,actbkgview.activitycode as activity_code,activity_port_code,activity_group_code,cast(actbkgview.reservationnumber as int) as reservationnumber ,actbkgview.voyagenumber,actbkgview.status, actdim.ship_code from actbkgDFDFTbl actbkgview  left join activitydimdfTbl1 actdim on actdim.activity_code=actbkgview.activitycode and actdim.voyage_id = actbkgview.voyageid""")
activitydimdfinal.createOrReplaceTempView("activitydimdfinalView")


val shipdimdf = spark.sql("select ship_id, ship  from vv_db.hvtb_nbx_core_sw_ship_dim")
shipdimdf.createOrReplaceTempView("shipdimdfTbl")


val questiondimdf = spark.sql("select question_id as question_dim_question_id, src_question_id as question_dim_src_question_id, variable_id as question_dim_variable_id, type as question_dim_type from vv_db.hvtb_nbx_core_postvoyage_survey_questions_dim")
questiondimdf.createOrReplaceTempView("questiondimdfTbl")

val questionanswerdimdf = spark.sql("select question_answer_id as questionanswer_dim_questionanswer_id,variable_id as questionanswer_dim_variable_id,answer_code as questionanswer_answer_code from vv_db.hvtb_mart_postvoyage_survey_question_answer_dim")
questionanswerdimdf.createOrReplaceTempView("questiondimanswerdfTbl")

/*val postvoyagedynamicQ11cAct = spark.sql("Select q11c.responseid, q11c.file_name, q11c.q11cqkey, q11c.q11cqvalue, q11c.hqshoreqkey,  q11c.hqshoreqvalue , actTbl.activity_skey,actTbl.activity_code,actTbl.reservationnumber,actTbl.voyagenumber,actTbl.activity_port_code,actTbl.ship_code from postvoyagedynamicQ11cTbl q11c left join activitydimdfinalView actTbl on upper(actTbl.activity_name)  = upper(q11c.hqshoreqvalue)")
postvoyagedynamicQ11cAct.createOrReplaceTempView("postvoyagedynamicQ11cActTbl")*/

val postvoyagedynamicQ11cAct = spark.sql("Select q11c.responseid, staticdf.src_res_id, q11c.file_name, q11c.q11cqkey, q11c.q11cqvalue, q11c.hqshoreqkey,  q11c.hqshoreqvalue , actTbl.activity_skey,actTbl.activity_code,actTbl.reservationnumber,actTbl.voyagenumber,actTbl.activity_port_code,actTbl.ship_code from postvoyagedynamicQ11cTbl q11c left join postvoyagestatic2dfTbl staticdf on q11c.responseid = staticdf.static_responseid left join  activitydimdfinalView actTbl on upper(actTbl.activity_name) = upper(q11c.hqshoreqvalue) and staticdf.src_res_id = actTbl.reservationnumber")
postvoyagedynamicQ11cAct.createOrReplaceTempView("postvoyagedynamicQ11cActTbl")


val bookingdetaildimdf = spark.sql("select booking_id,status,reservation_number from shipdw.hvtb_mart_dim_booking_detail where upper(status) = 'CONFIRMED' and src_deleted_flag = false")
bookingdetaildimdf.createOrReplaceTempView("bookingdetaildimdfTbl")

val postvoyagedynamicdfKey = postvoyagedynamicdfUnique.filter(postvoyagedynamicdfUnique.col("Qkey") rlike "^Q[0-9].*")
postvoyagedynamicdfKey.createOrReplaceTempView("postvoyagedynamicdfKeyTbl")

val final_df = spark.sql("""select distinct staticTbl.static_responseid as request_id, staticTbl.client_id, shipTbl.ship_id, sailTbl.sail_id, staticTbl.guest_id,static_interview_start as request_time, static_interview_end as response_time, q11b.hqpcityqvalue as itinerary_port, q11c.activity_skey as activity_skey,q11c.activity_code, q11c.reservationnumber,staticTbl.static_emailaddress, question_dim_question_id as question_id,case when lower(questionTbl.question_dim_type) = 'opentext' or lower(questionTbl.question_dim_type) = 'opentextlist' then null else dynamicTbl.qvalue end as answer_rating,case when lower(questionTbl.question_dim_type) = 'opentext' or lower(questionTbl.question_dim_type) = 'opentextlist' then dynamicTbl.qvalue else null end as answer_text,questionanswerTbl.questionanswer_dim_questionanswer_id as question_answer_id, staticTbl.static_file_name, sailTbl.sail_dim_sailfromdate, sailTbl.sail_dim_sailtodate, staticTbl.static_voyage_start, staticTbl.static_voyage_end
	 from postvoyagestatic2dfTbl staticTbl 
	 left join postvoyagedynamicdfKeyTbl dynamicTbl on staticTbl.static_responseid = dynamicTbl.responseid and staticTbl.file_type = dynamicTbl.file_type
	 left join shipdimdfTbl shipTbl on upper(staticTbl.static_shipcode) = upper(shipTbl.ship)
	 left join saildimdfTbl sailTbl on sailTbl.ship_id  = shipTbl.ship_id and sailTbl.sail_dim_sailfromdate = staticTbl.static_voyage_start and sailTbl.sail_dim_sailtodate = staticTbl.static_voyage_end
	 left join postvoyagedynamicQ11bTbl q11b on q11b.responseid = staticTbl.static_responseid and upper(q11b.q11bqkey) = upper(dynamicTbl.qkey)
	 left join questiondimdfTbl questionTbl on upper(dynamicTbl.qkey) = upper(questionTbl.question_dim_variable_id)
	 left join questiondimanswerdfTbl questionanswerTbl on  upper(dynamicTbl.qkey) = upper(questionanswerTbl.questionanswer_dim_variable_id) and dynamicTbl.qvalue = questionanswerTbl.questionanswer_answer_code
	 left join postvoyagedynamicQ11cActTbl q11c on  (q11c.responseid = staticTbl.static_responseid and staticTbl.src_res_id = cast(q11c.reservationnumber as int) and upper(q11c.q11cqkey) = upper(dynamicTbl.qkey) and q11c.ship_code = staticTbl.static_shipcode)
	 left join bookingdetaildimdfTbl bookingTbl on (upper(staticTbl.static_voyageid) = upper(q11c.voyagenumber) and q11c.reservationnumber = bookingTbl.reservation_number and staticTbl.src_res_id = cast(bookingTbl.reservation_number as int))
	 where questionTbl.question_dim_question_id is not null
	 """)


	 

	final_df.createOrReplaceTempView("getAllDetailsdfTbl")


	/*spark.sql(insertQuery)*/
	spark.catalog.refreshTable(sc.getConf.get("spark.postvoyage.response.fact"))
	 
		 
	 val required_columns = spark.sparkContext.getConf.get("spark.source.columns").split(",")
  
  
  val missing_columns = ArrayBuffer[String]()
  val avaliable_columns = ArrayBuffer[String]()
  def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
 
  for ( col <-required_columns ) 
        { 
             
            
            if(hasColumn(final_df, col) ){
               println(col,"column exists",avaliable_columns.toString)
               println("column exists",avaliable_columns.length)
               log.info(col,"column exists",avaliable_columns.toString,avaliable_columns.length)
               println(avaliable_columns.length,"lenthg")
               avaliable_columns.append(col)
            }
            else{
              println(col,"column missing",missing_columns.length)
              println(missing_columns.length,"length")
              log.info(col,"column exists",missing_columns.toString,missing_columns.length)
              println("column missing",missing_columns)
              missing_columns.append(col)
            }
        
        }
  print(missing_columns,"Here are the missing columns")
  val stage_final_df=missing_columns.foldLeft(final_df)((df, c) =>
  df.withColumn(s"$c",  lit(null))
)




	log.info("calling scd framework")
	loadDimFact(spark: SparkSession, stage_final_df)
	
log.info("Updating Metadata framework")
    ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
     
    }catch {

      case e: Exception =>
        {
          log.info("in the catch of updateStatus ****************** ")
          ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
          e.printStackTrace()
          throw new Exception("General Exception..please check the stacktrace")
        }
    }
  }
}
