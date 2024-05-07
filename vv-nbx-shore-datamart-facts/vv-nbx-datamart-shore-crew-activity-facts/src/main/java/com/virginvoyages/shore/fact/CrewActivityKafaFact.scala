package com.virginvoyages.shore.fact
import com.virginvoyages.metadataframework.ManageMetadata
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact
import org.apache.spark.broadcast.Broadcast
import java.net.UnknownHostException
import scala.util.parsing.json._
//import scalaj.http.Http
//import scalaj.http.HttpOptions
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.types.{ BooleanType, StringType, IntegerType, DateType, LongType, DecimalType }
object CrewActivityKafkaFact {
  // Creating logger
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
  // Defining the main method
  def main(args: Array[String]): Unit = {

    //creating spark session
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    // creating spark context
    val sc = spark.sparkContext

    val sparkConfiguration = sc.broadcast(sc.getConf.getAll.toMap)
    //Calling Metadataframework to get the batchtime and partdate which is used to  get incremental data from source database(Hive table)

    val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
    //batch_instance_id1, batch_id1
    val batch_id1 = metadata._1
    val batch_instance_id1 = metadata._2
    val batchStartTme = metadata._3
    val batchEndTme = metadata._4
    val partReadStart = metadata._5

    val partReadEnd = metadata._6
    val startExecutionTime = metadata._7
    val part_write_date = metadata._8
    val env = spark.sparkContext.getConf.get("spark.api.env")
    try {

      //val whereClause = s""" where 1=1 """
      val whereClause = s""" batchtime>= '$batchStartTme' and batchtime<='$batchEndTme' and part_date>='$partReadStart' and part_date<='$partReadEnd'"""
      println(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
      log.info(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
      import spark.sqlContext.implicits._
      import org.apache.spark.sql.types.{ BooleanType, StringType, IntegerType, DateType, LongType }
      println("#---------------------------Starting the Execution------------------#")
      var crewSchDf = spark.sql("select *,ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated DESC ,batchtime DESC) as rownum from shipdw.hvtb_parse_mxp_crew_schedule")
      var crewScheduleDf = crewSchDf.filter("rownum ==1").drop("rownum")
      val ladfileflag = spark.sparkContext.getConf.get("spark.lad.file").trim()
      if (ladfileflag.equalsIgnoreCase("false")) {
        crewScheduleDf = crewScheduleDf.where(whereClause)
      } else {
        crewSchDf = spark.sql("select *,ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated DESC ,batchtime DESC) as rownum from shipdw.hvtb_parse_mxp_crew_schedule_lad")
        crewScheduleDf = crewSchDf.filter("rownum ==1").drop("rownum")
      }
      log.info("-----crewScheduleDf----")      
      crewScheduleDf=crewScheduleDf.withColumn("person_internalCode", col("person_internalCode").cast(StringType))
      val orgUnitDf = spark.sql("select distinct org_unit_abbreviation,org_unit_skey from shipdw.hvtb_mart_dim_org_unit")
      import spark.sqlContext.implicits._
      val orgskeyDf = crewScheduleDf.join(orgUnitDf, crewScheduleDf.col("installation") === orgUnitDf.col("org_unit_abbreviation"), "left").select(crewScheduleDf("*"), orgUnitDf("org_unit_skey"))
      log.info("-----orgskeyDf----")
      val crewDetailAcivityDf = spark.sql("select crew_activity_detail_skey,CAST(crew_activity_guid as String) as crew_activity_guid,is_actuals from shipdw.hvtb_mart_dim_crew_activity_detail where is_actuals=true")
      log.info("-----crewDetailAcivityDf----")
      val crewActivDf = orgskeyDf.join(crewDetailAcivityDf, orgskeyDf.col("id") === crewDetailAcivityDf.col("crew_activity_guid"), "left").select(orgskeyDf("*"), crewDetailAcivityDf("crew_activity_detail_skey"))
      log.info("-----crewActivDf----")      
      val personDf=spark.sql("select person_id,person_skey,booking_arrival_date,booking_departure_date from shipdw.hvtb_mart_dim_person")      
      log.info("-----personDf----")
      val personskeyDf=crewActivDf.join(personDf, (crewActivDf.col("person_internalCode") === personDf.col("person_id")) and (crewActivDf("clockIn_time").between(personDf("booking_arrival_date"),personDf("booking_departure_date"))),"left").select(crewActivDf("*"),personDf("person_skey")).withColumn("is_actuals", lit("true")).withColumn("is_actuals", col("is_actuals").cast(BooleanType))     
      log.info("-----personskeyDf----")     
      val winSpec = Window.partitionBy("person_internalCode","id").orderBy($"person_skey".desc)
      val dupRempersonskeyDf = personskeyDf.withColumn("rownum", row_number().over(winSpec)).select("*").where(col("rownum") === 1).drop("rownum")            
      val crewActivityDimDf = spark.sql("select activity_name,crew_activity_skey from shipdw.hvtb_mart_dim_crew_activity")
      log.info("-----crewActivityDimDf----")
      val activitySkeyDf = dupRempersonskeyDf.join(crewActivityDimDf, dupRempersonskeyDf.col("activityType") === crewActivityDimDf.col("activity_name"), "left").select(dupRempersonskeyDf("*"), crewActivityDimDf("crew_activity_skey")).withColumnRenamed("crew_activity_skey", "activity_skey").withColumn("activity_skey", col("activity_skey").cast(IntegerType))
      log.info("-----activitySkeyDf----")
      val dateCalDf = activitySkeyDf.withColumnRenamed("clockIn_time", "schedule_activity_datetime_from").withColumnRenamed("clockOut_time", "schedule_activity_datetime_to").withColumn("schedule_activity_day", col("schedule_activity_datetime_from").cast(DateType))
      log.info("-----dateCalDf----")
      val diffHrDf = dateCalDf.withColumn("DiffInSeconds", col("schedule_activity_datetime_to").cast(LongType) - col("schedule_activity_datetime_from").cast(LongType)).withColumn("DiffInMinutes", round(col("DiffInSeconds") / 60)).withColumn("DiffInHours",(col("DiffInSeconds")/3600).cast(DecimalType(6,2))).withColumnRenamed("DiffInHours", "schedule_activity_duration_hours").withColumnRenamed("ShipCode", "ship_code")
      log.info("-----diffHrDf----")
      val additionalColsDf = diffHrDf.withColumnRenamed("confirmed", "is_actual_activity_confirmed").withColumnRenamed("isApproved", "is_actual_activity_approved").withColumnRenamed("isDeleted", "is_deleted").withColumnRenamed("VoyageId", "voyage_id").withColumnRenamed("ShipCode", "ship_code").withColumnRenamed("part_date", "part_dt").withColumn("crew_activity_detail_skey", when(diffHrDf("crew_activity_detail_skey").isNotNull,diffHrDf("crew_activity_detail_skey")).otherwise(lit(-1).cast(LongType))).withColumn("org_unit_skey", when(diffHrDf("org_unit_skey").isNotNull,diffHrDf("org_unit_skey")).otherwise(lit(-1).cast(IntegerType))).withColumn("person_skey", when(diffHrDf("person_skey").isNotNull,diffHrDf("person_skey")).otherwise(lit(-1).cast(LongType))).withColumn("activity_skey", when(diffHrDf("activity_skey").isNotNull,diffHrDf("activity_skey")).otherwise(lit(-1).cast(IntegerType))).select("voyage_id", "ship_code", "org_unit_skey", "crew_activity_detail_skey", "person_skey", "is_actuals", "activity_skey", "schedule_activity_day", "schedule_activity_datetime_from", "schedule_activity_datetime_to", "schedule_activity_duration_hours", "is_deleted", "is_actual_activity_confirmed", "is_actual_activity_approved", "id", "batchtime", "part_dt")      
      log.info("-----additionalColsDf----")

      if (!additionalColsDf.head(1).isEmpty) {
        loadDimFact(spark, additionalColsDf)
        // Calling Metadataframe to save the status of the job

      }
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)

      spark.stop()
    } catch {
      case e: Exception =>
        {
          ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
          log.info("******************in the vv-nbx-datamart-crew-activityfact ******************");
          e.printStackTrace();
          throw new Exception("General Exception..please check the stacktrace", e);
        }
        System.exit(1)
        spark.stop()

    }

  }

}
