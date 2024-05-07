package com.virginvoyages.metadataframework
import org.apache.spark.sql.SparkSession

import java.sql.SQLException
import java.time.format.DateTimeFormatter
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import com.virginvoyages.metadataframework.HBaseConnectionException
import org.apache.spark.sql.types.{ StringType, TimestampType }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import scala.collection.Seq
import org.apache.spark.sql.functions.{ lower, upper }
import org.apache.spark.broadcast.Broadcast
import java.util.Date
import java.sql.Timestamp
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.types.TimestampType

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object ManageExecution {

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def checkExecutionStatus(spark: SparkSession): (String,Boolean) = {
    import java.util.Calendar
    val now = Calendar.getInstance.getTime
    val dowText = new SimpleDateFormat("E")
    val dow = dowText.format(now)
    val date = new SimpleDateFormat("yyyy-MM-dd")
    val todaydate = date.format(now)
    var executionFlag = false
    //val givendow = spark.sparkContext.getConf.get("spark.manageExecution.dayOftheWeek").trim().toLowerCase()
    //spark.manageExecution.dayOftheWeek
    val setsail_process_name = spark.sparkContext.getConf.get("spark.talend.processName")
    val setsail_metadata_Database = spark.sparkContext.getConf.get("spark.mysql.db")
    val setsail_metadata_Login = spark.sparkContext.getConf.get("spark.mysql.login")
    val setsail_metadata_Password = spark.sparkContext.getConf.get("spark.mysql.password")
    val setsail_metadata_Server = spark.sparkContext.getConf.get("spark.mysql.url")
    val setsail_metadata_Query = spark.sparkContext.getConf.get("spark.mysql.table")
    val setsail_metadata_Database_Driver = spark.sparkContext.getConf.get("spark.mysql.driver")
    val mysqlDF = spark.read.format("jdbc")
      .option("url", setsail_metadata_Server)
      .option("user", setsail_metadata_Login)
      .option("password", setsail_metadata_Password)
      .option("dbtable", setsail_metadata_Query)
      .option("driver", setsail_metadata_Database_Driver)
      .load()
    mysqlDF.createOrReplaceTempView("mysqlDF")
    //val daysString = spark.sparkContext.getConf.get("spark.manageExecution.dayOftheWeek").trim().toLowerCase()
    //spark.sparkContext.getConf.get("spark.currency.list").trim()
    //val daysList = daysString.split(",")

    println(s"""select * from mysqlDF where job_name in (""" + setsail_process_name + """) and load_job_status is null""")
    val load_job_status_logDF = spark.sql(s"""select * from mysqlDF where job_name in (""" + setsail_process_name + """) and load_job_status is null""")
    //if (daysList.contains(dow.toLowerCase())) {
    var message="Job already ran or Landing jobs running."
    if (load_job_status_logDF.head(1).isEmpty == true) { //if(!load_job_status_logDF.head(1).isEmpty == false){

      val metatadataTableDF = spark.read.format(spark.sparkContext.getConf.get("spark.target.ops.table.format").trim()).option("table", spark.sparkContext.getConf.get("spark.target.enablerops_table").trim()).
        option("zkUrl", spark.sparkContext.getConf.get("spark.target.zkurl").trim()).load()
      val srcConfigId = spark.sparkContext.getConf.get("spark.src.config.id")
      val tgtConfigId = spark.sparkContext.getConf.get("spark.target.config.id")
      val batchID = srcConfigId.concat("-").concat(tgtConfigId)
      //metatadataTableDF.printSchema
      metatadataTableDF.createOrReplaceTempView("meta_view")
      val metaDF = spark.sql("select * from meta_view where  BATCH_ID = '" + batchID + "' and date(BATCH_EXECUTION_ENDTIME)='" + todaydate + "' and lower(STATUS) = 'successful'")
      //metaDF.show(false)
      if (metaDF.head(1).isEmpty == true) {
        executionFlag = true
        message="Execute"
        println("Execution To be Started--", executionFlag)
      } else {
        message="Executed"
        println("The Job has already executed today")
      }
    } else {
      message="LandingJobsRunning"
      println("The Parent Pricing Talend Job is running so cannot be executed", executionFlag)
    }
    //} else {
    //  println("Execution Not Scheduled today Not To be Started--", executionFlag)
    // }
    return (message,executionFlag)
  }

  //Update status fail or successful

}