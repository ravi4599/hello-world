package com.virginvoyages.shore.Ingestion

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{ col, to_date, to_timestamp, monotonically_increasing_id }
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions.{ col, lit, when }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import com.virginvoyages.metadataframework.ManageMetadata
import java.sql.DriverManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ to_date, to_timestamp }
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import java.net.URI

object SpaUtilisationIngest {

  def main(args: Array[String]) {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    spark.sqlContext.setConf("spark.sql.parquet.writeLegacyFormat", "true")
    val inputfilepath = spark.sparkContext.getConf.get("spark.input.filepath").trim()
    val outfilepath = spark.sparkContext.getConf.get("spark.target.location").trim()
    val EmptyDF = spark.emptyDataFrame
    val processfilepath = spark.sparkContext.getConf.get("spark.input.processedfilepath").trim()

    val originalfilepath = spark.sparkContext.getConf.get("spark.input.originalfilepath").trim()

    //val inputDataDf = spark.read.format("CSV").option("header", "true").load("s3://vv-dev-emr-cluster/data/landing/vts/lookup_data_latest.csv")
    //val inputDataDf = spark.read.format("CSV").option("header", "true").load("s3://vv-qa-emr-cluster/data/landing/vts/lookup_data_latest.csv")

    /*val EmptyDF = spark.emptyDataFrame
    EmptyDF.write.mode("Overwrite").parquet("s3://vv-dev-emr-cluster/data/mart/vts/test/")


    dataFrame.write.mode("Overwrite").parquet("s3://vv-dev-emr-cluster/data/mart/vts/test/")
    val data = spark.read.parquet("s3://vv-dev-emr-cluster/data/mart/vts/test/")
    data.printSchema()
    log.info("DATA:::::::::" + data.count())
    data.show(false)*/
    var batch_instance_id1: String = null
    var batch_id1: String = null
    var batchInstanceId: String = null

    try {

      if (args.length == 0) {
        val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)

        batch_id1 = metadata._1
        batchInstanceId = metadata._2
        val startExecutionTime = metadata._7
        val partWriteDate = metadata._8
        log.info(" Partition write date " + partWriteDate)
        val voyage_id = spark.sparkContext.getConf.get("spark.voyage.id")
        if (FileSystem.get(new URI(inputfilepath), sc.hadoopConfiguration).exists(new Path(inputfilepath))) {
          println("File Exist")
          
          val inputDataDf = spark.read.format("CSV").option("header", "true").load(inputfilepath)

          if (!inputDataDf.take(1).isEmpty) {

            /*   val colRenameDf = inputDataDf.withColumnRenamed("Calls % Handled", "target_pct_inbound_calls_handled")
      .withColumnRenamed("Cases SLA", "target_avg_total_case_resolution_hrs")
      .withColumnRenamed("Chats Handled", "target_pct_completed_chats")
      .withColumnRenamed("Post Chat Survey Response", "target_pct_avg_loved_happy_response")
      .withColumnRenamed("Average Case Value Duration", "target_avg_value_duration_hrs")
      .withColumnRenamed("Calls % Handled (IB & OB)", "target_pct_calls_handled_total")
      .withColumnRenamed("Net New Bookings Contribution - Phone ", "target_pct_phone_bookings")
      .withColumnRenamed("Net New Voyage Protection ", "target_pct_phone_voyage_protection_bookings")
      .withColumnRenamed("Casino Bookings", "target_count_casino_bookings")
      .withColumnRenamed("Case Productivity", "target_pct_cases_handled")
      .withColumnRenamed("Average Handle Time", "target_avg_handle_time_hrs")
      .withColumnRenamed("Phone Service Level", "target_phone_service_level")
      .withColumnRenamed("Abandon %", "target_pct_call_abandon")
      .withColumnRenamed("Outbound Calls", "target_count_outbound_calls")*/

            val modifiedDF = inputDataDf.withColumn("ShipCode", col("ShipCode").cast(StringType)).
              withColumn("VoyageNumber", col("VoyageNumber").cast(StringType)).
              withColumn("Date", to_date(col("Date"), "MM/dd/yyyy")).
              withColumn("SpaVenue", col("SpaVenue").cast(DoubleType)).
              withColumn("SpaServiceCategory", col("SpaServiceCategory").cast(StringType)).
              withColumn("SpaServiceType", col("SpaServiceType").cast(StringType)).
              withColumn("TotalAvailableHours", col("TotalAvailableHours").cast(IntegerType)).
              withColumn("TotalBookedHours", col("TotalBookedHours").cast(FloatType)).
              withColumn("NumberofReservations", col("NumberofReservations").cast(IntegerType)).
              withColumn("NumOfPreVoyageBookings", col("NumOfPreVoyageBookings").cast(IntegerType)).
              withColumn("ServiceRevenue", col("ServiceRevenue").cast(IntegerType)).
              withColumn("RetailRevenue", col("RetailRevenue").cast(IntegerType)).
              withColumn("TotalRevenue", col("TotalRevenue").cast(IntegerType)).
              withColumn("TotalTarget", col("TotalTarget").cast(IntegerType)).
              withColumn("voyageid", lit(voyage_id).cast((StringType))).
              withColumn("BatchTime", lit(startExecutionTime).cast(TimestampType)).
              withColumn("Part_Date", to_date(lit(partWriteDate)))
            val orderedColumnsDf = modifiedDF.select("ShipCode", "VoyageNumber", "Date", "SpaVenue", "SpaServiceCategory", "SpaServiceType", "TotalAvailableHours", "TotalBookedHours", "NumberofReservations", "NumOfPreVoyageBookings", "ServiceRevenue", "RetailRevenue", "TotalRevenue", "TotalTarget", "voyageid", "BatchTime", "Part_Date")
            orderedColumnsDf.printSchema()
            log.info("orderedColumnsDf:::::::::")
            //orderedColumnsDf.write.mode("Overwrite").parquet("s3://vv-dev-emr-cluster/data/mart/vts/hvtb_mart_vts_performance_targets_lkp")
            //orderedColumnsDf.write.mode("Overwrite").parquet("s3://vv-qa-emr-cluster/data/mart/vts/hvtb_mart_vts_performance_targets_lkp")
            orderedColumnsDf.show(5, false)
            sqlContext.setConf("hive.exec.dynamic.partition", "true")
            sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
            orderedColumnsDf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.hive.table"))

            inputDataDf.write.mode("append").csv(processfilepath)
            EmptyDF.write.mode("Overwrite").csv(originalfilepath)

          }
          
        }else{
          println("File/Path Doesn't Exist")
        }
        ManageMetadata.updateStatus(batchInstanceId, batch_id1, "Successful", spark)

      } else {
        log.info("This scripts does not require any parameters")
      }
    } catch {
      case e: SQLException => {
        e.printStackTrace();
        log.info("Exception while executing the SQL command");
      }
      case e: Exception => {
        log.info("******************in the catch of SPAutilisationIngestion ******************");
        e.printStackTrace();
        throw new Exception("General Exception..please check the stacktrace")
      }
    }

    spark.stop()
  }
}