package com.virginvoyages.shore.facts

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
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{ to_date, to_timestamp }
import org.apache.spark.sql.types._
    
object VTSPerformanceTargetLookup {
  def main(args: Array[String]) {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    spark.sqlContext.setConf("spark.sql.parquet.writeLegacyFormat", "true")
    val inputfilepath=spark.sparkContext.getConf.get("spark.input.filepath").trim()
    val outfilepath=spark.sparkContext.getConf.get("spark.target.location").trim()
    log.info("inputfilepath:::::::::outfilepath read from property file")
    //val inputDataDf = spark.read.format("CSV").option("header", "true").load("s3://vv-dev-emr-cluster/data/landing/vts/lookup_data_latest.csv")
    //val inputDataDf = spark.read.format("CSV").option("header", "true").load("s3://vv-qa-emr-cluster/data/landing/vts/lookup_data_latest.csv")
    val inputDataDf = spark.read.format("CSV").option("header", "true").load(inputfilepath)
    /*val EmptyDF = spark.emptyDataFrame
    EmptyDF.write.mode("Overwrite").parquet("s3://vv-dev-emr-cluster/data/mart/vts/test/")

    dataFrame.write.mode("Overwrite").parquet("s3://vv-dev-emr-cluster/data/mart/vts/test/")
    val data = spark.read.parquet("s3://vv-dev-emr-cluster/data/mart/vts/test/")
    data.printSchema()
    log.info("DATA:::::::::" + data.count())
    data.show(false)*/
    var batch_instance_id1: String = null
    var batch_id1: String = null
    try {
    if (args.length == 0) {
      val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)

        batch_instance_id1 = metadata._2
        batch_id1 = metadata._1
    
    
    val colRenameDf = inputDataDf.withColumnRenamed("Calls % Handled", "target_pct_inbound_calls_handled")
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
      .withColumnRenamed("Outbound Calls", "target_count_outbound_calls")

    val modifiedDF = colRenameDf.withColumn("target_range_start_date", to_date(col("target_range_start_date"), "MM/dd/yyyy")).
      withColumn("target_range_end_date", to_date(col("target_range_end_date"), "MM/dd/yyyy")).
      withColumn("target_pct_inbound_calls_handled", col("target_pct_inbound_calls_handled").cast(IntegerType)).
      withColumn("target_avg_total_case_resolution_hrs", col("target_avg_total_case_resolution_hrs").cast(IntegerType)).
      withColumn("target_pct_completed_chats", col("target_pct_completed_chats").cast(IntegerType)).
      withColumn("target_pct_avg_loved_happy_response", col("target_pct_avg_loved_happy_response").cast(IntegerType)).
      withColumn("target_avg_value_duration_hrs", col("target_avg_value_duration_hrs").cast(IntegerType)).
      withColumn("target_pct_calls_handled_total", col("target_pct_calls_handled_total").cast(IntegerType)).
      withColumn("target_pct_phone_bookings", col("target_pct_phone_bookings").cast(IntegerType)).
      withColumn("target_pct_phone_voyage_protection_bookings", col("target_pct_phone_voyage_protection_bookings").cast(IntegerType)).
      withColumn("target_count_casino_bookings", col("target_count_casino_bookings").cast(IntegerType)).
      //withColumn("target_rockstar_avg_value_duration_hrs", col("target_rockstar_avg_value_duration_hrs").cast(IntegerType)).
      withColumn("target_pct_cases_handled", col("target_pct_cases_handled").cast(IntegerType)).
      withColumn("target_avg_handle_time_hrs", col("target_avg_handle_time_hrs").cast(IntegerType)).
      withColumn("target_phone_service_level", col("target_phone_service_level").cast(IntegerType)).
      withColumn("target_pct_call_abandon", col("target_pct_call_abandon").cast(IntegerType)).
      withColumn("target_count_outbound_calls", col("target_count_outbound_calls").cast(IntegerType)).
      withColumn("etl_ld_dt", current_timestamp()).
      withColumn("etl_upd_dt", current_timestamp())
    val orderedColumnsDf = modifiedDF.select("target_range_start_date", "target_range_end_date", "target_team_name", "target_queue_name", "target_pct_inbound_calls_handled", "target_avg_total_case_resolution_hrs", "target_pct_completed_chats", "target_pct_avg_loved_happy_response", "target_avg_value_duration_hrs", "target_pct_calls_handled_total", "target_pct_phone_bookings", "target_pct_phone_voyage_protection_bookings", "target_count_casino_bookings", "target_pct_cases_handled","target_avg_handle_time_hrs", "target_phone_service_level", "target_pct_call_abandon", "target_count_outbound_calls", "etl_ld_dt", "etl_upd_dt")
    //orderedColumnsDf.printSchema()
    log.info("orderedColumnsDf:::::::::")
    //orderedColumnsDf.write.mode("Overwrite").parquet("s3://vv-dev-emr-cluster/data/mart/vts/hvtb_mart_vts_performance_targets_lkp")
    //orderedColumnsDf.write.mode("Overwrite").parquet("s3://vv-qa-emr-cluster/data/mart/vts/hvtb_mart_vts_performance_targets_lkp")
    orderedColumnsDf.write.mode("Overwrite").parquet(outfilepath)
    ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Succesful", spark)
    }else {
        log.info("This scripts does not require any parameters")
      }
    } catch {
      case e: SQLException => {
        e.printStackTrace();
        log.info("Exception while executing the SQL command");
      }
      case e: Exception => {
        log.info("******************in the catch of VTSPerformanceTargetLookup ******************");
        e.printStackTrace();
        throw new Exception("General Exception..please check the stacktrace")
      }
    }

    spark.stop()
  }
}