package com.virginvoyages.Incidentrpt


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{ col, to_date, monotonically_increasing_id }

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
import org.apache.spark.sql.functions.{ to_date }

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryException
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.Job
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.QueryJobConfiguration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import java.util.Calendar

object IncidentRptLoad {
  
  
  def main(args: Array[String]) {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)

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

        val temp_qry = s"call shipdw.vir_incident_rpt_sp()"
        try {
          val bigquery = BigQueryOptions.getDefaultInstance().getService()
          val config = QueryJobConfiguration.newBuilder(temp_qry).build()
          val job = bigquery.create(JobInfo.of(config))
          log.info("**Bigquery stored  Procedure **" + "job val= " + job);
          if (job.getStatus().getError() != null) {
            println("Job create query failed ..." + job.getStatus().getError())
            throw new RuntimeException(String.format("Job %s ended with error %s", job.getJobId(),
              job.getStatus().getError().getMessage()))
          } else println("Query executed ")

          print("bigquery Connection Succesful")
        } catch {
          //Handle errors for BQ
          case e: BigQueryException =>
            {
              log.info("******************in the catch of Bigquery stored  procedure  ******************");
              e.printStackTrace();
            }
          case e: Exception =>
            { log.info("******************in the catch of  Bigquery stored  procedure  ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace", e); }
            print("BigQuery Failed")
        }

        ManageMetadata.updateStatus(batchInstanceId, batch_id1, "Successful", spark)

      } else {
        log.info("This scripts does not require any parameters")
      }
    } catch {
      case e: SQLException => {
        ManageMetadata.updateStatus(batchInstanceId, batch_id1, "Failed", spark);
        e.printStackTrace();
        log.info("Exception while executing the SQL command");
      }
      case e: Exception => {
        log.info("******************in the catch of IncidentRpt ******************");
        e.printStackTrace();
        throw new Exception("General Exception..please check the stacktrace")
      }
    }

    spark.stop()
  }
  
}
  
