package com.virginvoyages

import java.sql.SQLException

import java.util.Date
import java.sql._;
import java.sql.SQLException;
import java.util.NoSuchElementException;
import org.apache.spark.sql.functions.hash
import java.util.Properties
import org.apache.spark.sql.SaveMode
import java.sql.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType, DateType, LongType };
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
import org.apache.spark.sql.functions._
//import scala.tools.scalap.Main
import scala.Array

import com.virginvoyages.metadataframework.ManageMetadata



object RPLCommissionFact {
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
  def main(args: Array[String]) {

     def getSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
  }

    import spark.implicits._
    val spark = getSparkSession()
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    var batch_instance_id1: String = null
    var batch_id1: String = null
    var batchInstanceId: String = null

    try {
      
      import spark.sqlContext.implicits._
      log.info(s"""Starting the Execution""")
      
      
      val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
      batch_id1 = metadata._1
      batchInstanceId = metadata._2
      val startExecutionTime = metadata._7
      val partWriteDate = metadata._8
      val sourceDf = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").trim())
        log.info("Loading request fact table")
        val final_df = sourceDf.withColumn("upd_dt", current_timestamp()).withColumn("load_dt", current_timestamp());
        
      
        final_df.repartition(15).write.mode("Overwrite").partitionBy("snapshot_date").parquet(spark.sparkContext.getConf.get("spark.target.location"))
        
      ManageMetadata.updateStatus(batchInstanceId, batch_id1, "Successful", spark)
      log.info("Updated the status successfully")
        
      }catch {
        case e: SQLException =>
          {
            ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
          log.info("******************in the catch of Commission Fact Load ******************");
          e.printStackTrace();
          throw new Exception("SQL Exception..please check the stacktrace", e);
        }
          println("#----------------------------Process Has Failed---------------------------#")
        System.exit(1)
        spark.stop()
      }
  }   
}