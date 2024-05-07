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

object SeaRoverIngest {

  def main(args: Array[String]) {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    spark.sqlContext.setConf("spark.sql.parquet.writeLegacyFormat", "true")
    
    val EmptyDF = spark.emptyDataFrame

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


          
          val inputDF = spark.sql(spark.sparkContext.getConf.get("spark.source.sql"))

          if (!inputDF.take(1).isEmpty) {
		  
             println("New searover clientid's found!")
			inputDF.show(5,false)
            inputDF.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.hive.table"))

          }
          
        else{
          println("New searover clientid's not found!")
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
        log.info("******************in the catch of SeaRoverIngestion ******************");
        e.printStackTrace();
        throw new Exception("General Exception..please check the stacktrace")
      }
    }

    spark.stop()
  }
}