package com.virginvoyages.fact

import com.virginvoyages.metadataframework.ManageMetadata
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import org.apache.spark.sql.types.{ StructType, StructField, TimestampType, StringType, IntegerType, DateType, LongType };
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact
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
import org.apache.spark.sql.types.{ BooleanType, StringType, IntegerType, DateType, LongType }
import java.sql.DriverManager
import java.time.ZonedDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.sql.SQLException

object IncidentGuestFactLoad {
  def main(args: Array[String]) {
    

    val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
  
  def getSparkSession() =
      {
        val spark = SparkSession
          .builder()
          .enableHiveSupport()
          .getOrCreate()

        spark

      }
    
    val spark = getSparkSession()
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    
/*************************************calling metadata framework*********************************************/
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
    
    try{
      
      val whereClause = s""" where incdent.batchtime>= '$batch_start_tme' and incdent.batchtime<='$batch_end_tme' and incdent.part_date >='$part_read_start' and incdent.part_date <='$part_read_end'"""
      
     
      //val sourceDf = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").trim()+whereClause)
	  //val sourceDf = spark.sql(spark.sparkContext.getConf.get("spark.source.sql"))
      val sourceDf = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").replace("*whereclause*", whereClause))
      
      
      
      
      
      
      
       
     if(!sourceDf.head(1).isEmpty){
       
       log.info("Loading Incident Guest fact table")
       loadDimFact(spark: SparkSession, sourceDf)
     }
         
      log.info("No incremental data found")
      log.info("Updating Metadata framework")
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
    }
    
    catch{
      
      case e: SQLException => { 
         ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
         log.info("******************in the catch of Incident Guest Fact Load ******************");
         e.printStackTrace(); 
         throw new Exception("SQL Exception..please check the stacktrace", e); 
         
       }

      
        println("#----------------------------Process Has Failed---------------------------#")
        System.exit(1)
        spark.stop()
      
      
    }
    
  }
  
 
}