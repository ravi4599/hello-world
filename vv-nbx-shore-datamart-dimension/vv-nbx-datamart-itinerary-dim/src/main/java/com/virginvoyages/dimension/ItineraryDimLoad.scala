package com.virginvoyages.dimension
import java.util.Date
import com.virginvoyages.metadataframework.ManageMetadata
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact

import java.util.Properties
import java.sql.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ StructType, StructField, TimestampType, StringType, IntegerType, DateType, LongType };
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

object ItineraryDimLoad {
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]) {
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
    log.info("/*************************************calling metadata framework*********************************************/")
    println("/*************************************calling metadata framework*********************************************/")
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

    try {

      import spark.sqlContext.implicits._
      val whereClause = s""" where cr_iti.batchtime>= '$batch_start_tme' and cr_iti.batchtime<='$batch_end_tme' and cr_iti.part_date>='$part_read_start' and cr_iti.part_date<='$part_read_end'"""
      println(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
      log.info(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")

      var stage_itineraryDF = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").trim()+whereClause)
      
      val src_itiDf = spark.sql(spark.sparkContext.getConf.get("spark.iti.sql").trim())
      
      val src_cruiseDf = spark.sql(spark.sparkContext.getConf.get("spark.cruise.sql").trim())
      
      val src_cityDf = spark.sql(spark.sparkContext.getConf.get("spark.cities.sql").trim())
    
      stage_itineraryDF = stage_itineraryDF.join(src_itiDf, stage_itineraryDF.col("iti_cruise_id") === src_itiDf.col("cruise_id") 
                          && src_itiDf.col("day_of_cruise") === 1 && src_itiDf.col("sorting_sequence") === 1 && src_itiDf.col("deleted") === false , "left")
                          .join(src_cityDf,src_itiDf.col("port_id") === src_cityDf.col("city_id"),"left")
                          .select(stage_itineraryDF.col("*"),src_cityDf.col("city_abbreviation"))
                          .withColumnRenamed("city_abbreviation", "itinerary_embark_port_code").drop("port_id")
                          
          
                          
      stage_itineraryDF = stage_itineraryDF.join(src_itiDf,stage_itineraryDF.col("next_cruise_id") === src_itiDf.col("CRUISE_ID")  
                          && src_itiDf.col("day_of_cruise") === 1 && src_itiDf.col("sorting_sequence") === 1 && src_itiDf.col("deleted") === false,"left")
                          .join(src_cityDf,src_itiDf.col("port_id") === src_cityDf.col("city_id"),"left")
                          .select(stage_itineraryDF.col("*"),src_cityDf.col("city_abbreviation"))
                          .withColumnRenamed("city_abbreviation", "itinerary_debark_port_code")
                          
             
      
      val finalItineraryDf = stage_itineraryDF.select("voyage_id","voyage_number","itinerary_id","voyage_itinerary_guid","itinerary_name","itinerary_day","itinerary_embark_dt","itinerary_debark_dt","itinerary_embark_port_code","itinerary_debark_port_code","gangway_open_time","gangway_close_time","itinerary_departure_time","itinerary_arrival_time","itinerary_isSeaDay","itinerary_day_date","src_active_flag","src_deleted_flag","batchtime","part_date")
      
      finalItineraryDf.printSchema
      
      println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxx-------------------------------Calling SCD Source-Stage Itinerary------------------------------------xxxxxxxxxxxxxxxxxxxxxx")
      log.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxx-------------------------------Calling SCD Source-Stage Itinerary------------------------------------xxxxxxxxxxxxxxxxxxxxxx")
      finalItineraryDf.show(20,false)
      
      if(!finalItineraryDf.head(1).isEmpty){
        
        
      loadDimFact(spark: SparkSession, finalItineraryDf)
      }

      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)

    } catch {

      case e: SQLException => { ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("******************in the catch of Itinerary Dimension Load ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace", e); }

      case e: Exception =>
        { ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("******************in the catch of Itinerary Dimension Load ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace", e); }
        println("#----------------------------Process Has Failed---------------------------#")
        log.info("#----------------------------Process Has Failed---------------------------#")
        System.exit(1)

    }

  }

}