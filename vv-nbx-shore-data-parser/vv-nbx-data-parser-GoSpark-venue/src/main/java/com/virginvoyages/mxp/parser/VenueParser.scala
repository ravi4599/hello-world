package com.virginvoyages.mxp.parser

import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{ Column, DataFrame }
import scala.collection.mutable.ArrayBuffer

import java.text.SimpleDateFormat

//XML validator imports
import org.apache.spark.SparkConf
import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import org.xml.sax.SAXException;
import java.io._
import java.nio.charset.StandardCharsets
import org.apache.spark.SparkFiles
import scala.util.Try
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
//import spark.implicits._
import org.apache.spark.sql.functions.col

import com.virginvoyages.metadataframework.ManageMetadata

object VenueParser {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    var batch_instance_id1: String = null
    var batch_id1: String = null
    try {
      
      def checkArray(df: DataFrame, colname: String): Boolean = {

        df.schema(colname).dataType match {
          case ArrayType(_, _) => return true
          case _               => return false
        }
      }

      def checkStructType(df: DataFrame, colname: String): Boolean = {
        df.schema(colname).dataType match {
          case StructType(_) => return true
          case _             => return false
        }
      }

      def checkStringType(df: DataFrame, colname: String): Boolean = {
        df.schema(colname).dataType match {
          case StringType => return true
          case _          => return false
        }
      }

      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

      val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
      metadata.productIterator.foreach(println)
      

      val batch_start_time = metadata._3
      val batch_end_time = metadata._4
      val part_start_time = metadata._5.toString()
      val part_end_time = metadata._6.toString()
      val start_execution_time = metadata._7.toString()
      val part_write_date = metadata._8.toString()
      batch_instance_id1 = metadata._2
      batch_id1 = metadata._1

      var venueDf = spark.sql(spark.sparkContext.getConf.get("spark.source.table"))
      //var data = venueDf.select("Message", "BatchTime", "Part_Date", "ShipCode")
      var data = venueDf.select("Message", "BatchTime", "Part_Date", "ShipCode").where($"BatchTime" >= lit(batch_start_time).cast(TimestampType) && $"BatchTime" <= lit(batch_end_time).cast(TimestampType) and ($"part_date".between(part_start_time, part_end_time)))
      //data.select("Message","ShipCode").show(false)
      var venueMsg = data.select("Message").rdd.map { x => x.toString }
      //var shipcode = data.select("ShipCode").first().getString(0)
      //println(venueMsg)

      //var venueData = spark.read.option("Multiline", true).json(venueMsg)
      //venueData = venueData.drop("_corrupt_record")

      var venueData = spark.read.json(venueMsg)
      venueData.show(false)
      venueData.printSchema()

      if (!venueData.head(1).isEmpty) {
         var shipcode = data.select("ShipCode").first().getString(0)

        venueData = venueData
          .withColumn("N", when(venueData.col("n").isNotNull, venueData.col("n")).otherwise(lit(null)))
          .withColumn("TS", when(venueData.col("ts").isNotNull, venueData.col("ts")).otherwise(lit(null)))
          .withColumn("CI", when(venueData.col("ci").isNotNull, venueData.col("ci")).otherwise(lit(null)))
          .withColumn("CN", when(venueData.col("cn").isNotNull, venueData.col("cn")).otherwise(lit(null)))
          .withColumn("TG", when(venueData.col("tg").isNotNull, venueData.col("tg")).otherwise(lit(null)))
          .withColumn("PLT", when(venueData.col("plt").isNotNull, venueData.col("plt")).otherwise(lit(null)))
          .withColumn("PL_Action", when(venueData.col("pl.action").isNotNull, venueData.col("pl.action")).otherwise(lit(null)))
          .withColumn("PL_Date", when(venueData.col("pl.date").isNotNull, venueData.col("pl.date")).otherwise(lit(null)))
          .withColumn("PL_ID", when(venueData.col("pl.id").isNotNull, venueData.col("pl.id")).otherwise(lit(null)))
          .withColumn("PL_Name", when(venueData.col("pl.name").isNotNull, venueData.col("pl.name")).otherwise(lit(null)))
          .withColumn("PL_ShipCode", when(venueData.col("pl.sailing").isNotNull, venueData.col("pl.sailing")).otherwise(lit(null)))
          .withColumn("VenueTypes", when(venueData.col("pl.venueTypes").isNotNull, venueData.col("pl.venueTypes")).otherwise(lit(null)))
          .withColumn("availability", when(venueData.col("pl.availability").isNotNull, venueData.col("pl.availability")).otherwise(lit(null)))
          .withColumn("MealPeriods", when(venueData.col("pl.mealPeriods").isNotNull, venueData.col("pl.mealPeriods")).otherwise(lit(null)))
          //.withColumn("ShipCode", when(venueData.col("ShipCode").isNotNull, venueData.col("ShipCode")).otherwise(lit(null)))
          .withColumn("ShipCode", lit(shipcode).cast(StringType))
          .withColumn("BatchTime", lit(start_execution_time).cast(TimestampType))
          .withColumn("Part_date", to_date(lit(part_write_date)))
          .withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
          
      if (hasColumn(venueData, "pl.sailing")) {
          venueData = venueData.withColumn("PL_ShipCode", when(venueData.col("pl.sailing").isNotNull, venueData.col("pl.sailing")).otherwise(lit(null)))
        } else {
          venueData = venueData.withColumn("PL_ShipCode", lit(null))
        }

        venueData.show(false)
        
        

        var venueId = venueData.select("N", "TS", "CI", "CN", "TG", "PLT", "PL_Action", "PL_Date", "PL_ID", "PL_Name", "BatchTime", "VoyageId", "ShipCode", "Part_date")

        venueId.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.venue.table"))

        /*venune types details - array or null */

        if (hasColumn(venueData, "VenueTypes")) {
          if (checkArray(venueData, "VenueTypes")) {
            venueData = venueData.withColumn("VenueTypes", explode_outer(venueData.col("VenueTypes")))
            print("VenueTypes")
            //venueData.show
            if (checkStructType(venueData, "VenueTypes")) {
              if (hasColumn(venueData, "VenueTypes.id")) {
                venueData = venueData.withColumn("VenueTypes_Id", when(venueData.col("VenueTypes.id").isNotNull, venueData.col("VenueTypes.id")).otherwise(lit(null)))
              } else {
                venueData = venueData.withColumn("VenueTypes_Id", lit(null))
              }

              if (hasColumn(venueData, "VenueTypes.name")) {
                venueData = venueData.withColumn("VenueTypes_Name", when(venueData.col("VenueTypes.name").isNotNull, venueData.col("VenueTypes.name")).otherwise(lit(null)))
              } else {
                venueData = venueData.withColumn("VenueTypes_Name", lit(null))
              }

              venueData = venueData.withColumn("BatchTime", lit(start_execution_time).cast(TimestampType))
                .withColumn("Part_date", to_date(lit(part_write_date)))
                .withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))

              var venueDetails = venueData.select("VenueTypes_Id", "VenueTypes_Name", "PL_ID", "TS", "BatchTime", "VoyageID", "ShipCode", "Part_date")
              //venueDetails.show
              venueDetails.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.venuedetails.table"))
            }
          }

        }
        /* meal periods array or null */

        if (hasColumn(venueData, "MealPeriods")) {
          if (checkArray(venueData, "MealPeriods")) {
            venueData = venueData.withColumn("MealPeriods_date", explode_outer(venueData.col("MealPeriods")))
            print("Mealperiods data")
           // venueData.show

            if (checkStructType(venueData, "MealPeriods")) {
              if (hasColumn(venueData, "MealPeriods_date.code")) {
                venueData = venueData.withColumn("MealPeriods_code", when(venueData.col("MealPeriods_date.code").isNotNull, venueData.col("MealPeriods_date.code")).otherwise(lit(null)))
              } else {
                venueData = venueData.withColumn("MealPeriods_code", lit(null))
              }

              if (hasColumn(venueData, "MealPeriods_date.duration")) {
                venueData = venueData.withColumn("MealPeriods_duration", when(venueData.col("MealPeriods_date.duration").isNotNull, venueData.col("MealPeriods_date.duration")).otherwise(lit(null)))
              } else {
                venueData = venueData.withColumn("MealPeriods_duration", lit(null))
              }
              if (hasColumn(venueData, "MealPeriods_date.type")) {
                venueData = venueData.withColumn("MealPeriods_type", when(venueData.col("MealPeriods_date.type").isNotNull, venueData.col("MealPeriods_date.type")).otherwise(lit(null)))
              } else {
                venueData = venueData.withColumn("MealPeriods_type", lit(null))
              }
              if (hasColumn(venueData, "MealPeriods_date.startTime.hour")) {
                venueData = venueData.withColumn("StartTime_Hour", when(venueData.col("MealPeriods_date.startTime.hour").isNotNull, venueData.col("MealPeriods_date.startTime.hour")).otherwise(lit(null)))
              } else {
                venueData = venueData.withColumn("StartTime_Hour", lit(null))
              }
              if (hasColumn(venueData, "MealPeriods_date.startTime.minute")) {
                venueData = venueData.withColumn("StartTime_Minute", when(venueData.col("MealPeriods_date.startTime.minute").isNotNull, venueData.col("MealPeriods_date.startTime.minute")).otherwise(lit(null)))
              } else {
                venueData = venueData.withColumn("StartTime_Minute", lit(null))
              }

              venueData = venueData.withColumn("BatchTime", lit(start_execution_time).cast(TimestampType))
                .withColumn("Part_date", to_date(lit(part_write_date)))
                .withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))

              var venueMealData = venueData.select("MealPeriods_code", "MealPeriods_duration", "MealPeriods_type", "StartTime_Hour", "StartTime_Minute", "PL_ID", "TS", "VenueTypes_Id", "BatchTime", "VoyageID", "Shipcode", "Part_date")
              venueMealData.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.mealdata.table"))
            }
          }
        }
        /*availability array */

        if (hasColumn(venueData, "availability")) {
          if (checkArray(venueData, "availability")) {
            venueData = venueData.withColumn("Availability_data", explode_outer(venueData.col("availability")))

            if (checkStructType(venueData, "Availability_data")) {
              if (hasColumn(venueData, "Availability_data.duration")) {
                venueData = venueData.withColumn("Availability_duration", when(venueData.col("Availability_data.duration").isNotNull, venueData.col("Availability_data.duration")).otherwise(lit(null)))
              } else {
                venueData = venueData.withColumn("Availability_duration", lit(null))
              }

              if (hasColumn(venueData, "Availability_data.type")) {
                venueData = venueData.withColumn("Availability_type", when(venueData.col("Availability_data.type").isNotNull, venueData.col("Availability_data.type")).otherwise(lit(null)))
              } else {
                venueData = venueData.withColumn("Availability_type", lit(null))
              }
              if (hasColumn(venueData, "Availability_data.startTime.hour")) {
                venueData = venueData.withColumn("StartTime_Hour", when(venueData.col("Availability_data.startTime.hour").isNotNull, venueData.col("Availability_data.startTime.hour")).otherwise(lit(null)))
              } else {
                venueData = venueData.withColumn("StartTime_Hour", lit(null))
              }
              if (hasColumn(venueData, "Availability_data.startTime.minute")) {
                venueData = venueData.withColumn("StartTime_Minute", when(venueData.col("Availability_data.startTime.minute").isNotNull, venueData.col("Availability_data.startTime.minute")).otherwise(lit(null)))
              } else {
                venueData = venueData.withColumn("StartTime_Minute", lit(null))
              }
              if (hasColumn(venueData, "MealPeriods_code")) {
                venueData = venueData.withColumn("MealPeriods_code", when(venueData.col("MealPeriods_code").isNotNull, venueData.col("MealPeriods_code")).otherwise(lit(null)))
              } else {
                venueData = venueData.withColumn("MealPeriods_code", lit(null))
              }

              venueData = venueData.withColumn("BatchTime", lit(start_execution_time).cast(TimestampType))
                .withColumn("Part_date", to_date(lit(part_write_date)))
                .withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
                .withColumn("ShipCode", when(venueData.col("ShipCode").isNotNull, venueData.col("ShipCode")).otherwise(lit(null)))

              var venueAvailabilityData = venueData.select("Availability_duration", "Availability_type", "StartTime_Hour", "StartTime_Minute", "PL_ID", "TS", "VenueTypes_Id", "MealPeriods_code","PL_ShipCode" ,"BatchTime", "VoyageID", "ShipCode", "Part_date")
              println(" Availability Data ");
              venueAvailabilityData.show(false)
              
              venueAvailabilityData.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.Availability_data.table"))

            }
          }

        }
      }

      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
    } catch {

      case e: Exception =>
        {
          ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark)

          log.info("in the catch of updateStatus ******************")
          e.printStackTrace()
          throw new Exception("General Exception..please check the stacktrace")
        }

    }

  }

}