package com.virginvoyages.mxp.parser

import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Column, DataFrame }
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkConf
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
import com.virginvoyages.metadataframework.ManageMetadata

object SwToVxpWearableFulfillmentParser {
  
  
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

    var batchInstanceId: String = null
    var batchId: String = null

    try {

      val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
      metadata.productIterator.foreach(println)
      val batchStartTime = metadata._3
      batchId = metadata._1
      batchInstanceId = metadata._2
      val batchEndTime = metadata._4
      val partStartTime = metadata._5.toString()
      val partEndTime = metadata._6.toString()
      val startExecutionTime = metadata._7.toString()
      val partDate = metadata._8.toString()

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

      var wearableDF = spark.sql(spark.sparkContext.getConf.get("spark.source.table").trim())

      var data = wearableDF.select("Message", "BatchTime", "Part_Date").where($"BatchTime" >= lit(batchStartTime).cast(TimestampType) && $"BatchTime" <= lit(batchEndTime).cast(TimestampType) and ($"part_date".between(partStartTime, partEndTime)))

      var wearableMsg = data.select("Message").rdd.map(x => x.toString)

      var wearableData = spark.read.json(wearableMsg)//.withColumn("BatchTime", lit(startExecutionTime)).withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id"))).withColumn("Part_date", lit(partDate))
      
       if (!wearableData.head(1).isEmpty) {
         
         wearableData = wearableData.withColumn("guestList", when(wearableData.col("pl.guestList").isNotNull, wearableData.col("pl.guestList")).otherwise(lit(null)))
                                    .withColumn("flag", lit("Y"))
                                    
         if (hasColumn(wearableData, "ts")) {
           
          wearableData = wearableData.withColumn("change_dt", when(wearableData.col("ts").isNotNull, wearableData.col("ts").cast(TimestampType)).otherwise(lit(current_timestamp)))
          
        } else {
          
          wearableData = wearableData.withColumn("change_dt", lit(current_timestamp))
        }
        
        if (hasColumn(wearableData, "pl.reservationId")) {
          wearableData = wearableData.withColumn("src_res_id", when(wearableData.col("pl.reservationId").isNotNull, wearableData.col("pl.reservationId").cast(IntegerType)).otherwise(lit(null)))
            

        } else {
          wearableData = wearableData.withColumn("src_res_id", lit(null))
        }
 
       if (hasColumn(wearableData, "guestList")) {
         
         if (checkArray(wearableData, "guestList")) {
           
          wearableData = wearableData.withColumn("guestList", explode_outer(wearableData.col("guestList")))
          
          
          if (hasColumn(wearableData, "guestList.clientID")) {
          wearableData = wearableData.withColumn("client_id", when(wearableData.col("guestList.clientID").isNotNull, wearableData.col("guestList.clientID").cast(IntegerType)).otherwise(lit(null)))
          }
          else 
          {
            wearableData = wearableData.withColumn("client_id", lit(null))
            
          }
          
        }

      } 
       
       else {
          
          
          wearableData = wearableData.withColumn("client_id", lit(null))
        }
         
      
      
   val finalWearableDf = wearableData.select("src_res_id","client_id","flag","change_dt")
     
   //finalWearableDf.show(200,false)
   finalWearableDf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.table"))  
         
       }
   
     ManageMetadata.updateStatus(batchInstanceId, batchId, "Successful", spark)
      
    }
    
    catch {

      case e: Exception =>
        {

          log.info("in the catch of updateStatus ******************")
          ManageMetadata.updateStatus(batchInstanceId, batchId, "Failed", spark)
          e.printStackTrace()
          throw new Exception("General Exception..please check the stacktrace")
        }

    }
     
   }
}