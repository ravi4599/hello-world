package com.virginvoyages.apollo.parser
import com.virginvoyages.metadataframework.ManageMetadata
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import org.apache.spark.SparkConf
import java.sql.SQLException
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.Row

import org.apache.spark.sql.functions.{ avg, explode, concat, lit, trim }
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.to_json
import java.sql.Struct
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.Column
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Column, DataFrame }
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import java.text.SimpleDateFormat

object ApolloPosChangeParser {
  def main(args: Array[String]): Unit ={
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
    
    var batchInstanceId : String = null
    var batchId : String = null
    
    try {
      
      val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
      metadata.productIterator.foreach(println)
      val batchStartTime = metadata._3
      batchId = metadata._1
      batchInstanceId = metadata._2
      val batchEndTime = metadata._4
      val partStartTime = metadata._5.toString()
      val partEndTime = metadata._6.toString()
      val startExecutionTime=metadata._7.toString()
      val partDate=metadata._8.toString()
      
      
      var posChangeSrcDf = spark.sql(spark.sparkContext.getConf.get("spark.source.table").trim())
      
      var Data = posChangeSrcDf.select("Message", "BatchTime","ShipCode", "Part_Date").where($"BatchTime" >= lit(batchStartTime).cast(TimestampType) && $"BatchTime" <= lit(batchEndTime).cast(TimestampType) and ($"part_date".between(partStartTime, partEndTime)))
					
		  var posChangeDf = Data.select("Message").rdd.map{x => x.toString}
      
      var shipcode=Data.select("ShipCode").toString()
       var posChange = spark.read.json(posChangeDf).withColumn("BatchTime",lit(startExecutionTime)).withColumn("VoyageId", lit(spark.sparkContext.getConf.get("spark.voyage.id"))).withColumn("partdate",lit(partDate))
      		posChange =posChange.withColumn("ShipCode", lit(shipcode).cast(StringType))
      
      if(hasColumn(posChange ,"pl" ))
      {
      if(checkArray(posChange,"pl"))
      {
      posChange = posChange.withColumn("explode_pl",explode_outer(posChange.col("pl"))).withColumn("ts", when(posChange.col("ts").isNotNull,posChange.col("ts")).otherwise(lit(null)))
      
      posChange = posChange
      .withColumn("plunr", when(posChange.col("explode_pl.PluNr").isNotNull,posChange.col("explode_pl.PluNr")).otherwise(lit(null)))
      .withColumn("pludesc", when(posChange.col("explode_pl.PluDesc").isNotNull,posChange.col("explode_pl.PluDesc")).otherwise(lit(null)))
      .withColumn("costofgoods", when(posChange.col("explode_pl.COGS").isNotNull,posChange.col("explode_pl.COGS")).otherwise(lit(null)))
      .withColumnRenamed("tg", "voyage_no")
	 // .withColumn("ShipCode", when(posChange.col("ShipCode").isNotNull, posChange.col("ShipCode")).otherwise(lit(null)))
      val posChangetbl = posChange.select("VoyageId","BatchTime","ts","plunr","pludesc","costofgoods" ,"voyage_no","ShipCode","partdate")
      posChangetbl.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.table").trim())
      
      }
      
      }
      
      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
      
        def checkArray(df: DataFrame, colname: String): Boolean = {

        df.schema(colname).dataType match {
          case ArrayType(_, _) => return true
          case _               => return false
        }
      }
      
      
      
    ManageMetadata.updateStatus(batchInstanceId, batchId, "Successful", spark)  
    }
    catch {
       case e: Exception =>
        {
         
          ManageMetadata.updateStatus(batchInstanceId, batchId, "Failed", spark)
          log.info("in the catch of updateStatus ******************")
          e.printStackTrace()
          throw new Exception("General Exception..please check the stacktrace")
        }
      
    }
    
    
    
  }
  
  
  
  
  
}