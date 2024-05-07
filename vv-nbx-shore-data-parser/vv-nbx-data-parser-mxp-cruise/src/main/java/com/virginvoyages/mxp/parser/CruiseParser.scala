package com.virginvoyages.mxp.parser

import org.apache.spark.sql.Column

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

import com.virginvoyages.metadataframework.ManageMetadata

object CruiseParser {

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

      val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
      metadata.productIterator.foreach(println)

      val batch_start_time = metadata._3
      val batch_end_time = metadata._4
      val part_start_time = metadata._5.toString()
      val part_end_time = metadata._6.toString()
       val start_execution_time=metadata._7.toString()
   val part_write_date=metadata._8.toString()
      batch_instance_id1 = metadata._2
      batch_id1 = metadata._1

      val HiveDF = spark.sql(spark.sparkContext.getConf.get("spark.source.table"))

      val SourceSchema = StructType(Array(StructField("id", LongType), StructField("guid", StringType), StructField("lastChanged", StringType),StructField("number", StringType), StructField("description", StringType), StructField("orgUnitId", StringType), StructField("orgUnit", StringType), StructField("shipCode", StringType), StructField("startDate", StringType), StructField("endDate", StringType), StructField("cruiseStatus", StringType)))

      val Data = HiveDF.select("Message", "batchtime", "part_date").where($"BatchTime" >= lit(batch_start_time).cast(TimestampType) && $"BatchTime" <= lit(batch_end_time).cast(TimestampType) and ($"part_date".between(part_start_time, part_end_time)))
     // var shipcode=Data.select("ShipCode").toString()
      val Datacon = Data.withColumn("message", from_json(col("message"), SourceSchema)).select("message.*", "batchtime", "part_date")
        
		val FinalData = Datacon.withColumn("BatchTime",lit(start_execution_time).cast(TimestampType))
		.withColumn("Part_date", to_date(lit(part_write_date)))
		.withColumn("VoyageID",lit(spark.sparkContext.getConf.get("spark.voyage.id")))//.withColumn("ShipCode", lit(shipcode).cast(StringType))
		.select("id","guid","lastChanged","number","description","orgUnitId","orgUnit","shipCode","startDate","endDate","cruiseStatus","VoyageID","BatchTime","Part_date")
		//val tgtLocation = spark.sparkContext.getConf.get("spark.target.location")
		val hiveTable = spark.sparkContext.getConf.get("spark.target.table")
		
		
		FinalData.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.table"))
		ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)

    } 
    catch { 

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