package com.virginvoyages.shore.vxp.parser


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
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.broadcast.Broadcast
import java.net.UnknownHostException
import scala.util.parsing.json._
import org.apache.log4j.LogManager
import org.apache.log4j.Level

import java.text.SimpleDateFormat

import com.virginvoyages.metadataframework.ManageMetadata


object Idcrossreference {

  
  
val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
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

  val IdcrossreferenceDf = spark.sql("select * from %s".format(spark.sparkContext.getConf.get("spark.source.table")))
      //val ActivitytypeMappingDf = spark.sql("select * from spark.sparkContext.getConf.get("spark.source.table"))
IdcrossreferenceDf.show(2,false)
      var Data = IdcrossreferenceDf.select("Message", "BatchTime", "Part_Date").where($"BatchTime" >= lit(batch_start_time).cast(TimestampType) && $"BatchTime" <= lit(batch_end_time).cast(TimestampType) and ($"part_date".between(part_start_time, part_end_time)))
	  
	  
      if (!Data.head(1).isEmpty) {

      var IdcrossreferenceMsg = Data.select("Message").rdd.map { x => x.toString }

      var IdcrossreferenceData = spark.read.json(IdcrossreferenceMsg).drop("BatchTime","Part_Date")
      IdcrossreferenceData.show
      import spark.implicits._

      IdcrossreferenceData.show()
      IdcrossreferenceData.printSchema()


            if (hasColumn(IdcrossreferenceData, "idcrossReferenceId")) {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("idcrossReferenceId",         when(IdcrossreferenceData.col("idcrossReferenceId").isNotNull,
                            IdcrossreferenceData.col("idcrossReferenceId")).otherwise(lit(null)))
                      }
                      else {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("idcrossReferenceId",lit(null))
                      }
           if (hasColumn(IdcrossreferenceData, "idtypeid")) {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("idtypeid", when(IdcrossreferenceData.col("idtypeid").isNotNull,
                            IdcrossreferenceData.col("idtypeid")).otherwise(lit(null)))
                      }
                      else {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("idtypeid",lit(null))
                      }
           if (hasColumn(IdcrossreferenceData, "idsourceid")) {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("idsourceid", when(IdcrossreferenceData.col("idsourceid").isNotNull,
                            IdcrossreferenceData.col("idsourceid")).otherwise(lit(null)))
                      }
                      else {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("idsourceid",lit(null))
                      }
           if (hasColumn(IdcrossreferenceData, "idvalue")) {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("idvalue", when(IdcrossreferenceData.col("idvalue").isNotNull,
                            IdcrossreferenceData.col("idvalue")).otherwise(lit(null)))
                      }
                      else {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("idvalue",lit(null))
                      }
           if (hasColumn(IdcrossreferenceData, "linkId")) {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("linkId", when(IdcrossreferenceData.col("linkId").isNotNull,
                            IdcrossreferenceData.col("linkId")).otherwise(lit(null)))
                      }
                      else {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("linkId",lit(null))
                      }
       if (hasColumn(IdcrossreferenceData, "expirydate")) {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("expirydate", when(IdcrossreferenceData.col("expirydate").isNotNull,
                            IdcrossreferenceData.col("expirydate")).otherwise(lit(null)))
                      }
                      else {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("expirydate",lit(null))
                      }
         if (hasColumn(IdcrossreferenceData, "additionaldetail")) {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("additionaldetail", when(IdcrossreferenceData.col("additionaldetail").isNotNull,
                            IdcrossreferenceData.col("additionaldetail")).otherwise(lit(null)))
                      }
                      else {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("additionaldetail",lit(null))
                      }
     if (hasColumn(IdcrossreferenceData, "addeddate")) {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("addeddate", when(IdcrossreferenceData.col("addeddate").isNotNull,
                            IdcrossreferenceData.col("addeddate")).otherwise(lit(null)))
                      }
                      else {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("addeddate",lit(null))
                      }
if (hasColumn(IdcrossreferenceData, "addedby")) {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("addedby", when(IdcrossreferenceData.col("addedby").isNotNull,
                            IdcrossreferenceData.col("addedby")).otherwise(lit(null)))
                      }
                      else {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("addedby",lit(null))
                      }
if (hasColumn(IdcrossreferenceData, "lastmodifieddate")) {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("lastmodifieddate", when(IdcrossreferenceData.col("lastmodifieddate").isNotNull,
                            IdcrossreferenceData.col("lastmodifieddate")).otherwise(lit(null)))
                      }
                      else {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("lastmodifieddate",lit(null))
                      }
if (hasColumn(IdcrossreferenceData, "lastmodifiedby")) {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("lastmodifiedby", when(IdcrossreferenceData.col("lastmodifiedby").isNotNull,
                            IdcrossreferenceData.col("lastmodifiedby")).otherwise(lit(null)))
                      }
                      else {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("lastmodifiedby",lit(null))
                      }
if (hasColumn(IdcrossreferenceData, "isdeleted")) {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("isdeleted", when(IdcrossreferenceData.col("isdeleted").isNotNull,
                            IdcrossreferenceData.col("isdeleted")).otherwise(lit(null)))
                      }
                      else {
                        IdcrossreferenceData = IdcrossreferenceData.withColumn("isdeleted",lit(null))
                      }
 

if (hasColumn(IdcrossreferenceData, "VoyageID")) {
IdcrossreferenceData = IdcrossreferenceData.withColumn("VoyageID", when(IdcrossreferenceData.col("VoyageID").isNotNull,
IdcrossreferenceData.col("VoyageID")).otherwise(lit(null)))
}
else {
IdcrossreferenceData = IdcrossreferenceData.withColumn("VoyageID",lit(null))
}
     
IdcrossreferenceData.show(2,false)
IdcrossreferenceData.printSchema()
     
      var finalIdcrossreferenceData=IdcrossreferenceData.withColumn("BatchTime", lit(start_execution_time).cast(TimestampType))
          .withColumn("Part_date", to_date(lit(part_write_date))).select("idcrossreferenceid","idtypeid","idsourceid","idvalue","linkid", "expirydate","additionaldetail","addeddate","addedby","lastmodifieddate", "lastmodifiedby","isdeleted","VoyageId","BatchTime","Part_date")
         
          finalIdcrossreferenceData.show(2,false)
         
          finalIdcrossreferenceData.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.table"))
}
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