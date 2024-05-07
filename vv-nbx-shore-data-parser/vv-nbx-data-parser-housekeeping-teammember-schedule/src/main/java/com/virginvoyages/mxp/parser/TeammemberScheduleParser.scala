package com.virginvoyages.mxp.parser

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
//import scalaj.http.Http
//import scalaj.http.HttpOptions
import org.apache.log4j.LogManager
import org.apache.log4j.Level

import java.text.SimpleDateFormat

import com.virginvoyages.metadataframework.ManageMetadata

object TeammemberScheduleParser {

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

      val tmsDF = spark.sql(spark.sparkContext.getConf.get("spark.source.table"))

      var Data = tmsDF.select("Message", "BatchTime", "Part_Date").where($"BatchTime" >= lit(batch_start_time).cast(TimestampType) && $"BatchTime" <= lit(batch_end_time).cast(TimestampType) and ($"part_date".between(part_start_time, part_end_time)))

      var tmsMsg = Data.select("Message").rdd.map { x => x.toString }

      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

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

      var MsgData = spark.read.json(tmsMsg)

      var tmsMsgData = MsgData.drop("BatchTime", "Part_Date")

      import spark.implicits._

      if (!tmsMsgData.head(1).isEmpty) {

        if (hasColumn(tmsMsgData, "id")) {
          tmsMsgData = tmsMsgData
            .withColumn("id", when(tmsMsgData.col("id").isNotNull, tmsMsgData.col("id")).otherwise(lit(null)))

        } else {
          tmsMsgData = tmsMsgData.withColumn("id", lit(null))

        }

        if (hasColumn(tmsMsgData, "public_id")) {
          tmsMsgData = tmsMsgData
            .withColumn("public_id", when(tmsMsgData.col("public_id").isNotNull, tmsMsgData.col("public_id")).otherwise(lit(null)))

        } else {
          tmsMsgData = tmsMsgData.withColumn("public_id", lit(null))

        }

        if (hasColumn(tmsMsgData, "tms_date")) {
          tmsMsgData = tmsMsgData
            .withColumn("tms_date", when(tmsMsgData.col("tms_date").isNotNull, tmsMsgData.col("tms_date")).otherwise(lit(null)))

        } else {
          tmsMsgData = tmsMsgData.withColumn("tms_date", lit(null))
        }

        
        if (hasColumn(tmsMsgData, "day_period")) {
          tmsMsgData = tmsMsgData
            .withColumn("day_period", when(tmsMsgData.col("day_period").isNotNull, tmsMsgData.col("day_period")).otherwise(lit(null)))

        } else {
          tmsMsgData = tmsMsgData.withColumn("day_period", lit(null))

        }

        if (hasColumn(tmsMsgData, "team_member_id")) {
          tmsMsgData = tmsMsgData
            .withColumn("team_member_id", when(tmsMsgData.col("team_member_id").isNotNull, tmsMsgData.col("team_member_id")).otherwise(lit(null)))

        } else {
          tmsMsgData = tmsMsgData.withColumn("team_member_id", lit(null))

        }

        if (hasColumn(tmsMsgData, "team_member_role")) {
          tmsMsgData = tmsMsgData
            .withColumn("team_member_role", when(tmsMsgData.col("team_member_role").isNotNull, tmsMsgData.col("team_member_role")).otherwise(lit(null)))

        } else {
          tmsMsgData = tmsMsgData.withColumn("team_member_role", lit(null))

        }
		
		 if (hasColumn(tmsMsgData, "modified_at")) {
          tmsMsgData = tmsMsgData
            .withColumn("modified_at", when(tmsMsgData.col("modified_at").isNotNull, tmsMsgData.col("modified_at")).otherwise(lit(null)))

        } else {
          tmsMsgData = tmsMsgData.withColumn("modified_at", lit(null))
        }
         if (hasColumn(tmsMsgData, "ShipCode")) {
          tmsMsgData = tmsMsgData
            .withColumn("ShipCode", when(tmsMsgData.col("ShipCode").isNotNull, tmsMsgData.col("ShipCode")).otherwise(lit(null)))

        } else {
          tmsMsgData = tmsMsgData.withColumn("ShipCode", lit(null))
		  }

        tmsMsgData = tmsMsgData.withColumn("place_deck_ids", when(tmsMsgData.col("place_deck_ids").isNotNull, tmsMsgData.col("place_deck_ids")).otherwise(lit(null)))
        tmsMsgData = tmsMsgData.withColumn("place_section_ids", when(tmsMsgData.col("place_section_ids").isNotNull, tmsMsgData.col("place_section_ids")).otherwise(lit(null)))

        var finaltmsMsgData = tmsMsgData.withColumn("BatchTime", lit(batch_start_time).cast(TimestampType))
          .withColumn("Part_date", to_date(lit(part_start_time)))
          .withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
          .select("id", "public_id", "tms_date", "day_period", "team_member_id", "team_member_role","modified_at", "VoyageID", "BatchTime","ShipCode","Part_date")

        finaltmsMsgData.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.table"))

        if (hasColumn(tmsMsgData, "place_deck_ids")) {

          if (checkArray(tmsMsgData, "place_deck_ids")) {
            tmsMsgData = tmsMsgData.withColumn("place_deck_ids", explode_outer(tmsMsgData.col("place_deck_ids")))
            tmsMsgData = tmsMsgData
              .withColumn("place_deck_ids", when(tmsMsgData.col("place_deck_ids").isNotNull, tmsMsgData.col("place_deck_ids")).otherwise(lit(null)))

              .withColumn("BatchTime", lit(batch_start_time).cast(TimestampType))
              .withColumn("Part_date", to_date(lit(part_start_time)))
              .withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))

          }
        } else {
          tmsMsgData = tmsMsgData.withColumn("place_deck_ids", lit(null))

        }

        var placeDeck = tmsMsgData.select("id", "public_id", "team_member_id", "place_deck_ids","modified_at", "VoyageID", "BatchTime","ShipCode", "Part_date")

        placeDeck.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.placeDeck.target.table"))

        if (hasColumn(tmsMsgData, "place_section_ids")) {

          if (checkArray(tmsMsgData, "place_section_ids")) {
            tmsMsgData = tmsMsgData.withColumn("place_section_ids", explode_outer(tmsMsgData.col("place_section_ids")))
            tmsMsgData = tmsMsgData
              .withColumn("place_section_ids", when(tmsMsgData.col("place_section_ids").isNotNull, tmsMsgData.col("place_section_ids")).otherwise(lit(null)))
              .withColumn("BatchTime", lit(batch_start_time).cast(TimestampType))
              .withColumn("Part_date", to_date(lit(part_start_time)))
              .withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))

          } else if (checkStructType(tmsMsgData, "place_section_ids")) {
            tmsMsgData = tmsMsgData.withColumn("place_section_ids", col("place_section_ids"))
            tmsMsgData = tmsMsgData
              .withColumn("place_section_ids", when(tmsMsgData.col("place_section_ids").isNotNull, tmsMsgData.col("place_section_ids")).otherwise(lit(null)))

              .withColumn("BatchTime", lit(start_execution_time).cast(TimestampType))
              .withColumn("Part_date", to_date(lit(part_write_date)))
              .withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))

          }
        } 
        else {
          tmsMsgData = tmsMsgData.withColumn("place_section_ids", lit(null))

        }

        var placeSection = tmsMsgData.select("id", "public_id", "team_member_id", "place_section_ids","modified_at", "VoyageID","ShipCode", "BatchTime", "Part_date")

        placeSection.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.placeSection.target.table"))

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