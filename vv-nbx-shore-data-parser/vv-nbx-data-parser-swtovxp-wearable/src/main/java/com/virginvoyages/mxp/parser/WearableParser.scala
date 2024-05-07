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
import com.virginvoyages.metadataframework.ManageMetadata

object WearableParser {

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

      var wearableData = spark.read.json(wearableMsg).withColumn("BatchTime", lit(startExecutionTime)).withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id"))).withColumn("Part_date", lit(partDate))

      if (!wearableData.head(1).isEmpty) {

        wearableData = wearableData
          .withColumn("CI", when(wearableData.col("ci").isNotNull, wearableData.col("ci")).otherwise(lit(null)))
          .withColumn("Source", when(wearableData.col("source").isNotNull, wearableData.col("source")).otherwise(lit(null)))
          .withColumn("TS", when(wearableData.col("ts").isNotNull, wearableData.col("ts")).otherwise(lit(null)))
          .withColumn("N", when(wearableData.col("n").isNotNull, wearableData.col("n")).otherwise(lit(null)))
          .withColumn("CN", when(wearableData.col("cn").isNotNull, wearableData.col("cn")).otherwise(lit(null)))
          .withColumn("TG", when(wearableData.col("tg").isNotNull, wearableData.col("tg")).otherwise(lit(null)))
          .withColumn("ReservationId", when(wearableData.col("pl.reservationId").isNotNull, wearableData.col("pl.reservationId")).otherwise(lit(null)))
          //.withColumn("AddressLine1", when(wearableData.col("pl.mailingAddress.addressLine1").isNotNull, wearableData.col("pl.mailingAddress.addressLine1")).otherwise(lit(null)))
          //.withColumn("AddressLine2", when(wearableData.col("pl.mailingAddress.addressLine2").isNotNull, wearableData.col("pl.mailingAddress.addressLine2")).otherwise(lit(null)))
          //.withColumn("AddressLine3", when(wearableData.col("pl.mailingAddress.addressLine3").isNotNull, wearableData.col("pl.mailingAddress.addressLine3")).otherwise(lit(null)))
          .withColumn("City", when(wearableData.col("pl.mailingAddress.city").isNotNull, wearableData.col("pl.mailingAddress.city")).otherwise(lit(null)))
          .withColumn("State", when(wearableData.col("pl.mailingAddress.state").isNotNull, wearableData.col("pl.mailingAddress.state")).otherwise(lit(null)))
          .withColumn("Zip", when(wearableData.col("pl.mailingAddress.zip").isNotNull, wearableData.col("pl.mailingAddress.zip")).otherwise(lit(null)))
          .withColumn("CountryCode", when(wearableData.col("pl.mailingAddress.countryCode").isNotNull, wearableData.col("pl.mailingAddress.countryCode")).otherwise(lit(null)))
          .withColumn("guestList", when(wearableData.col("pl.guestList").isNotNull, wearableData.col("pl.guestList")).otherwise(lit(null)))
          .withColumn("shipcode", when(wearableData.col("pl.shipCode").isNotNull, wearableData.col("pl.shipCode")).otherwise(lit(null)))
          .withColumn("shipnm", when(wearableData.col("pl.shipName").isNotNull, wearableData.col("pl.shipName")).otherwise(lit(null)))
          .withColumn("sailStrtdt", when(wearableData.col("pl.sailStart").isNotNull, wearableData.col("pl.sailStart")).otherwise(lit(null)).cast(TimestampType))

        if (hasColumn(wearableData, "pl.mailingAddress.addressLine1")) {
          wearableData = wearableData
            .withColumn("AddressLine1", when(wearableData.col("pl.mailingAddress.addressLine1").isNotNull, wearableData.col("pl.mailingAddress.addressLine1")).otherwise(lit(null)))

        } else {
          wearableData = wearableData.withColumn("AddressLine1", lit(null))
        }
        if (hasColumn(wearableData, "pl.mailingAddress.addressLine2")) {
          wearableData = wearableData
            .withColumn("AddressLine2", when(wearableData.col("pl.mailingAddress.addressLine2").isNotNull, wearableData.col("pl.mailingAddress.addressLine2")).otherwise(lit(null)))

        } else {
          wearableData = wearableData.withColumn("AddressLine2", lit(null))
        }

        if (hasColumn(wearableData, "pl.mailingAddress.addressLine3")) {
          wearableData = wearableData
            .withColumn("AddressLine3", when(wearableData.col("pl.mailingAddress.addressLine3").isNotNull, wearableData.col("pl.mailingAddress.addressLine3")).otherwise(lit(null)))

        } else {
          wearableData = wearableData.withColumn("AddressLine3", lit(null))
        }
        if (hasColumn(wearableData, "guestList")) {

          if (checkArray(wearableData, "guestList")) {
            wearableData = wearableData.withColumn("guestList", explode_outer(wearableData.col("guestList")))
            wearableData = wearableData
              .withColumn("GuestSequence", when(wearableData.col("guestList.guestSequence").isNotNull, wearableData.col("guestList.guestSequence")).otherwise(lit(null)))
              .withColumn("FirstName", when(wearableData.col("guestList.firstName").isNotNull, wearableData.col("guestList.firstName")).otherwise(lit(null)))
              .withColumn("LastName", when(wearableData.col("guestList.lastName").isNotNull, wearableData.col("guestList.lastName")).otherwise(lit(null)))
              .withColumn("ClientId", when(wearableData.col("guestList.clientID").isNotNull, wearableData.col("guestList.clientID")).otherwise(lit(null)))
              .withColumn("guestID", when(wearableData.col("guestList.guestID").isNotNull, wearableData.col("guestList.guestID")).otherwise(lit(null)))
              .withColumn("guestType", when(wearableData.col("guestList.guestType").isNotNull, wearableData.col("guestList.guestType")).otherwise(lit(null)))
              .withColumn("guestQR", when(wearableData.col("guestList.guestQR").isNotNull, wearableData.col("guestList.guestQR")).otherwise(lit(null)))
              .withColumn("GuestList_cabinCategory", when(wearableData.col("guestList.cabinCategory").isNotNull, wearableData.col("guestList.cabinCategory")).otherwise(lit(null)))
              .withColumn("GuestList_cabinNumber", when(wearableData.col("guestList.cabinNumber").isNotNull, wearableData.col("guestList.cabinNumber")).otherwise(lit(null)))
              .withColumn("GuestList_decknum", when(wearableData.col("guestList.deckNumber").isNotNull, wearableData.col("guestList.deckNumber")).otherwise(lit(null)))

          } else if (checkStructType(wearableData, "guestList")) {
            wearableData = wearableData.withColumn("guestList", col("guestList"))
            wearableData = wearableData
              .withColumn("GuestSequence", when(wearableData.col("guestList.guestSequence").isNotNull, wearableData.col("guestList.guestSequence")).otherwise(lit(null)))
              .withColumn("FirstName", when(wearableData.col("guestList.firstName").isNotNull, wearableData.col("guestList.firstName")).otherwise(lit(null)))
              .withColumn("LastName", when(wearableData.col("guestList.lastName").isNotNull, wearableData.col("guestList.lastName")).otherwise(lit(null)))
              .withColumn("ClientId", when(wearableData.col("guestList.clientID").isNotNull, wearableData.col("guestList.clientID")).otherwise(lit(null)))
              .withColumn("guestID", when(wearableData.col("guestList.guestID").isNotNull, wearableData.col("guestList.guestID")).otherwise(lit(null)))
              .withColumn("guestType", when(wearableData.col("guestList.guestType").isNotNull, wearableData.col("guestList.guestType")).otherwise(lit(null)))
              .withColumn("guestQR", when(wearableData.col("guestList.guestQR").isNotNull, wearableData.col("guestList.guestQR")).otherwise(lit(null)))
              .withColumn("GuestList_cabinCategory", when(wearableData.col("guestList.cabinCategory").isNotNull, wearableData.col("guestList.cabinCategory")).otherwise(lit(null)))
              .withColumn("GuestList_cabinNumber", when(wearableData.col("guestList.cabinNumber").isNotNull, wearableData.col("guestList.cabinNumber")).otherwise(lit(null)))
              .withColumn("GuestList_decknum", when(wearableData.col("guestList.deckNumber").isNotNull, wearableData.col("guestList.deckNumber")).otherwise(lit(null)))

          }
        } else {
          wearableData = wearableData.withColumn("GuestSequence", lit(null))
            .withColumn("FirstName", lit(null))
            .withColumn("LastName", lit(null))
            .withColumn("ClientID", lit(null))
            .withColumn("guestID", lit(null))
            .withColumn("guestType", lit(null))
            .withColumn("guestQR", lit(null))
            .withColumn("GuestList_cabinCategory", lit(null))
            .withColumn("GuestList_cabinNumber", lit(null))
            .withColumn("GuestList_decknum", lit(null))
        }

        var finalDf = wearableData.select("ReservationId", "AddressLine1", "AddressLine2", "AddressLine3", "city", "State", "Zip", "CountryCode", "GuestSequence", "FirstName", "LastName", "ClientID", "guestID", "guestType", "guestQR", "GuestList_cabinCategory", "GuestList_cabinNumber", "GuestList_decknum", "shipcode", "shipnm", "sailStrtdt", "BatchTime", "VoyageID", "Part_date")

        finalDf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.table"))

      }

      ManageMetadata.updateStatus(batchInstanceId, batchId, "Successful", spark)

    } catch {

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