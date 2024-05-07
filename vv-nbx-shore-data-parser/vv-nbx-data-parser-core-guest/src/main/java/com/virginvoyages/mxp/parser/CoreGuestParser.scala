package com.virginvoyages.mxp.parser

import org.apache.spark.SparkConf
import java.sql.SQLException
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark.streaming.Seconds
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
import org.apache.spark.streaming.dstream.DStream
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

object CoreGuestParser {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(20))
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

      val batchtStartTime1 = args(0)
      val batchEndTime1 = args(1)

      val batchtStartTime = batchtStartTime1.replace("T", " ")
      val batchEndTime = batchEndTime1.replace("T", " ")

      var partReadStart: String = null
      var partReadEnd: String = null
      val pattern = "\\d{4}-\\d{2}-\\d{2}".r
      partReadStart = (pattern findFirstIn batchtStartTime).map(_.toString).getOrElse("")
      partReadEnd = (pattern findFirstIn batchEndTime).map(_.toString).getOrElse("")

      val whereClause = s""" where batchtime>= '$batchtStartTime' and batchtime<='$batchEndTime' and Part_Date>='$partReadStart' and Part_Date<='$partReadEnd'"""

      println(whereClause)
      val coreGuestDF = spark.sql(spark.sparkContext.getConf.get("spark.source.table").trim() + whereClause)
      var data = coreGuestDF.select("Message", "BatchTime", "Part_Date") //.where(col("BatchTime") >= lit(batchtStartTime) && col("BatchTime") <= lit(batchtStartTime)and (col("part_date")>=lit(partReadStart) and col("part_date")<=lit(partReadStart)))

      //println(data.count)

      if (!data.head(1).isEmpty) {
        spark.sql("set spark.sql.caseSensitive=true")
        val coreGuestMsg = data.select("Message").rdd.map(x => x.toString)
        var coreGuestData = spark.read.json(coreGuestMsg)

        println("schema")
        coreGuestData.printSchema()

         //data for reservation created or update 
        var reservationGuest = coreGuestData.where(coreGuestData.col("n") === "reservation:created" || coreGuestData.col("n") === "reservation:updated")

        println("strting")
        reservationGuest.show()

        if (hasColumn(reservationGuest, "ci")) {
          reservationGuest = reservationGuest
            .withColumn("CI", when(reservationGuest.col("ci").isNotNull, reservationGuest.col("ci")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("CI", lit(null))

        }

        if (hasColumn(reservationGuest, "source")) {
          reservationGuest = reservationGuest
            .withColumn("Source", when(reservationGuest.col("source").isNotNull, reservationGuest.col("source")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("Source", lit(null))

        }

        if (hasColumn(coreGuestData, "currencyCode")) {
          reservationGuest = reservationGuest
            .withColumn("CurrencyCode", when(reservationGuest.col("pl.currencyCode").isNotNull, reservationGuest.col("pl.currencyCode")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("CurrencyCode", lit(null))

        }

        if (hasColumn(reservationGuest, "ts")) {
          reservationGuest = reservationGuest
            .withColumn("TS", when(reservationGuest.col("ts").isNotNull, reservationGuest.col("ts")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("TS", lit(null))

        }

        if (hasColumn(reservationGuest, "n")) {
          reservationGuest = reservationGuest
            .withColumn("N", when(reservationGuest.col("n").isNotNull, reservationGuest.col("n")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("N", lit(null))

        }

        if (hasColumn(reservationGuest, "cn")) {
          reservationGuest = reservationGuest
            .withColumn("CN", when(reservationGuest.col("cn").isNotNull, reservationGuest.col("cn")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("CN", lit(null))

        }

        if (hasColumn(reservationGuest, "tg")) {
          reservationGuest = reservationGuest
            .withColumn("TG", when(reservationGuest.col("tg").isNotNull, reservationGuest.col("tg")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("TG", lit(null))

        }

        if (hasColumn(reservationGuest, "plt")) {
          reservationGuest = reservationGuest
            .withColumn("PLT", when(reservationGuest.col("plt").isNotNull, reservationGuest.col("plt")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("PLT", lit(null))

        }

        if (hasColumn(reservationGuest, "mrc")) {
          reservationGuest = reservationGuest
            .withColumn("MRC", when(reservationGuest.col("mrc").isNotNull, reservationGuest.col("mrc")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("MRC", lit(null))

        }

        if (hasColumn(reservationGuest, "rc")) {
          reservationGuest = reservationGuest

            .withColumn("RC", when(reservationGuest.col("rc").isNotNull, reservationGuest.col("rc")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("RC", lit(null))

        }

        if (hasColumn(reservationGuest, "ri")) {
          reservationGuest = reservationGuest

            .withColumn("RI", when(reservationGuest.col("ri").isNotNull, reservationGuest.col("ri")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("RI", lit(null))

        }

        if (hasColumn(reservationGuest, "encrypted")) {
          reservationGuest = reservationGuest

            .withColumn("Encrypted", when(reservationGuest.col("encrypted").isNotNull, reservationGuest.col("encrypted")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("Encrypted", lit(null))

        }

        if (hasColumn(reservationGuest, "pl.reservationGuestId")) {
          reservationGuest = reservationGuest
            .withColumn("ReservationGuestID", when(reservationGuest.col("pl.reservationGuestId").isNotNull, reservationGuest.col("pl.reservationGuestId")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("ReservationGuestID", lit(null))

        }

        if (hasColumn(reservationGuest, "pl.reservationId")) {
          reservationGuest = reservationGuest
            .withColumn("ReservationID", when(reservationGuest.col("pl.reservationId").isNotNull, reservationGuest.col("pl.reservationId")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("ReservationID", lit(null))

        }

        if (hasColumn(reservationGuest, "pl.reservationNumber")) {
          reservationGuest = reservationGuest
            .withColumn("ReservationNumber", when(reservationGuest.col("pl.reservationNumber").isNotNull, reservationGuest.col("pl.reservationNumber")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("ReservationNumber", lit(null))

        }

        if (hasColumn(reservationGuest, "pl.bookedByGuestId")) {
          reservationGuest = reservationGuest
            .withColumn("BookedbyGuestId", when(reservationGuest.col("pl.bookedByGuestId").isNotNull, reservationGuest.col("pl.bookedByGuestId")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("BookedbyGuestId", lit(null))

        }

        if (hasColumn(reservationGuest, "pl.shipCode")) {
          reservationGuest = reservationGuest
            .withColumn("ShipCode", when(reservationGuest.col("pl.shipCode").isNotNull, reservationGuest.col("pl.shipCode")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("ShipCode", lit(null))

        }

        if (hasColumn(reservationGuest, "pl.sailingPackageCode")) {
          reservationGuest = reservationGuest
            .withColumn("SailingPackageCode", when(reservationGuest.col("pl.sailingPackageCode").isNotNull, reservationGuest.col("pl.sailingPackageCode")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("SailingPackageCode", lit(null))

        }

        if (hasColumn(reservationGuest, "pl.fromDate")) {
          reservationGuest = reservationGuest
            .withColumn("FromDate", when(reservationGuest.col("pl.fromDate").isNotNull, reservationGuest.col("pl.fromDate")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("FromDate", lit(null))

        }

        if (hasColumn(reservationGuest, "pl.toDate")) {
          reservationGuest = reservationGuest
            .withColumn("ToDate", when(reservationGuest.col("pl.toDate").isNotNull, reservationGuest.col("pl.toDate")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("ToDate", lit(null))

        }

        if (hasColumn(reservationGuest, "pl.bookedDate")) {
          reservationGuest = reservationGuest
            .withColumn("BookedDate", when(reservationGuest.col("pl.bookedDate").isNotNull, reservationGuest.col("pl.bookedDate")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("BookedDate", lit(null))

        }

        if (hasColumn(reservationGuest, "pl.guestCount")) {
          reservationGuest = reservationGuest
            .withColumn("GuestCount", when(reservationGuest.col("pl.guestCount").isNotNull, reservationGuest.col("pl.guestCount")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("GuestCount", lit(null))

        }

        if (hasColumn(coreGuestData, "pl.stateroomcount")) {
          reservationGuest = reservationGuest
            .withColumn("StateRoomCount", when(reservationGuest.col("pl.stateroomcount").isNotNull, reservationGuest.col("pl.stateroomcount")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("StateRoomCount", lit(null))

        }

        if (hasColumn(reservationGuest, "pl.voyageNumber")) {
          reservationGuest = reservationGuest
            .withColumn("VoyageNumber", when(reservationGuest.col("pl.voyageNumber").isNotNull, reservationGuest.col("pl.voyageNumber")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("VoyageNumber", lit(null))

        }

        if (hasColumn(reservationGuest, "pl.isCabinCruiseContractOptIn")) {
          reservationGuest = reservationGuest
            .withColumn("isCabinCruiseContractOptIn", when(reservationGuest.col("pl.isCabinCruiseContractOptIn").isNotNull, reservationGuest.col("pl.isCabinCruiseContractOptIn")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("isCabinCruiseContractOptIn", lit(null))

        }

        if (hasColumn(reservationGuest, "pl.isPartyExpenseLimitOptIn")) {
          reservationGuest = reservationGuest
            .withColumn("isPartyExpenseLimitOptIn", when(reservationGuest.col("pl.isPartyExpenseLimitOptIn").isNotNull, reservationGuest.col("pl.isPartyExpenseLimitOptIn")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("isPartyExpenseLimitOptIn", lit(null))

        }

        if (hasColumn(reservationGuest, "pl.reservationStatusCode")) {
          reservationGuest = reservationGuest
            .withColumn("ReservationStatusCode", when(reservationGuest.col("pl.reservationStatusCode").isNotNull, reservationGuest.col("pl.reservationStatusCode")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("ReservationStatusCode", lit(null))

        }

        if (hasColumn(reservationGuest, "pl.guestTypeCode")) {
          reservationGuest = reservationGuest
            .withColumn("GuestTypeCode", when(reservationGuest.col("pl.guestTypeCode").isNotNull, reservationGuest.col("pl.guestTypeCode")).otherwise(lit(null)))

        } else {
          reservationGuest = reservationGuest.withColumn("GuestTypeCode", lit(null))

        }

        reservationGuest = reservationGuest.withColumn("Folios", when(reservationGuest.col("pl.folios").isNotNull, reservationGuest.col("pl.folios")).otherwise(lit(null)))
          .withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
          .withColumn("Batchtime", lit(batchtStartTime).cast(TimestampType))
          .withColumn("Part_date", to_date(lit(partReadStart)))

        val pl_guestData = reservationGuest.select("CI", "Source", "TS", "N", "CN", "TG", "PLT", "MRC", "RC", "RI", "Encrypted",
          "ReservationGuestID", "ReservationID", "ReservationNumber", "BookedbyGuestId", "ShipCode", "SailingPackageCode",
          "FromDate", "ToDate", "BookedDate", "CurrencyCode", "GuestCount", "StateRoomCount", "VoyageNumber",
          "ReservationStatusCode", "isCabinCruiseContractOptIn", "isPartyExpenseLimitOptIn",
          "VoyageID", "Batchtime", "Part_date")

        pl_guestData.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.reservation.target.table"))

         // Event type :- reservationguest created or updated

        var reservationGuestUpdt = coreGuestData.where(coreGuestData.col("n") === "reservationguest:updated" || coreGuestData.col("n") === "reservationguest:created")

        if (hasColumn(reservationGuestUpdt, "ci")) {
          reservationGuestUpdt = reservationGuestUpdt
            .withColumn("CI", when(reservationGuestUpdt.col("ci").isNotNull, reservationGuestUpdt.col("ci")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("CI", lit(null))

        }

        if (hasColumn(reservationGuestUpdt, "source")) {
          reservationGuestUpdt = reservationGuestUpdt

            .withColumn("Source", when(reservationGuestUpdt.col("source").isNotNull, reservationGuestUpdt.col("source")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("Source", lit(null))

        }

        if (hasColumn(reservationGuestUpdt, "ts")) {
          reservationGuestUpdt = reservationGuestUpdt
            .withColumn("TS", when(reservationGuestUpdt.col("ts").isNotNull, reservationGuestUpdt.col("ts")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("TS", lit(null))

        }

        if (hasColumn(reservationGuestUpdt, "n")) {
          reservationGuestUpdt = reservationGuestUpdt
            .withColumn("N", when(reservationGuestUpdt.col("n").isNotNull, reservationGuestUpdt.col("n")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("N", lit(null))

        }

        if (hasColumn(reservationGuestUpdt, "cn")) {
          reservationGuestUpdt = reservationGuestUpdt
            .withColumn("CN", when(reservationGuestUpdt.col("cn").isNotNull, reservationGuestUpdt.col("cn")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("CN", lit(null))

        }

        if (hasColumn(reservationGuestUpdt, "tg")) {
          reservationGuestUpdt = reservationGuestUpdt
            .withColumn("TG", when(reservationGuestUpdt.col("tg").isNotNull, reservationGuestUpdt.col("tg")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("TG", lit(null))

        }

        if (hasColumn(reservationGuestUpdt, "plt")) {
          reservationGuestUpdt = reservationGuestUpdt
            .withColumn("PLT", when(reservationGuestUpdt.col("plt").isNotNull, reservationGuestUpdt.col("plt")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("PLT", lit(null))

        }

        if (hasColumn(reservationGuestUpdt, "mrc")) {
          reservationGuestUpdt = reservationGuestUpdt
            .withColumn("MRC", when(reservationGuestUpdt.col("mrc").isNotNull, reservationGuestUpdt.col("mrc")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("MRC", lit(null))

        }

        if (hasColumn(reservationGuestUpdt, "rc")) {
          reservationGuestUpdt = reservationGuestUpdt
            .withColumn("RC", when(reservationGuestUpdt.col("rc").isNotNull, reservationGuestUpdt.col("rc")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("RC", lit(null))

        }

        if (hasColumn(reservationGuestUpdt, "ri")) {
          reservationGuestUpdt = reservationGuestUpdt
            .withColumn("RI", when(reservationGuestUpdt.col("ri").isNotNull, reservationGuestUpdt.col("ri")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("RI", lit(null))

        }

        if (hasColumn(reservationGuestUpdt, "encrypted")) {
          reservationGuestUpdt = reservationGuestUpdt
            .withColumn("Encrypted", when(reservationGuestUpdt.col("encrypted").isNotNull, reservationGuestUpdt.col("encrypted")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("Encrypted", lit(null))

        }

        if (hasColumn(reservationGuestUpdt, "pl.reservationGuestId")) {
          reservationGuestUpdt = reservationGuestUpdt
            .withColumn("ReservationGuestID", when(reservationGuestUpdt.col("pl.reservationGuestId").isNotNull, reservationGuestUpdt.col("pl.reservationGuestId")).otherwise(lit(null)))
          println("if part")

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("ReservationGuestID", lit(null))
          println("else")

        }

        if (hasColumn(reservationGuestUpdt, "pl.reservationId")) {
          reservationGuestUpdt = reservationGuestUpdt
            .withColumn("ReservationID", when(reservationGuestUpdt.col("pl.reservationId").isNotNull, reservationGuestUpdt.col("pl.reservationId")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("ReservationID", lit(null))

        }

        if (hasColumn(reservationGuestUpdt, "pl.reservationID")) {
          reservationGuestUpdt = reservationGuestUpdt
            .withColumn("PL_ReservationID", when(reservationGuestUpdt.col("pl.reservationID").isNotNull, reservationGuestUpdt.col("pl.reservationID")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("PL_ReservationID", lit(null))

        }

        if (hasColumn(reservationGuestUpdt, "pl.guestId")) {
          reservationGuestUpdt = reservationGuestUpdt
            .withColumn("GuestID", when(reservationGuestUpdt.col("pl.guestId").isNotNull, reservationGuestUpdt.col("pl.guestId")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("GuestID", lit(null))

        }

        if (hasColumn(reservationGuestUpdt, "pl.embarkDate")) {
          reservationGuestUpdt = reservationGuestUpdt
            .withColumn("EmbarkDate", when(reservationGuestUpdt.col("pl.embarkDate").isNotNull, reservationGuestUpdt.col("pl.embarkDate")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("EmbarkDate", lit(null))

        }

        if (hasColumn(reservationGuestUpdt, "pl.debarkDate")) {
          reservationGuestUpdt = reservationGuestUpdt
            .withColumn("DebarkDate", when(reservationGuestUpdt.col("pl.debarkDate").isNotNull, reservationGuestUpdt.col("pl.debarkDate")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("DebarkDate", lit(null))

        }

        if (hasColumn(reservationGuestUpdt, "pl.reservationStatusCode")) {
          reservationGuestUpdt = reservationGuestUpdt

            .withColumn("ReservationStatusCode", when(reservationGuestUpdt.col("pl.reservationStatusCode").isNotNull, reservationGuestUpdt.col("pl.reservationStatusCode")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("ReservationStatusCode", lit(null))

        }
        if (hasColumn(reservationGuestUpdt, "pl.guestTypeCode")) {
          reservationGuestUpdt = reservationGuestUpdt

            .withColumn("GuestTypeCode", when(reservationGuestUpdt.col("pl.guestTypeCode").isNotNull, reservationGuestUpdt.col("pl.guestTypeCode")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("GuestTypeCode", lit(null))

        }
        if (hasColumn(reservationGuestUpdt, "pl.modifiedByUser")) {
          reservationGuestUpdt = reservationGuestUpdt

            .withColumn("ModifiedByUser", when(reservationGuestUpdt.col("pl.modifiedByUser").isNotNull, reservationGuestUpdt.col("pl.modifiedByUser")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("ModifiedByUser", lit(null))

        }
        if (hasColumn(reservationGuestUpdt, "pl.modificationTime")) {
          reservationGuestUpdt = reservationGuestUpdt

            .withColumn("ModificationTime", when(reservationGuestUpdt.col("pl.modificationTime").isNotNull, reservationGuestUpdt.col("pl.modificationTime")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("ModificationTime", lit(null))

        }
        if (hasColumn(reservationGuestUpdt, "pl.isVIP")) {
          reservationGuestUpdt = reservationGuestUpdt

            .withColumn("isVIP", when(reservationGuestUpdt.col("pl.isVIP").isNotNull, reservationGuestUpdt.col("pl.isVIP")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("isVIP", lit(null))

        }
        if (hasColumn(reservationGuestUpdt, "pl.isPrimary")) {
          reservationGuestUpdt = reservationGuestUpdt

            .withColumn("isPrimary", when(reservationGuestUpdt.col("pl.isPrimary").isNotNull, reservationGuestUpdt.col("pl.isPrimary")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("isPrimary", lit(null))

        }
        if (hasColumn(reservationGuestUpdt, "pl.stateroom")) {
          reservationGuestUpdt = reservationGuestUpdt

            .withColumn("Stateroom", when(reservationGuestUpdt.col("pl.stateroom").isNotNull, reservationGuestUpdt.col("pl.stateroom")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("Stateroom", lit(null))

        }
        if (hasColumn(reservationGuestUpdt, "pl.isInsuranceDeclined")) {
          reservationGuestUpdt = reservationGuestUpdt

            .withColumn("InsuranceDeclined", when(reservationGuestUpdt.col("pl.isInsuranceDeclined").isNotNull, reservationGuestUpdt.col("pl.isInsuranceDeclined")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("InsuranceDeclined", lit(null))

        }
        if (hasColumn(reservationGuestUpdt, "pl.isRoutedTo")) {
          reservationGuestUpdt = reservationGuestUpdt

            .withColumn("isRoutedTo", when(reservationGuestUpdt.col("pl.isRoutedTo").isNotNull, reservationGuestUpdt.col("pl.isRoutedTo")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("isRoutedTo", lit(null))

        }
        if (hasColumn(reservationGuestUpdt, "pl.isBackToBack")) {
          reservationGuestUpdt = reservationGuestUpdt

            .withColumn("isBackToBack", when(reservationGuestUpdt.col("pl.isBackToBack").isNotNull, reservationGuestUpdt.col("pl.isBackToBack")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("isBackToBack", lit(null))

        }
        if (hasColumn(reservationGuestUpdt, "pl.guestTypeCode")) {
          reservationGuestUpdt = reservationGuestUpdt

            .withColumn("GuestTypeCode", when(reservationGuestUpdt.col("pl.guestTypeCode").isNotNull, reservationGuestUpdt.col("pl.guestTypeCode")).otherwise(lit(null)))

        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("GuestTypeCode", lit(null))

        }

        reservationGuestUpdt = reservationGuestUpdt.withColumn("Folios", when(reservationGuestUpdt.col("pl.folios").isNotNull, reservationGuestUpdt.col("pl.folios")).otherwise(lit(null)))
          .withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
          .withColumn("Batchtime", lit(batchtStartTime).cast(TimestampType))
          .withColumn("Part_date", to_date(lit(partReadStart)))

        val pl_reseGuest = reservationGuestUpdt.select("CI", "Source", "TS", "N", "CN", "TG", "PLT", "MRC", "RC", "RI", "Encrypted",
          "ReservationGuestID", "ReservationID", "GuestID", "EmbarkDate", "DebarkDate", "ReservationStatusCode",
          "GuestTypeCode", "ModifiedByUser", "ModificationTime", "isVIP", "isPrimary", "Stateroom",
          "InsuranceDeclined", "isRoutedTo", "isBackToBack", "VoyageID", "Batchtime", "Part_date")

        println("pl_res guest")
        pl_reseGuest.show
        pl_reseGuest.printSchema
        pl_reseGuest.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.resguest.target.table"))

        //folio array for reservation guest

        if (hasColumn(reservationGuestUpdt, "Folios")) {
          println("chcek array")
          if (checkArray(reservationGuestUpdt, "Folios")) {
            reservationGuestUpdt = reservationGuestUpdt.withColumn("Folios", explode_outer(reservationGuestUpdt.col("Folios")))
            reservationGuestUpdt = reservationGuestUpdt
              .withColumn("CreatedByUser", when(reservationGuestUpdt.col("Folios.createdByUser").isNotNull, reservationGuestUpdt.col("Folios.createdByUser")).otherwise(lit(null)))
              .withColumn("CreationTime", when(reservationGuestUpdt.col("Folios.creationTime").isNotNull, reservationGuestUpdt.col("Folios.creationTime")).otherwise(lit(null)))
              .withColumn("ModifiedByUser", when(reservationGuestUpdt.col("Folios.modifiedByUser").isNotNull, reservationGuestUpdt.col("Folios.modifiedByUser")).otherwise(lit(null)))
              .withColumn("ModificationTime", when(reservationGuestUpdt.col("Folios.modificationTime").isNotNull, reservationGuestUpdt.col("Folios.modificationTime")).otherwise(lit(null)))
              .withColumn("FolioID", when(reservationGuestUpdt.col("Folios.folioId").isNotNull, reservationGuestUpdt.col("Folios.folioId")).otherwise(lit(null)))
              .withColumn("Folio_ReservationGuestID", when(reservationGuestUpdt.col("Folios.reservationGuestId").isNotNull, reservationGuestUpdt.col("Folios.reservationGuestId")).otherwise(lit(null)))
              .withColumn("AccessCardNumber", when(reservationGuestUpdt.col("Folios.accessCardNumber").isNotNull, reservationGuestUpdt.col("Folios.accessCardNumber")).otherwise(lit(null)))
              .withColumn("Folio_AtRisk", when(reservationGuestUpdt.col("Folios.isAtRisk").isNotNull, reservationGuestUpdt.col("Folios.isAtRisk")).otherwise(lit(null)))
              .withColumn("Folio_GangwayAllowed", when(reservationGuestUpdt.col("Folios.isGangwayAllowed").isNotNull, reservationGuestUpdt.col("Folios.isGangwayAllowed")).otherwise(lit(null)))
              .withColumn("Folio_Deleted", when(reservationGuestUpdt.col("Folios.isDeleted").isNotNull, reservationGuestUpdt.col("Folios.isDeleted")).otherwise(lit(null)))
              .withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
              .withColumn("Batchtime", lit(batchtStartTime).cast(TimestampType))
              .withColumn("Part_date", to_date(lit(partReadStart)))

          } else if (checkStructType(reservationGuestUpdt, "Folios")) {
            reservationGuestUpdt = reservationGuestUpdt.withColumn("Folios", col("Folios"))
            reservationGuestUpdt = reservationGuestUpdt
              .withColumn("CreatedByUser", when(reservationGuestUpdt.col("Folios.createdByUser").isNotNull, reservationGuestUpdt.col("Folios.createdByUser")).otherwise(lit(null)))
              .withColumn("CreationTime", when(reservationGuestUpdt.col("Folios.creationTime").isNotNull, reservationGuestUpdt.col("Folios.creationTime")).otherwise(lit(null)))
              .withColumn("ModifiedByUser", when(reservationGuestUpdt.col("Folios.modifiedByUser").isNotNull, reservationGuestUpdt.col("Folios.modifiedByUser")).otherwise(lit(null)))
              .withColumn("ModificationTime", when(reservationGuestUpdt.col("Folios.modificationTime").isNotNull, reservationGuestUpdt.col("Folios.modificationTime")).otherwise(lit(null)))
              .withColumn("FolioID", when(reservationGuestUpdt.col("Folios.folioId").isNotNull, reservationGuestUpdt.col("Folios.folioId")).otherwise(lit(null)))
              .withColumn("FolioID", when(reservationGuestUpdt.col("Folios.folioNumber").isNotNull, reservationGuestUpdt.col("Folios.folioNumber")).otherwise(lit(null)))
              .withColumn("FolioStatus", when(reservationGuestUpdt.col("Folios.folioStatus").isNotNull, reservationGuestUpdt.col("Folios.folioStatus")).otherwise(lit(null)))
              .withColumn("FolioType", when(reservationGuestUpdt.col("Folios.folioType").isNotNull, reservationGuestUpdt.col("Folios.folioType")).otherwise(lit(null)))
              .withColumn("Folio_ReservationGuestID", when(reservationGuestUpdt.col("Folios.reservationGuestId").isNotNull, reservationGuestUpdt.col("Folios.reservationGuestId")).otherwise(lit(null)))
              .withColumn("AccessCardNumber", when(reservationGuestUpdt.col("Folios.accessCardNumber").isNotNull, reservationGuestUpdt.col("Folios.accessCardNumber")).otherwise(lit(null)))
              .withColumn("Folio_AtRisk", when(reservationGuestUpdt.col("Folios.isAtRisk").isNotNull, reservationGuestUpdt.col("Folios.isAtRisk")).otherwise(lit(null)))
              .withColumn("Folio_GangwayAllowed", when(reservationGuestUpdt.col("Folios.isGangwayAllowed").isNotNull, reservationGuestUpdt.col("Folios.isGangwayAllowed")).otherwise(lit(null)))
              .withColumn("Folio_Deleted", when(reservationGuestUpdt.col("Folios.isDeleted").isNotNull, reservationGuestUpdt.col("Folios.isDeleted")).otherwise(lit(null)))
              .withColumn("Folio_creditBalance", when(reservationGuestUpdt.col("Folios.creditBalance").isNotNull, reservationGuestUpdt.col("Folios.creditBalance")).otherwise(lit(null)))
              .withColumn("Folio_debitbalance", when(reservationGuestUpdt.col("Folios.debitBalance").isNotNull, reservationGuestUpdt.col("Folios.debitBalance")).otherwise(lit(null)))
              .withColumn("ReservationGuestID", when(reservationGuestUpdt.col("pl.reservationGuestId").isNotNull, reservationGuestUpdt.col("pl.reservationGuestId")).otherwise(lit(null)))
              .withColumn("ReservationID", when(reservationGuestUpdt.col("pl.reservationId").isNotNull, reservationGuestUpdt.col("pl.reservationId")).otherwise(lit(null)))
              .withColumn("GuestID", when(reservationGuestUpdt.col("pl.guestId").isNotNull, reservationGuestUpdt.col("pl.guestId")).otherwise(lit(null)))
              .withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
              .withColumn("Batchtime", lit(batchtStartTime).cast(TimestampType))
              .withColumn("Part_date", to_date(lit(partReadStart)))

          }
        } else {
          reservationGuestUpdt = reservationGuestUpdt.withColumn("CreatedByUser", lit(null))
            .withColumn("CreationTime", lit(null))
            .withColumn("ModifiedByUser", lit(null))
            .withColumn("ModificationTime", lit(null))
            .withColumn("FolioID", lit(null))
            .withColumn("ReservationGuestID", lit(null))
            .withColumn("AccessCardNumber", lit(null))
            .withColumn("Folio_AtRisk", lit(null))
            .withColumn("Folio_GangwayAllowed", lit(null))
            .withColumn("Folio_Deleted", lit(null))
            .withColumn("VoyageID", lit(null))

        }

        val folioresGuest = reservationGuestUpdt.select("CreatedByUser", "CreationTime", "ModifiedByUser", "ModificationTime", "FolioID",
          "Folio_ReservationGuestID", "AccessCardNumber", "Folio_AtRisk", "Folio_GangwayAllowed", "Folio_Deleted", "ReservationGuestID", "ReservationID", "GuestID", "VoyageID", "Batchtime", "Part_date")

        println("folio guest data")
        folioresGuest.show()

        folioresGuest.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.resguest.folio.target.table"))

      }

    } catch {

      case e: Exception =>
        {

          log.info("in the catch of updateStatus ******************")
          e.printStackTrace()
          throw new Exception("General Exception..please check the stacktrace")
        }

    }
  }
}
