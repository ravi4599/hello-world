package com.virginvoyages.vxp.parser

import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.DataFrame
import java.io._
import scala.util.Try
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ BooleanType, StringType}
import com.virginvoyages.metadataframework.ManageMetadata

object ErrorReportingSewareWearableParser {
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
      val batchStartTime = metadata._3
      batchId = metadata._1
      batchInstanceId = metadata._2
      val batchEndTime = metadata._4
      val partStartTime = metadata._5.toString()
      val partEndTime = metadata._6.toString()
      val startExecutionTime = metadata._7.toString()
      val partDate = metadata._8.toString()

      var errorReportingSeawareWearablesDf = spark.sql("select * from shipdw.hvtb_lnd_error_reporting_seaware_wearables")
      var data = errorReportingSeawareWearablesDf.select("message", "batchtime", "part_date", "shipcode").where($"batchtime" >= lit(batchStartTime).cast(TimestampType) && $"batchtime" <= lit(batchEndTime).cast(TimestampType) and ($"part_date".between(partStartTime, partEndTime)))

      var errReptSWwareMsg = errorReportingSeawareWearablesDf.select("message").rdd.map { x => x.toString }
      val shipcode = errorReportingSeawareWearablesDf.select("shipcode").head(1)(0).mkString

      var errReptSWwareData = spark.read.json(errReptSWwareMsg)

      if (!errReptSWwareData.head(1).isEmpty) {

        errReptSWwareData = errReptSWwareData.withColumn("ci", when(errReptSWwareData.col("ci").isNotNull, errReptSWwareData.col("ci")).otherwise(lit(null)))
          .withColumn("cn", when(errReptSWwareData.col("cn").isNotNull, errReptSWwareData.col("cn")).otherwise(lit(null)))
          .withColumn("n", when(errReptSWwareData.col("n").isNotNull, errReptSWwareData.col("n")).otherwise(lit(null)))
          .withColumn("plt", when(errReptSWwareData.col("plt").isNotNull, errReptSWwareData.col("plt")).otherwise(lit(null)))
          .withColumn("source", when(errReptSWwareData.col("source").isNotNull, errReptSWwareData.col("source")).otherwise(lit(null)))
          .withColumn("ts", when(errReptSWwareData.col("ts").isNotNull, errReptSWwareData.col("ts")).otherwise(lit(null)))
          .withColumn("tg", when(errReptSWwareData.col("tg").isNotNull, errReptSWwareData.col("tg")).otherwise(lit(null)))
          .withColumn("seawareListenerSuccessful", when(errReptSWwareData.col("pl.info.seawareListenerSuccessful").isNotNull, errReptSWwareData.col("pl.info.seawareListenerSuccessful")).otherwise(lit(null)))
          .withColumn("wearableType", when(errReptSWwareData.col("pl.info.wearableType").isNotNull, errReptSWwareData.col("pl.info.wearableType")).otherwise(lit(null)))
          .withColumn("reservationId", when(errReptSWwareData.col("pl.wearableAddressPayload.reservationId").isNotNull, errReptSWwareData.col("pl.wearableAddressPayload.reservationId")).otherwise(lit(null)))
          .withColumn("errors", when(errReptSWwareData.col("pl.errors").isNotNull, errReptSWwareData.col("pl.errors")).otherwise(lit(null)))
          .withColumn("guestList", when(errReptSWwareData.col("pl.guestList").isNotNull, errReptSWwareData.col("pl.guestList")).otherwise(lit(null)))
          .withColumn("voyageid", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
          .withColumn("batchtime", lit(startExecutionTime)).withColumn("shipcode", lit(shipcode).cast(StringType))
          .withColumn("part_date", lit(partDate))

        var errorFirstDf = errReptSWwareData.select("ci", "cn", "n", "plt", "source", "ts", "tg", "seawareListenerSuccessful", "wearableType", "reservationId", "voyageid", "batchtime", "shipcode", "part_date")

        errorFirstDf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.errorreservation.table"))
        log.info("errorReservation")
        
        /*error details - array or null */

        if (hasColumn(errReptSWwareData, "errors")) {
          if (checkArray(errReptSWwareData, "errors")) {
            errReptSWwareData = errReptSWwareData.withColumn("errors", explode_outer(errReptSWwareData.col("errors")))
          } else {
            errReptSWwareData = errReptSWwareData.withColumn("errors", col("errors"))
          }
          errReptSWwareData = errReptSWwareData.withColumn("errorCode", when(errReptSWwareData.col("errors.errorCode").isNotNull, errReptSWwareData.col("errors.errorCode")).otherwise(lit(null)))
            .withColumn("errorDetail", when(errReptSWwareData.col("errors.errorDetail").isNotNull, errReptSWwareData.col("errors.errorDetail")).otherwise(lit(null)))
            .withColumn("voyageid", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
            .withColumn("batchtime", lit(startExecutionTime)).withColumn("shipcode", lit(shipcode).cast(StringType))
            .withColumn("part_date", lit(partDate))
        } else {
          errReptSWwareData = errReptSWwareData.withColumn("errorCode", lit(null))
            .withColumn("errorDetail", lit(null))
            .withColumn("voyageid", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
            .withColumn("batchtime", lit(startExecutionTime)).withColumn("shipcode", lit(shipcode).cast(StringType))
            .withColumn("part_date", lit(partDate))
        }
        var errorDetails = errReptSWwareData.select("errorCode", "errorDetail", "reservationId", "ts", "voyageid", "batchtime", "shipcode", "part_date")
        errorDetails.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.errordetails.table"))
        
        log.info("errorDetails")
        /* guest details array or null */

        if (hasColumn(errReptSWwareData, "guestList")) {
          if (checkArray(errReptSWwareData, "guestList")) {
            errReptSWwareData = errReptSWwareData.withColumn("guestList", explode_outer(errReptSWwareData.col("guestList")))
          } else {
            errReptSWwareData = errReptSWwareData.withColumn("guestList", col("guestList"))
          }
          errReptSWwareData = errReptSWwareData.withColumn("clientID", when(errReptSWwareData.col("guestList.clientID").isNotNull, errReptSWwareData.col("guestList.clientID")).otherwise(lit(null)))
          .withColumn("guestID", when(errReptSWwareData.col("guestList.guestID").isNotNull, errReptSWwareData.col("guestList.guestID")).otherwise(lit(null)))
          .withColumn("guestQR", when(errReptSWwareData.col("guestList.guestQR").isNotNull, errReptSWwareData.col("guestList.guestQR")).otherwise(lit(null)))
          .withColumn("guestSequence", when(errReptSWwareData.col("guestList.guestSequence").isNotNull, errReptSWwareData.col("guestList.guestSequence")).otherwise(lit(null)))
          .withColumn("guestType", when(errReptSWwareData.col("guestList.guestType").isNotNull, errReptSWwareData.col("guestList.guestType")).otherwise(lit(null)))
          .withColumn("voyageid", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
          .withColumn("batchtime", lit(startExecutionTime)).withColumn("shipcode", lit(shipcode).cast(StringType))
          .withColumn("part_date", lit(partDate))
        } else {
          errReptSWwareData = errReptSWwareData.withColumn("clientID", lit(null)).withColumn("guestID", lit(null))
          .withColumn("guestQR", lit(null))
          .withColumn("guestSequence", lit(null)).withColumn("guestType", lit(null))
          .withColumn("voyageid", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
          .withColumn("batchtime", lit(startExecutionTime)).withColumn("shipcode", lit(shipcode).cast(StringType))
          .withColumn("part_date", lit(partDate))
        }

        var guestDetails = errReptSWwareData.select("clientID", "guestID", "guestQR", "guestSequence", "guestType", "reservationId", "ts" , "voyageid", "batchtime", "shipcode", "part_date")
        guestDetails.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.errorguestdetails.table"))
        log.info("guestDetails")
        val interData = errReptSWwareData.select("clientID","reservationId","errorDetail","ts").filter(errReptSWwareData("clientID").isNotNull && errReptSWwareData("reservationId").isNotNull)
        interData.createOrReplaceTempView("intermediateTable")        
        var errDetails=spark.sql("""select clientID,reservationId,ts,concat_ws(' ', collect_list(errorDetail)) as errorDetail from intermediateTable group by clientID,reservationId,ts""")
        errDetails = errDetails.withColumn("clientID", errDetails("clientID")).withColumn("reservationId", errDetails("reservationId"))        
        errDetails = errDetails.withColumnRenamed("reservationId", "src_res_id").withColumnRenamed("clientID", "client_id").withColumnRenamed("errorDetail", "sw_listener_error_details")
        .withColumnRenamed("ts", "change_dt").withColumn("sw_listener_error_flg", lit(true).cast(BooleanType)).select("src_res_id","client_id","sw_listener_error_details","sw_listener_error_flg","change_dt")
        errDetails.coalesce(spark.sparkContext.getConf.get("spark.coalesce.count").trim.toInt).write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.interem.table"))
        log.info("errDetails")        
      }

      ManageMetadata.updateStatus(batchInstanceId, batchId, "Successful", spark)
    } catch {

      case e: Exception =>
        {
          ManageMetadata.updateStatus(batchInstanceId, batchId, "Failed", spark)

          log.info("ErrorReportingSewareWearableParser ::: in the catch of updateStatus")
          e.printStackTrace()
          throw new Exception("General Exception..please check the stacktrace")
        }

    }

  }

}