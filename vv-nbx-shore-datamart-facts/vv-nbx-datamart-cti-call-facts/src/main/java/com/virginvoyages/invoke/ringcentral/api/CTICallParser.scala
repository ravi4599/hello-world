package com.virginvoyages.invoke.ringcentral.api

import org.apache.spark.SparkConf
import java.sql.SQLException
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.Row
import java.sql.Timestamp
import com.virginvoyages.metadataframework.ManageMetadata
import org.apache.spark.sql.functions.{ avg, explode, concat, lit, trim }
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
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
import org.apache.spark.sql.functions._
import scala.util.Try
import java.text.SimpleDateFormat

object CTICallParser {

  def main(args: Array[String]): Unit = {
    /*if (args.length <= 1) {
      println("This job required at least two parameters")
      System.exit(1)
    }

    val batchStartTime1 = args(0)
    val batchEndTime1 = args(1)

    val batchStartTime = batchStartTime1.replace("T", " ")
    val batchEndTime = batchEndTime1.replace("T", " ")*/

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

/**********************************MetaData framework**********************************************/
    val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
    metadata.productIterator.foreach(println)
    val batch_id1 = metadata._1
    val batch_instance_id1 = metadata._2
    var batchStartTime = metadata._3
    var batchEndTime = metadata._4

    val inputFormat = "yyyy-MM-dd HH:mm:ss"
    val outputFormat = "yyyy-MM-dd HH:mm:ss"
    batchStartTime = dateMinusSec(batchStartTime, 7200, inputFormat, outputFormat)
    batchEndTime = dateMinusSec(batchEndTime, 7200, inputFormat, outputFormat)
    println("After conversion starttime:" + batchStartTime + " endtime:" + batchEndTime)

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    import spark.implicits._

    try {

      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
      val whereClause = s"""where batchtime >= from_unixtime(unix_timestamp('$batchStartTime', "yyyy-MM-dd' 'HH:mm:ss")) and batchtime <= from_unixtime(unix_timestamp('$batchEndTime', "yyyy-MM-dd' 'HH:mm:ss")) and part_date>=to_date('$batchStartTime') and part_date<=to_date('$batchEndTime') and message is not null and message!="" """
      log.info("select * from %s %s".format(sc.getConf.get("spark.source.cticall.table"), whereClause))
      var cticalldatadf = spark.sql("select * from %s %s".format(sc.getConf.get("spark.source.cticall.table"), whereClause))

      println("lnding count:" + cticalldatadf.count)
      if (!cticalldatadf.head(1).isEmpty) {
        var cticalldf = cticalldatadf.select("message").rdd.map { x => x.toString }

        var cticallParseDf = spark.read.json(cticalldf).withColumn("etl_load_dt", current_timestamp())
          .withColumn("batchtime", unix_timestamp(lit(batchStartTime), "yyyy-MM-dd' 'HH:mm:ss").cast(TimestampType))
          .withColumn("part_date", lit(to_date(col("batchtime"), "yyyy-MM-dd")))

        cticallParseDf = cticallParseDf.withColumn("completedContacts_explode", explode_outer(cticallParseDf.col("completedContacts")))

        //cticallParseDf.printSchema()
        //cticallParseDf.show(10, false)

        if (hasColumn(cticallParseDf, "completedContacts_explode.contactId")) {
          cticallParseDf = cticallParseDf.withColumn("contactId", when(
            cticallParseDf.col("completedContacts_explode.contactId").isNotNull,
            cticallParseDf.col("completedContacts_explode.contactId")).otherwise(lit(null)))
        } else {
          cticallParseDf = cticallParseDf.withColumn("contactId", lit(null))
        }
        //println("first count :"+cticallParseDf.count)
        //cticallParseDf = cticallParseDf.withColumn("contactId", cticallParseDf.col("completedContacts_explode.contactId"))
        cticallParseDf = cticallParseDf.withColumn("masterContactId", cticallParseDf.col("completedContacts_explode.masterContactId"))
          .withColumn("pointOfContactName", cticallParseDf.col("completedContacts_explode.pointOfContactName"))
          .withColumn("agentId", cticallParseDf.col("completedContacts_explode.agentId"))
          .withColumn("teamId", cticallParseDf.col("completedContacts_explode.teamId"))
          .withColumn("skillId", cticallParseDf.col("completedContacts_explode.skillId"))
          .withColumn("campaignId", cticallParseDf.col("completedContacts_explode.campaignId"))
          .withColumn("contactStart", cticallParseDf.col("completedContacts_explode.contactStart"))
          .withColumn("preQueueSeconds", cticallParseDf.col("completedContacts_explode.preQueueSeconds"))
          .withColumn("inQueueSeconds", cticallParseDf.col("completedContacts_explode.inQueueSeconds"))
          .withColumn("postQueueSeconds", cticallParseDf.col("completedContacts_explode.postQueueSeconds"))
          .withColumn("totalDurationSeconds", cticallParseDf.col("completedContacts_explode.totalDurationSeconds"))
          .withColumn("abandonSeconds", cticallParseDf.col("completedContacts_explode.abandonSeconds"))
          .withColumn("callbackTime", cticallParseDf.col("completedContacts_explode.callbackTime"))
          .withColumn("agentSeconds", cticallParseDf.col("completedContacts_explode.agentSeconds"))
          .withColumn("isOutbound", cticallParseDf.col("completedContacts_explode.isOutbound"))
          .withColumn("transferIndicatorId", cticallParseDf.col("completedContacts_explode.transferIndicatorId").cast(BooleanType))
          .withColumn("abandoned", cticallParseDf.col("completedContacts_explode.abandoned"))
          .withColumn("ACWSeconds", cticallParseDf.col("completedContacts_explode.ACWSeconds"))
          .withColumn("confSeconds", cticallParseDf.col("completedContacts_explode.confSeconds"))
          .withColumn("isLogged", cticallParseDf.col("completedContacts_explode.isLogged"))
          .withColumn("isShortAbandon", cticallParseDf.col("completedContacts_explode.isShortAbandon"))
          .withColumn("isTakeover", cticallParseDf.col("completedContacts_explode.isTakeover"))
          .withColumn("releaseSeconds", cticallParseDf.col("completedContacts_explode.releaseSeconds"))
          .withColumn("routingTime", cticallParseDf.col("completedContacts_explode.routingTime"))
          .withColumn("holdCount", cticallParseDf.col("completedContacts_explode.holdCount"))
          .withColumn("holdSeconds", cticallParseDf.col("completedContacts_explode.holdSeconds"))

        cticallParseDf = cticallParseDf.withColumn("contactStart_mod", unix_timestamp($"contactStart", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast(TimestampType))
        // println("second count :"+cticallParseDf.count)
        // cticallParseDf = cticallParseDf.filter(row => !row.anyNull);
        //var cticallTableDf = cticallParseDf.select("contactId", "masterContactId", "pointOfContactName", "agentId", "teamId", "skillId", "campaignId", "contactStart_mod", "preQueueSeconds", "inQueueSeconds", "postQueueSeconds", "totalDurationSeconds", "abandonSeconds", "callbackTime", "agentSeconds", "isOutbound", "transferIndicatorId", "abandoned", "etl_load_dt", "batchtime", "part_date")
        // println("third count :"+cticallParseDf.count)
        var cticallTableDf = cticallParseDf.select("contactId", "masterContactId", "pointOfContactName", "agentId", "teamId", "skillId", "campaignId", "contactStart_mod", "preQueueSeconds", "inQueueSeconds", "postQueueSeconds", "totalDurationSeconds", "abandonSeconds", "callbackTime", "agentSeconds", "isOutbound", "transferIndicatorId", "abandoned", "ACWSeconds", "confSeconds", "isLogged", "isShortAbandon", "isTakeover", "releaseSeconds", "routingTime", "holdCount", "holdSeconds", "etl_load_dt", "batchtime", "part_date")
        //cticallTableDf1 = cticallTableDf1.filter($"contactId".isNotNull && $"masterContactId".isNotNull && $"pointOfContactName".isNotNull && $"agentId".isNotNull && $"teamId".isNotNull && $"skillId".isNotNull && $"campaignId".isNotNull && $"contactStart_mod".isNotNull && $"preQueueSeconds".isNotNull && $"inQueueSeconds".isNotNull && $"postQueueSeconds".isNotNull && $"totalDurationSeconds".isNotNull && $"abandonSeconds".isNotNull && $"callbackTime".isNotNull && $"agentSeconds".isNotNull && $"isOutbound".isNotNull && $"transferIndicatorId".isNotNull && $"abandoned".isNotNull && $"etl_load_dt".isNotNull && $"batchtime".isNotNull && $"part_date".isNotNull)
        // val cticallTableDf = cticallTableDf1.select("contactId", "masterContactId", "pointOfContactName", "agentId", "teamId", "skillId", "campaignId", "contactStart_mod", "preQueueSeconds", "inQueueSeconds", "postQueueSeconds", "totalDurationSeconds", "abandonSeconds", "callbackTime", "agentSeconds", "isOutbound", "transferIndicatorId","abandoned", "etl_load_dt", "batchtime", "part_date")

        //val cticallTableDf = cticallTableDf1.select(  "pointOfContactName", "agentId", "teamId", "skillId")
        // cticallTableDf1 = cticallTableDf1.filter($"contactId".isNotNull)
        // cticallTableDf1= cticallTableDf1.filter(!($"contactId".isNull || ($"contactId" === "NULL") || ($"contactId" === "null")))
        /*val cticallTableDf = cticallTableDf1.select("contactId")
        cticallTableDf.show(2000, false)*/
        println("***********")
        //println(cticallTableDf.count)
        //cticallTableDf.printSchema()
        // cticallTableDf.show(1000,false)
        cticallTableDf.show(false)
        println("fourth count :" + cticallParseDf.count)
        println("final count :" + cticallTableDf.count)
        /*   cticallTableDf.createOrReplaceTempView("ttmp")
        spark.sql("""select * from ttmp where contactid in
(129247148375, 193478767970, 193478768902, 193478770629, 193478772642, 193478773667, 193478773746, 193478773843, 193478773876, 193478773977, 193478774881, 193478774977, 193478781029, 193478785948, 193478786246, 193478787317)
""").show(100, false)*/
        cticallTableDf.coalesce(5).write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.cticall.table"))
        ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark);
      }
    } catch {

      case e: Exception =>
        {
          log.info("in the catch of updateStatus ****************** ")
          ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
          e.printStackTrace()
          throw new Exception("General Exception..please check the stacktrace")
        }
    }

  }
  def dateMinusSec(date: String, seconds: Int, inputFormat: String, outputFormat: String): String = {
    import java.util.Calendar
    val dateAux = Calendar.getInstance()
    dateAux.setTime(new SimpleDateFormat(inputFormat).parse(date))
    dateAux.add(Calendar.SECOND, -seconds)
    return new SimpleDateFormat(outputFormat).format(dateAux.getTime())
  }

}

