package com.virginvoyages.hbasemetadatahousekeeping

import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.log4j.Level
import org.apache.log4j.LogManager

import org.apache.spark.sql.types.{ StringType, TimestampType }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{ lower, upper }
import org.apache.spark.broadcast.Broadcast
import java.time.LocalDate
import org.apache.spark.sql.SaveMode
import org.apache.phoenix.jdbc.PhoenixDriver;
import java.sql.{ Driver, DriverManager };
import java.time.{ ZonedDateTime, ZoneId }
import java.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.types.TimestampType

object HBaseStreamingOffsetArchive {

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val sparkConfiguration = sc.broadcast(sc.getConf.getAll.toMap)
    val noBackupDays = spark.sparkContext.getConf.get("spark.housekeeping.days").toInt

    val yesterday = ZonedDateTime.now(ZoneId.of("UTC")).minusDays(noBackupDays)
    val today = ZonedDateTime.now(ZoneId.of("UTC"))
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val dateformatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val dateDiff = dateformatter format yesterday

    val jdbcurl_var = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.housekeeping.jdbcurl"))

    val streamingingestoffset_delete_query = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.streamingingestoffset.deletequery")).value + "<to_date('" + dateDiff + "')"
    println("""Delete query streamingingestoffset_delete_query """ + streamingingestoffset_delete_query)

    val streamingingestoffsetfilepath = sparkConfiguration.value.get("spark.filepath.housekeeping").get.trim + java.time.LocalDate.now.toString() + "/" + sparkConfiguration.value.get("spark.streamingingestoffset.streamingingestoffsettable").get.trim.toString().toLowerCase() + "/"

    log.info("streamingingestoffset table path: " + streamingingestoffsetfilepath)

    try {

      var streamingingestoffset_df = spark.read.format(spark.sparkContext.getConf.get("spark.housekeeping.format"))
        .option("table", spark.sparkContext.getConf.get("spark.streamingingestoffset.streamingingestoffsettable"))
        .option("zkUrl", spark.sparkContext.getConf.get("spark.housekeeping.readhbasezkurl"))
        .load()

      streamingingestoffset_df = streamingingestoffset_df.filter(substring(streamingingestoffset_df.col("BATCH_INSTANCE_ID"), 1, 3) === lit("Src"))

      streamingingestoffset_df = streamingingestoffset_df.withColumn("date", split(streamingingestoffset_df.col("BATCH_INSTANCE_ID"), "-").getItem(3))

      streamingingestoffset_df = streamingingestoffset_df.withColumn("timestamp", coalesce(to_timestamp(streamingingestoffset_df.col("date"), "yyyyMMddHHmmss")))

      var streamingingestoffset_df1 = streamingingestoffset_df.filter(to_date(streamingingestoffset_df("timestamp")).lt(date_sub(lit(current_date), noBackupDays)))

      streamingingestoffset_df1.printSchema()
      streamingingestoffset_df1.show(20, false)

      streamingingestoffset_df1 = streamingingestoffset_df1.select("BATCH_INSTANCE_ID", "TOPIC", "PARTITION", "OFFSET_START", "OFFSET_END")
      if (!streamingingestoffset_df1.head(1).isEmpty) {

        streamingingestoffset_df1.repartition(7).write.mode(SaveMode.Overwrite).parquet(streamingingestoffsetfilepath)

        val s3_count = spark.read.parquet(streamingingestoffsetfilepath).count
        println("s3_count", s3_count, "streamingingestoffset_df1", streamingingestoffset_df1.count)
        if (streamingingestoffset_df1.count == s3_count) {
          Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
          var con1 = DriverManager.getConnection(jdbcurl_var.value)

          val del = con1.prepareStatement(streamingingestoffset_delete_query)
          //del1.setString(1, batch_instance_id)
          del.executeUpdate()

          con1.commit();
          con1.close()
        } else {
          println("#---------------------------backup not taken successfull--------------#")
          sys.exit(1)
        }
        log.info(" records older than " + dateDiff + " in kafkaoffset_delete_query hbase table are deleted and backed up in location")

      } else {
        log.info("There are no records older than " + dateDiff + " in kafkaoffset_ hbase table")
        println("There are no records older than " + dateDiff + " in kafkaoffset hbase table")
      }
    } catch {
      case e: SQLException => {
        log.info("HBase connectioin issue..please check HBase service")
        e.printStackTrace()
        throw new HBaseConnectionException("Hbase Connection Exception")
      }

      case e: Exception => {
        log.info("in the catch of upsertVoyageOperationMetadata ******************")
        e.printStackTrace()
        throw new Exception("General Exception..please check the stacktrace")
      }
    }
  }
}
