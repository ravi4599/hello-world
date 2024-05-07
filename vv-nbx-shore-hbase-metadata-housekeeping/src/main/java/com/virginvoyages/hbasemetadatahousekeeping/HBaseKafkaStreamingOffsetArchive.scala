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

object HBaseKafkaStreamingOffsetArchive {

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
  def unixEpochTimeForNumberOfDaysAgo(numDaysAgo: Int): Long = {
    import java.time._
    val numDaysAgoDateTime: LocalDateTime = LocalDateTime.now().minusDays(numDaysAgo)
    val zdt: ZonedDateTime = numDaysAgoDateTime.atZone(ZoneId.of("UTC"))
    val numDaysAgoDateTimeInMillis = zdt.toInstant.toEpochMilli
    val unixEpochTime = numDaysAgoDateTimeInMillis  /// 1000L
    unixEpochTime
}

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._
    val noBackupDays = spark.sparkContext.getConf.get("spark.housekeeping.days").toInt

    val yesterday = ZonedDateTime.now(ZoneId.of("UTC")).minusDays(noBackupDays)
    val today = ZonedDateTime.now(ZoneId.of("UTC"))
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val dateformatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val sessiondate = dateformatter format yesterday

    val dateDiff = sessiondate //.map(_.toString(targetFormat))
    val unixdateDiff=unixEpochTimeForNumberOfDaysAgo(noBackupDays)
    val sparkConfiguration = sc.broadcast(sc.getConf.getAll.toMap)
    
    println("#----------------------------unixdateDiff---------------#"+unixdateDiff)

    val jdbcurl_var = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.housekeeping.jdbcurl"))
    val kafkaoffset_delete_query = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.kafkastreamoffset.deletequery")).value + "<" + unixdateDiff
    println("#-----------DELETE QUERY ------------#",kafkaoffset_delete_query)
    val kafkaoffsetfilepath = sparkConfiguration.value.get("spark.filepath.housekeeping").get.trim + java.time.LocalDate.now.toString() + "/" + sparkConfiguration.value.get("spark.kafkastreamoffset.kafkastreamoffsettable").get.trim.toString().toLowerCase() + "/"

    log.info("kafkaoffset table path: " + kafkaoffsetfilepath)

    try {

      var kafkastreamoffset_df = spark.read.format(spark.sparkContext.getConf.get("spark.housekeeping.format"))
        .option("table", spark.sparkContext.getConf.get("spark.kafkastreamoffset.kafkastreamoffsettable"))
        .option("zkUrl", spark.sparkContext.getConf.get("spark.housekeeping.readhbasezkurl"))
        .load()

      var kafkastreamoffset_df1 = kafkastreamoffset_df.withColumn("TIMESTAMPNonUnix", from_unixtime(kafkastreamoffset_df.col("TIMESTAMP").divide(1000)))

      kafkastreamoffset_df1 = kafkastreamoffset_df1.filter(to_date(kafkastreamoffset_df1("TIMESTAMPNonUnix")).lt(date_sub(lit(current_date), noBackupDays)))

      kafkastreamoffset_df1.printSchema()
      kafkastreamoffset_df1.show(20, false)
      
      kafkastreamoffset_df1.select("TOPIC_NAME", "PARTITION", "UPDATED_OFFSET", "TIMESTAMP", "ROWKEY")
      if (!kafkastreamoffset_df1.head(1).isEmpty) {
        kafkastreamoffset_df1.repartition(7).write.mode(SaveMode.Overwrite).parquet(kafkaoffsetfilepath)
        val s3_count=spark.read.parquet(kafkaoffsetfilepath).count
        println("s3_count",s3_count,"kafkastreamoffset_df1",kafkastreamoffset_df1.count)
        if (kafkastreamoffset_df1.count==s3_count) {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        var con = DriverManager.getConnection(jdbcurl_var.value)
        val del = con.prepareStatement(kafkaoffset_delete_query)
        //del.setString(1, dateDiff)

        del.executeUpdate()
        con.commit();
        con.close();
        println("#---------Deletion Query Executed-----# ")}
        else{
          println("#---------------------------backup not taken successfull--------------#")
          sys.exit(1)
        }
        log.info(" records older than " + dateDiff + " in kafkaoffset_delete_query hbase table are deleted and backed up in location")
        

        //        kafkastreamoffset_df1.foreachPartition { rows =>
        //          Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //          var con = DriverManager.getConnection(jdbcurl_var.value)
        //          rows.foreach { row =>
        //            var rowkey = row.getString(4)
        //            val del = con.prepareStatement(kafkaoffset_delete_query.value)
        //            del.setString(1, rowkey)
        //            //del.executeUpdate()
        //          }
        //          con.commit();
        //          con.close()
        //        }
      } else {
        log.info("There are no records older than "+dateDiff+" in kafkaoffset_ hbase table")
        println("There are no records older than "+dateDiff+" in kafkaoffset hbase table")
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