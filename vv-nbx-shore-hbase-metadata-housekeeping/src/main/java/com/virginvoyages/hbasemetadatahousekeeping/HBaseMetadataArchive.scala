package com.virginvoyages.hbasemetadatahousekeeping

import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.log4j.Level
import org.apache.log4j.LogManager
//import com.virginvoyages.hbasemetadatahousekeeping.HBaseConnectionException
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



object HBaseMetadataArchive {

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .getOrCreate()
    val noBackupDays= spark.sparkContext.getConf.get("spark.housekeeping.days").toInt
    val sc = spark.sparkContext
    val yesterday = ZonedDateTime.now(ZoneId.of("UTC")).minusDays(noBackupDays)
    val today = ZonedDateTime.now(ZoneId.of("UTC"))
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val dateformatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val sessiondate = dateformatter format yesterday
    
    val dateDiff = sessiondate//.map(_.toString(targetFormat))
    
    import spark.implicits._

    val sparkConfiguration = sc.broadcast(sc.getConf.getAll.toMap)

    val jdbcurl_var = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.housekeeping.jdbcurl"))

    val ingestmetadata_delete_query = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.ingestmetadata.deletequery")).value+"<to_date('"+dateDiff+"')"
    println("#-----------Deletion Query--------#", ingestmetadata_delete_query)

    val ingestmetadatafilepath = sparkConfiguration.value.get("spark.filepath.housekeeping").get.trim + java.time.LocalDate.now.toString() + "/" + sparkConfiguration.value.get("spark.ingestmetadata.ingestmetadatatable").get.trim.toString().toLowerCase() + "/"
    println("#----------ingestmetadatafilepath Query--------#", ingestmetadatafilepath)

    log.info("ingestmetadata table path: " + ingestmetadatafilepath)

    try {

      var ingestmetadata_df = spark.read.format(spark.sparkContext.getConf.get("spark.housekeeping.format"))
        .option("table", spark.sparkContext.getConf.get("spark.ingestmetadata.ingestmetadatatable"))
        .option("zkUrl", spark.sparkContext.getConf.get("spark.housekeeping.readhbasezkurl"))
        .load()

      var ingestmetadata_df1 = ingestmetadata_df.filter(to_date(ingestmetadata_df("BATCH_EXECUTION_STARTTIME"))
        .lt(date_sub(lit(current_date), noBackupDays)))

      ingestmetadata_df1.printSchema()
      //ingestmetadata_df1.filter(ingestmetadata_df1("BATCHSTARTTIME")==='').show(20, false)
     // ingestmetadata_df1.show(20, false)
      
      if (!ingestmetadata_df1.head(1).isEmpty) {
        ingestmetadata_df1.repartition(7).write.mode(SaveMode.Overwrite).parquet(ingestmetadatafilepath)
        val s3_count=spark.read.parquet(ingestmetadatafilepath).count
        println("s3_count",s3_count,"ingestmetadataframe",ingestmetadata_df1.count)
        if (ingestmetadata_df1.count==s3_count) {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        var con = DriverManager.getConnection(jdbcurl_var.value)
        val del = con.prepareStatement(ingestmetadata_delete_query)
        //del.setString(1, dateDiff)
        
        del.executeUpdate()
        println("#---------Deletion Query Executed-----# ")
        log.info(" records older than "+dateDiff+" in ingestmetadata hbase table are deleted and backed up in location")
        con.commit();
        con.close()}
        else{
          println("#---------------------------backup not taken successfull--------------#")
          sys.exit(1)
        }
     
      } else {
        log.info("There are no records older than "+dateDiff+" in ingestmetadata hbase table")
        println("There are no records older than "+dateDiff+" in ingestmetadata hbase table")
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