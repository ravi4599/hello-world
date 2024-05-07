package com.virginvoyages.hbasemetadatahousekeeping

import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import com.virginvoyages.hbasemetadatahousekeeping.HBaseConnectionException
import org.apache.spark.sql.types.{ StringType, TimestampType }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{ lower, upper }
import org.apache.spark.broadcast.Broadcast
import java.time.LocalDate
import org.apache.spark.sql.SaveMode
import org.apache.phoenix.jdbc.PhoenixDriver;
import java.sql.{ Driver, DriverManager };

import org.apache.spark.sql.types.TimestampType

object HBaseMetadataHousekeep {

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val sparkConfiguration = sc.broadcast(sc.getConf.getAll.toMap)

    val jdbcurl_var = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.housekeeping.jdbcurl"))
    val kafkaoffset_delete_query = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.kafkastreamoffset.deletequery"))
    val streamingingestoffset_delete_query = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.streamingingestoffset.deletequery"))
    val ingestmetadata_delete_query = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.ingestmetadata.deletequery"))

    val kafkaoffsetfilepath = sparkConfiguration.value.get("spark.filepath.housekeeping").get.trim + java.time.LocalDate.now.toString() + "/" + sparkConfiguration.value.get("spark.kafkastreamoffset.kafkastreamoffsettable").get.trim.toString().toLowerCase() + "/"
    val streamingingestoffsetfilepath = sparkConfiguration.value.get("spark.filepath.housekeeping").get.trim + java.time.LocalDate.now.toString() + "/" + sparkConfiguration.value.get("spark.streamingingestoffset.streamingingestoffsettable").get.trim.toString().toLowerCase() + "/"
    val ingestmetadatafilepath = sparkConfiguration.value.get("spark.filepath.housekeeping").get.trim + java.time.LocalDate.now.toString() + "/" + sparkConfiguration.value.get("spark.ingestmetadata.ingestmetadatatable").get.trim.toString().toLowerCase() + "/"

    log.info("kafkaoffset table path: " + kafkaoffsetfilepath)
    log.info("streamingingestoffset table path: " + streamingingestoffsetfilepath)
    log.info("ingestmetadata table path: " + ingestmetadatafilepath)

    try {

      var kafkastreamoffset_df = spark.read.format(spark.sparkContext.getConf.get("spark.housekeeping.format"))
        .option("table", spark.sparkContext.getConf.get("spark.kafkastreamoffset.kafkastreamoffsettable"))
        .option("zkUrl", spark.sparkContext.getConf.get("spark.housekeeping.readhbasezkurl"))
        .load()

      var kafkastreamoffset_df1 = kafkastreamoffset_df.withColumn("TIMESTAMP1", from_unixtime(kafkastreamoffset_df.col("TIMESTAMP").divide(1000)))

      kafkastreamoffset_df1 = kafkastreamoffset_df1.filter(to_date(kafkastreamoffset_df1("TIMESTAMP1")).lt(date_sub(lit(current_date), 30)))

      kafkastreamoffset_df1.printSchema()
      kafkastreamoffset_df1.show(20, false)

      kafkastreamoffset_df1.select("TOPIC_NAME", "PARTITION", "UPDATED_OFFSET", "TIMESTAMP", "ROWKEY")
      if (!kafkastreamoffset_df1.head(1).isEmpty) {
        kafkastreamoffset_df1.repartition(7).write.mode(SaveMode.Overwrite).parquet(kafkaoffsetfilepath)

        kafkastreamoffset_df1.foreachPartition { rows =>
          Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
          var con = DriverManager.getConnection(jdbcurl_var.value)
          rows.foreach { row =>
            var rowkey = row.getString(4)
            val del = con.prepareStatement(kafkaoffset_delete_query.value)
            del.setString(1, rowkey)
            del.executeUpdate()
          }
          con.commit();
          con.close()
        }
      } else {
        log.info("There are no records less than 30 days old in kafkastreamoffset hbase table")
      }

      var streamingingestoffset_df = spark.read.format(spark.sparkContext.getConf.get("spark.housekeeping.format"))
        .option("table", spark.sparkContext.getConf.get("spark.streamingingestoffset.streamingingestoffsettable"))
        .option("zkUrl", spark.sparkContext.getConf.get("spark.housekeeping.readhbasezkurl"))
        .load()

      streamingingestoffset_df = streamingingestoffset_df.filter(substring(streamingingestoffset_df.col("BATCH_INSTANCE_ID"), 1, 3) === lit("Src"))

      streamingingestoffset_df = streamingingestoffset_df.withColumn("date", split(streamingingestoffset_df.col("BATCH_INSTANCE_ID"), "-").getItem(3))

      streamingingestoffset_df = streamingingestoffset_df.withColumn("timestamp", coalesce(to_timestamp(streamingingestoffset_df.col("date"), "yyyyMMddHHmmss")))

      var streamingingestoffset_df1 = streamingingestoffset_df.filter(to_date(streamingingestoffset_df("timestamp")).lt(date_sub(lit(current_date), 30)))

      streamingingestoffset_df1.printSchema()
      streamingingestoffset_df1.show(20, false)

      streamingingestoffset_df1 = streamingingestoffset_df1.select("BATCH_INSTANCE_ID", "TOPIC", "PARTITION", "OFFSET_START", "OFFSET_END")
      if (!streamingingestoffset_df1.head(1).isEmpty) {
        streamingingestoffset_df1.repartition(7).write.mode(SaveMode.Overwrite).parquet(streamingingestoffsetfilepath)

        streamingingestoffset_df1.foreachPartition { rows =>
          Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
          var con1 = DriverManager.getConnection(jdbcurl_var.value)
          rows.foreach { row =>
            var batch_instance_id = row.getString(0)
            val del1 = con1.prepareStatement(streamingingestoffset_delete_query.value)
            del1.setString(1, batch_instance_id)
            del1.executeUpdate()
          }
          con1.commit();
          con1.close()
        }
      } else {
        log.info("There are no records less than 30 days old in streamingingestoffset hbase table")
      }

      var ingestmetadata_df = spark.read.format(spark.sparkContext.getConf.get("spark.housekeeping.format"))
        .option("table", spark.sparkContext.getConf.get("spark.ingestmetadata.ingestmetadatatable"))
        .option("zkUrl", spark.sparkContext.getConf.get("spark.housekeeping.readhbasezkurl"))
        .load()

      var ingestmetadata_df1 = ingestmetadata_df.filter(to_date(ingestmetadata_df("BATCH_EXECUTION_STARTTIME")).lt(date_sub(lit(current_date), 30)))

      ingestmetadata_df1.printSchema()
      ingestmetadata_df1.show(20, false)

      if (!ingestmetadata_df1.head(1).isEmpty) {
        ingestmetadata_df1.repartition(7).write.mode(SaveMode.Overwrite).parquet(ingestmetadatafilepath)

        ingestmetadata_df1.foreachPartition { rows =>
          Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
          var con2 = DriverManager.getConnection(jdbcurl_var.value)
          rows.foreach { row =>
            var batch_instance_id1 = row.getString(0)
            val del2 = con2.prepareStatement(ingestmetadata_delete_query.value)
            del2.setString(1, batch_instance_id1)
            del2.executeUpdate()
          }
          con2.commit();
          con2.close()
        }
      } else {
        log.info("There are no records less than 30 days old in ingestmetadata hbase table")
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