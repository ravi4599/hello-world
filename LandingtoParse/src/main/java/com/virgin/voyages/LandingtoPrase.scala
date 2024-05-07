package com.virgin.voyages

import java.io.ByteArrayInputStream
import java.sql.SQLException
import java.io.File
import java.io.IOException
import java.nio.charset.StandardCharsets
import scala.collection.mutable.AbstractBuffer
import scala.collection.mutable.ArrayBuffer

import scala.util.Try
import scala.collection.Seq
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.Assign
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.{ OffsetRange, HasOffsetRanges, KafkaUtils }
import java.time.LocalDateTime
//****************
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Column
//import java.util.uuid
import java.nio.ByteBuffer
import java.time.Instant
import org.apache.spark.rdd.RDD
//import com.virginvoyages.kafka.ingestion.OffSetManager
import com.virginvoyages.metadataframework.ManageMetadata
import org.apache.spark.sql.types.StringType
import java.sql.Timestamp
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.{ StructType, StructField, TimestampType }
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs.{ FileSystem, Path }




object LandingtoPrase {
  
  
  
  def checkStructType(df: DataFrame, colname: String): Boolean = {
    df.schema(colname).dataType match {
      case StructType(_) => return true
      case _             => return false
    }
  }
  
  
def writetoTable(spark: SparkSession, df: DataFrame, batch_start_time: String, part_write_date: String, voyageId: String, batchInstanceId: String, debug_flag: String) = {
    import spark.implicits._

    def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
    var shipCode: String = null
    if (spark.sparkContext.getConf.contains("spark.ship.code")) {

      shipCode = spark.sparkContext.getConf.get("spark.ship.code").toString()
    }
    val connector = df.select(col("source.connector")).first.get(0).toString
    println("connector :" + connector)

    val inupdatedf = df.filter($"op" === "c" || $"op" === "u" || $"op" === "r")
    val deletedf = df.filter($"op" === "d")
    var inupdatefinaldf = spark.emptyDataFrame
    var afterdf = spark.emptyDataFrame
    var beforedf = spark.emptyDataFrame
    var parseDF = spark.emptyDataFrame
    if (checkStructType(inupdatedf, "after")) {

      afterdf = inupdatedf.select("after.*", "op", "ts_ms", "source.lsn")
    }

    if (!deletedf.head(1).isEmpty) {
      if (checkStructType(deletedf, "before")) {
        beforedf = deletedf.select("before.*", "op", "ts_ms","source.lsn")
      }
      val dfs = Seq(afterdf, beforedf)
      parseDF = dfs.reduce(_ union _)
    } else {
      parseDF = afterdf

    }
    if (debug_flag.trim().toUpperCase().equals("TRUE")) {
      parseDF.printSchema
      parseDF.show(false)
    }
    if (!parseDF.head(1).isEmpty) {
      val tableNames = spark.sparkContext.getConf.get("spark.target.parse.table").toString()
      val timestampcols = spark.catalog.listColumns(tableNames).filter($"dataType" === "timestamp").select($"*").select("name").collect().map(_.getAs[String]("name")).mkString(",").trim

      val tempcolslist = timestampcols.split(",")
      val tzcols = tempcolslist.filter(!_.contains("batchtime"))
      //   println("tabname :" + connector)
      if (connector == "sqlserver") {
        println("Sql server Connector")
        for (tzcol <- tzcols) {
          var cols = tzcol.toString()
          //MXP tmistamps are timestamp3 debezium converts all timestamp values to milliseconds while sending to kafka
          parseDF = parseDF.withColumn(cols, from_unixtime(parseDF.col(cols) / 1000).cast(TimestampType))
        }
        val currTZ = spark.conf.getOption("spark.sql.session.timeZone").toString
        val beginIndex = currTZ.indexOf("(") + 1
        val endIndex = currTZ.indexOf(')')
        val tzSession = currTZ.substring(beginIndex, endIndex)

        for (tzcol <- tzcols) {
          println("column converted:::::::::::::::;" + tzcol)
          var cols = tzcol.toString()
          parseDF = parseDF.withColumn(cols, to_utc_timestamp(parseDF.col(cols).cast(TimestampType), tzSession))
        }
      } else {
        /*
         * This part is added for Postgress source tables
         */
        val tempcolslist1 = tempcolslist.filter(!_.contains("batchtime"))
        val tzcols = tempcolslist1.filter(!_.contains("ts_ms"))
        for (tzcol <- tzcols) {
          println("tzcol:::::::::::::::;" + tzcol)
          var cols = tzcol.toString()
          parseDF = parseDF.withColumn(cols, from_unixtime(parseDF.col(cols) / 1000000).cast(TimestampType))
        }
        //parseDF = parseDF.withColumn("ts_ms", from_unixtime(parseDF.col("ts_ms") / 1000).cast(TimestampType))
        parseDF = parseDF.withColumn("ts_ms", concat(from_unixtime(parseDF.col("ts_ms") / 1000,"yyyy-MM-dd HH:mm:ss"), typedLit("."), substring(parseDF.col("ts_ms"), 11, 3)).cast(TimestampType))

        // parseDF.show(false)
        val currTZ = spark.conf.getOption("spark.sql.session.timeZone").toString
        val beginIndex = currTZ.indexOf("(") + 1
        val endIndex = currTZ.indexOf(')')
        val tzSession = currTZ.substring(beginIndex, endIndex)

        for (tzcol <- tzcols) {
          println("tzcol:::::::::::::::;" + tzcol)
          var cols = tzcol.toString()
          parseDF = parseDF.withColumn(cols, to_utc_timestamp(parseDF.col(cols).cast(TimestampType), tzSession))
        }

        parseDF.show(false)

      }

      //  parseDF.show(false)

      //val shipcode_flag = tzcols.map(_.toLowerCase()).contains("shipcode")
      val b = df.toDF(df.columns.map(_.toLowerCase): _*)
      val shipcode_flag = hasColumn(b, "shipcode")
      val colList = spark.catalog.listColumns(tableNames).select("name").collect().map(_.getAs[String]("name")).mkString(",").trim
      val required_columns = colList.split(",")
      val missing_columns = ArrayBuffer[String]()
      val avaliable_columns = ArrayBuffer[String]()

      for (col <- required_columns) {

        if (hasColumn(parseDF, col)) {
          avaliable_columns.append(col)
        } else {
          println(col, "column missing", missing_columns.length)
          println(missing_columns.length, "length")
          //log.info(col, "column exists", missing_columns.toString, missing_columns.length)
          println("column missing", missing_columns)
          missing_columns.append(col)
        }

      }

      print(missing_columns, "Here are the missing columns")
      var targetDF = missing_columns.foldLeft(parseDF)((df, c) =>
        df.withColumn(s"$c", lit(null)))

      //targetDF.show(false)
      var dataDf = targetDF.withColumn("batchtime", lit(batch_start_time).cast(TimestampType))
        .withColumn("part_date", to_date(lit(part_write_date)))
        .withColumn("voyageid", lit(voyageId).cast(StringType))
      if (shipcode_flag == false) {
        dataDf = dataDf.withColumn("shipcode", lit(shipCode).cast(StringType))
      }

      val temp_tab = spark.sparkContext.getConf.get("spark.target.parse.table").replace('.', '_')
      dataDf.createOrReplaceTempView(temp_tab)
      val finaleQuery = s"""select """ + colList + s""" from $temp_tab"""

      println(finaleQuery)

      val parseDf = spark.sql(finaleQuery)
      if (debug_flag.trim().toUpperCase().equals("TRUE")) {
        println(parseDf.count)
        parseDf.show(false)
      }
      val path = s"""desc formatted """ + spark.sparkContext.getConf.get("spark.target.parse.table")
      val target_location = spark.sql(path).toDF.filter('col_name === "Location").collect()(0)(1).toString
  
      if (spark.sparkContext.getConf.contains("spark.partition.column")) {
      parseDf.repartition(15).write.mode("Overwrite").partitionBy(spark.sparkContext.getConf.get("spark.partition.column").toString()).parquet(target_location)
      }
      else 
      {
        parseDf.repartition(15).write.mode("Overwrite").parquet(target_location)
      }
      
    }

  }
  
  def main(args: Array[String]): Unit = {
    
    
  
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
  var cnt = 1
    
    
    
    
  val  debug_flag = "False";
  val spark = SparkSession.builder() //.config("spark.sql.broadcastTimeout", "36000")
      .enableHiveSupport()
      .getOrCreate()
      
    val sc = spark.sparkContext
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  
  var batchInstanceId: String = null
  var batch_id1: String = null
  var transCommit: Boolean = true;
  
  
  
  val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
  
  val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
        batch_id1 = metadata._1
        batchInstanceId = metadata._2
        val batchStartTime = metadata._3
        val batchEndTime = metadata._4
        val partReadStart = metadata._5
        val partReadEnd = metadata._6
        val startExecutionTime = metadata._7
        val partWriteDate = metadata._8
        log.info(" Partition write date " + partWriteDate)
        val voyageId = spark.sparkContext.getConf.get("spark.voyage.id")
  
    val landing = spark.sparkContext.getConf.get("spark.landing.data")
        val newsourcedata = s"""select message from $landing """.stripMargin
        val messageDf = spark.sql(newsourcedata)

        val newdatadf = messageDf.select("message").rdd.map { x => x.toString }
          
        val newjsondf =  spark.read.json(newdatadf)
        

          if (!newjsondf.head(1).isEmpty) {
            println("This is the new code")
            println("Starting to write data in the parse")
            writetoTable(spark, newjsondf, batchStartTime, partWriteDate, voyageId, batchInstanceId, debug_flag)
            println("*********** data has been written to parse table **************")
          }
  }    
        
  
}