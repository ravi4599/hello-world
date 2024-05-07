
package com.virginvoyages.kafka.ingestion

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
import org.apache.spark.sql.types._
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
import com.virginvoyages.kafka.ingestion.OffSetManager
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

object DebeziumSnapShip {

  /**
   * Initialize logger
   */
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
  var cnt = 1
  def ConvertSchema(spark: SparkSession, dff: DataFrame, tgtTbl: String): DataFrame = {
    val SelectStr = "select * from  " + tgtTbl + " where 1 = 0"
    println(SelectStr)
    val tbltp = spark.sql(SelectStr)
    //dff.dtypes.foreach(f => println(f._1 + "," + f._2))
    //tbltp.dtypes.foreach(f => println(f._1 + "," + f._2))
    val differences = (dff.schema.fields.sortBy { case (x: StructField) => x.name } zip tbltp.schema.fields.sortBy { case (x: StructField) => x.name }).collect {
      case (origin: StructField, target: StructField) if origin.dataType != target.dataType =>
        (origin.name, target.dataType)
    }
    var targetDF = spark.emptyDataFrame
    if (differences.nonEmpty) {
      targetDF = differences.foldLeft(dff)((df, value) =>
        df.withColumn(value._1, df.col(value._1).cast(value._2)))
      targetDF.dtypes.foreach(f => println(f._1 + "," + f._2))
    } else { targetDF = dff }
    //targetDF.show(false)
    return targetDF
  }

  def writetoSnapshote(spark: SparkSession, tblname: String, snapshoteTimestamp: String) = {
    import spark.implicits._

    val snapshoteDF = spark.sparkContext.parallelize(Seq(tblname)).toDF("TABLE_NAME")
    snapshoteDF.show(false)
    val snapshoteDFFinal = snapshoteDF.withColumn("SNAPSHOT_DATE", lit(snapshoteTimestamp).cast(TimestampType))

    snapshoteDFFinal.show(false)
    snapshoteDFFinal.write
      .format(spark.sparkContext.getConf.get("spark.target.ops.table.format").trim)
      .mode(spark.sparkContext.getConf.get("spark.target.ops.table.mode").trim())
      .option("table", spark.sparkContext.getConf.get("spark.snapshot.table").trim())
      .option("zkUrl", spark.sparkContext.getConf.get("spark.target.zkurl").trim())
      .save

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

      afterdf = inupdatedf.select("after.*", "op", "source.ts_ms")
    }

    if (!deletedf.head(1).isEmpty) {
      if (checkStructType(deletedf, "before")) {
        beforedf = deletedf.select("before.*", "op", "source.ts_ms")
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
        parseDF = parseDF.withColumn("ts_ms", from_unixtime(parseDF.col("ts_ms") / 1000).cast(TimestampType))

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
          log.info(col, "column exists", missing_columns.toString, missing_columns.length)
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
      val parsetbl = spark.sparkContext.getConf.get("spark.target.parse.table")
      val temp_tab = spark.sparkContext.getConf.get("spark.target.parse.table").replace('.', '_')
      dataDf.createOrReplaceTempView(temp_tab)
      val finaleQuery = s"""select """ + colList + s""" from $temp_tab"""

      println(finaleQuery)

      val parseDf = spark.sql(finaleQuery)
      println("Before calling")
      val parseFinalDf=ConvertSchema(spark,parseDf,parsetbl)
      println("Schema Converted")
      if (debug_flag.trim().toUpperCase().equals("TRUE")) {
        println(parseDf.count)
        parseDf.show(false)
      }

      // parseDf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.parse.table"))

      val tgtLocationparse = spark.sql(s"""desc formatted $parsetbl""").toDF.filter('col_name === "Location").collect()(0)(1).toString
      parseFinalDf.write.mode(org.apache.spark.sql.SaveMode.Append).parquet(tgtLocationparse + "/part_date=" + part_write_date)
      val insertString = s"""alter table $parsetbl add IF NOT EXISTS  partition(part_date='$part_write_date') location '$tgtLocationparse/part_date=$part_write_date'""".stripMargin
      println(insertString)
      spark.sql(insertString)
      spark.sql("refresh table " + parsetbl)
      spark.sql("Msck repair table " + parsetbl)

    }

  }

  def main(args: Array[String]): Unit = {

    /**
     * Initialize spark context
     */
    val spark = SparkSession.builder() //.config("spark.sql.broadcastTimeout", "36000")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    var intv = 300
    if (spark.sparkContext.getConf.contains("spark.time.int")) {
      intv = spark.sparkContext.getConf.get("spark.time.int").toInt
    }
    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(intv))
    val topics = Array(spark.sparkContext.getConf.get("spark.src.kafkatopic").trim)
    /**
     * Initialize kafka parameters
     */

    import spark.implicits._
    var sslflag = false
    if (spark.sparkContext.getConf.contains("spark.ssl.enabled")) {
      sslflag = spark.sparkContext.getConf.get("spark.ssl.enabled").toBoolean
    }
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> spark.sparkContext.getConf.get("spark.kafkabrokers").trim,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> spark.sparkContext.getConf.get("spark.mpx.consumer").trim(),
      //ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> spark.sparkContext.getConf.get("spark.src.personOffSet").trim(),
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean))
    /*
       * Reading offset from Hbase
       */

    log.info("Reading From offset from Hbase****************")
    val fromOffsets = OffSetManager.getLastCommittedOffset(spark.sparkContext.getConf.get("spark.src.kafkatopic").trim, spark.sqlContext, spark)
    log.info("Completed Reading offset from Hbase****************")

    /*-
    * Create a Direct Stream to read the Messages from kafka topic
    */
    val messages = KafkaUtils.createDirectStream[String, String](streamingContext, PreferConsistent, Assign[String, String](fromOffsets.keys, kafkaParams, fromOffsets))
    // val  cachedmessages=messages.cache()
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    log.info("Start Reading kafka")

    messages.foreachRDD { (rdd, batchTime) =>
       if (!rdd.isEmpty) {
         println("rdd is not empty")
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(offset =>

        log.info(" offsets : " + offset.topic, offset.partition, offset.fromOffset, offset.untilOffset))
      var debug_flag = "False";
      if (spark.sparkContext.getConf.contains("spark.debug.flag")) {
        debug_flag = spark.sparkContext.getConf.get("spark.debug.flag").trim()
      }
      var batchInstanceId: String = null
      var batch_id1: String = null
      var transCommit: Boolean = true;
      log.info("###################     Processing kafka stream msgs started  ########################")
      try {
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
//        sqlContext.setConf("hive.exec.dynamic.partition", "true")
//        sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
        //spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
       // var initialmessageDf = rdd.map(row => row.value().toString()).toDF("message")
        var initialmessageDf =rdd.filter(row => row.value()!= null).map(row => row.value().toString()).toDF("message")
        initialmessageDf.show
        println("Initial ccount")
        println(initialmessageDf.count)
        var messageDf = spark.emptyDataFrame
        messageDf = initialmessageDf.filter(row => !row.anyNull);

        /*
           * If fromlanding is Y the data will be read from the landing table and will be
           * written to parse only
           */

        val landingtable = spark.sparkContext.getConf.get("spark.target.parse.table")
        var fromlanding = "N"
        if (spark.sparkContext.getConf.contains("spark.landing.data")) {
          fromlanding = spark.sparkContext.getConf.get("spark.landing.data") //add where clause
        }
        if (fromlanding == "Y") {
          val sourcedata = s"""select message from $landingtable """.stripMargin
          messageDf = spark.sql(sourcedata)
        } else { messageDf = messageDf.select("message") }
        /*
 				* ********************************************************************
				 */

        if (!messageDf.head(1).isEmpty) {

          val datadf = messageDf.select("message").rdd.map { x => x.toString }
          val jsondf = spark.read.json(datadf)
          // val jsondf = jsoninitialdf.select(col("payload.*"))
          if (debug_flag.trim().toUpperCase().equals("TRUE")) {
            jsondf.printSchema()
            jsondf.show(false)
          }

          /*
           *Check if the source table name exist in hbase snapshot table
           */
          val tabname = jsondf.select(col("source.table")).first.get(0).toString
          println("tabname :" + tabname)
          val snapshoteDF = spark.read.format(spark.sparkContext.getConf.get("spark.target.ops.table.format").trim())
            .option("table", spark.sparkContext.getConf.get("spark.snapshot.table").trim()).
            option("zkUrl", spark.sparkContext.getConf.get("spark.target.zkurl").trim()).load()
          // snapshoteDF.show(false)
          val snapshoteFilterDF = snapshoteDF.filter($"TABLE_NAME" === tabname)
          var maxDateSnap: Timestamp = null
          var maxDatekafka: Timestamp = null
          var maxtsms: Timestamp = null
          /*
           * Get the ts_ms value from hbase snapshot table
           */
          if (!snapshoteFilterDF.head(1).isEmpty) {
            val snapDate = snapshoteFilterDF.select($"SNAPSHOT_DATE") //.cast(TimestampType).as("SNAPSHOT_DATE"))
            maxDateSnap = snapDate.first.getTimestamp(0)

          } else {
            maxDateSnap = Timestamp.valueOf("1900-01-01 00:00:00")
          }
          var parseDF = spark.emptyDataFrame
          var incrementfilterDf = spark.emptyDataFrame
          /*
           * Fetch all records where op=r
           */
          val initialLoadDf = jsondf.where($"op" === lit("r"))
            .withColumn("writeflag", lit("true").cast(BooleanType))
            .select("*")
          if (!initialLoadDf.head(1).isEmpty) {

            val maxDatestring = initialLoadDf.select(from_unixtime($"source.ts_ms" / 1000).alias("ts_ms_new")).agg(max($"ts_ms_new")).toDF("ts_ms_new") //.first.getString(0)
            val s = maxDatestring.select($"ts_ms_new".cast(TimestampType).as("ts_ms_new"))
            maxDatekafka = s.first.getTimestamp(0)
            // parseDF = initialLoadDf.filter($"writeflag" === true)
          } else {
            maxDatekafka = Timestamp.valueOf("1900-01-01 00:00:00")
          }

          if (maxDatekafka.compareTo(maxDateSnap) > 0) {
            maxtsms = maxDatekafka
          } else {
            maxtsms = maxDateSnap
          }
          println("Maxtsms")
          println(maxtsms)
          if (debug_flag.trim().toUpperCase().equals("TRUE")) {
            println("initial parsecount")
            println(initialLoadDf.count)
          }

          /* if (!initialLoadDf.head(1).isEmpty) {
            writetoTable(spark, initialLoadDf, batchStartTime, partWriteDate, voyageId, batchInstanceId)
            println("initialload data has written to parse")
            println("parsecount")
            println(initialLoadDf.count)

          }*/

          val incrementalDf = jsondf.filter($"op" === "c" || $"op" === "u" || $"op" === "d")
            .withColumn("writeflag", lit("true").cast(BooleanType))
            .select("*")

          if (!incrementalDf.head(1).isEmpty) {

            val incrementPayloadDf = incrementalDf.withColumn("ts_ms_new", from_unixtime($"ts_ms" / 1000))
              .filter($"ts_ms_new" > maxtsms).select("*").drop($"ts_ms_new")
            incrementfilterDf = incrementPayloadDf.filter($"writeflag" === true)
            if (debug_flag.trim().toUpperCase().equals("TRUE")) {
              println("incremental parsecount")
              println(incrementfilterDf.count)
            }
          }
          if (!initialLoadDf.head(1).isEmpty && !incrementfilterDf.head(1).isEmpty) {

            val dfs = Seq(initialLoadDf, incrementfilterDf)
            parseDF = dfs.reduce(_ union _)
          } else if (!initialLoadDf.head(1).isEmpty) {
            parseDF = initialLoadDf
          } else if (!incrementfilterDf.head(1).isEmpty) {

            parseDF = incrementfilterDf
          }

          if (!parseDF.head(1).isEmpty) {
            writetoTable(spark, parseDF, batchStartTime, partWriteDate, voyageId, batchInstanceId, debug_flag)
            println("*********** data has been written to parse table **************")
          }
          //
          if (maxtsms.compareTo(maxDateSnap) > 0) {
            val maxDateS = maxtsms.toString()
            writetoSnapshote(spark, tabname, maxDateS)
          }

          /*
           * Write to landing only if the fromlanding flag is N
           */

          if (fromlanding == "N") {
            var shipCode: String = null
            if (spark.sparkContext.getConf.contains("spark.ship.code")) {
              shipCode = spark.sparkContext.getConf.get("spark.ship.code").toString()
            }
            val lndDf = messageDf.withColumn("batchtime", lit(batchStartTime).cast(TimestampType))
              .withColumn("part_date", to_date(lit(partWriteDate)))
              .withColumn("voyageid", lit(voyageId).cast(StringType))
              .withColumn("shipcode", lit(shipCode).cast(StringType))
              .withColumn("batchinstanceid", lit(batchInstanceId)).select("batchinstanceid", "voyageid", "batchtime", "message", "shipcode", "part_date")
            if (debug_flag.trim().toUpperCase().equals("TRUE")) {
              println("In landing")
              println(lndDf.count)
              lndDf.show(false)
            }

            //lndDf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.lnd.table"))
            val lndtble = spark.sparkContext.getConf.get("spark.target.lnd.table")
            val tgtLocationlnd = spark.sql(s"""desc formatted $lndtble""").toDF.filter('col_name === "Location").collect()(0)(1).toString
            lndDf.write.mode(org.apache.spark.sql.SaveMode.Append).parquet(tgtLocationlnd + "/part_date=" + partWriteDate)
            val insertString = s"""alter table $lndtble add IF NOT EXISTS  partition(part_date='$partWriteDate') location '$tgtLocationlnd/part_date=$partWriteDate'""".stripMargin
            println(insertString)
            spark.sql(insertString)
            spark.sql("refresh table " + lndtble)
            spark.sql("Msck repair table " + lndtble)
            log.info("***********SAVING OFFSET IN HBASE**************")
            OffSetManager.saveOffsets(offsetRanges, batchInstanceId, sparkConfiguration, spark)
            log.info("***********SAVE OFFSET COMPLETED  IN HBASE**************")

          }

        }
        ManageMetadata.updateStatus(batchInstanceId, batch_id1, "Successful", spark)

/**********************************Flag to run in bacth ****************************************/
        var flag = "N"
        if (spark.sparkContext.getConf.contains("spark.stop.flag")) {

          flag = spark.sparkContext.getConf.get("spark.stop.flag")
        }
        if (flag == "Y") {

          log.info("*************** Stoping the streaming ******************")
          println("***************  Stoping the streaming ******************")
          streamingContext.stop()
        }
/**************************************************************************/
        /*
 * Code snippet to dynamically stop the job
 */
        var TrgFile = "N"
        var ConfigFileName = batch_id1.replace('_', '-')
        var TrgFileLocn = s"""hdfs://data/streamingtriggers/""" + ConfigFileName + """.trg"""
        println(TrgFileLocn)
        if (spark.sparkContext.getConf.contains("spark.stop.trgFile")) {
          TrgFile = spark.sparkContext.getConf.get("spark.stop.trgFile")
        }

        if (spark.sparkContext.getConf.contains("spark.trgFileName")) {
          TrgFileLocn = spark.sparkContext.getConf.get("spark.trgFileName")
        }
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        if (TrgFile == "Y") {
          val fileExists = fs.exists(new Path(TrgFileLocn))
          if (fileExists) {
            log.info("*************** Stoping the streaming Dynamically ******************")
            println("***************  Stoping the streaming Dynamically ******************")
            streamingContext.stop()

          }

        }

      } catch {

        case e: Exception =>
          //ManageMetadata.updateStatus(batchInstanceId, batch_id1, "Failed", spark)
          log.info("Exception................................................................" + e.getMessage)
          transCommit = false;
          log.info("transCommit value in exception ---> " + transCommit)
          log.info("Exception stack trace  " + e.printStackTrace()); throw new Exception("General Exception..please check the stacktrace")

          gracefulStop(streamingContext);
          System.exit(1)
      }
    }

    }

    streamingContext.start()
    streamingContext.awaitTermination()
    //def trim(strings: Seq[String]) = strings.map(_.trim)

  }
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
  def gracefulStop(streamingContext: StreamingContext) = {
    log.info("****************************************** Closing the SparkStram ****************")
    streamingContext.stop(true);
  }

}