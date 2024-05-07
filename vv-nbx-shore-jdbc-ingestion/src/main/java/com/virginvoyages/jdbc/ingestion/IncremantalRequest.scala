package com.virginvoyages.jdbc.ingestion
import org.apache.spark.sql.SQLContext
import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import com.virginvoyages.metadataframework.ManageMetadata
import org.apache.spark.sql.types.{ StringType, TimestampType }
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import java.sql.SQLException
import org.apache.spark._
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import java.sql.SQLException
import scala.collection.mutable.ArrayBuffer
import scala.util.Try


import java.util.TimeZone

import java.util.Properties

object IncremantalRequest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    var batchInstanceId: String = null
    var batchId: String = null
    try {
      sqlContext.setConf("hive.exec.dynamic.partition", "true")
      sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
      val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
/*************************************calling metadata framework*********************************************/
      val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)

      metadata.productIterator.foreach(println)
      batchId = metadata._1
      batchInstanceId = metadata._2
      val batchStartTime = metadata._3
      val batchEndTime = metadata._4
      val startExecutionTiime = metadata._7
      val partWriteDate = metadata._8

      val voyageId = spark.sparkContext.getConf.get("spark.voyage.id")
      var shipCode: String = null
      if (spark.sparkContext.getConf.contains("spark.ship.code")) {

        shipCode = spark.sparkContext.getConf.get("spark.ship.code").toString()
      }
      // val selectQuery = spark.sparkContext.getConf.get("spark.src.table.query")
      val conditionColumn = spark.sparkContext.getConf.get("spark.condition.column") //.toInt

      import org.apache.spark.sql.types.{ StringType, TimestampType, IntegerType }

      import org.apache.spark.sql.functions.{ unix_timestamp, to_date }
      import java.sql.Timestamp
      import spark.implicits._

      var maxLoadDate: String = null
      var maxLoadDateT = spark.sql("select max(loadDate) as loadDate from shipdw.hvtb_lnd_vxp_housekeeping_requests").first().getTimestamp(0) //.getString(0) //.getTimestamp(0)

      // maxLoadDate = maxLoadDateT.toString()
      // maxLoadDate = spark.sql(spark.sparkContext.getConf.get("spark.hive.table.maxid")).first().getString(0)//.getLong(0)//.getString(0)
      if (maxLoadDateT == null) {
        maxLoadDate = Timestamp.valueOf("1900-01-01 00:00:00").toString()
        //maxLoadDate = "1900-01-01 00:00:00"
      } else {
        maxLoadDate = maxLoadDateT.toString()
      }

      print("MAX LOADDATE ")
      println(maxLoadDate)

      val currTZ = spark.conf.getOption("spark.sql.session.timeZone").toString
      val beginIndex = currTZ.indexOf("(") + 1
      val endIndex = currTZ.indexOf(')')
      val tzSession = currTZ.substring(beginIndex, endIndex)

      var currenttime1 = current_timestamp().cast(TimestampType)
      val ts = currenttime1.expr.eval().toString.toLong
      val currenttime = new java.sql.Timestamp(ts / 1000)
      var endBatchTime: String = currenttime.toString()
      val dfStartEST = Seq(endBatchTime).toDF("endBatchTime")
      val dfStartUTC = dfStartEST.select($"endBatchTime", to_utc_timestamp($"endBatchTime".cast(TimestampType), tzSession).alias("UTCEND")).select("UTCEND")
      var currentEndTimeUtc: String = dfStartUTC.first().getTimestamp(0).toString()
      val inputFormat = "yyyy-MM-dd HH:mm:ss.SSS"
      val outputFormat = "yyyy-MM-dd HH:mm:ss"
      val minsec = 120
      currentEndTimeUtc = dateMinusSec(currentEndTimeUtc, minsec, inputFormat, outputFormat)

      // val maxDateDf = data.withColumn("MaxTimestamp", greatest("created","started_at", "completed_at", "assigned_at", "at_risk_at", "overdue_at", "cancelled_at","come_back_later_at","deleted"))
      println("Max timestamp added")
      // maxDateDf.show(false)
      val MaxTimestamp = s"""greatest(created AT TIME ZONE 'UTC',started_at AT TIME ZONE 'UTC', completed_at AT TIME ZONE 'UTC', assigned_at AT TIME ZONE 'UTC', at_risk_at AT TIME ZONE 'UTC', overdue_at AT TIME ZONE 'UTC', cancelled_at AT TIME ZONE 'UTC',come_back_later_at AT TIME ZONE 'UTC',deleted AT TIME ZONE 'UTC')"""
      val whereClause = s"""where  $MaxTimestamp> '$maxLoadDate' and  $MaxTimestamp<='$currentEndTimeUtc'"""
      val selectQuery1 = s"""select * from  report.requests """ + whereClause
      val selectQuery=s"""($selectQuery1)as request"""

      println(selectQuery)
      //Connecting to source database
      val passwordEncode = sparkConfiguration.value.get("spark.src.password").get
      println(passwordEncode)
      //val passworddec = DecodeParam.deCodeVal(passwordEncode)
    //  println(passworddec)

      val data = spark.read.format(sparkConfiguration.value.get("spark.src.con.format").get).options(Map("url" -> sparkConfiguration.value.get("spark.src.con.url").get, "user" -> sparkConfiguration.value.get("spark.src.user").get, "password" -> passwordEncode,
        "dbtable" -> selectQuery, "driver" -> sparkConfiguration.value.get("spark.src.con.driver").get)).load()
      data.show(false)

      //  println(whereClause1)
      var maxfilteredDataDF = data //.where(whereClause)
      println("After Where")
      //  maxfilteredDataDF.show(false)
      var filteredDataDF = maxfilteredDataDF.drop("MaxTimestamp").select("*")
      val loadDate = currentEndTimeUtc
      //val filteredDataDF = data.where(conditionColumn > lit(maxID)).select("*")
      if (!filteredDataDF.head(1).isEmpty) {
        println("Inside filter")
        filteredDataDF.show(false)
        val source_column_list = filteredDataDF.columns.toSeq
        val source_column_count = source_column_list.length
        val required_columns: Seq[String] = spark.sparkContext.getConf.get("spark.source.columns").split(",")
        val required_columns_count = required_columns.length

        if (source_column_count != required_columns_count) {
          var flag = "Y"
          if (source_column_count > required_columns_count) {

            log.info("*************** Extra column added in Source ******************")
            println("*************** Extra column added in Source ******************")

          } else if (source_column_count < required_columns_count) {
            log.info("*************** Column removed in Source ******************")
            println("*************** Column removed in Source ******************")

            if (spark.sparkContext.getConf.contains("spark.stop.flag")) {

              flag = spark.sparkContext.getConf.get("spark.stop.flag")
            }
            if (flag == "Y") {

              log.info("*************** Please fix the column mismatch and rerun ******************")
              println("*************** Please fix the column mismatch and rerun ******************")
              System.exit(1)
            }

          }
          println("Source column count :" + source_column_count + " Target column count :" + required_columns_count)
          log.info("Source column count :" + source_column_count + " Target column count :" + required_columns_count)

        }
        val missing_columns = ArrayBuffer[String]()
        val avaliable_columns = ArrayBuffer[String]()
        def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

        for (trimcol <- required_columns) {
          val col = trimcol.trim()

          if (hasColumn(filteredDataDF, col)) {
            println(col, "column exists", avaliable_columns.toString)
            println("column exists", avaliable_columns.length)
            log.info(col, "column exists", avaliable_columns.toString, avaliable_columns.length)
            println(avaliable_columns.length, "lenthg")
            avaliable_columns.append(col)
          } else {
            println(col, "column missing", missing_columns.length)
            println(missing_columns.length, "length")
            log.info(col, "column exists", missing_columns.toString, missing_columns.length)
            println("column missing", missing_columns)
            missing_columns.append(col)
          }

        }

        println(missing_columns, "Here are the missing columns")
        filteredDataDF = missing_columns.foldLeft(filteredDataDF)((df, c) =>
          df.withColumn(s"$c", lit(null).cast(StringType)))

/********code chnage to convert timestamp columns to utc Start:****/

        val currTZ = spark.conf.getOption("spark.sql.session.timeZone").toString
        val beginIndex = currTZ.indexOf("(") + 1
        val endIndex = currTZ.indexOf(')')
        val tzSession = currTZ.substring(beginIndex, endIndex)
        if (sparkConfiguration.value.contains("spark.tz.columns")) {
          val tzCols = spark.sparkContext.getConf.get("spark.tz.columns").split(",")

          for (tzcol <- tzCols) {
            println("tzcol:::::::::::::::;" + tzcol)
            filteredDataDF = filteredDataDF.withColumn(tzcol, to_utc_timestamp(filteredDataDF.col(tzcol), tzSession))
          }
        }
/******************************End***********************/
        var finalDF = filteredDataDF.withColumn("VoyageId", lit(voyageId).cast(StringType))
          .withColumn("BatchTime", lit(batchStartTime).cast(TimestampType))
          .withColumn("Part_Date", to_date(lit(partWriteDate)))
          .withColumn("loadDate", lit(loadDate).cast(TimestampType)).withColumn("ShipCode", lit(shipCode).cast(StringType))

        val temp_tab = spark.sparkContext.getConf.get("spark.target.hive.table").replace('.', '_')
        finalDF.createOrReplaceTempView(temp_tab)
        //sparkConfiguration.value.
        var finaleQuery: String = null
        if (sparkConfiguration.value.contains("spark.source.query")) {

          finaleQuery = "select " + spark.sparkContext.getConf.get("spark.source.query") + s""",VoyageId,cast(loadDate as timestamp) as loadDate,BatchTime,ShipCode,Part_Date from $temp_tab"""
        } else {
          finaleQuery = "select " + spark.sparkContext.getConf.get("spark.source.columns") + s""",VoyageId,cast(loadDate as timestamp) as loadDate,BatchTime,ShipCode,Part_Date from $temp_tab"""

        }
        println(finaleQuery)
        val stageFinalDf = spark.sql(finaleQuery)
        stageFinalDf.printSchema
        stageFinalDf.show(false)

        val tgtLocation = spark.sparkContext.getConf.get("spark.target.location")
        val hiveTable = spark.sparkContext.getConf.get("spark.target.hive.table")
        stageFinalDf.write.mode(org.apache.spark.sql.SaveMode.Append).parquet(tgtLocation + "/" + partWriteDate)

        val insertString = s"""alter table $hiveTable add IF NOT EXISTS  partition(Part_Date='$partWriteDate') location '$tgtLocation/$partWriteDate'""".stripMargin

        println(insertString)
        spark.sql(insertString)
        spark.sql("refresh table " + hiveTable)
        spark.sql("Msck repair table " + hiveTable)

      }
      //function call to update status as successful
      ManageMetadata.updateStatus(batchInstanceId, batchId, "Successful", spark)
    } catch {

      case e: SQLException => {
        ManageMetadata.updateStatus(batchInstanceId, batchId, "Failed", spark)
        log.info("***************in the catch of Jdbc Ingestion ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace")
      }
      case e: Exception =>
        {
          ManageMetadata.updateStatus(batchInstanceId, batchId, "Failed", spark)
          log.info("***************in the catch of Jdbc Ingestion ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace")
        }
        System.exit(1)

    }

    def dateMinusSec(date: String, seconds: Int, inputFormat: String, outputFormat: String): String = {
      import java.util.Calendar
      val dateAux = Calendar.getInstance()
      dateAux.setTime(new SimpleDateFormat(inputFormat).parse(date))
      dateAux.add(Calendar.SECOND, -seconds)
      return new SimpleDateFormat(outputFormat).format(dateAux.getTime())
    }
  }

}