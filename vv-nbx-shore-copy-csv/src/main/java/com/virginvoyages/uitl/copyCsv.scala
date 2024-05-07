package com.virginvoyages.uitl
import java.util.Calendar;
import java.text.SimpleDateFormat;
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{ col, to_date, to_timestamp, monotonically_increasing_id }
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions.{ col, lit, when }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import java.sql.DriverManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ to_date, to_timestamp }
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import java.net.URI
import com.crealytics.spark.excel

object copyCsv {
  def main(args: Array[String]) {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    spark.sqlContext.setConf("spark.sql.parquet.writeLegacyFormat", "true")

    val sourceFile = spark.sparkContext.getConf.get("spark.srcfile").trim()
    val sourceFile1 = sourceFile.replace(".xlsx", "")
    val inputfilepath = spark.sparkContext.getConf.get("spark.input.srcpath").trim()

    val outfilepath = spark.sparkContext.getConf.get("spark.input.tgtpath").trim()

    val archivepath = spark.sparkContext.getConf.get("spark.input.archivepath").trim()

    val ourform = new SimpleDateFormat("yMd")
    val formatted_Date = ourform.format(Calendar.getInstance().getTime())

    val outfilepathwitdate = outfilepath.concat(sourceFile1 + ".parquet")
    val archivepathwithdate = archivepath.concat(sourceFile1 + "_" + formatted_Date + ".csv")

    val inputDataDf = spark.read.format("com.crealytics.spark.excel").option("sheetName", "Sheet1").option("header", "true").option("treatEmptyValuesAsNulls", "false").load(inputfilepath)
    inputDataDf.show(false)

if (!inputDataDf.head(1).isEmpty)
{
    println("*** File Has been Loaded ***")
    val modifiedColumn = inputDataDf.select(col("Start Date"), col("End Date"), col("Ship"), col("Event"),col("Answer Code")).withColumnRenamed("Start Date", "start_date").withColumnRenamed("End Date", "end_date").withColumnRenamed("Ship", "Ship").withColumnRenamed("Event", "Event").withColumnRenamed("Answer Code", "EventCode")
    modifiedColumn.show(false)
    modifiedColumn.write.mode("Overwrite").parquet(outfilepathwitdate)
    println(outfilepathwitdate)
    println("*** File Has been Copied ***")

    modifiedColumn.write.option("header", "true").mode("append").csv(archivepathwithdate)
    println(archivepathwithdate)
    println("*** File Has been Archived ***")
}
    spark.stop()
  }

}

