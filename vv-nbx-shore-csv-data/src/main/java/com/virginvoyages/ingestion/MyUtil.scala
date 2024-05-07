package com.virginvoyages.ingestion


import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat
import scala.util.control._
import org.apache.spark.storage.StorageLevel
import java.util.Date
import java.net.UnknownHostException
import org.apache.spark.broadcast.Broadcast
import java.text.SimpleDateFormat
import scala.util.parsing.json._
import java.util.Properties
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ StructType, StructField, TimestampType, StringType, IntegerType, DateType, LongType };
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import scala.collection.JavaConversions._
import org.apache.spark.sql.functions._
import scala.util.Try
import org.apache.spark.sql.DataFrame
import java.time.{ ZonedDateTime, ZoneId }
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{ Column, DataFrame }
import scala.collection.mutable.ArrayBuffer
import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.sql.SaveMode
import scala.util.Random

object MyUtil {
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def fileTransfer(spark: SparkSession, inputfilepath: String, filename: String, targetPath: String, filenamepath: String) {

    
    import java.util.Calendar
    val now = Calendar.getInstance.getTime

    val date = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val todaydate = date.format(now)
   
      
      try {
        val src = new Path(inputfilepath)
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = src.getFileSystem(conf)
        val targetPathFile = s"${targetPath}${filename}"
        println("targetPath   ==> " + targetPathFile)
        val targetHadoopPath = new Path(targetPathFile)
        fs.rename(new Path(filenamepath), targetHadoopPath);

        log.info(todaydate + "------  File moved to Error Location : " + targetPathFile)
      } catch {
        case e: SQLException => {
          e.printStackTrace();
          log.info(todaydate + "connectioin issue..please check service");
        }
        case e: Exception => {
          throw new Exception("General Exception..please check the stacktrace")
        }
      }
      

    
  }
}