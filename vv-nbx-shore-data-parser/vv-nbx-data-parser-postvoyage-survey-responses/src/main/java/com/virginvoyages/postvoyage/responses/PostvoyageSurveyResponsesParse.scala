package com.virginvoyages.postvoyage.responses

import com.virginvoyages.metadataframework.ManageMetadata
import org.apache.spark.SparkConf
import java.sql.SQLException
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.Row

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
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Column
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Column, DataFrame }
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat
import org.apache.spark.broadcast.Broadcast

//XML validator imports
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.xml.sax.SAXException;
import org.apache.spark.SparkFiles
import scala.util.Try

import scala.collection.mutable.ListBuffer
import org.apache.hadoop.fs.{ FileSystem, Path }
import java.net.URI
import java.io.IOException
import java.time.{LocalDate, ZoneId}

object PostvoyageSurveyResponsesParse {
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
  val spark = SparkSession
    .builder()
    .enableHiveSupport()
    .getOrCreate()
  def main(args: Array[String]): Unit = {

  	val currentDate = java.time.LocalDate.now
  	println("Current Date = " + currentDate)
  	val batchStartDate = currentDate.minusDays(1)
	  println("Batch Start Time = "+ batchStartDate + " " + "00:00:00")
	  val batchStartTime = batchStartDate + " " + "00:00:00"
    val batchEndTime = batchStartDate + " " + "23:59:59"

    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    
    val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
    metadata.productIterator.foreach(println)
    val batch_id1 = metadata._1
    val batch_instance_id1 = metadata._2
    val batch_start_tme = metadata._3
    val batch_end_tme = metadata._4
    val part_read_start = metadata._5
    val part_read_end = metadata._6
    val start_execution_time = metadata._7
    val part_write_date = metadata._8

    
    
    //    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    //    val log = LogManager.getRootLogger
    //    log.setLevel(Level.INFO)
    import spark.implicits._
    
//    
//    val hdfsURI = spark.sparkContext.getConf.get("spark.source.postvoyage.resp.hdfs.postvoyage.file")
//    
//    log.info("URI path"+hdfsURI)
//    
//    val hdfsURI1 = spark.sparkContext.getConf.get("spark.source.postvoyage.resp.hdfs.remi.file")
//    log.info("URI path"+hdfsURI1)
//    FileSystem.setDefaultUri(spark.sparkContext.hadoopConfiguration, new URI(hdfsURI))
//
//    val hdfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
//    log.info("URI path"+hdfs)
//    FileSystem.setDefaultUri(spark.sparkContext.hadoopConfiguration, new URI(hdfsURI1))
//    
//    val hdfs1: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
//    log.info("URI path1"+hdfs1)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    var postfile_check_flag = 0
    
    val fs1 = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    var remifile_check_flag = 0
    
    try {
      postfile_check_flag = fs.listStatus(new Path(spark.sparkContext.getConf.get("spark.source.postvoyage.resp.hdfs.postvoyage.file"))).filter(_.isDir).map(_.getPath).length
      println("----------------------The Path is " + spark.sparkContext.getConf.get("spark.source.postvoyage.resp.hdfs.postvoyage.file") + "--------------------------------")
      postfile_check_flag = 1
    } catch {
      case e: IOException => { postfile_check_flag = 0; log.info("******************Post voyage response Folder not found ******************"); 
      println("****************** Post voyage response Folder not found ******************"); }
      } 
    
    try {
      remifile_check_flag = fs.listStatus(new Path(spark.sparkContext.getConf.get("spark.source.postvoyage.resp.hdfs.remi.file"))).filter(_.isDir).map(_.getPath).length
      println("----------------------The Path is " + spark.sparkContext.getConf.get("spark.source.postvoyage.resp.hdfs.remi.file") + "--------------------------------")
      remifile_check_flag = 1
    } catch {
      case e: IOException => { remifile_check_flag = 0; log.info("******************Postvoyage Remi Folder not found ******************"); 
      println("******************Postvoyage Remi Folder not found ******************"); }
      } 
    
    try {
      if (postfile_check_flag != 0) {
        print("The file status is ", postfile_check_flag, "1 ==> files are present ")
        log.info("postvoyage response CSV File exists!")
        val postvoyagedfsrc = spark.read.
          option("encoding", "UTF-8").
          option("ignoreLeadingWhiteSpace", "true").
          option("ignoreTrailingWhiteSpace", "true").
          option("header", "true").
          option("treatEmptyValuesAsNulls", "true").
          option("inferSchema", "true").
          option("escape", "\"").
          option("quote", "\"").
          option("multiLine", "true").
          csv(spark.sparkContext.getConf.get("spark.source.postvoyage.resp.hdfs.postvoyage.file") + "/*.*").withColumn("file_name", input_file_name) 
        val file_name = spark.sparkContext.getConf.get("spark.source.postvoyage.resp.hdfs.postvoyage.file")
        loadFiletoHive(postvoyagedfsrc, batchStartTime, sparkConfiguration, file_name)
      } else {
        log.info("postvoyage response CSV File deos not exists!")
      }
      
      if (remifile_check_flag != 0) {
        print("The file status is ", remifile_check_flag, "1 ==> files are present ")
        log.info("postvoyage response remi CSV File exists!")
        val postvoyagedfsrc = spark.read.
          option("encoding", "UTF-8").
          option("ignoreLeadingWhiteSpace", "true").
          option("ignoreTrailingWhiteSpace", "true").
          option("header", "true").
          option("treatEmptyValuesAsNulls", "true").
          option("inferSchema", "true").
          option("escape", "\"").
          option("quote", "\"").
          option("multiLine", "true").
          csv(spark.sparkContext.getConf.get("spark.source.postvoyage.resp.hdfs.remi.file") + "/*.*").withColumn("file_name", input_file_name)
        val file_name = spark.sparkContext.getConf.get("spark.source.postvoyage.resp.hdfs.remi.file")
        loadFiletoHive(postvoyagedfsrc, batchStartTime, sparkConfiguration, file_name)
      } else {
        log.info("postvoyage response remi CSV File does not exists!")
      }
      log.info("Updating Metadata framework")
    ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
    
    } catch {

      case e: Exception =>
        {
          //          log.info("in the catch of updateStatus ****************** ")
          e.printStackTrace()
          ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
          throw new Exception("General Exception..please check the stacktrace")
        }
    }

    
  }

  def loadFiletoHive(postvoyagedfsrc: DataFrame, batchStartTime: String, configMap: Broadcast[Map[String, String]], file_name: String) = {

    val static_columns: Seq[String] = spark.sparkContext.getConf.get("spark.source.static.columns").split(",")

    var postvoyagedf = postvoyagedfsrc.withColumn("file_name", reverse(substring_index(reverse(postvoyagedfsrc.col("file_name")),"/",1)))
      .withColumn("etl_load_dt", current_timestamp())
      .withColumn("batchtime", unix_timestamp(lit(batchStartTime), "yyyy-MM-dd' 'HH:mm:ss").cast(TimestampType))
      .withColumn("part_date", lit(to_date(col("batchtime"), "yyyy-MM-dd")))

//    postvoyagedf.show(10, false)
//    postvoyagedf.printSchema()

    var postvoyage_static = postvoyagedf.select("responseid", "respid", "interview_start", "interview_end", "status", "email", "emailAddress", "CountryId", "Datebehaviour", "Time", "MemberId", "OptOutLink", "ExternalTargetGroupId", "ExternalTargetGroupName", "TargetGroupNameFromFile", "City", "Address", "PurchaseAmount", "VisitId", "CandidateId", "InternalTargetGroupId", "SampleId", "CandidateHash", "clientId", "FILE_GENERATION_DATE", "SHIPCODE", "SHIPNAME", "VOYAGEID", "VOYAGE_START", "VOYAGE_END", "ITINERARY_PORTS", "SHOREX_NAME", "QFlag", "RespStatus", "src", "Id", "ForeignId", "HQCountry", "l", "ErrorMessage_1", "ErrorMessage_2", "ErrorMessage_3", "ErrorMessage_4", "ErrorMessage_5", "ErrorMessage_6", "ErrorMessage_7", "ErrorMessage_8", "ErrorMessage_9", "Session", "IPAddress", "Beginning_time", "Beginning_Date", "BussXX", "Week", "month", "year", "UserAgent", "BrowserV", "BrowserT", "BrowserOS", "BrowserFeatures", "IsMobile", "RenderingVersion", "HQTrack_1", "HQTrack_2", "ClientName", "LOI", "Incentive", "HQSpeeder_1", "HQStraightLiners_1", "End_time", "End_Date", "intdur", "intdurMinutes", "file_name", "etl_load_dt", "batchtime", "part_date")

    postvoyage_static = postvoyage_static.withColumn("responseid", postvoyage_static.col("responseid").cast(LongType))
      .withColumn("respid", postvoyage_static.col("respid").cast(LongType))
      .withColumn("interview_start", unix_timestamp(postvoyage_static.col("interview_start"), "M/dd/yyyy HH:mm").cast(TimestampType))
      .withColumn("interview_end", unix_timestamp(postvoyage_static.col("interview_end"), "M/dd/yyyy HH:mm").cast(TimestampType))
      .withColumn("CountryId", postvoyage_static.col("CountryId").cast(LongType))
      .withColumn("MemberId", postvoyage_static.col("MemberId").cast(LongType))
      .withColumn("ExternalTargetGroupId", postvoyage_static.col("ExternalTargetGroupId").cast(LongType))
      .withColumn("VisitId", postvoyage_static.col("VisitId").cast(LongType))
      .withColumn("CandidateId", postvoyage_static.col("CandidateId").cast(LongType))
      .withColumn("InternalTargetGroupId", postvoyage_static.col("InternalTargetGroupId").cast(LongType))
      .withColumn("SampleId", postvoyage_static.col("SampleId").cast(LongType))
      .withColumn("clientId", postvoyage_static.col("clientId").cast(LongType))
      .withColumn("VOYAGEID", postvoyage_static.col("VOYAGEID").cast(StringType))
      .withColumn("VOYAGE_START", unix_timestamp(postvoyage_static.col("VOYAGE_START"), "MM/dd/yyyy").cast(TimestampType))
.withColumn("VOYAGE_END", unix_timestamp(postvoyage_static.col("VOYAGE_END"), "MM/dd/yyyy").cast(TimestampType))
      .withColumn("Id", postvoyage_static.col("Id").cast(LongType))
      .withColumn("ForeignId", postvoyage_static.col("ForeignId").cast(LongType))
      .withColumn("Beginning_Date", to_date(postvoyage_static.col("Beginning_Date"), "MM/dd/yyyy").cast(DateType))
      .withColumn("End_Date", to_date(postvoyage_static.col("End_Date"), "MM/dd/yyyy").cast(DateType))

//    postvoyage_static.show(10, false)
//    postvoyage_static.printSchema()

    val header: Seq[String] = postvoyagedf.columns.toSeq.map(x => x.trim)

    val dynamiccolumns: Seq[String] = header diff static_columns

    log.info("dynamic columns: " + dynamiccolumns)
    log.info("number of dynamic columns: " + dynamiccolumns.size)

    val dynamiccolumns1: Seq[String] = "responseid" +: dynamiccolumns :+ "file_name" :+ "etl_load_dt" :+ "batchtime" :+ "part_date"

    var dynamicarraystr = ""
    var dynamiccolslistbuff = new ListBuffer[String]()
    var dynamiccolslist: List[String] = List()

    for (col <- dynamiccolumns) {
      dynamicarraystr = s"'${col}', ${col}"
      dynamiccolslistbuff += dynamicarraystr
    }

    dynamiccolslist = dynamiccolslistbuff.toList
    var dynamiccolsstr = dynamiccolslist.mkString(",")
    var dynamiccolsstr1 = s"stack(${dynamiccolumns.size}, " + dynamiccolsstr + " ) as (Qkey,Qvalue)"
    val e = expr(dynamiccolsstr1)

    var postvoyagedf1 = postvoyagedf.select(postvoyagedf.columns.filter(colName => dynamiccolumns1.contains(colName)).map(colName => new Column(colName)): _*)
    
    postvoyagedf1 = postvoyagedf1.select(postvoyagedf1.columns.map(c => col(c).cast(StringType)) : _*)
    
    postvoyagedf1 = postvoyagedf1.select(dynamiccolumns1.map(col) :+ e: _*)

    //      postvoyagedf1.show()
    //      postvoyagedf1.printSchema()

    postvoyagedf1 = postvoyagedf1.withColumn("Qprefix", when(postvoyagedf1.col("Qkey").contains("_"), split(postvoyagedf1.col("Qkey"), "_").getItem(0)).otherwise(postvoyagedf1.col("Qkey")))
      .withColumn("Qindex", when(postvoyagedf1.col("Qkey").contains("_"), split(postvoyagedf1.col("Qkey"), "_").getItem(1)).otherwise(lit(0)))

    postvoyagedf1 = postvoyagedf1.withColumn("responseid", postvoyagedf1.col("responseid").cast(LongType))
      .withColumn("Qindex", postvoyagedf1.col("Qindex").cast(LongType))

    postvoyagedf1 = postvoyagedf1.select("responseid", "Qkey", "Qvalue", "Qprefix", "Qindex", "file_name", "etl_load_dt", "batchtime", "part_date")
    
    postvoyagedf1 = postvoyagedf1.filter((postvoyagedf1.col("Qkey") rlike "^Q[0-9].*|^HQ.*"))
//    postvoyagedf1.show(500, false)
//    postvoyagedf1.printSchema()
    postvoyage_static.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.parse.static.table"))
    postvoyagedf1.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.parse.dynamic.table"))
  }

}