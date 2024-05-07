package com.virginvoyages.ingestion

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import com.virginvoyages.metadataframework.ManageMetadata
import org.apache.spark.sql.types.{ StringType, TimestampType }
import org.apache.spark.sql.functions._
import java.sql.SQLException
import org.apache.spark._
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import org.apache.hadoop.fs._
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.ZonedDateTime
import java.time.Instant
import java.time.Duration
import java.io.File
import com.virginvoyages.ingestion.MyUtil

object csvCreditCardLoad {
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
	  
	    val inputfilepath=spark.sparkContext.getConf.get("spark.input.inputfilepath").trim()
//	    val sourceFormat = spark.sparkContext.getConf.get("spark.source.format")
	    val errorPath = spark.sparkContext.getConf.get("spark.error.filepath").trim()
	    var csvHeader: String = null
	    if (sparkConfiguration.value.contains("spark.csv.header")){
	      csvHeader = spark.sparkContext.getConf.get("spark.csv.header")
	    }
	    var src: String = null
	    var fullpath: String = inputfilepath
	    var csvDF =spark.emptyDataFrame
	   
	    import org.apache.hadoop.fs._
	    var hiveTable = spark.sparkContext.getConf.get("spark.target.table")
	    val conf = sc.hadoopConfiguration
      val gcsBucket = new Path(inputfilepath)
      val filesIter = gcsBucket.getFileSystem(conf).listFiles(gcsBucket, true)
      var flag=false
      var files = Seq[Path]()
      val tableStruct = spark.catalog.listColumns(hiveTable).select("name","dataType")
      tableStruct.show(false)
      val length = tableStruct.count()
      val len2 = length - 3

      while (filesIter.hasNext) {
        val filenamepath= filesIter.next().getPath.toString()
        val filename = filenamepath.substring(filenamepath.lastIndexOf("/") +1)
        println(filenamepath)
        println(filename)
        if(filenamepath.contains(".CSV") || filenamepath.contains(".csv")){
          if ((!(sparkConfiguration.value.contains("spark.csv.header"))) || csvHeader.equalsIgnoreCase("true")){
            csvDF = spark.read.format("csv").option("header","true").load(filenamepath)
          }
          else if(csvHeader.equalsIgnoreCase("false")) {
            csvDF = spark.read.format("csv").load(filenamepath)
          }
          val tgtLocation = spark.sparkContext.getConf.get("spark.target.location")
          var hiveTable = spark.sparkContext.getConf.get("spark.target.table")
          //csvDF.show(false)
          csvDF.createOrReplaceTempView("csv_data")
          val csvStruct = spark.catalog.listColumns("csv_data").select("name","dataType")
          val csvlen = csvStruct.count()
          var finaleQuery: String = null
          if (sparkConfiguration.value.contains("spark.source.query")) {
            finaleQuery = spark.sparkContext.getConf.get("spark.source.query")+ s""" from csv_data """
          }
          else {
            finaleQuery = "select " + spark.sparkContext.getConf.get("spark.source.columns").trim() + s""" from csv_data """
            }
          println(finaleQuery)
          val stageDf = spark.sql(finaleQuery)
          
        try{
          if(csvlen == len2){
            val stageFinalDf = stageDf.withColumn("batchtime",lit(batchStartTime).cast(TimestampType)).withColumn("part_date", lit(current_date())).withColumn("file_name",lit(filename).cast(StringType))
            stageFinalDf.printSchema
            stageFinalDf.show(false)
            stageFinalDf.repartition(15).write.mode("append").partitionBy("part_date").parquet(tgtLocation)
            spark.sql("Msck repair table " + hiveTable)
            
          }
      /*    else{
            println(" Columns mismatch. Please check the source csv file ")
            log.info(" Columns mismatch. Please check the source csv file ");         
          }			*/
          
          ManageMetadata.updateStatus(batchInstanceId, batchId, "Successful", spark)
        }
          catch{
            case e: Exception => {
            println(" Columns mismatch. Please check the source csv file: " + filenamepath)
            log.info(" Columns mismatch. Please check the source csv file " + filenamepath);
            MyUtil.fileTransfer(spark,inputfilepath,filename,errorPath,filenamepath)
            ManageMetadata.updateStatus(batchInstanceId, batchId, "Failed", spark)
            e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace")
            }
            
            System.exit(1)
          }
        }
          
	        //stageFinalDf.write.mode("append").insertInto(hiveTable)
	    }
        
        
          //else { println("No file found") }       
	  
    } catch {

      case e: SQLException => {
        ManageMetadata.updateStatus(batchInstanceId, batchId, "Failed", spark)
        log.info("***************in the catch of csv to table data load Ingestion ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace")
      }
      case e: Exception =>
        {
          ManageMetadata.updateStatus(batchInstanceId, batchId, "Failed", spark)
          log.info("***************in the catch of csv to table data load Ingestion ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace")
        }
        System.exit(1)

    }
  }
  
}