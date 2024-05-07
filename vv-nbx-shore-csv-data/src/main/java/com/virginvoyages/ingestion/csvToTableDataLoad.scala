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

object csvToTableDataLoad {
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
      while (filesIter.hasNext) {
        val filename= filesIter.next().getPath.toString()
        println(filename)
        if (sparkConfiguration.value.contains("spark.source.file")) {
          src = spark.sparkContext.getConf.get("spark.source.file")
          if(filename.contains(src)){
            fullpath=inputfilepath+"/"+s"""*$src*.CSV""" 
            csvDF = spark.read.format("csv").option("header","true").load(fullpath)
            val tgtLocation = spark.sparkContext.getConf.get("spark.target.location")
            val hiveTable = spark.sparkContext.getConf.get("spark.target.table")
            csvDF.createOrReplaceTempView("csv_data")
            var finaleQuery: String = null
            if (sparkConfiguration.value.contains("spark.source.query")) {
              finaleQuery = spark.sparkContext.getConf.get("spark.source.query")+ s""" from csv_data """
              }
            else {
              finaleQuery = "select " + spark.sparkContext.getConf.get("spark.source.columns").trim() + s""" from csv_data """
              } 
            println(finaleQuery)
            val stageDf = spark.sql(finaleQuery)
            val stageFinalDf = stageDf.withColumn("BatchTime",lit(batchStartTime).cast(TimestampType)).withColumn("part_date", lit(current_date()))
            stageFinalDf.printSchema
            stageFinalDf.show(false)
            stageFinalDf.repartition(15).write.mode("append").partitionBy("part_date").parquet(tgtLocation)
            }
          }
        else if(filename.contains(".CSV") || filename.contains(".csv")){
          if ((!(sparkConfiguration.value.contains("spark.csv.header"))) || csvHeader.equalsIgnoreCase("true")){
            csvDF = spark.read.format("csv").option("header","true").load(filename)
          }
          else if(csvHeader.equalsIgnoreCase("false")) {
            csvDF = spark.read.format("csv").load(filename)
          }
          val tgtLocation = spark.sparkContext.getConf.get("spark.target.location")
          var hiveTable = spark.sparkContext.getConf.get("spark.target.table")
          //csvDF.show(false)
          csvDF.createOrReplaceTempView("csv_data")
          var finaleQuery: String = null
          if (sparkConfiguration.value.contains("spark.source.query")) {
            finaleQuery = spark.sparkContext.getConf.get("spark.source.query")+ s""" from csv_data """
            }
          else {
            finaleQuery = "select " + spark.sparkContext.getConf.get("spark.source.columns").trim() + s""" from csv_data """
            } 
          println(finaleQuery)
          val stageDf = spark.sql(finaleQuery)
          val stageFinalDf = stageDf.withColumn("BatchTime",lit(batchStartTime).cast(TimestampType)).withColumn("part_date", lit(current_date()))
          stageFinalDf.printSchema
          stageFinalDf.show(false)
	        stageFinalDf.repartition(15).write.mode("append").partitionBy("part_date").parquet(tgtLocation)
	        
	        //stageFinalDf.write.mode("append").insertInto(hiveTable)
	        }
        
        
          //else { println("No file found") }
      }  
      if ( spark.catalog.listColumns(hiveTable).where(col("ispartition") === true).count()!=0){
	  spark.sql("Msck repair table " + hiveTable)
      }
	   ManageMetadata.updateStatus(batchInstanceId, batchId, "Successful", spark)
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