package com.virginvoyages.sfdc.parser

import com.sforce.soap.partner.fault.UnexpectedErrorFault
import com.sforce.soap.partner.{Connector, PartnerConnection, SaveResult}
import com.sforce.ws.ConnectorConfig
import com.springml.spark.salesforce.metadata.MetadataConstructor
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SizeEstimator

import org.apache.spark.sql.SQLContext
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


object PreVoyageSFDCParserFramework {
   def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")  

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
      
      println("******************************************************************************")
      println(batchId)
      println(batchInstanceId)
      
      val batchStartTime = metadata._3
      val batchEndTime = metadata._4
      val startExecutionTiime = metadata._7
      val partWriteDate = metadata._8
      
      
      log.info("After Metadata Framework")
      
      val partitionColumns = spark.sparkContext.getConf.get("spark.src.partitionbyId.columns")trim
      val conditionColumn = spark.sparkContext.getConf.get("spark.src.condition.column").trim() 
      val sourceTable = spark.sparkContext.getConf.get("spark.source.table").trim
      val targetTable = spark.sparkContext.getConf.get("spark.target.table").trim
      //val targetLocation= spark.sparkContext.getConf.get("spark.target.location").trim
      

      
      log.info("Extracted config File Details")
	  
      var query: String = null
	  
      if (sparkConfiguration.value.contains("spark.source.query")) {
        query = spark.sparkContext.getConf.get("spark.source.query").trim
      } else {
        query = "select * from (select *, row_number()over( partition by " + partitionColumns + " order by " + conditionColumn + " desc) as rn from " + sourceTable + ") where rn = 1 "
      }
	  print(" Query = " + query )

      val stageDf = spark.sql(query)
      
      val stageFinalDf = stageDf.drop("rn")
      
      stageFinalDf.printSchema
      
      val targetLocation = spark.sql("desc formatted "+targetTable).toDF.filter('col_name === "Location").collect()(0)(1).toString

      if (sparkConfiguration.value.contains("spark.partitionby"))
      {
        stageFinalDf.repartition(15).write.mode("Overwrite").partitionBy(spark.sparkContext.getConf.get("spark.partitionby").trim).parquet(targetLocation)
      }
      else {
      stageFinalDf.repartition(15).write.mode("Overwrite").partitionBy("part_date").parquet(targetLocation)
      }
       // stageFinalDf.write.mode("overwrite").insertInto(targetTable)

        //function call to update status as successful
       

  
       ManageMetadata.updateStatus(batchInstanceId, batchId, "Successful", spark)
    } catch {

      case e: SQLException => {
        ManageMetadata.updateStatus(batchInstanceId, batchId, "Failed", spark)
        log.info("***************in the catch of Salesforce Ingestion ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace")
      }
      case e: Exception =>
        {
          ManageMetadata.updateStatus(batchInstanceId, batchId, "Failed", spark)
          log.info("***************in the catch of Salesforcei Ingestion ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace")
        }
        System.exit(1)

    }
    
  }
}