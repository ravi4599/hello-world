package com.virginvoyages.sfdc.ingestion

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

object PreVoyageSFDCFramework {
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
      
      println("******************************************************************************")
      println(batchId)
      println(batchInstanceId)
      
      val batchStartTime = metadata._3
      val batchEndTime = metadata._4
      val startExecutionTiime = metadata._7
      val partWriteDate = metadata._8
      
      
      log.info("After Metadata Framework")
      
      val selectQuery = spark.sparkContext.getConf.get("spark.src.table.query")
      val conditionColumn = spark.sparkContext.getConf.get("spark.src.condition.column").trim() 
      val userName = spark.sparkContext.getConf.get("spark.src.salesforce.username")
      val password = spark.sparkContext.getConf.get("spark.src.salesforce.password")
      val authEndpoint = spark.sparkContext.getConf.get("spark.src.salesforce.endpoint")
      val version = spark.sparkContext.getConf.get("spark.src.salesforce.version")
      
      log.info("After Extracted config File Details")

      import org.apache.spark.sql.types.{ StringType, TimestampType, IntegerType }
      import org.apache.spark.sql.functions.{ unix_timestamp, to_date }
      
      val bStartTime = (batchStartTime.substring(0,10)) +"T"+(batchStartTime.substring(11))+"-00:00"
      val bEndTime = (batchEndTime.substring(0,10)) +"T"+(batchEndTime.substring(11))+"-00:00"
      
      //val whereClause = " where LastModifiedDate>= "+ bStartTime +" and LastModifiedDate<= " + bEndTime
      val whereClause = " where "+ conditionColumn + " >= "+ bStartTime +" and " + conditionColumn + " <= " + bEndTime
      println(whereClause)
       
      println(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
      log.info(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")

      import spark.sqlContext.implicits._

      val selectQuery_f =  selectQuery + whereClause
      //val selectQuery_f =  selectQuery 
      println(selectQuery_f)

      // Connecting to Salesforce Objects
      val filteredDataDF = spark.read.format("com.springml.spark.salesforce").option("username", userName).option("password", password).option("login", authEndpoint).option("soql", selectQuery_f).option("version", version).load()
      filteredDataDF.show(false)
      println(filteredDataDF.count)
      filteredDataDF.printSchema
      
      
      if (!filteredDataDF.head(1).isEmpty) {
        println("***********************************************Data in new batch************************************************")
        
       //val hiveTable = spark.sparkContext.getConf.get("spark.target.hive.table")
        
        val finalDF = filteredDataDF.withColumn("batchtime", lit(batchStartTime).cast(TimestampType)).withColumn("part_date", to_date(lit(partWriteDate)))
        val temp_tab = spark.sparkContext.getConf.get("spark.target.table").replace('.', '_')
        finalDF.createOrReplaceTempView(temp_tab)

        //var clause = s""",BatchStartTime,BatchEndTime,Part_Date from $temp_tab"""
        var clause = s""",batchtime,part_date from $temp_tab"""

        var finaleQuery: String = null
        finaleQuery = "select " + spark.sparkContext.getConf.get("spark.source.columns") + clause
        println(finaleQuery)

        //println(finalDF)
        val stageFinalDf = spark.sql(finaleQuery)
        //stageFinalDf.printSchema
        stageFinalDf.show(2,false)

        stageFinalDf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.table"))

        //function call to update status as successful
       

      } else {
        println("***********************************************No Data in new batch************************************************")
        log.info("No Records in the Batch")
      } 
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