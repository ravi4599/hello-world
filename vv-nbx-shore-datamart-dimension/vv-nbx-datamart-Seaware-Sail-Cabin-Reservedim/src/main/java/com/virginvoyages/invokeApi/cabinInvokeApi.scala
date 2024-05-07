package com.virginvoyages.invokeApi

import java.util.Date
import com.virginvoyages.metadataframework.ManageMetadata
import java.sql.{ ResultSet, PreparedStatement, Connection, Driver, DriverManager, ResultSetMetaData, SQLException }
import scala.collection.immutable.Map
import scala.util.Try
import scala.xml.XML
import org.apache.spark.sql.Row
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.explode_outer
import org.apache.spark.sql.types.ArrayType
import com.databricks.spark.xml.XmlReader
import scalaj.http.Http
import scalaj.http.HttpOptions
import java.sql.Date
import scala.xml.Node
import scala.xml.Elem
import org.apache.spark.sql.functions.current_timestamp
import java.util.Properties
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import java.sql.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ StructType, StructField, TimestampType, StringType, IntegerType, DateType, LongType };
import org.apache.spark.sql.expressions.Window
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConversions._
import org.apache.spark.sql.functions._
import java.time.{ ZonedDateTime, ZoneId }
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{ Column, DataFrame }
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SaveMode
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact
import java.time.LocalDateTime


object cabinInvokeApi {
   val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]) {
    def getSparkSession() =
      {
        val spark = SparkSession
          .builder()
          .enableHiveSupport()
          .getOrCreate()

        spark

      }
    val spark = getSparkSession()
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)

    try {

    import spark.sqlContext.implicits._ 
    
    def findCabinDetail(sessionGuid: String, configMap: Broadcast[Map[String, String]],sailid: Int, sailStart: String, sailEnd: String, shipCode: String, depRefId: Int, arrRefId: Int): String = {
      log.info("Getting ManageShipInventory_IN Details")
      try {
        var manageShipInventoryIn = "<ManageShipInventory_IN><MsgHeader><Version>1.0</Version><SessionGUID>" + sessionGuid + "</SessionGUID><Language>ENG</Language></MsgHeader><Action><GetSailData><Sail><Ship>" + shipCode + "</Ship><From><Date>" + sailStart + "</Date></From><To><Date>" + sailEnd + "</Date></To><From><SailRefID>" + depRefId + "</SailRefID></From><To><SailRefID>" + arrRefId + "</SailRefID></To></Sail><Options><IncludeAvailData>Y</IncludeAvailData><IncludeCabinData>Y</IncludeCabinData><IncludeAllocations>N</IncludeAllocations></Options></GetSailData></Action></ManageShipInventory_IN>"
        val response = Http(configMap.value.get("spark.cabin.seaware.xml.api.url").get.trim).postData(manageShipInventoryIn)
          .header("Content-Type", "application/x-versonix-api")
          .header("Charset", "UTF-8")
          .option(HttpOptions.readTimeout(100000)).asString
        log.info("Response " + response.body)
        val manageShipInventoryOutXml = response.body
        return manageShipInventoryOutXml
      } catch {
        case e: Exception => { e.printStackTrace(); throw new Exception("General Exception..please check stacktrace ") }
      }
    }
    
    def getSessionGuid(configMap: Broadcast[Map[String, String]]): String = {
      log.info("Getting session guid")
      try {
        var postData = "<Login_IN><Version>1.0</Version><UserInfo><ResAgent><Username>" + configMap.value.get("spark.cabin.seaware.xml.api.username").get.trim + "</Username><Password>" + configMap.value.get("spark.cabin.seaware.xml.api.password").get.trim + "</Password></ResAgent></UserInfo></Login_IN>"
        val response = Http(configMap.value.get("spark.cabin.seaware.xml.api.url").get.trim).postData(postData)
          .header("Content-Type", "application/x-versonix-api")
          .header("Charset", "UTF-8")
          .option(HttpOptions.readTimeout(100000)).asString
        log.info("ResponseGiven " + response.body)
        val responseReceived = response.body
        val xml = XML.loadString(responseReceived)
        val sessionGuid = (xml \\ "MsgHeader" \ "SessionGUID").text
        log.info("SessionGuid " + sessionGuid)
        return sessionGuid
      } catch {
        case e: Exception => { e.printStackTrace(); throw new Exception("General Exception..please check stacktrace in Getting session guid") }
      }
    }
     
    var query = spark.sparkContext.getConf.get("spark.apiInvoke.sourcequery").trim
    var inputDf1 = spark.sql(query)
    import spark.sqlContext.implicits._
    var inputArray = Array[String]()
    var inputDf: DataFrame = null
       println("Start of API Call: " + LocalDateTime.now() )
       for (iter <- inputDf1.rdd.collect)
        {      
         val sail_id = iter.mkString(",").split(",")(0).toInt
         val ship_id = iter.mkString(",").split(",")(1)
         val sail_date_from = iter.mkString(",").split(",")(2)
         val sail_date_to = iter.mkString(",").split(",")(3)
         val ship_code = iter.mkString(",").split(",")(4)
         val is_active = iter.mkString(",").split(",")(5)
         val dep_ref_id = iter.mkString(",").split(",")(6).toInt
         val arr_ref_id = iter.mkString(",").split(",")(7).toInt
         val sessionGuid = getSessionGuid(sparkConfiguration)
         var apiXmlData = findCabinDetail(sessionGuid, sparkConfiguration,  sail_id, sail_date_from, sail_date_to, ship_code, dep_ref_id, arr_ref_id)
         inputArray= inputArray:+iter.mkString(",")+","+apiXmlData       
        }
       println("End of API Call: " + LocalDateTime.now() )
        val inputRDD = sc.parallelize(inputArray)
        inputDf = inputRDD.map { t =>       
          val sail_id = t.split(",")(0)
          val ship_id = t.split(",")(1)
          val sail_date_from = t.split(",")(2)
          val sail_date_to = t.split(",")(3)
          val ship_code = t.split(",")(4)
          val is_active = t.split(",")(5)
          val dep_ref_id = t.split(",")(6)
          val arr_ref_id = t.split(",")(7)
          val apiXmlData = t.split(",")(8)
        ( sail_id, apiXmlData, is_active, ship_id , ship_code, dep_ref_id, arr_ref_id)
        }.toDF("sailID", "xmlMessage",  "is_active", "ship_id", "ship_code", "depRefId", "arrRefId")
        
        
        println(" Starting XML Data write to GS Location " + LocalDateTime.now())
        val location = spark.sparkContext.getConf.get("spark.xmlData.location").trim
        inputDf.write.mode("overwrite").parquet(location)
        println(" Finished XML Data write to GS Location " + LocalDateTime.now())

    }catch{
      case e: SQLException =>
        { 
          log.info("******************in the catch of Res Addon Rel Load ******************");
          e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace", e); 
          }
        case e: Exception =>
          { 
            log.info("******************in the catch of Res Addon Rel Load ******************");
            e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace", e); 
            }
          }
    spark.stop()   
  }
}