package com.virginvoyages.dimension

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

object SailCabinReserveLoad {
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
/*************************************calling metadata framework*********************************************/

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

    try {

    import spark.sqlContext.implicits._ 
    
    def findCabinDetail(sessionGuid: String, configMap: Broadcast[Map[String, String]], ship: String, sailid: Int, depRefId: Int, arrRefId: Int): String = {
      log.info("Getting ManageShipInventory_IN Details")
      try {
        var manageShipInventoryIn = "<ManageShipInventory_IN><MsgHeader><Version>1.0</Version><SessionGUID>" + sessionGuid + "</SessionGUID><Language>ENG</Language></MsgHeader><Action><GetSailData><Sail><Ship>" + ship + "</Ship><From><SailRefID>" + depRefId + "</SailRefID></From><To><SailRefID>" + arrRefId + "</SailRefID></To></Sail><Options><IncludeAvailData>Y</IncludeAvailData><IncludeCabinData>Y</IncludeCabinData><IncludeAllocations>N</IncludeAllocations></Options></GetSailData></Action></ManageShipInventory_IN>"
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
     
    var query = spark.sparkContext.getConf.get("spark.cabin.sourcequery").trim
    var inputDf1 = spark.sql(query)
    import spark.sqlContext.implicits._
    var inputArray = Array[String]()
    var inputDf: DataFrame = null
     if(spark.sparkContext.getConf.get("spark.xmlData.DirectRead").trim.equals("N")){
       println("Start of API Call: " + LocalDateTime.now() )
       for (iter <- inputDf1.rdd.collect)
        {      
          val ship = iter.mkString(",").split(",")(0)
          val sail_id = iter.mkString(",").split(",")(1).toInt
          val dep_ref_id = iter.mkString(",").split(",")(2).toInt
          val arr_ref_id = iter.mkString(",").split(",")(3).toInt
          val sessionGuid = getSessionGuid(sparkConfiguration)
          var apiXmlData = findCabinDetail(sessionGuid, sparkConfiguration, ship, sail_id, dep_ref_id, arr_ref_id)
          inputArray= inputArray:+iter.mkString(",")+","+apiXmlData       
        }
       println("End of API Call: " + LocalDateTime.now() )
        val inputRDD = sc.parallelize(inputArray)
        inputDf = inputRDD.map { t =>
          val ship = t.split(",")(0)
          val sail_id = t.split(",")(1)
          val dep_ref_id = t.split(",")(2)
          val arr_ref_id = t.split(",")(3)
          val apiXmlData = t.split(",")(4)
        ( sail_id, apiXmlData, ship, dep_ref_id, arr_ref_id)
        }.toDF("sailID", "xmlMessage", "ship_code", "depRefId", "arrRefId")
        println("XML Data Read from API" )
        
     }else if(spark.sparkContext.getConf.get("spark.xmlData.DirectRead").trim.equals("Y")){
          inputDf = spark.read.parquet(spark.sparkContext.getConf.get("spark.xmlDataRead.location").trim)
          println("XML Data Read from GS Location" )
     }
    
    parseCabinDetail(inputDf, sparkConfiguration)
    
    /**
     * Function to check to a DataFrame has column or not
     */
    def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
    /**
     * Function to return boolean (True/False) column is of type Array
     */
    def checkArray(df: DataFrame, colname: String): Boolean = {
      df.schema(colname).dataType match {
        case ArrayType(_, _) => return true
        case _               => return false
      }
    }
    /**
     * Function to check if a column is of Struct data type
     */
    def checkExplode(df: DataFrame, colname: String): Boolean = {
      df.schema(colname).dataType match {
        case StructType(_) => return true
        case _             => return false
      }
    }
    
    def parseCabinDetail(cabinDetailXml: DataFrame, configMap: Broadcast[Map[String, String]]): DataFrame = {
      try {
        var dfnoEmpty = cabinDetailXml.filter($"xmlMessage" =!= "")
        var xmlStringRDD = dfnoEmpty.select("xmlMessage").map(r => r.getString(0)).rdd
        var startingDF2 = new XmlReader().xmlRdd(spark.sqlContext, xmlStringRDD) //PARSE ONE
        var cabinDetailXml2 = cabinDetailXml.withColumn("rowId1", monotonically_increasing_id())
        startingDF2 = startingDF2.withColumn("rowId1", monotonically_increasing_id())
        var w = Window.orderBy("rowId1")
        // Use row number with the window specification and Drop the created increasing data column
        cabinDetailXml2 = cabinDetailXml2.withColumn("index", row_number().over(w)).drop("rowId1")
        startingDF2 = startingDF2.withColumn("index", row_number().over(w)).drop("rowId1")
        var df = cabinDetailXml2.join(startingDF2, cabinDetailXml2("index") === startingDF2("index"), "inner").select(cabinDetailXml2.col("sailID"), cabinDetailXml2.col("ship_code"), cabinDetailXml2.col("depRefId"), cabinDetailXml2.col("arrRefId"), startingDF2.col("*"))
        log.info("---------Final Match with ID---------")
        log.info("---------Final Match DONE with ID---------")
        var checkErrorsfield = hasColumn(df, "Errors")
	    	if (checkErrorsfield.equals(true)) {
             df = df.where(df.col("Errors").isNull)
            df = df.drop("Errors") 
        }
        var checkActionTagPresent = hasColumn(df, "Action")
        if (checkActionTagPresent.equals(true)) {
          log.info("Inside checkActionTagPresent")
          var manageShipInventoryDF = df.withColumn("Action", explode(array(df.col("Action"))))
          .withColumn("MsgHeader", explode(array(df.col("MsgHeader"))));
          manageShipInventoryDF = manageShipInventoryDF.withColumn("GetSailData", manageShipInventoryDF.col("Action.GetSailData"))
          log.info("Inside getsaildaat")
          manageShipInventoryDF = manageShipInventoryDF.withColumn("CabinData", manageShipInventoryDF.col("GetSailData.CabinData"))
          log.info("after getsailinfo")
          if (checkExplode(manageShipInventoryDF, "CabinData")) {
            manageShipInventoryDF = manageShipInventoryDF.withColumn("Cabin", manageShipInventoryDF.col("CabinData.Cabin")).drop("CabinData")
          }           
          if (checkArray(manageShipInventoryDF, "Cabin")) {
              println("before cabin number")            
              manageShipInventoryDF = manageShipInventoryDF.withColumn("CabinInfo", explode_outer(manageShipInventoryDF.col("Cabin")))            
              manageShipInventoryDF = manageShipInventoryDF.withColumn("CabinNumber", manageShipInventoryDF.col("CabinInfo.CabinNumber"))
              manageShipInventoryDF.printSchema
              println("after Cabin_number")
              manageShipInventoryDF = manageShipInventoryDF.withColumn("ReserveInfo", manageShipInventoryDF.col("Cabininfo.ReserveInfo"))
              manageShipInventoryDF = manageShipInventoryDF.withColumn("ReserveInfoEx", explode(array(manageShipInventoryDF.col("ReserveInfo"))))
              println("after explode reserveinfo")
              manageShipInventoryDF.printSchema
              manageShipInventoryDF = manageShipInventoryDF.drop("GetSailData")
              .drop("Action")
              .drop("MsgHeader")
              .drop("Cabin")
              .drop("ReserveInfo")
              .drop("CabinInfo")
              val reserve_columns = spark.sparkContext.getConf.get("spark.reserve.columns").trim.split(",")
              val missing_columns_r = ArrayBuffer[String]()
              val avaliable_columns_r = ArrayBuffer[String]()
              for (col <- reserve_columns) {
                if (hasColumn( manageShipInventoryDF, "ReserveInfoEx."+col)) {
                  println(col, "column exists", avaliable_columns_r.toString)
                  println("column exists", avaliable_columns_r.length)
                  log.info(col, "column exists", avaliable_columns_r.toString, avaliable_columns_r.length)
                  println(avaliable_columns_r.length, "length")
                  avaliable_columns_r.append(col)
                  }else{
                    println(col, "column missing", missing_columns_r.length)
                    println(missing_columns_r.length, "length")
                    log.info(col, "column exists", missing_columns_r.toString, missing_columns_r.length)
                    println("column missing", missing_columns_r)
                    missing_columns_r.append(col)
                    }
                }
              print(missing_columns_r, "Here are the missing columns")
              val stage_df = avaliable_columns_r.foldLeft(manageShipInventoryDF)((df, c) => df.withColumn(s"$c", manageShipInventoryDF.col("ReserveInfoEx."+c)))
              var stage_df1 = missing_columns_r.foldLeft(stage_df)((df, c) => df.withColumn(s"$c", lit(null)))        
              val stage_df2 = stage_df1.withColumn("reserve_type_comments", lit(null))
              .withColumn("batchtime",lit(batch_start_tme).cast(TimestampType))              
              stage_df2.createOrReplaceTempView("temptable")  
              val sourceQuery = spark.sparkContext.getConf.get("spark.source.sql").trim
              val stage_final_df = spark.sql(sourceQuery)
              stage_df2.printSchema
              println("============================")
              stage_final_df.printSchema
              stage_final_df.show(10,false)
              println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxx-------------------------------Source-Stage voyage------------------------------------xxxxxxxxxxxxxxxxxxxxxx")
              println("Before SCD: " + LocalDateTime.now() )
              if (!stage_final_df.head(1).isEmpty) {
                loadDimFact(spark: SparkSession, stage_final_df)
                }
              println("After SCD: " + LocalDateTime.now() )
              log.info("after SCD")
            } 
          }
        return startingDF2
        }catch{
          case e: Exception =>
            {
              e.printStackTrace(); throw new Exception("General Exception..please check stacktrace ") 
              }
            }
        }
    ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
    }catch{
      case e: SQLException =>
        { 
          ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("******************in the catch of Res Addon Rel Load ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace", e); 
          }
        case e: Exception =>
          { 
            ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("******************in the catch of Res Addon Rel Load ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace", e); 
            }
          }
    spark.stop()    
  }
}