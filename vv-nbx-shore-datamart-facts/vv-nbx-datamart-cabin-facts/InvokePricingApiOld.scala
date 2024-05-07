package com.virginvoyages.invoke.api
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.immutable.Map
import scala.util.parsing.json._
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import scalaj.http.Http
import scalaj.http.HttpOptions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.xml.XML
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml.XmlReader
import scala.util.Try

object InvokePricingApiOld {

    
  
  val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    
  def main(args: Array[String]): Unit = {

    
  val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
    
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    
    
  
    var dfSailId = spark.sql(spark.sparkContext.getConf.get("spark.source.query").trim())
    
    
  dfSailId = dfSailId.rdd.map{t =>
    
       val  sail_id = t.getInt(0)
     val sail_date_from = t.getDate(2)
     val sail_date_to = t.getDate(3)
     val ship_code = t.getString(4)
    

    
   val sessionGuid = getSessionGuid(sparkConfiguration)
   val priceDetail  = findPriceDetail(sessionGuid,sail_id,sail_date_from,sail_date_to,ship_code,sparkConfiguration) 
   (priceDetail)
    
  }.toDF("xmlMessage");
    
    
    
   
    parsePriceDetail(dfSailId,sparkConfiguration)
    
    def getSessionGuid(configMap: Broadcast[Map[String, String]]): String = {
      log.info("Getting session guid")
      
      try{
        
        val username = configMap.value.get("spark.username").get
        val password = configMap.value.get("spark.password").get
        val api_url = configMap.value.get("spark.api.url").get
     
        val postData ="<Login_IN><Version>1.0</Version><UserInfo><ResAgent><Username>"+username+"</Username><Password>"+password+"</Password></ResAgent></UserInfo></Login_IN>"
         val response = Http(api_url).postData(postData)
           .header("Content-Type", "application/x-versonix-api")
             .header("Charset", "UTF-8")
              .option(HttpOptions.readTimeout(100000)).asString
               log.info("ResponseGiven " + response.body)
 
               val responseReceived = response.body
               val xml = XML.loadString(responseReceived)
               
               val sessionGuid = (xml \\ "MsgHeader" \ "SessionGUID").text
               log.info("SessionGuid " + sessionGuid)
                 return sessionGuid
      }
      catch{
        case e: Exception => { e.printStackTrace(); throw new Exception("General Exception..please check stacktrace ") } 
      }
  
    }
    
    def findPriceDetail(sessionGuid: String,sail_id: Int,sail_date_from : Date,sail_date_to: Date,ship_code: String,configMap: Broadcast[Map[String, String]]): String = {
      
      log.info("Getting Price Details")
      
      try{
        
        log.info(sail_id)
        log.info(sail_date_from)
        log.info(sail_date_to)
        log.info(ship_code)
        val api_url = configMap.value.get("spark.api.url").get
        var priceData ="<GetAvailPrimPackages_IN> <MsgHeader> <Version>1.0</Version> <SessionGUID>"+sessionGuid+"</SessionGUID> <Language>ENG</Language> </MsgHeader> <SearchParams> <PackageStartRange> <From>"+sail_date_from+"</From> <To>"+sail_date_to+"</To> </PackageStartRange> <PackageEndRange> <From>"+sail_date_from+"</From> <To>"+sail_date_to+"</To> </PackageEndRange> </SearchParams> <SearchOptions> <IncludeCategories>Y</IncludeCategories> <CalcPrices>Y</CalcPrices> <PriceDetails>Y</PriceDetails> </SearchOptions> <ResShell> <ResHeader> <ResStatus>BK</ResStatus> </ResHeader> <ResGuests> <ResGuest> <GuestSeqN>1</GuestSeqN> </ResGuest> <ResGuest> <GuestSeqN>2</GuestSeqN> </ResGuest> <ResGuest> <GuestSeqN>3</GuestSeqN> </ResGuest> <ResGuest> <GuestSeqN>4</GuestSeqN> </ResGuest> </ResGuests> </ResShell> </GetAvailPrimPackages_IN>"
         val response = Http(api_url).postData(priceData)
           .header("Content-Type", "application/x-versonix-api")
             .header("Charset", "UTF-8")
              .option(HttpOptions.readTimeout(100000)).asString
               log.info("ResponseGiven " + response.body)
               
               val responseReceived = response.body
               val xml = XML.loadString(responseReceived)
               
               val priceDetailXml = (xml \ "AvailPackages" \ "AvailPackage")
               var parsestring = priceDetailXml.toString()         
               log.info("ParseString" + parsestring)
               return parsestring
        
      }
      catch{
        case e: Exception => { e.printStackTrace(); throw new Exception("General Exception..please check stacktrace ") }
        
      }
      
      
    }
    
    
    def isEmpty(x: String) = x == null || x.trim.isEmpty
    
    def parsePriceDetail(parsestring: DataFrame ,configMap: Broadcast[Map[String, String]]): DataFrame = {
      
      try{
        
        log.info("Inside Df method")
        
        val pricingTable = configMap.value.get("spark.target.table").get
        var dfnoEmpty = parsestring.filter($"xmlMessage" =!= "")

           
               var xmlStringRDD = dfnoEmpty.select("xmlMessage").map(r => r.getString(0)).rdd
               var priceDetailDf = new XmlReader().xmlRdd(spark.sqlContext, xmlStringRDD)

               priceDetailDf = priceDetailDf.withColumn("SailID", priceDetailDf.col("Package.SailID"))
                               .withColumn("AvailCategories_new", priceDetailDf.col("AvailCategories.AvailCategory"))
               
               if (checkArray(priceDetailDf, "AvailCategories_new")) {
               
               priceDetailDf = priceDetailDf.withColumn("AvailCategory_explode", explode_outer(priceDetailDf.col("AvailCategories_new")))
               
               
               priceDetailDf = priceDetailDf.withColumn("PriceDetails", priceDetailDf.col("AvailCategory_explode.PriceDetails"))
               
               priceDetailDf = priceDetailDf.withColumn("Priceitem" , priceDetailDf.col("PriceDetails.PriceItem"))
               
               if (checkArray(priceDetailDf, "Priceitem")) {
               
               priceDetailDf = priceDetailDf.withColumn("Priceitem_explode" , explode_outer(priceDetailDf.col("Priceitem")))
               
               priceDetailDf = priceDetailDf.withColumn("PriceCode" , priceDetailDf.col("Priceitem_explode.Code"))
                 .withColumn("GuestSeq" , priceDetailDf.col("Priceitem_explode.GuestSeqN"))
                .withColumn("Amount" , priceDetailDf.col("Priceitem_explode.Amount"))
                .withColumn("Category" , priceDetailDf.col("AvailCategory_explode.Category"))
               }
               }
        
               else{
                 
                 priceDetailDf = priceDetailDf.withColumn("PriceCode", lit(null))
                                 .withColumn("GuestSeq", lit(null))
                                 .withColumn("Amount", lit(null))
                                 .withColumn("Category", lit(null))
                 
                 
               }
        
//               priceDetailDf = priceDetailDf.filter($"GuestSeq" === "1")
        
               priceDetailDf = priceDetailDf.dropDuplicates()
               
               
           val priceDetailDf1  =  priceDetailDf.select("SailID","PriceCode","GuestSeq","Amount","Category")
           val priceDetailDf_TaxsFees = priceDetailDf1.filter(trim($"PriceCode") === "TAXES & FEES" && $"GuestSeq" === "1")
           val priceDetailDf_NoTaxsFees = priceDetailDf1.filter(trim($"PriceCode") =!= "TAXES & FEES")
           val priceDetailfinalDf = priceDetailDf_NoTaxsFees.union(priceDetailDf_TaxsFees)
           priceDetailfinalDf.write.mode("overwrite").insertInto(pricingTable)
        
          return priceDetailDf
        
      }
      
      
      catch{
        
        
       case e: Exception => { e.printStackTrace(); throw new Exception("General Exception..please check stacktrace ") } 
      }
    } 
      
      
       def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
       
       
       def checkArray(df: DataFrame, colname: String): Boolean = {

        df.schema(colname).dataType match {
          case ArrayType(_, _) => return true
          case _               => return false
        }
      }
    
      
    
  }
                  
}