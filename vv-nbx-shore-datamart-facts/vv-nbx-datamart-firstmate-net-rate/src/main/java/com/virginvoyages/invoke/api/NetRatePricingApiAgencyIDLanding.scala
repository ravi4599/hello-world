package com.virginvoyages.invoke.api
import java.text.SimpleDateFormat
import org.apache.spark.sql.expressions.Window
import java.util.Date
import org.apache.spark.storage.StorageLevel
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
import scala.collection.breakOut
import java.time._
import java.io.Serializable;
import scala.collection.mutable.ListBuffer

case class Response(
  agency: String,
  guestnum: String,
  sail_date_from: String,
  sail_date_to: String,
  priceDetail: String
)

object NetRatePricingApiAgencyIDLanding extends Serializable{

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    def findPriceDetail(guest_num: String, sail_date_from: String, sail_date_to: String, agency: String, configMap: Broadcast[Map[String, String]]): String = {

      //log.info("Getting Price Details")
      //println("*** Printing Logs")
      try {

       /* log.info("Guest Number " + guest_num)
        log.info("sail_date_from" + sail_date_from)
        log.info("sail_date_to" + sail_date_to)
        log.info("currency" + agency) */
        //println("*** Printing Logs***************")
        val api_url = configMap.value.get("spark.custapi.url").get //(Existing)

        var priceData = s"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<vx:OTA_CruiseSailAvailRQ xmlns:ns3="http://www.versonix.com/ota" xmlns:vx="http://www.opentravel.org/OTA/2003/05" xmlns:ns4="http://schemas.xmlsoap.org/soap/envelope/" Version="1.0">
    <vx:POS>
        <vx:Source>
            <vx:RequestorID Type="5" ID_Context="SEAWARE" ID="$agency"/> <!-- Net Rates GBP -->
            <!-- <vx:RequestorID Type="5" ID_Context="SEAWARE" ID="141"/> Web Rates GBP -->
          <!--  <vx:RequestorID Type="24" ID_Context="SEAWARE" ID="7770"/>   Net Rates AUD -->
            <!-- <vx:RequestorID Type="5" ID_Context="SEAWARE" ID="7770"/> Web Rates AUD -->
            <vx:BookingChannel Type="1">
                <vx:CompanyName>OPENTRAVEL</vx:CompanyName>
            </vx:BookingChannel>
        </vx:Source>
    </vx:POS>
    <vx:GuestCounts>
        <vx:GuestCount Quantity="$guest_num" Code="10"/>
    </vx:GuestCounts>
    <vx:SailingDateRange>
        <vx:StartDateWindow EarliestDate="$sail_date_from" LatestDate="$sail_date_to"/>
    </vx:SailingDateRange>
    <vx:TPA_Extensions UseCacheInd="false" IncludeCategoriesInd="true"/>
</vx:OTA_CruiseSailAvailRQ>"""
        //println("prining date and response--" + priceData)
        val response = Http(api_url).postData(priceData)
          .header("Content-Type", "application/xml")
          .header("Charset", "UTF-8")
          .option(HttpOptions.readTimeout(100000)).asString
       // log.info("ResponseGiven " + response.body)
        //println("The Response is:-" + response)

        val responseReceived = response.body
        val xml = XML.loadString(responseReceived)

        val priceDetailXml = xml //(xml \ "AvailPackages" \ "AvailPackage")
        var parsestring = priceDetailXml.toString()
       // print("printing xml string" + parsestring)
      //  log.info("ParseString" + parsestring)
        return (parsestring)

      } catch {
        case e: Exception => { e.printStackTrace(); throw new Exception("General Exception..please check stacktrace ") }

      }

    }
    def checkArray(df: DataFrame, colname: String): Boolean = {

      df.schema(colname).dataType match {
        case ArrayType(_, _) => return true
        case _               => return false
      }
    }

    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    val pricingTable = spark.sparkContext.getConf.get("spark.target.table").trim()
    //println(pricingTable)
    val pricingLandingTable = spark.sparkContext.getConf.get("spark.landing.table").trim()
    // println(pricingLandingTable)
    val curr_date = current_date()
    println(curr_date)
    val no_of_months = spark.sparkContext.getConf.get("spark.maxcutoff.months").trim()
    //println(no_of_months)
    val whereclause = s"where   date(sail_date_to)<= add_months(current_date(),$no_of_months)"
    //print("printing where clause" + whereclause)

    val sail_date_to = spark.sql("select date(max(sail_date_to))as max_sail_date_to from ( " + spark.sparkContext.getConf.get("spark.maxdate.query").trim() + " " + whereclause + ")a").first().get(0).toString()
    //println("**********printing sail_date_to********" + sail_date_to)

    println(spark.sparkContext.getConf.get("spark.maxdate.query").trim() + " " + whereclause)

    val sail_date_from = spark.sql("select current_date()").first().get(0).toString()

    //println("printing sail_date_from " + sail_date_from)

    val NumAdults = spark.sparkContext.getConf.get("spark.numadults.list").trim()
    println("printing NumAdults" + NumAdults)
    val NumAdultsArray = NumAdults.split(",")
    val NumAdultsRDD = spark.sparkContext.parallelize(NumAdultsArray)
    //println("printing NumAdultsRDD" + NumAdultsRDD)

    val Agency = spark.sparkContext.getConf.get("spark.agency.list").trim()
    val AgencyArray = Agency.split(",")
    println(" printing AgencyArray" + AgencyArray)
    val AgencyRDD = spark.sparkContext.parallelize(AgencyArray)

    log.info("#--------------------------Pricing API-------------------#")
    import spark.implicits._

    val NumAdultsDF = NumAdultsRDD.map { t =>
      val guest_num = t
      (t, 1)
    
    }.toDF("GuestNum", "num");
    //NumAdultsDF.show(false)

    val AgencyDF = AgencyRDD.map { t =>
      val guest_num = t
      (t, 1)
      

    }.toDF("Agency", "cno");
    val AgencyNumAdultDF = AgencyDF.join(NumAdultsDF, col("cno") === col("num"), "inner")

      .selectExpr("Agency", "GuestNum", "cno")

    AgencyNumAdultDF.show(2,false)
    println("#-------------------------------API is about to requested for response -----------------------#")
    val noofRecords = spark.sparkContext.getConf.get("spark.noofRecords").trim()
    val DF = sc.parallelize(
      Seq.fill(noofRecords.toInt) { (1) }).toDF("key").withColumn("some_date", current_date())
    val w = Window.orderBy(DF("some_date") desc)
    val noofDays = spark.sparkContext.getConf.get("spark.noofDays").trim()
    val seqnoDF = DF.withColumn("row_number", ((row_number().over(w)) * noofDays))
    println("printing seqnoDF")
    //seqnoDF.show(false)
    seqnoDF.createOrReplaceTempView("seqnoDF")
    val interimseqDF = spark.sql("select *, date_add(current_date(),row_number) as sail_date_to from seqnoDF")
    println("printing interimseqDF")
    //interimseqDF.show(false)
    interimseqDF.createOrReplaceTempView("interimseqDF")
    println(s"""select * from(select *, lead(sail_date_from,1,current_date()) over (partition by key order by sail_date_from desc ) as  sail_date_to from interimseqDF)a where date(sail_date_to)<=date('""" + sail_date_to.toString() + """') """) //.select("key", "sail_date_from","sail_date_to")

    val fulldtDF = spark.sql(s"""select key,sail_date_from,date_add(sail_date_to,-1) as sail_date_to, sail_date_to as sail_date_to_original from(select *, lead(sail_date_to,1,current_date()) over (partition by key order by sail_date_to desc ) as  sail_date_from from interimseqDF)a where date(sail_date_to)<=last_day(date('""" + sail_date_to.toString() + """'))""").select("key", "sail_date_from", "sail_date_to")
    println("printing fulldtDF")
   // fulldtDF.show(2,false)
    val monthAgencyNumAdultDF = AgencyNumAdultDF.join(fulldtDF, col("cno") === col("key"), "inner").drop("cno").drop("key")
    println("monthAgencyNumAdultDF ....."+ monthAgencyNumAdultDF.count)
/*
    val responseDF = monthAgencyNumAdultDF.rdd.map { t =>

      val agency = t.getString(0)
      val guestnum = t.getString(1)
      val sail_date_from = t.getDate(2)
      val sail_date_to = t.getDate(3)  
      println("*** Before Calling Function******")
      * 
     
      var responseArray = Array[String]()
      for (row <- monthAgencyNumAdultDF.rdd.collect)
  {   
    val agency = row.mkString(",").split(",")(0)
    val guestnum = row.mkString(",").split(",")(1)
    val sail_date_from = row.mkString(",").split(",")(2) 
    val sail_date_to = row.mkString(",").split(",")(3)
    val priceDetail = findPriceDetail(guestnum,sail_date_from, sail_date_to, agency, sparkConfiguration)
    
    responseArray = responseArray :+row.mkString(",")+","+priceDetail

  } 
    val responseRDD = spark.sparkContext.parallelize(responseArray)
    val responseDF = responseRDD.map { t =>
      val agency = t.split(",")(0)
      val guestnum = t.split(",")(1)
      val sail_date_from = t.split(",")(2)
      val sail_date_to = t.split(",")(3)
      val response = t.split(",")(4) 
      (agency,guestnum,sail_date_from,sail_date_to,response)

    }.toDF("agency", "guestnum", "sail_date_from", "sail_date_to", "response").withColumn("load_dt", current_date())
    * 
    */
    val results = new ListBuffer[Response]()
    
  for (row <- monthAgencyNumAdultDF.rdd.collect)
  {   
    val agency = row.mkString(",").split(",")(0)
    val guestnum = row.mkString(",").split(",")(1)
    val sail_date_from = row.mkString(",").split(",")(2) 
    val sail_date_to = row.mkString(",").split(",")(3)
    val priceDetail = findPriceDetail(guestnum, sail_date_from, sail_date_to, agency, sparkConfiguration)
    
    val currentResult = Response (
    agency = agency,
    guestnum = guestnum,
    sail_date_from = sail_date_from,
    sail_date_to= sail_date_to,
    priceDetail = priceDetail)
    results += currentResult
  }
    val responseDF = spark.createDataFrame(results).withColumn("load_dt", current_date())
    println("printing  responseDF ******")
    println(responseDF.show(2,false))

    //val responseDFFinal = responseDF.join(AgencyDF, col("Agency") === col("cno"), "inner").select(col("Agency"), col("guestnum"), col("sail_date_from"), col("sail_date_to"), col("response"), col("load_dt"))
    //val responseDFFinal2 = responseDFFinal.withColumnRenamed("Agency", "agency")
    println("printing  responseDF ******")

    responseDF.write.mode("Overwrite").insertInto(pricingLandingTable)

    println("**** Data Refreshed ***")

    spark.stop();

  }

}