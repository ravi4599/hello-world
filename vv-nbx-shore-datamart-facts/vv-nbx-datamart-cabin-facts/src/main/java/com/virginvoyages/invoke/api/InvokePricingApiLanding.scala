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
import scala.collection.mutable.ListBuffer

case class Respons(
  currency: String,
  guestnum: String,
  sail_date_from: String,
  sail_date_to: String,
  priceDetail: String
)


object InvokePricingApiLanding {

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    def findPriceDetail(guest_num: String, sail_date_from: String, sail_date_to: String, currency: String, configMap: Broadcast[Map[String, String]]): String = {

      log.info("Getting Price Details")

      try {

        log.info("Guest Number " + guest_num)
        log.info("sail_date_from" + sail_date_from)
        log.info("sail_date_to" + sail_date_to)

        val api_url = configMap.value.get("spark.custapi.url").get
        var priceData = "<GetAvailPrimPkgsCustom_IN><MsgHeader><Version>1.0</Version><CallerInfo><UserInfo><Internal/></UserInfo></CallerInfo><Language>ENG</Language></MsgHeader><SearchOptions><IncludeComponents>Y</IncludeComponents><IncludeSailActivities>Y</IncludeSailActivities><IncludeClassifications>Y</IncludeClassifications><IncludePkgDef>Y</IncludePkgDef><IncludePriceDetails>Y</IncludePriceDetails><CacheSearchMode>BypassCache</CacheSearchMode></SearchOptions><CustomParams><Scenario>ONEWAY</Scenario><Param><Code>DATEFROM</Code><Value><Date>" + sail_date_from + "</Date></Value></Param>      <Param><Code>DATETO</Code><Value><Date>" + sail_date_to + "</Date></Value></Param><Param><Code>Currency</Code><Value><Str>" + currency + "</Str></Value></Param><Param><Code>ReserveType</Code><Value><Str/></Value></Param><Param><Code>NumAdults</Code><Value><Num>" + guest_num + "</Num></Value></Param><ResultModes><ResultMode>ALL</ResultMode></ResultModes></CustomParams></GetAvailPrimPkgsCustom_IN>"
        val response = Http(api_url).postData(priceData)
          .header("Content-Type", "application/x-versonix-api")
          .header("Charset", "UTF-8")
          .option(HttpOptions.readTimeout(100000)).asString
        log.info("ResponseGiven " + response.body)

        val responseReceived = response.body
        val xml = XML.loadString(responseReceived)

        val priceDetailXml = xml //(xml \ "AvailPackages" \ "AvailPackage")
        var parsestring = priceDetailXml.toString()
        log.info("ParseString" + parsestring)
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
    val pricingLandingTable = spark.sparkContext.getConf.get("spark.landing.table").trim()
    val curr_date = current_date()
    val no_of_months = spark.sparkContext.getConf.get("spark.maxcutoff.months").trim()
    val whereclause = s"where   date(sail_date_to)<= add_months(current_date(),$no_of_months)"
    
    val sail_date_to = spark.sql("select date(max(sail_date_to))as max_sail_date_to from ( " + spark.sparkContext.getConf.get("spark.maxdate.query").trim() + " " + whereclause + ")a").first().get(0).toString()
    
    //val sail_date_to = spark.sql("select date(max(sail_date_to))as max_sail_date_to from (Select sail_date_to from vv_db.hvtb_nbx_core_sw_sail_dim sail inner join vv_db.hvtb_nbx_core_sw_ship_dim shipd on ( shipd.ship !='VC' and shipd.ship !='XE' and sail.ship_id == shipd.ship_id)  " + whereclause + ")a").first().get(0).toString()
    
    println(spark.sparkContext.getConf.get("spark.maxdate.query").trim() + " " + whereclause)
    //val sail_date_to = spark.sql("")
    val sail_date_from = spark.sql("select current_date()").first().get(0).toString()

    val NumAdults = spark.sparkContext.getConf.get("spark.numadults.list").trim()
    val NumAdultsArray = NumAdults.split(",")
    val NumAdultsRDD = spark.sparkContext.parallelize(NumAdultsArray)

    val Currency = spark.sparkContext.getConf.get("spark.currency.list").trim()
    val CurrencyArray = Currency.split(",")
    val CurrencyRDD = spark.sparkContext.parallelize(CurrencyArray)

    log.info("#--------------------------Pricing API-------------------#")
    import spark.implicits._

    val NumAdultsDF = NumAdultsRDD.map { t =>
      val guest_num = t
      (t, 1)

    }.toDF("GuestNum", "num");
    //NumAdultsDF.show(false)
    val CurrencyDF = CurrencyRDD.map { t =>
      val guest_num = t
      (t, 1)

    }.toDF("Currency", "cno");
    //CurrencyDF.show(false)
    val currencyNumAdultDF = CurrencyDF.join(NumAdultsDF, col("cno") === col("num"), "inner") //.drop("cno")
      //.withColumn("sail_date_from",lit( sail_date_from))
      .selectExpr("Currency", "GuestNum", "cno")
    println("#-------------------------------API is about to requested for response -----------------------#")
    val noofRecords = spark.sparkContext.getConf.get("spark.noofRecords").trim()
    val DF = sc.parallelize(
      Seq.fill(noofRecords.toInt) { (1) }).toDF("key").withColumn("some_date", current_date())
    val w = Window.orderBy(DF("some_date") desc)
	val noofDays = spark.sparkContext.getConf.get("spark.noofDays").trim()
    val seqnoDF = DF.withColumn("row_number", ((row_number().over(w))*noofDays))
    seqnoDF.createOrReplaceTempView("seqnoDF")
    val interimseqDF = spark.sql("select *, date_add(current_date(),row_number) as sail_date_to from seqnoDF")
    interimseqDF.createOrReplaceTempView("interimseqDF")
    println(s"""select * from(select *, lead(sail_date_from,1,current_date()) over (partition by key order by sail_date_from desc ) as  sail_date_to from interimseqDF)a where date(sail_date_to)<=date('""" + sail_date_to.toString() + """') """) //.select("key", "sail_date_from","sail_date_to")
    
    /*val fulldtDF = spark.sql(s"""select * from(select *, lead(sail_date_to,1,current_date()) over (partition by key order by sail_date_to desc ) as  sail_date_from from interimseqDF)a where date(sail_date_to)<=date('""" + sail_date_to.toString() + """')""").select("key", "sail_date_from", "sail_date_to")*/
    //println("fulldtDF count "+fulldtDF.count())
    
    /*Modified fulldtDF query to resolve prod issue*/
    val fulldtDF = spark.sql(s"""select key,sail_date_from,date_add(sail_date_to,-1) as sail_date_to, sail_date_to as sail_date_to_original from(select *, lead(sail_date_to,1,current_date()) over (partition by key order by sail_date_to desc ) as  sail_date_from from interimseqDF)a where date(sail_date_to)<=last_day(date('""" + sail_date_to.toString() + """'))""").select("key", "sail_date_from", "sail_date_to")
    
    val monthcurrencyNumAdultDF = currencyNumAdultDF.join(fulldtDF, col("cno") === col("key"), "inner").drop("cno").drop("key")
    //println("monthcurrencyNumAdultDF count "+monthcurrencyNumAdultDF.count())
    //monthcurrencyNumAdultDF.show(50,false)
    //sys.exit(1)
    
    
  val results = new ListBuffer[Respons]()
    
  for (row <- monthcurrencyNumAdultDF.rdd.collect)
  {   
    val currency = row.mkString(",").split(",")(0)
    val guestnum = row.mkString(",").split(",")(1)
    val sail_date_from = row.mkString(",").split(",")(2) 
    val sail_date_to = row.mkString(",").split(",")(3)
    val priceDetail = findPriceDetail(guestnum, sail_date_from, sail_date_to, currency, sparkConfiguration)
    
    val currentResult = Respons (
    currency = currency,
    guestnum = guestnum,
    sail_date_from = sail_date_from,
    sail_date_to= sail_date_to,
    priceDetail = priceDetail)
    results += currentResult
  }
    val responseDF = spark.createDataFrame(results).withColumn("load_dt", current_date())
    
   /* var responseArray = Array[String]()
    
   for (row <- monthcurrencyNumAdultDF.rdd.collect)
  {   
    val currency = row.mkString(",").split(",")(0)
    val guestnum = row.mkString(",").split(",")(1)
    val sail_date_from = row.mkString(",").split(",")(2) 
    val sail_date_to = row.mkString(",").split(",")(3)
    val priceDetail = findPriceDetail(guestnum, sail_date_from, sail_date_to, currency, sparkConfiguration)
    
    responseArray = responseArray :+row.mkString(",")+","+priceDetail

  } 
    val responseRDD = spark.sparkContext.parallelize(responseArray)
    
    val responseDF = responseRDD.map { t =>
      val Currency = t.split(",")(0)
      val guestnum = t.split(",")(1)
      val sail_date_from = t.split(",")(2)
      val sail_date_to = t.split(",")(3)
      val response = t.split(",")(4) 
      (Currency,guestnum,sail_date_from,sail_date_to,response)

    }.toDF("Currency","guestnum","sail_date_from","sail_date_to","response").withColumn("load_dt", current_date())*/
    
    
     /* Old Code: Have Serialization Issue : 
    val responseDF = monthcurrencyNumAdultDF.rdd.map { t =>

      val currency = t.getString(0)
      val guestnum = t.getString(1)
      val sail_date_from = t.getDate(2)
      val sail_date_to = t.getDate(3)
      val priceDetail = findPriceDetail(guestnum, sail_date_from, sail_date_to, currency, sparkConfiguration)
      (currency, guestnum, sail_date_from, sail_date_to, priceDetail)
    }.toDF("currency", "guestnum", "sail_date_from", "sail_date_to", "response").withColumn("load_dt", current_date())
    //responseDF.show(false)
    //responseDF.printSchema()
    //responseDF.createOrReplaceTempView("response")
    //val finalDF=spark.sql("select * from response  where response like '%<SailID>1494</SailID>%'")*/
    responseDF.write.mode("Overwrite").insertInto(pricingLandingTable)

   // responseDF.write.mode("Overwrite").parquet("gs://vv-prod-nbx-cluster/data/temp/cabinapi/pricingApi/apiData/")
    
    spark.stop();

  }

}