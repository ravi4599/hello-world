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
import java.io.Serializable;




object InvokeNetRatePricingApiLanding extends Serializable{

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    def findPriceDetail(guest_num: String, sail_date_from: String, sail_date_to: String, currency: String, configMap: Broadcast[Map[String, String]]): String = {

      log.info("Getting Price Details")
      println("*** Printing Logs")
      try {

        log.info("Guest Number: " + guest_num)
        log.info("sail_date_from :" + sail_date_from)
        log.info("sail_date_to :" + sail_date_to)
        log.info("currency : " + currency)
        println("*** Printing Logs***************")
        val api_url = configMap.value.get("spark.custapi.url").get //(Existing)

        var priceData = s"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<vx:OTA_CruiseSailAvailRQ xmlns:ns3="http://www.versonix.com/ota" xmlns:vx="http://www.opentravel.org/OTA/2003/05" xmlns:ns4="http://schemas.xmlsoap.org/soap/envelope/" Version="1.0">
    <vx:POS>
        <vx:Source>
            <vx:RequestorID Type="24" ID_Context="SEAWARE" ID="$currency"/> <!-- Net Rates GBP -->
            <!-- <vx:RequestorID Type="5" ID_Context="SEAWARE" ID="141"/> Web Rates GBP -->
          <!--  <vx:RequestorID Type="24" ID_Context="SEAWARE" ID="$currency"/>   Net Rates AUD -->
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
        println("prining date and response--" + priceData)
        val response = Http(api_url).postData(priceData)
          .header("Content-Type", "application/xml")
          .header("Charset", "UTF-8")
          .option(HttpOptions.readTimeout(100000)).asString
        log.info("ResponseGiven " + response.body)
        //println("Response From API :" + response.code)
        //println("The Response is:-" + response)

        val responseReceived = response.body
        val xml = XML.loadString(responseReceived)

        val priceDetailXml = xml //(xml \ "AvailPackages" \ "AvailPackage")
        var parsestring = priceDetailXml.toString()
        //print("printing xml string" + parsestring)
        //log.info("ParseString" + parsestring)
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
    print(pricingTable)
    val pricingLandingTable = spark.sparkContext.getConf.get("spark.landing.table").trim()
    print(pricingLandingTable)
    val curr_date = current_date()
    print(curr_date)
    val no_of_months = spark.sparkContext.getConf.get("spark.maxcutoff.months").trim()
    print(no_of_months)
    val whereclause = s"where   date(sail_date_to)<= add_months(current_date(),$no_of_months)"
    print("printing where clause" + whereclause)

    val sail_date_to = spark.sql("select date(max(sail_date_to))as max_sail_date_to from ( " + spark.sparkContext.getConf.get("spark.maxdate.query").trim() + " " + whereclause + ")a").first().get(0).toString()
    println("**********printing sail_date_to********" + sail_date_to)
    //val sail_date_to = spark.sql("select date(max(sail_date_to))as max_sail_date_to from (Select sail_date_to from vv_db.hvtb_nbx_core_sw_sail_dim sail inner join vv_db.hvtb_nbx_core_sw_ship_dim shipd on ( shipd.ship !='VC' and shipd.ship !='XE' and sail.ship_id == shipd.ship_id)  " + whereclause + ")a").first().get(0).toString()

    println(spark.sparkContext.getConf.get("spark.maxdate.query").trim() + " " + whereclause)
    //val sail_date_to = spark.sql("")
    val sail_date_from = spark.sql("select current_date()").first().get(0).toString()
    println("printing sail_date_from " + sail_date_from)

    val NumAdults = spark.sparkContext.getConf.get("spark.numadults.list").trim()
    println("printing NumAdults" + NumAdults)
    val NumAdultsArray = NumAdults.split(",")
    val NumAdultsRDD = spark.sparkContext.parallelize(NumAdultsArray)
    println("printing NumAdultsRDD" + NumAdultsRDD)

    val Currency = spark.sparkContext.getConf.get("spark.currency.list").trim()
    val CurrencyArray = Currency.split(",")
    println(" printing CurrencyArray" + CurrencyArray)
    val CurrencyRDD = spark.sparkContext.parallelize(CurrencyArray)
    val CurrencyValue = spark.sparkContext.getConf.get("spark.currency.value").trim()
    val CurrencyValueArray = CurrencyValue.split(",")
    val currValuMap = (CurrencyArray zip CurrencyValueArray)(breakOut)
    //val currValuMap1 = currValuMap.toMap
    //val currValuMap2 = currValuMap1.values
    //val currValuMap3=spark.sparkContext.parallelize(currValuMap2)

    log.info("#--------------------------Pricing API-------------------#")
    import spark.implicits._
    
    val NumAdultsDF = NumAdultsRDD.map { t =>
      val guest_num = t
      (t, 1)
  
    }.toDF("GuestNum", "num");
    NumAdultsDF.show(false)
    
    val CurrencyDF = currValuMap.map { t =>
      val guest_num = t._2
      (guest_num, 1)

    }.toDF("Currency", "cno");
    CurrencyDF.show(false)
     
    val currencyNumAdultDF = CurrencyDF.join(NumAdultsDF, col("cno") === col("num"), "inner") //.drop("cno")
      //.withColumn("sail_date_from",lit( sail_date_from))
      .selectExpr("Currency", "GuestNum", "cno")

    currencyNumAdultDF.show(false)
    println("#-------------------------------API is about to requested for response -----------------------#")
    val noofRecords = spark.sparkContext.getConf.get("spark.noofRecords").trim()
    val DF = sc.parallelize(
      Seq.fill(noofRecords.toInt) { (1) }).toDF("key").withColumn("some_date", current_date())
    val w = Window.orderBy(DF("some_date") desc)
    val noofDays = spark.sparkContext.getConf.get("spark.noofDays").trim()
    val seqnoDF = DF.withColumn("row_number", ((row_number().over(w)) * noofDays))
    println("printing seqnoDF")
    seqnoDF.show(false)
    seqnoDF.createOrReplaceTempView("seqnoDF")
    val interimseqDF = spark.sql("select *, date_add(current_date(),row_number) as sail_date_to from seqnoDF")
    println("printing interimseqDF")
    interimseqDF.show(false)
    interimseqDF.createOrReplaceTempView("interimseqDF")
    println(s"""select * from(select *, lead(sail_date_from,1,current_date()) over (partition by key order by sail_date_from desc ) as  sail_date_to from interimseqDF)a where date(sail_date_to)<=date('""" + sail_date_to.toString() + """') """) //.select("key", "sail_date_from","sail_date_to")

    /*val fulldtDF = spark.sql(s"""select * from(select *, lead(sail_date_to,1,current_date()) over (partition by key order by sail_date_to desc ) as  sail_date_from from interimseqDF)a where date(sail_date_to)<=date('""" + sail_date_to.toString() + """')""").select("key", "sail_date_from", "sail_date_to")*/
    //println("fulldtDF count "+fulldtDF.count())

    /*Modified fulldtDF query to resolve prod issue*/
    val fulldtDF = spark.sql(s"""select key,sail_date_from,date_add(sail_date_to,-1) as sail_date_to, sail_date_to as sail_date_to_original from(select *, lead(sail_date_to,1,current_date()) over (partition by key order by sail_date_to desc ) as  sail_date_from from interimseqDF)a where date(sail_date_to)<=last_day(date('""" + sail_date_to.toString() + """'))""").select("key", "sail_date_from", "sail_date_to")
    println("printing fulldtDF")
    fulldtDF.show(false)
    val monthcurrencyNumAdultDF = currencyNumAdultDF.join(fulldtDF, col("cno") === col("key"), "inner").drop("cno").drop("key")
    //println("monthcurrencyNumAdultDF count "+monthcurrencyNumAdultDF.count())
    //monthcurrencyNumAdultDF.show(50,false)
    //sys.exit(1)
    monthcurrencyNumAdultDF.show()
    val CurrencyNameDF = currValuMap.map { t =>
      val guest_num = t._1
      val guest_num1 = t._2
      (t._1, t._2)
    }.toDF("currency_name", "currency_code");

    CurrencyNameDF.show(false)
  
  var responseArray = Array[String]()
    
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
    //println(responseRDD.take(20).foreach(println)) //-- To display the RDD
    
    /*for (t <- responseRDD.collect)
    {
      println(t.split(",")(4))
    } -- To display each line
    */ 
   val responseDF = responseRDD.map { t =>
      val Currency = t.split(",")(0)
      val guestnum = t.split(",")(1)
      val sail_date_from = t.split(",")(2)
      val sail_date_to = t.split(",")(3)
      val response = t.split(",")(4) 
      (Currency,guestnum,sail_date_from,sail_date_to,response)

    }.toDF("Currency","guestnum","sail_date_from","sail_date_to","response").withColumn("load_dt", current_date())
    
     /* Old Code: Have Serialization Issue : 
  
  val responseDF = monthcurrencyNumAdultDF.rdd.map { t =>
      
     
     
      val currency = t.getString(0)
      val guestnum = t.getString(1)
      val sail_date_from = t.getDate(2)
      val sail_date_to = t.getDate(3)
      println("*** Before Calling Function******")
      val priceDetail = findPriceDetail(guestnum, sail_date_from, sail_date_to, currency, sparkConfiguration)
      (currency, guestnum, sail_date_from, sail_date_to, priceDetail)
      

    }.toDF("currency", "guestnum", "sail_date_from", "sail_date_to", "response").withColumn("load_dt", current_date())
    
      // val finalDF1 = currValuMap.foldLeft(priceDetail){ case (acc, (k, v)) => acc.withColumn("currency_name",when(col("currency") === lit(k), v)) }
     */
    
    println("printing  responseDF ******")
    println(responseDF.show())

    val responseDFFinal = responseDF.join(CurrencyNameDF, col("currency_code") === col("currency"), "inner").select(col("currency_name"), col("guestnum"), col("sail_date_from"), col("sail_date_to"), col("response"), col("load_dt"))
    val responseDFFinal2 = responseDFFinal.withColumnRenamed("currency_name", "currency")
    //responseDFFinal2.show(10, false)
    //responseDF.write.parquet("s3://vv-dev-emr-cluster/data/test_vivek_landing/")

    //responseDFFinal2.printSchema()

    responseDFFinal2.write.mode("Overwrite").insertInto(pricingLandingTable)

    spark.stop();

  }
}
