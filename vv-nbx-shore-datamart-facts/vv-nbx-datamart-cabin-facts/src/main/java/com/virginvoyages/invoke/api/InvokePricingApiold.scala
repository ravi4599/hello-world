package com.virginvoyages.invoke.api
import java.text.SimpleDateFormat
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

object InvokePricingApiold {

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    def findPriceDetail(guest_num: String, sail_date_from: Date, sail_date_to: Date, currency: String, configMap: Broadcast[Map[String, String]]):String = {

      log.info("Getting Price Details")

      try {

        log.info("Guest Number " + guest_num)
        log.info("sail_date_from" + sail_date_from)
        log.info("sail_date_to" + sail_date_to)

        val api_url = configMap.value.get("spark.custapi.url").get
        var priceData = "<GetAvailPrimPkgsCustom_IN><MsgHeader><Version>1.0</Version><CallerInfo><UserInfo><Internal/></UserInfo></CallerInfo><Language>ENG</Language></MsgHeader><SearchOptions><IncludeComponents>Y</IncludeComponents><IncludeSailActivities>Y</IncludeSailActivities><IncludeClassifications>Y</IncludeClassifications><IncludePkgDef>Y</IncludePkgDef><IncludePriceDetails>N</IncludePriceDetails><CacheSearchMode>PopulateCache</CacheSearchMode></SearchOptions><CustomParams><Scenario>ONEWAY</Scenario><Param><Code>DATEFROM</Code><Value><Date>" + sail_date_from + "</Date></Value></Param>      <Param><Code>DATETO</Code><Value><Date>" + sail_date_to + "</Date></Value></Param><Param><Code>Currency</Code><Value><Str>" + currency + "</Str></Value></Param><Param><Code>ReserveType</Code><Value><Str/></Value></Param><Param><Code>NumAdults</Code><Value><Num>" + guest_num + "</Num></Value></Param><ResultModes><ResultMode>ALL</ResultMode></ResultModes></CustomParams></GetAvailPrimPkgsCustom_IN>"
        val response = Http(api_url).postData(priceData)
          .header("Content-Type", "application/x-versonix-api")
          .header("Charset", "UTF-8")
          .option(HttpOptions.readTimeout(100000)).asString
        log.info("ResponseGiven " + response.body)

        val responseReceived = response.body
        val xml = XML.loadString(responseReceived)

        val priceDetailXml = xml//(xml \ "AvailPackages" \ "AvailPackage")
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

    def parsePriceDetail(responseDF: DataFrame, configMap: Broadcast[Map[String, String]]): DataFrame = {

      log.info("Inside Df method")

      val pricingTable = configMap.value.get("spark.target.table").get
      var dfnoEmpty = responseDF.filter(col("response") =!= "")

      import spark.implicits._
      var xmlStringRDD = dfnoEmpty.select("response").map(r => r.getString(0)).rdd
      var priceDetailDf = new XmlReader().xmlRdd(spark.sqlContext, xmlStringRDD)

      priceDetailDf = priceDetailDf//.withColumn("src_sail_id", priceDetailDf.col("Package.SailID"))
        .withColumn("AvailPackage_new", priceDetailDf.col("AvailPackages.AvailPackage"))

      if (checkArray(priceDetailDf, "AvailPackage_new")) {
       priceDetailDf = priceDetailDf
        .withColumn("AvailPackage_explode", explode_outer(priceDetailDf.col("AvailPackage_new")))
        //.withColumn("AvailPackage_explode",priceDetailDf.col("AvailPackage_new"))
        priceDetailDf = priceDetailDf
        .withColumn("src_sail_id", priceDetailDf.col("AvailPackage_explode.Package.SailID"))
        priceDetailDf = priceDetailDf.withColumn("AvailCategories_new", priceDetailDf.col("AvailPackage_explode.AvailCategories.AvailCategory"))
  
      }
      else{
        priceDetailDf = priceDetailDf.withColumn("src_sail_id", lit(null))
      }
      
      
      
      
      
      if (checkArray(priceDetailDf, "AvailCategories_new")) {

        priceDetailDf = priceDetailDf.withColumn("AvailCategory_explode", explode_outer(priceDetailDf.col("AvailCategories_new")))

        priceDetailDf = priceDetailDf.withColumn("currency", priceDetailDf.col("AvailCategory_explode.Currency"))
        priceDetailDf = priceDetailDf.withColumn("category", priceDetailDf.col("AvailCategory_explode.Category"))
        priceDetailDf = priceDetailDf.withColumn("maxavailablecapacity", priceDetailDf.col("AvailCategory_explode.MaxAvailableCapacity"))
        priceDetailDf = priceDetailDf.withColumn("capacity", priceDetailDf.col("AvailCategory_explode.Capacity"))
        priceDetailDf = priceDetailDf.withColumn("price_totals", priceDetailDf.col("AvailCategory_explode.PriceTotals.PriceTotal"))
        priceDetailDf = priceDetailDf.withColumn("items_explode", explode_outer(priceDetailDf.col("AvailCategory_explode.PriceTotals.DynamicTotals.Item")))
        priceDetailDf = priceDetailDf.withColumn("price_code", priceDetailDf.col("items_explode.code"))
        priceDetailDf = priceDetailDf.withColumn("amount", priceDetailDf.col("items_explode.total"))
        priceDetailDf = priceDetailDf.withColumn("guest_explode", priceDetailDf.col("AvailCategory_explode.PriceTotals.GuestTotal.GuestSeqN"))
        //
        if (checkArray(priceDetailDf, "guest_explode")) {
          priceDetailDf = priceDetailDf.withColumn("guestseq", explode_outer(priceDetailDf.col("guest_explode")))
          //priceDetailDf = priceDetailDf.withColumn("guest_seq_no", priceDetailDf.col("guest_explode.GuestSeqN"))
        } else {
          priceDetailDf = priceDetailDf.withColumn("guestseq", priceDetailDf.col("guest_explode"))
        }

      } else {

        priceDetailDf = priceDetailDf
          .withColumn("currency", lit(null))
          .withColumn("price_totals", lit(null))
          .withColumn("price_code", lit(null))
          .withColumn("amount", lit(null))
          .withColumn("guestseq", lit(null))
          .withColumn("maxavailablecapacity", lit(null))
          .withColumn("capacity", lit(null))
          .withColumn("category", lit(null))

      }

      priceDetailDf = priceDetailDf.dropDuplicates()

      return priceDetailDf
        .select("src_sail_id", "currency", "price_totals", "price_code", "amount", "guestseq", "maxavailablecapacity", "capacity", "category")

    }
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    val pricingTable = spark.sparkContext.getConf.get("spark.target.table").trim()
    val curr_date = current_date()
    val no_of_months = spark.sparkContext.getConf.get("spark.maxcutoff.months").trim()
    val whereclause = s"where   date(sail_date_to)<= add_months(current_date(),$no_of_months)"
    val sail_date_to = spark.sql(spark.sparkContext.getConf.get("spark.maxdate.query").trim() + " " + whereclause).first().get(0).toString()
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
      (t, sail_date_to.toString(), sail_date_from.toString(), 1)

    }.toDF("GuestNum", "sail_date_to", "sail_date_from", "num");
    //NumAdultsDF.show(false)
    val CurrencyDF = CurrencyRDD.map { t =>
      val guest_num = t
      (t, 1)

    }.toDF("Currency", "cno");
    //CurrencyDF.show(false)
    val currencyNumAdultDF = CurrencyDF.join(NumAdultsDF, col("cno") === col("num"), "inner").drop("cno").drop("num")
      //.withColumn("sail_date_from",lit( sail_date_from))
      .selectExpr("Currency", "GuestNum", "date(sail_date_to) as sail_date_to", "date(sail_date_from) as sail_date_from")
    println("#-------------------------------API is about to requested for response -----------------------#")
    val responseDF = currencyNumAdultDF.rdd.map { t =>

      val currency = t.getString(0)
      val guestnum = t.getString(1)
      val sail_date_from = t.getDate(3)
      val sail_date_to = t.getDate(2)
      val priceDetail = findPriceDetail(guestnum, sail_date_from, sail_date_to, currency, sparkConfiguration)
      (currency, guestnum, sail_date_from, sail_date_to, priceDetail)
    }.toDF("currency", "guestnum", "sail_date_from", "sail_date_to", "response")
    //responseDF.show(false)
    responseDF.persist(StorageLevel.MEMORY_AND_DISK)
    val parseDF = parsePriceDetail(responseDF, sparkConfiguration)
    //parseDF.printSchema()
    //parseDF.createOrReplaceTempView("pricing")
    //spark.sql(s"""select * from pricing  where src_sail_id=1605""").show(false)
    //parseDF.show(5,false)
    //sys.exit(1)
    //parseDF.createOrReplaceTempView("pricing")
    val parseFilterDF = parseDF//.filter($"guestseq" === $"maxavailablecapacity")
      .withColumn("load_dt", current_date())
    println("#-------------------------------API output parsed and ready to be ingested-----------------------#")
    parseFilterDF.createOrReplaceTempView("pricing")
    
    val tempNewDF = spark.sql(s""" select price.src_sail_id,NVL(dim.sail_id,-1) as sail_id,price.price_code,price.guestseq,price.amount,price.category,price.currency,price.price_totals,dim.sail_date_from,dim.sail_date_to,price.load_dt  from pricing price left join vv_db.hvtb_nbx_core_sw_sail_dim dim on(price.src_sail_id == dim.src_sail_id) where to_date(rec_end_dttm) = '9999-12-31'""")
    //price.src_sail_id,NVL(dim.sail_id,-1) as sail_id,price.price_code,price.guestseq,price.amount,price.category,price.currency,price.price_totals,price.load_dt
    val tempOldDF = spark.sql("select * from " + pricingTable + " where sail_date_from<current_date()")
    //sale date less than current date pickup the records as the prices wont change once sail has started.
    val dfs = Seq(tempNewDF, tempOldDF)
    val unionDF = dfs.reduce(_ union _)
    
    unionDF.createOrReplaceTempView("final_view")
    val tempDF=spark.sql("select src_sail_id,sail_id,price_code,guestseq,amount,category,currency,price_totals,sail_date_from,sail_date_to,load_dt from (select src_sail_id,sail_id,price_code,guestseq,amount,category,currency,price_totals,sail_date_from,sail_date_to,load_dt,row_number() over(partition by src_sail_id,sail_id,price_code,guestseq,amount,category,currency,price_totals order by load_dt)as rn from final_view)price where price.rn=1")
     //tempDF.filter(col("sail_id")===446).filter(col("guestseq")===2).filter(col("currency")==="USD").filter(col("category")==="SS").show(false)
    tempDF.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
    val finalDF = spark.read.parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
    finalDF.write.mode("Overwrite").insertInto(pricingTable)

    //finalDF.printSchema()
    //finalDF.show()

    spark.stop();

  }

}