package com.virginvoyages.invoke.api
import java.sql.SQLException
import java.util.Date
import org.apache.spark.storage.StorageLevel
import scala.collection.immutable.Map
import scala.util.parsing.json._
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.AnalysisException
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
import org.apache.spark.sql.expressions.Window

object InvokeNetRatePricingApi {

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

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
      var xmlStringRDD = dfnoEmpty.selectExpr("concat('<response> ',response,'<num_of_adults_passed> ',cast(guestnum as bigint),' </num_of_adults_passed> </response>')").map(r => r.getString(0)).rdd
      xmlStringRDD.collect

      var priceDetailDf = new XmlReader().xmlRdd(spark.sqlContext, xmlStringRDD)

      priceDetailDf = priceDetailDf.withColumn("num_of_adults_passed", priceDetailDf.col("num_of_adults_passed"))
        .withColumn("AvailPackage_new", priceDetailDf.col("vx:OTA_CruiseSailAvailRS.vx:SailingOptions.vx:SailingOption"))

      if (checkArray(priceDetailDf, "AvailPackage_new")) {
        priceDetailDf = priceDetailDf
          .withColumn("AvailPackage_explode", explode_outer(priceDetailDf.col("AvailPackage_new")))

        priceDetailDf = priceDetailDf.withColumn("sail_date_from", priceDetailDf.col("AvailPackage_explode.vx:SelectedSailing._Start"))
        priceDetailDf = priceDetailDf.withColumn("sail_date_to", priceDetailDf.col("AvailPackage_explode.vx:SelectedSailing._End"))
        priceDetailDf = priceDetailDf.withColumn("voyage_id", priceDetailDf.col("AvailPackage_explode.vx:SelectedSailing._VoyageID"))

        priceDetailDf = priceDetailDf.withColumn("AvailCategories_new", priceDetailDf.col("AvailPackage_explode.vx:TPA_Extensions.ns3:CategoryOptions.ns3:CategoryOption"))

      } else {
        priceDetailDf = priceDetailDf.withColumn("voyage_id", lit(null))
      }

      if (checkArray(priceDetailDf, "AvailCategories_new")) {

        priceDetailDf = priceDetailDf.withColumn("AvailCategory_explode", explode_outer(priceDetailDf.col("AvailCategories_new")))

        priceDetailDf = priceDetailDf.withColumn("PricedCategoryCode", priceDetailDf.col("AvailCategory_explode._PricedCategoryCode"))
        priceDetailDf = priceDetailDf.withColumn("category", priceDetailDf.col("AvailCategory_explode._GenericCategoryCode"))
        priceDetailDf = priceDetailDf.withColumn("max_available_capacity", priceDetailDf.col("AvailCategory_explode._MaxOccupancy"))

        priceDetailDf = priceDetailDf.withColumn("capacity", lit(-1))

        priceDetailDf = priceDetailDf.withColumn("AvailPrice_new", priceDetailDf.col("AvailCategory_explode.vx:PriceInfos.vx:PriceInfo"))

      } else {
        priceDetailDf = priceDetailDf.withColumn("voyage_id", lit(null))

      }

      if (checkArray(priceDetailDf, "AvailPrice_new")) {
        priceDetailDf = priceDetailDf
          .withColumn("AvailPrice_explode", explode_outer(priceDetailDf.col("AvailPrice_new")))

        priceDetailDf = priceDetailDf.withColumn("amount", priceDetailDf.col("AvailPrice_explode._Amount"))
        priceDetailDf = priceDetailDf.withColumn("price_code", priceDetailDf.col("AvailPrice_explode._BreakdownType"))

        priceDetailDf = priceDetailDf.withColumn("currency", priceDetailDf.col("AvailPrice_explode._CurrencyCode"))
        priceDetailDf = priceDetailDf.withColumn("guestseq", priceDetailDf.col("AvailPrice_explode._BreakdownType"))

      } else {
        priceDetailDf = priceDetailDf
          .withColumn("currency", lit(null))
          .withColumn("price_totals", lit(null))
          .withColumn("price_code", lit(null))
          .withColumn("num_of_adults_passed", lit(null))
          .withColumn("amount", lit(null))
          .withColumn("guestseq", lit(null))
          .withColumn("max_available_capacity", lit(null))
          .withColumn("capacity", lit(null))
          .withColumn("PricedCategoryCode", lit(null))
          .withColumn("category", lit(null))
      }

      priceDetailDf = priceDetailDf.dropDuplicates()
      println("*************************Printing data with specific query filter **************")

      return priceDetailDf.selectExpr("sail_date_from", "sail_date_to", "voyage_id", "currency", "amount", "price_code", "guestseq", "cast(num_of_adults_passed as int) as num_of_adults_passed", "max_available_capacity", "PricedCategoryCode", "category", "capacity")
    }
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    val pricingTable = spark.sparkContext.getConf.get("spark.target.table").trim()
    val tempLocation = spark.sparkContext.getConf.get("spark.target.temp").trim()
    val curr_date = current_date()

    val pricingLandingTable = spark.sparkContext.getConf.get("spark.landing.table").trim()
    log.info("#--------------------------Pricing API-------------------#")
    import spark.implicits._
    val responseDF = spark.sql("select * from " + pricingLandingTable)

    responseDF.persist(StorageLevel.MEMORY_AND_DISK)
    val parseDF = parsePriceDetail(responseDF, sparkConfiguration)
    println("******** Printing Final Data Frame ***********")

    parseDF.select(col("sail_date_from"), col("sail_date_to"), col("voyage_id"), col("currency"), col("amount"), col("price_code"), col("guestseq"), col("num_of_adults_passed"), col("max_available_capacity"), col("PricedCategoryCode"), col("category"), col("capacity")).show(false)
    val parsefinalDF = parseDF.select(col("sail_date_from"), col("sail_date_to"), col("voyage_id"), col("currency"), col("amount"), col("price_code"), col("guestseq"), col("num_of_adults_passed"), col("max_available_capacity"), col("PricedCategoryCode"), col("category"), col("capacity"))
    parsefinalDF.show()
    //parsefinalDF.write.mode("overwrite").parquet("s3://vv-dev-emr-cluster/data/test_netrate_pricing_api_new/")

    parsefinalDF.createOrReplaceTempView("temp_price")
    //val df1 = spark.sql("""SELECT * FROM   (SELECT sail_date_from,sail_date_to,voyage_id, currency, amount, price_code, guestseq, num_of_adults_passed, max_available_capacity, pricedcategorycode, category,capacity, Row_number() OVER( partition BY voyage_id, currency, amount, price_code, guestseq, max_available_capacity, pricedcategorycode, category ORDER BY voyage_id DESC) rn FROM   temp_price) r WHERE  r.rn = 1 """)
    val df1 =spark.sql("""SELECT sail_date_from,sail_date_to,voyage_id, currency, amount, price_code, guestseq, num_of_adults_passed, max_available_capacity, pricedcategorycode, category,capacity from temp_price group by sail_date_from,sail_date_to,voyage_id, currency, amount, price_code, guestseq, num_of_adults_passed, max_available_capacity, pricedcategorycode, category,capacity""")
    
    
    
    df1.createOrReplaceTempView("temp_price1")

    //val df2 = spark.sql("""select p1.sail_date_from, p1.sail_date_to, p1.voyage_id, p1.currency, p1.amount, p2.amounts as Price_Total, p1.price_code, p1.price_codes, p1.guestseq, p1.num_of_adults_passed, p1.max_available_capacity, p1.PricedCategoryCode, p1.category, p1.capacity from (select sail_date_from, sail_date_to, voyage_id, currency, price_code, price_codes, amount, guestseq, num_of_adults_passed, max_available_capacity, PricedCategoryCode, category, capacity from (SELECT sail_date_from, sail_date_to, voyage_id, currency, substr(price_code, 1,3) as price_code, price_code as price_codes, amount, case when guestseq like '%1GT%' then 1 when guestseq like '%2GT%' then 2 when guestseq like '%3GT%' then 3 when guestseq like '%4GT%' then 4 else null end as guestseq , num_of_adults_passed, max_available_capacity, PricedCategoryCode, category, capacity from temp_price1) p0) p1 INNER JOIN (select p0.amounts, p0.price_code, p0.voyage_id, p0.currency, p0.PricedCategoryCode from (select sum(amount) as amounts, substr(price_code,1,3) as price_code, voyage_id, currency, PricedCategoryCode from temp_price1 where substr(price_code,1,4) in ('1GT/','2GT/','3GT/','4GT/') and currency in ('GBP','AUD') group by substr(price_code,1,3), voyage_id, currency, PricedCategoryCode) p0) p2 ON (p1.price_code = p2.price_code and p1.voyage_id = p2.voyage_id and p1.currency = p2.currency and p1.PricedCategoryCode = p2.PricedCategoryCode) order by price_codes""")
    val df2 = spark.sql("""select p1.sail_date_from, p1.sail_date_to, p1.voyage_id, p1.currency, p1.amount, p2.amounts as Price_Total, p1.price_code, p1.price_codes, p1.guestseq, p1.num_of_adults_passed, p1.max_available_capacity, p1.PricedCategoryCode, p1.category, p1.capacity from ( select sail_date_from, sail_date_to, voyage_id, currency, price_code, price_codes, amount, guestseq, num_of_adults_passed, max_available_capacity, PricedCategoryCode, category, capacity from ( select sail_date_from, sail_date_to, voyage_id, currency, substr(price_code,1,3) as price_code, price_code as price_codes, amount, case when guestseq like '%1GT%' then 1 when guestseq like '%2GT%' then 2 when guestseq like '%3GT%' then 3 when guestseq like '%4GT%' then 4 else null end as guestseq , num_of_adults_passed, max_available_capacity, PricedCategoryCode, category, capacity from temp_price1) p0) p1 inner join ( select p0.amounts, p0.price_code, p0.voyage_id, p0.currency, p0.PricedCategoryCode, p0.num_of_adults_passed from ( select sum(amount) as amounts, substr(price_code,1,3) as price_code, voyage_id, currency, PricedCategoryCode, num_of_adults_passed from temp_price1 where substr(price_code, 1, 4) in ('1GT/', '2GT/', '3GT/', '4GT/') and currency in ('GBP', 'AUD') group by substr(price_code, 1, 3), voyage_id, currency, PricedCategoryCode, num_of_adults_passed) p0) p2 on (p1.price_code = p2.price_code and p1.voyage_id = p2.voyage_id and p1.currency = p2.currency and p1.PricedCategoryCode = p2.PricedCategoryCode and p1.num_of_adults_passed = p2.num_of_adults_passed) order by price_codes""")
    
    
    val df3 = df2.select(col("sail_date_from"), col("sail_date_to"), col("voyage_id"), col("currency"), col("amount"), col("Price_Total"),col("price_code"),col("price_codes"), col("guestseq"), col("num_of_adults_passed"), col("max_available_capacity"), col("PricedCategoryCode"), col("category"), col("capacity")).withColumnRenamed("price_codes", "price_codes").withColumnRenamed("Price_Total", "price_totals")
    //parseDF.write.parquet("s3://vv-dev-emr-cluster/data/test_vivek_apinotjoin/")
    println("**** Printing Tranformed Data ***********")

    println("Printing Voyage Total")

    val parseFilterDF = df3.withColumn("load_dt", current_date())
    //parseFilterDF.write.parquet("s3://vv-dev-emr-cluster/data/test_vivek_apinotjoin/")

    parseFilterDF.show(false)
    println("#-------------------------------API output parsed and ready to be ingested-----------------------#")
    println("****Joining sail dim and package dime to get src_sail_id****")
    val sail_dim = spark.sql("""select pkg_dim.src_package_id as src_package_id, pkg_dim.package_code as package_code, NVL(sail_dim.sail_id, -1) as sail_id, sail_dim.src_sail_id as src_sail_id, sail_dim.sail_date_from as sail_date_from, sail_dim.sail_date_to as sail_date_to from ( select src_package_id, package_code, src_sail_id, load_dt, rec_end_dttm, package_class, row_number() over(partition by src_package_id, package_code, src_sail_id order by load_dt desc) rn from vv_db.hvtb_nbx_core_sw_package_dim where package_class = 'VOYAGE' and to_date(rec_end_dttm) ='9999-12-31') pkg_dim left join ( select sail_id, src_sail_id, sail_date_from, sail_date_to, load_dt,is_active from vv_db.hvtb_nbx_core_sw_sail_dim where to_date(rec_end_dttm)='9999-12-31' and is_active = 'Y') sail_dim on (pkg_dim.src_sail_id == sail_dim.src_sail_id) """)
    sail_dim.show(false)
    sail_dim.createOrReplaceTempView("dim")
    parseFilterDF.createOrReplaceTempView("pricing")

    val tempNewDF = spark.sql(s""" select price.voyage_id,NVL(dim.sail_id,-1) as sail_id,dim.src_sail_id,price.price_code,price.price_codes,price.num_of_adults_passed ,price.max_available_capacity,price.PricedCategoryCode,price.guestseq,price.category,price.currency,price.amount,price.price_totals,dim.sail_date_from,dim.sail_date_to,price.load_dt  from pricing price left join dim on(price.voyage_id == dim.package_code)""")
    println("**** Printing Tranformed Data with joining condition ***********")
    tempNewDF.show()
    tempNewDF.createOrReplaceTempView("final_view")
    val tempDF = spark.sql("""select CAST(voyage_id as  string ) as voyage_id,CAST(src_sail_id as  string ) as src_sail_id,CAST(sail_id as  long ) as sail_id,CAST(price_code as  string ) as price_code,CAST(price_codes as  string ) as price_codes,CAST(num_of_adults_passed as  string ) as num_of_adults_passed,CAST(max_available_capacity as  string ) as max_available_capacity,CAST(PricedCategoryCode as  string ) as PricedCategoryCode,CAST(guestseq as  long ) as guestseq,CAST(amount as  double ) as amount,CAST(price_totals as  double ) as price_totals,CAST(category as  string ) as category,CAST(currency as  string ) as currency,CAST(sail_date_from as  date ) as sail_date_from,CAST(sail_date_to as  date ) as sail_date_to,CAST(load_dt as  date ) as load_dt  from (select voyage_id,src_sail_id,sail_id,price_code,price_codes,guestseq,num_of_adults_passed ,max_available_capacity,PricedCategoryCode,amount,category,currency,price_totals,sail_date_from,sail_date_to,load_dt,row_number() over(partition by src_sail_id,sail_id,price_code,price_codes,guestseq,amount,category,currency,price_totals order by load_dt)as rn from final_view)price""")
    println("***** Printing final data *****")
    tempDF.printSchema()
     tempNewDF.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
    // tempNewDF.write.mode("overwrite").parquet("s3://vv-dev-emr-cluster/data/test_netrate_pricing_api_2/")
      val data = spark.read.parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
    //val data = spark.read.parquet("s3://vv-dev-emr-cluster/data/test_netrate_pricing_api_2/*")
    data.createOrReplaceTempView("pricetbl")
    spark.sql(""" insert overwrite table vv_db.hvtb_nbx_core_net_rate_pricing_detail select * from pricetbl""")
    println("*** Data Refreshed ***")

    println("*** data written ***")

    spark.stop();

  }

}
  
