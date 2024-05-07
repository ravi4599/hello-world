package com.virginvoyages.invoke.api
import java.text.SimpleDateFormat
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

object InvokePricingApiCabinFact {

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
      //.selectExpr("response").map(r => r.getString(0)).rdd
      var priceDetailDf = new XmlReader().xmlRdd(spark.sqlContext, xmlStringRDD)
      
      //priceDetailDf.printSchema()
      
      priceDetailDf = priceDetailDf.withColumn("num_of_adults_passed", priceDetailDf.col("num_of_adults_passed"))
        .withColumn("AvailPackage_new", priceDetailDf.col("GetAvailPrimPkgsCustom_OUT.AvailPackages.AvailPackage"))

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
   //
       // priceDetailDf = priceDetailDf.withColumn("max_available_capacity", priceDetailDf.col("AvailCategory_explode.MaxAvailableCapacity"))
        priceDetailDf = priceDetailDf.withColumn("currency", priceDetailDf.col("AvailCategory_explode.Currency"))
        priceDetailDf = priceDetailDf.withColumn("category", priceDetailDf.col("AvailCategory_explode.Category"))
        priceDetailDf = priceDetailDf.withColumn("max_available_capacity", priceDetailDf.col("AvailCategory_explode.MaxAvailableCapacity"))
        priceDetailDf = priceDetailDf.withColumn("capacity", priceDetailDf.col("AvailCategory_explode.Capacity"))
        //priceDetailDf = priceDetailDf.withColumn("items_explode", explode_outer(priceDetailDf.col("AvailCategory_explode.PriceTotals.GuestTotal")))
        
        //
        
        priceDetailDf = priceDetailDf.withColumn("guesttotal_explode", explode_outer(priceDetailDf.col("AvailCategory_explode.PriceTotals.GuestTotal")))
        priceDetailDf = priceDetailDf.withColumn("guesttotalitem_explode", explode_outer(priceDetailDf.col("guesttotal_explode.DynamicTotals.Item")))
        priceDetailDf = priceDetailDf.withColumn("price_totals", priceDetailDf.col("guesttotal_explode.PriceTotal"))
        
    
        priceDetailDf = priceDetailDf.withColumn("price_code", priceDetailDf.col("guesttotalitem_explode.code"))
        priceDetailDf = priceDetailDf.withColumn("amount", priceDetailDf.col("guesttotalitem_explode.total"))
        priceDetailDf = priceDetailDf.withColumn("guestseq", priceDetailDf.col("guesttotal_explode.GuestSeqN"))
        //priceDetailDf.printSchema()
          //priceDetailDf = priceDetailDf.withColumn("guestseq", explode_outer(priceDetailDf.col("guest_explode")))
          //priceDetailDf = priceDetailDf.withColumn("guest_seq_no", priceDetailDf.col("guest_explode.GuestSeqN"))
        

      } else {

        priceDetailDf = priceDetailDf
          .withColumn("currency", lit(null))
          .withColumn("price_totals", lit(null))
          .withColumn("price_code", lit(null))
          .withColumn("num_of_adults_passed",lit(null))
          .withColumn("amount", lit(null))
          .withColumn("guestseq", lit(null))
          .withColumn("max_available_capacity", lit(null))
          .withColumn("capacity", lit(null))
          .withColumn("category", lit(null))

      }

      priceDetailDf = priceDetailDf.dropDuplicates()

      return priceDetailDf
        .selectExpr("src_sail_id", "currency", "price_totals", "price_code", "amount", "guestseq","cast(num_of_adults_passed as int) as num_of_adults_passed", "max_available_capacity", "capacity", "category")

    }
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    val pricingTable = spark.sparkContext.getConf.get("spark.target.table").trim()
    val curr_date = current_date()
    
    
    val pricingLandingTable= spark.sparkContext.getConf.get("spark.landing.table").trim()
    log.info("#--------------------------Pricing API-------------------#")
    import spark.implicits._
    val responseDF=spark.sql("select * from "+pricingLandingTable)
    responseDF.persist(StorageLevel.MEMORY_AND_DISK)
    val parseDF = parsePriceDetail(responseDF, sparkConfiguration)
   
    val parseFilterDF = parseDF//.filter($"num_of_adults_passed".cast(IntegerType) === $"max_available_capacity".cast(IntegerType))
      .withColumn("load_dt", current_date())
    parseDF.filter(col("src_sail_id")===1500).filter(col("category")==="SF").show(false)
    println("#-------------------------------API output parsed and ready to be ingested-----------------------#")
    parseFilterDF.createOrReplaceTempView("pricing")
    
    val tempNewDF = spark.sql(s""" select price.src_sail_id,NVL(dim.sail_id,-1) as sail_id,price.price_code,price.num_of_adults_passed ,price.max_available_capacity,price.guestseq,price.amount,price.category,price.currency,price.price_totals,dim.sail_date_from,dim.sail_date_to,price.load_dt  from pricing price left join vv_db.hvtb_nbx_core_sw_sail_dim dim on(price.src_sail_id == dim.src_sail_id)where to_date(rec_end_dttm) = '9999-12-31' """)
    val tempOldDF = spark.sql("select * from " + pricingTable + " where sail_date_from<current_date()")
    //sale date less than current date pickup the records as the prices wont change once sail has started.
    val dfs = Seq(tempNewDF, tempOldDF)
    val unionDF = dfs.reduce(_ union _)
    
    unionDF.createOrReplaceTempView("final_view")
    val tempDF=spark.sql("select src_sail_id,sail_id,price_code,num_of_adults_passed ,max_available_capacity,guestseq,amount,category,currency,price_totals,sail_date_from,sail_date_to,load_dt from (select src_sail_id,sail_id,price_code,guestseq,num_of_adults_passed ,max_available_capacity,amount,category,currency,price_totals,sail_date_from,sail_date_to,load_dt,row_number() over(partition by src_sail_id,sail_id,price_code,guestseq,amount,category,currency,price_totals order by load_dt)as rn from final_view)price where price.rn=1")
     
    tempDF.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
    val finalDF = spark.read.parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
    finalDF.write.mode("Overwrite").insertInto(pricingTable)

   

    spark.stop();

  }

}