package com.virginvoyages.invoke.api
import java.sql.{ ResultSet, PreparedStatement, Connection, Driver, DriverManager, ResultSetMetaData, SQLException }
import scala.collection.immutable.Map
import scala.util.Try
import scala.xml.XML
import org.apache.spark.sql.Row
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.explode_outer
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StructType
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions._
import com.databricks.spark.xml.XmlReader

import scalaj.http.Http
import scalaj.http.HttpOptions
import java.sql.Date
import scala.xml.Node
import scala.xml.Elem
import org.apache.spark.sql.expressions.Window
import java.sql.SQLException
import scala.collection.mutable.ListBuffer



case class CabinFact(
  sailID: Int,
  xmlMessage : String, 
  is_active : String, 
  ship_id : String,
  ship_code : String
)

object CabinPricingApiCabinReserveCount {

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {

    def getSparkSession() =
      {
        val spark = SparkSession
          .builder()
          .enableHiveSupport().getOrCreate()
        spark
      }
    val sparkSession = getSparkSession()
    val sc = sparkSession.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = sparkSession.sparkContext.broadcast(sparkSession.sparkContext.getConf.getAll.toMap)
    
    
    def findCabinDetail(sessionGuid: String, configMap: Broadcast[Map[String, String]], sailid: Int, sailStart: String, sailEnd: String, shipCode: String): String = {
      log.info("Getting ManageShipInventory_IN Details")
      try {
        var manageShipInventoryIn = "<ManageShipInventory_IN><MsgHeader><Version>1.0</Version><SessionGUID>" + sessionGuid + "</SessionGUID><Language>ENG</Language></MsgHeader><Action><GetSailData><Sail><Ship>" + shipCode + "</Ship><From><DateTime>" + sailStart + "</DateTime></From><To><DateTime>" + sailEnd + "</DateTime></To></Sail><Options><IncludeAvailData>Y</IncludeAvailData><IncludeCabinData>Y</IncludeCabinData><IncludeAllocations>N</IncludeAllocations></Options></GetSailData></Action></ManageShipInventory_IN>"
        //val response = Http("https://dev.virginvoyages.com/seaware/SwBizLogic/Service.svc/ProcessRequest").postData(manageShipInventoryIn)
        val response = Http(configMap.value.get("spark.cabinfact.seaware.xml.api.url").get.trim).postData(manageShipInventoryIn)
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
        var postData = "<Login_IN><Version>1.0</Version><UserInfo><ResAgent><Username>" + configMap.value.get("spark.cabinfact.seaware.xml.api.username").get.trim + "</Username><Password>" + configMap.value.get("spark.cabinfact.seaware.xml.api.password").get.trim + "</Password></ResAgent></UserInfo></Login_IN>"
        // val response = Http("https://dev.virginvoyages.com/seaware/SwBizLogic/Service.svc/ProcessRequest").postData(postData)
        val response = Http(configMap.value.get("spark.cabinfact.seaware.xml.api.url").get.trim).postData(postData)
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
    
    var query = sparkSession.sparkContext.getConf.get("spark.cabinfact.sourcequery").trim
    //var query = "SELECT s.sail_id, (s.ship_id) as ship_id, to_date(sail_date_from) as sail_date_from, to_date(sail_date_to) as sail_date_to ,(sh.ship) as ship,s.is_active as is_activ from vv_db.hvtb_nbx_core_sw_sail_dim s join vv_db.hvtb_nbx_core_sw_ship_dim sh on sh.ship_id = s.ship_id where to_date(s.rec_end_dttm) = '9999-12-31' aND sail_date_from LIKE '2020-04-05%'  aND ship <> 'XE'  limit 10"
    var inputDf1 = sparkSession.sql(query)
    //inputDf.show()

    import sparkSession.implicits._
    
    val results = new ListBuffer[CabinFact]()
    
  for (iter <- inputDf1.rdd.collect)
  {   
    val sail_id = iter.mkString(",").split(",")(0).toInt
    val ship_id = iter.mkString(",").split(",")(1)
    val sail_date_from = iter.mkString(",").split(",")(2)
    val sail_date_to = iter.mkString(",").split(",")(3)
    val ship_code = iter.mkString(",").split(",")(4)
    val is_active = iter.mkString(",").split(",")(5)
    val sessionGuid = getSessionGuid(sparkConfiguration)
    
    val priceDetail = findCabinDetail(sessionGuid, sparkConfiguration, sail_id, sail_date_from, sail_date_to, ship_code)
    
    val currentResult = CabinFact (
        sailID= sail_id,
        xmlMessage = priceDetail, 
  is_active = is_active, 
  ship_id = ship_id,
  ship_code = ship_code
  )
        
        
    results += currentResult    
    
  }
    
    val responseDF = sparkSession.createDataFrame(results)
    
    
    
    
    
    
    /*var inputArray = Array[String]()
    
    for (iter <- inputDf1.rdd.collect)
    {
      
        val sail_id = iter.mkString(",").split(",")(0).toInt
        val ship_id = iter.mkString(",").split(",")(1)
        val sail_date_from = iter.mkString(",").split(",")(2)
        val sail_date_to = iter.mkString(",").split(",")(3)
        val ship_code = iter.mkString(",").split(",")(4)
        val is_active = iter.mkString(",").split(",")(5)
        val sessionGuid = getSessionGuid(sparkConfiguration)
        var priceDetail1 = findCabinDetail(sessionGuid, sparkConfiguration, sail_id, sail_date_from, sail_date_to, ship_code)
        inputArray= inputArray:+iter.mkString(",")+","+priceDetail1   
    
    }
    
    val inputRDD = sc.parallelize(inputArray)
    val inputDf = inputRDD.map { t =>
        val sail_id = t.split(",")(0)
        val ship_id = t.split(",")(1)
        val sail_date_from = t.split(",")(2)
        val sail_date_to = t.split(",")(3)
        val ship_code = t.split(",")(4)
        val is_active = t.split(",")(5)
        val priceDetail1 = t.split(",")(6)
      (sail_id, priceDetail1, is_active, ship_id,ship_code)

    }.toDF("sailID", "xmlMessage", "is_active", "ship_id","ship_code")*/
    
    
    
    /* Old Code -- org.apache.spark.SparkException: Task not serializable
    inputDf = inputDf.rdd.map {
      iter =>
        log.info("*****row => " + iter)
        log.info("row 1" + iter.getAs[String](0));
        log.info("row 2" + iter.getAs[String](1));
        log.info("row 3" + iter.getAs[String](2));
        log.info("row 4" + iter.getAs[String](3));
        val sail_id = iter.getInt(0)
        val ship_id = iter.getInt(1)
        val sail_date_from = iter.getDate(2)
        val sail_date_to = iter.getDate(3)
        val ship_code = iter.getString(4)
        val is_active = iter.getString(5)
        val sessionGuid = getSessionGuid(sparkConfiguration)
        var priceDetail1 = findCabinDetail(sessionGuid, sparkConfiguration, sail_id, sail_date_from, sail_date_to, ship_code)
        (sail_id, priceDetail1, is_active, ship_id,ship_code)
    }.toDF("sailID", "xmlMessage", "is_active", "ship_id","ship_code");*/

    //println(inputDf.show(false))
    parseCabinDetail(responseDF, sparkConfiguration)

    

    

    def parseCabinDetail(priceDetailXml: DataFrame, configMap: Broadcast[Map[String, String]]): DataFrame = {
      try {
        var dfnoEmpty = priceDetailXml.filter($"xmlMessage" =!= "")
        var xmlStringRDD = dfnoEmpty.select("xmlMessage").map(r => r.getString(0)).rdd
        var startingDF2 = new XmlReader().xmlRdd(sparkSession.sqlContext, xmlStringRDD) //PARSE ONE
        //startingDF2.printSchema()
        //startingDF2.show()
        var priceDetailXml2 = priceDetailXml.withColumn("rowId1", monotonically_increasing_id())
        startingDF2 = startingDF2.withColumn("rowId1", monotonically_increasing_id())
        var w = Window.orderBy("rowId1")
        // Use row number with the window specification and Drop the created increasing data column
        priceDetailXml2 = priceDetailXml2.withColumn("index", row_number().over(w)).drop("rowId1")
        startingDF2 = startingDF2.withColumn("index", row_number().over(w)).drop("rowId1")
        //priceDetailXml2.show();
        //startingDF2.show()
        var df = priceDetailXml2.join(startingDF2, priceDetailXml2("index") === startingDF2("index"), "inner").select(priceDetailXml2.col("sailID"), priceDetailXml2.col("is_active"), priceDetailXml2.col("ship_id"), priceDetailXml2.col("ship_code"), startingDF2.col("*"))

        //        df.printSchema()
        //        df.show(100, false)

        log.info("---------Final Match with ID---------")
        //df.show(false)
        log.info("---------Final Match DONE with ID---------")

        //startingDF.show(2)
        var checkErrorsfield = hasColumn(df, "Errors")
		if (checkErrorsfield.equals(true)) {
        
        df = df.where(df.col("Errors").isNull)
        df = df.drop("Errors") }
        var checkActionTagPresent = hasColumn(df, "Action")
        if (checkActionTagPresent.equals(true)) {
          log.info("Inside checkActionTagPresent")
          var manageShipInventoryDF = df.withColumn("Action", explode(array(df.col("Action"))))
            .withColumn("MsgHeader", explode(array(df.col("MsgHeader"))));
          manageShipInventoryDF = manageShipInventoryDF.withColumn("GetSailData", manageShipInventoryDF.col("Action.GetSailData"))
          log.info("Inside getsaildaat")
          manageShipInventoryDF = manageShipInventoryDF.withColumn("Totals", manageShipInventoryDF.col("GetSailData.Totals"))
          log.info("after getsailinfo")
          if (checkExplode(manageShipInventoryDF, "Totals")) {

            //manageShipInventoryDF = manageShipInventoryDF.withColumn("CabinDataInfo", manageShipInventoryDF.col("CabinData"))).drop("CabinData")

            manageShipInventoryDF = manageShipInventoryDF.withColumn("Item", manageShipInventoryDF.col("Totals.Item")).drop("Totals")
          } /*else {
              manageShipInventoryDF = manageShipInventoryDF.withColumn("CabinDataInfo", manageShipInventoryDF.col("CabinData")).drop("CabinData")
            }*/

          if (checkArray(manageShipInventoryDF, "Item")) {
            manageShipInventoryDF = manageShipInventoryDF.withColumn("ItemInfo", explode_outer(manageShipInventoryDF.col("Item")))
              .drop("GetSailData")
              .drop("Action")
              .drop("MsgHeader")
              .drop("Item")
            //            manageShipInventoryDF.show(100, false)
            //            manageShipInventoryDF.printSchema()
			
			val manageShipInventoryDF2 = manageShipInventoryDF
            
            manageShipInventoryDF = manageShipInventoryDF.where(manageShipInventoryDF.col("ItemInfo.Type") === lit("CATEGORY"))
			
			println ("manageShipInventoryDF is ")
			
			//manageShipInventoryDF.show(5,false)
			
			//val manageShipInventoryDF2 = manageShipInventoryDF
			
			println ("manageShipInventoryDF2 is ")
			
			//manageShipInventoryDF2.show(5,false)
			
			 val required_columns_c = sparkSession.sparkContext.getConf.get("spark.source.columns_c").split(",")
      val missing_columns_c = ArrayBuffer[String]()
      val avaliable_columns_c = ArrayBuffer[String]()
	  
	  for (col <- required_columns_c) {

        if (hasColumn( manageShipInventoryDF, "ItemInfo."+col)) {
           println(col, "column exists", avaliable_columns_c.toString)
          println("column exists", avaliable_columns_c.length)
          log.info(col, "column exists", avaliable_columns_c.toString, avaliable_columns_c.length)
          println(avaliable_columns_c.length, "lenthg")
          avaliable_columns_c.append(col)
        } else {
          println(col, "column missing", missing_columns_c.length)
          println(missing_columns_c.length, "length")
          log.info(col, "column exists", missing_columns_c.toString, missing_columns_c.length)
          println("column missing", missing_columns_c)
          missing_columns_c.append(col)
        }

      }
            
			print(missing_columns_c, "Here are the missing columns")
	  val stage_final_df_c = avaliable_columns_c.foldLeft(manageShipInventoryDF)((df, c) => df.withColumn(s"$c", manageShipInventoryDF.col("ItemInfo."+c)))
	  val stage_final_df2_c = missing_columns_c.foldLeft(stage_final_df_c)((df, c) => df.withColumn(s"$c", lit(null)))
	  
	  var manageShipInventoryfinalDF = stage_final_df2_c.withColumn("cabincount", stage_final_df2_c.col("nofrsrvnotavl")).withColumn("reservecount", stage_final_df2_c.col("okgtyrstrabs"))
			
            manageShipInventoryfinalDF = manageShipInventoryfinalDF.select("sailID", "is_active", "ship_id", "category", "cabincount", "reservecount","ship_code")
            
            //manageShipInventoryfinalDF.printSchema()
            //manageShipInventoryfinalDF.show(100, false)
            
            manageShipInventoryfinalDF.createOrReplaceTempView("temptable")
            var insert_hive_qry = configMap.value.get("spark.cabinfact.insert.overwrite.query").get.trim
            //var insert_hive_qry = "insert overwrite table vv_db.tmp_n_cab_res_cnt"
            sparkSession.sql("%s select sailID,is_active,ship_id,category,cabincount,reservecount,current_timestamp() from temptable".format(insert_hive_qry))
			
// sail cabin detail fact code 

 val required_columns = sparkSession.sparkContext.getConf.get("spark.source.columns").split(",")
      val missing_columns = ArrayBuffer[String]()
      val avaliable_columns = ArrayBuffer[String]()
      
      
      for (col <- required_columns) {

        if (hasColumn( manageShipInventoryDF2, "ItemInfo."+col)) {
           println(col, "column exists", avaliable_columns.toString)
          println("column exists", avaliable_columns.length)
          log.info(col, "column exists", avaliable_columns.toString, avaliable_columns.length)
          println(avaliable_columns.length, "lenthg")
          avaliable_columns.append(col)
        } else {
          println(col, "column missing", missing_columns.length)
          println(missing_columns.length, "length")
          log.info(col, "column exists", missing_columns.toString, missing_columns.length)
          println("column missing", missing_columns)
          missing_columns.append(col)
        }

      }
      
      print(missing_columns, "Here are the missing columns")
	  val stage_final_df = avaliable_columns.foldLeft(manageShipInventoryDF2)((df, c) => df.withColumn(s"$c", manageShipInventoryDF2.col("ItemInfo."+c)))
	  val stage_final_df2 = missing_columns.foldLeft(stage_final_df)((df, c) => df.withColumn(s"$c", lit(null)))
	  
	  log.info(" stage_final_df2 is ")
	  
	  //stage_final_df2.show(100,false)
	  
	  //stage_final_df2.printSchema()
	  
var sailcabindetailfactfinalDF = stage_final_df2.withColumn("cabin_category", stage_final_df2.col("category"))
.withColumn("cabin_category_rank", stage_final_df2.col("rank"))
.withColumn("capacity", stage_final_df2.col("capacity"))
.withColumn("max_capacity", stage_final_df2.col("maxcapacity"))
.withColumn("num_of_cabins", stage_final_df2.col("nofcabins"))
.withColumn("ok_abs", stage_final_df2.col("okabs"))
.withColumn("ok_wgt", stage_final_df2.col("okwgt"))
.withColumn("gty_abs", stage_final_df2.col("gtyabs"))
.withColumn("gty_wgt", stage_final_df2.col("gtywgt"))
.withColumn("wtl_abs", stage_final_df2.col("wtlabs"))
.withColumn("wtl_wgt", stage_final_df2.col("wtlwgt"))
.withColumn("avail_cabins", stage_final_df2.col("availcabins"))
.withColumn("avail_abs", stage_final_df2.col("availabs"))
.withColumn("avail_wgt", stage_final_df2.col("availwgt"))
.withColumn("avail_nested", stage_final_df2.col("availnested"))
.withColumn("num_of_reserved", stage_final_df2.col("nofreserved"))
.withColumn("in_allotments", stage_final_df2.col("inallotments"))
.withColumn("num_of_upgrade", stage_final_df2.col("nofupgrades"))
.withColumn("num_of_downgrade", stage_final_df2.col("nofdowngrades"))
.withColumn("shared_abs", stage_final_df2.col("nofdowngrades"))
.withColumn("shared_allotment_abs", stage_final_df2.col("nofdowngrades"))
.withColumn("na_abs", stage_final_df2.col("nofrsrvnotavl"))
.withColumn("ok_gty_rstr_abs", stage_final_df2.col("okgtyrstrabs"))
.withColumn("type", stage_final_df2.col("type"))
.withColumn("sail_id", stage_final_df2.col("sailID"))
.withColumn("is_active", stage_final_df2.col("is_active"))

  			
 sailcabindetailfactfinalDF = sailcabindetailfactfinalDF.select("sail_id", "cabin_category", "cabin_category_rank", "capacity", "max_capacity", "num_of_cabins", "ok_abs", "ok_wgt", "gty_abs", "gty_wgt", "wtl_abs", "wtl_wgt", "avail_cabins", "avail_abs", "avail_wgt", "avail_nested", "num_of_reserved", "in_allotments", "nofupgrades", "num_of_upgrade", "num_of_downgrade", "shared_abs", "shared_allotment_abs","na_abs", "ok_gty_rstr_abs","ship_code","type","is_active")
		  
		  sailcabindetailfactfinalDF.createOrReplaceTempView("temptable")
		  
		  print(" sailcabindetailfactfinalDF is ")
		  
		  //sailcabindetailfactfinalDF.show(100,false)
		  
		  val tgt = sparkSession.sql("select current_timestamp snapshot_time  , tt.sail_id,tt.cabin_category,tt.cabin_category_rank, cabin_category_generic generic_category,tt.capacity,tt.max_capacity,tt.num_of_cabins,tt.ok_abs,tt.ok_wgt,tt.gty_abs,tt.gty_wgt,tt.wtl_abs,tt.wtl_wgt,tt.avail_cabins,tt.avail_abs,tt.avail_wgt,tt.avail_nested,tt.num_of_reserved,tt.in_allotments,tt.num_of_upgrade,tt.num_of_downgrade,tt.shared_abs,tt.shared_allotment_abs,tt.na_abs,tt.ok_gty_rstr_abs,current_timestamp etl_ld_dt ,current_timestamp  etl_upd_dt, CURRENT_DATE snapshot_date,type,ship_code,is_active from temptable tt left join 	(select distinct ship,cabin_category,cabin_category_generic from vv_db.hvtb_nbx_core_sw_cabin_master_dim ) cm on tt.cabin_category = cm.cabin_category and tt.ship_code = cm.ship where cm.ship <> 'XE' ")
			tgt.createOrReplaceTempView("tgttable")
			
		//	var tgt_count = tgt.count
			
					//	print("tgt count is"+tgt_count)
			
			
			
			val tgttable_CAPACITY = sparkSession.sql(" select * from tgttable where type = 'CAPACITY'")
			tgttable_CAPACITY.createOrReplaceTempView("tgttable_CAPACITYtbl")
			
		//	var  tgttable_CAPACITY_count = tgttable_CAPACITY.count
			
			//print("CAPACITY count is"+tgttable_CAPACITY_count)
			
			
			
			val tgttable_CATEGORY = sparkSession.sql(" select * from tgttable where type = 'CATEGORY'")
			tgttable_CATEGORY.createOrReplaceTempView("tgttable_CATEGORYtbl")
			
			//var  tgttable_CATEGORY_count = tgttable_CATEGORY.count
			
			//print("CATEGORY count is"+tgttable_CATEGORY_count)
			
			
			
			val tgttable_final = sparkSession.sql("""SELECT c.snapshot_time, sd.sail_id, c.cabin_category, c.cabin_category_rank, c.generic_category, c.capacity, ct.max_capacity, c.num_of_cabins, c.ok_abs, c.ok_wgt, c.gty_abs, c.gty_wgt, c.wtl_abs, c.wtl_wgt, c.avail_cabins, c.avail_abs, c.avail_wgt, c.avail_nested, c.num_of_reserved, c.in_allotments, c.num_of_upgrade, c.num_of_downgrade, c.shared_abs, c.shared_allotment_abs, c.na_abs, c.ok_gty_rstr_abs as ok_gtry_rstr_abs, c.etl_ld_dt, c.etl_upd_dt, c.snapshot_date FROM tgttable_CAPACITYtbl c join tgttable_CATEGORYtbl ct on c.cabin_category = ct.cabin_category and c.sail_id = ct.sail_id and c.ship_code = ct.ship_code and c.is_active = ct.is_active 
			left join vv_db.hvtb_nbx_core_sw_sail_dim sd on sd.src_sail_id = c.sail_id and sd.rec_end_dttm like '999%' 
			where c.type = 'CAPACITY'""")
			tgttable_final.createOrReplaceTempView("tgttable_finaltbl")
			
					
			var ss_dt =  sparkSession.sql("""select CURRENT_DATE ss_dt""")
			var curr_date_str = ss_dt.first().getDate(0)
			
			sparkSession.sql("""SET hive.exec.dynamic.partition.mode=nonstrict""")
			
			sparkSession.sql("SET hive.exec.dynamic.partition=true")
			
			var insert_hive_qry_dtl_scf = "insert overwrite table vv_db.hvtb_nbx_staging_sw_sail_cabin_fact SELECT c.snapshot_time , sd.sail_id , c.cabin_category , c.cabin_category_rank , c.generic_category , c.max_capacity , c.num_of_cabins , c.ok_abs , c.ok_wgt , c.gty_abs , c.gty_wgt , c.wtl_abs , c.wtl_wgt , c.avail_cabins , c.avail_abs , c.avail_wgt , c.avail_nested , c.num_of_reserved , c.in_allotments , c.num_of_upgrade , c.num_of_downgrade , c.shared_abs , c.shared_allotment_abs , c.etl_ld_dt , c.etl_upd_dt , c.na_abs , c.ok_gty_rstr_abs AS ok_gtry_rstr_abs , sailapi1.taxes_and_fees AS taxes_and_fees , swprlkp.promotion_id AS promotion_id , sailapi1.voyage_fare_guest1 AS voyage_fare_guest1 , sailapi1.voyage_fare_guest2 AS voyage_fare_guest2 , sailapi1.voyage_fare_guest3 AS voyage_fare_guest3 , sailapi1.voyage_fare_guest4 AS voyage_fare_guest4 , sailapi1.discount_guest1 , sailapi1.discount_guest2 , sailapi1.discount_guest3 , sailapi1.discount_guest4 , sailapi1.voyage_fare_guest1_gbp , sailapi1.voyage_fare_guest2_gbp , sailapi1.voyage_fare_guest3_gbp , sailapi1.voyage_fare_guest4_gbp , sailapi1.discount_guest1_gbp , sailapi1.discount_guest2_gbp , sailapi1.discount_guest3_gbp , sailapi1.discount_guest4_gbp , sailapi1.taxes_and_fees_gbp , sailapi1.voyage_fare_guest1_aud , sailapi1.voyage_fare_guest2_aud , sailapi1.voyage_fare_guest3_aud , sailapi1.voyage_fare_guest4_aud , sailapi1.discount_guest1_aud , sailapi1.discount_guest2_aud , sailapi1.discount_guest3_aud , sailapi1.discount_guest4_aud , sailapi1.taxes_and_fees_aud , sailapi1.voyage_fare_guest1_cad , sailapi1.voyage_fare_guest2_cad , sailapi1.voyage_fare_guest3_cad , sailapi1.voyage_fare_guest4_cad , sailapi1.discount_guest1_cad , sailapi1.discount_guest2_cad , sailapi1.discount_guest3_cad , sailapi1.discount_guest4_cad , sailapi1.taxes_and_fees_cad , sailapi1.voyage_fare_guest1_nzd , sailapi1.voyage_fare_guest2_nzd , sailapi1.voyage_fare_guest3_nzd , sailapi1.voyage_fare_guest4_nzd , sailapi1.discount_guest1_nzd , sailapi1.discount_guest2_nzd , sailapi1.discount_guest3_nzd , sailapi1.discount_guest4_nzd , sailapi1.taxes_and_fees_nzd , sailapi1.voyage_fare_guest1_eur , sailapi1.voyage_fare_guest2_eur , sailapi1.voyage_fare_guest3_eur , sailapi1.voyage_fare_guest4_eur , sailapi1.discount_guest1_eur , sailapi1.discount_guest2_eur , sailapi1.discount_guest3_eur , sailapi1.discount_guest4_eur , sailapi1.taxes_and_fees_eur , c.snapshot_date FROM tgttable_CATEGORYtbl c LEFT JOIN vv_db.hvtb_nbx_core_sw_sail_dim sd ON sd.src_sail_id = c.sail_id AND sd.rec_end_dttm LIKE '999%' LEFT JOIN ( SELECT sailapi.prc_sail_id , sailapi.prc_category , sailapi.promo_code , sum(sailapi.taxes_and_fees) AS taxes_and_fees , sum(sailapi.voyage_fare_guest1) AS voyage_fare_guest1 , sum(sailapi.voyage_fare_guest2) AS voyage_fare_guest2 , sum(sailapi.voyage_fare_guest3) AS voyage_fare_guest3 , sum(sailapi.voyage_fare_guest4) AS voyage_fare_guest4 , sum(sailapi.DISCOUNT_GUEST1) AS DISCOUNT_GUEST1 , sum(sailapi.DISCOUNT_GUESt2) AS DISCOUNT_GUEST2 , sum(sailapi.DISCOUNT_GUEST3) AS DISCOUNT_GUEST3 , sum(sailapi.DISCOUNT_GUEST4) AS DISCOUNT_GUEST4 , sum(sailapi.taxes_and_fees_gbp) AS taxes_and_fees_gbp , sum(sailapi.voyage_fare_guest1_gbp) AS voyage_fare_guest1_gbp , sum(sailapi.voyage_fare_guest2_gbp) AS voyage_fare_guest2_gbp , sum(sailapi.voyage_fare_guest3_gbp) AS voyage_fare_guest3_gbp , sum(sailapi.voyage_fare_guest4_gbp) AS voyage_fare_guest4_gbp , sum(sailapi.DISCOUNT_GUEST1_gbp) AS DISCOUNT_GUEST1_gbp , sum(sailapi.DISCOUNT_GUEST2_gbp) AS DISCOUNT_GUEST2_gbp , sum(sailapi.DISCOUNT_GUEST3_gbp) AS DISCOUNT_GUEST3_gbp , sum(sailapi.DISCOUNT_GUEST4_gbp) AS DISCOUNT_GUEST4_gbp , sum(sailapi.taxes_and_fees_aud) AS taxes_and_fees_aud , sum(sailapi.voyage_fare_guest1_aud) AS voyage_fare_guest1_aud , sum(sailapi.voyage_fare_guest2_aud) AS voyage_fare_guest2_aud , sum(sailapi.voyage_fare_guest3_aud) AS voyage_fare_guest3_aud , sum(sailapi.voyage_fare_guest4_aud) AS voyage_fare_guest4_aud , sum(sailapi.DISCOUNT_GUEST1_aud) AS DISCOUNT_GUEST1_aud , sum(sailapi.DISCOUNT_GUEST2_aud) AS DISCOUNT_GUEST2_aud , sum(sailapi.DISCOUNT_GUEST3_aud) AS DISCOUNT_GUEST3_aud , sum(sailapi.DISCOUNT_GUEST4_aud) AS DISCOUNT_GUEST4_aud , sum(sailapi.taxes_and_fees_cad) AS taxes_and_fees_cad , sum(sailapi.voyage_fare_guest1_cad) AS voyage_fare_guest1_cad , sum(sailapi.voyage_fare_guest2_cad) AS voyage_fare_guest2_cad , sum(sailapi.voyage_fare_guest3_cad) AS voyage_fare_guest3_cad , sum(sailapi.voyage_fare_guest4_cad) AS voyage_fare_guest4_cad , sum(sailapi.DISCOUNT_GUEST1_cad) AS DISCOUNT_GUEST1_cad , sum(sailapi.DISCOUNT_GUEST2_cad) AS DISCOUNT_GUEST2_cad , sum(sailapi.DISCOUNT_GUEST3_cad) AS DISCOUNT_GUEST3_cad , sum(sailapi.DISCOUNT_GUEST4_cad) AS DISCOUNT_GUEST4_cad , sum(sailapi.taxes_and_fees_nzd) AS taxes_and_fees_nzd , sum(sailapi.voyage_fare_guest1_nzd) AS voyage_fare_guest1_nzd , sum(sailapi.voyage_fare_guest2_nzd) AS voyage_fare_guest2_nzd , sum(sailapi.voyage_fare_guest3_nzd) AS voyage_fare_guest3_nzd , sum(sailapi.voyage_fare_guest4_nzd) AS voyage_fare_guest4_nzd , sum(sailapi.DISCOUNT_GUEST1_nzd) AS DISCOUNT_GUEST1_nzd , sum(sailapi.DISCOUNT_GUEST2_nzd) AS DISCOUNT_GUEST2_nzd , sum(sailapi.DISCOUNT_GUEST3_nzd) AS DISCOUNT_GUEST3_nzd , sum(sailapi.DISCOUNT_GUEST4_nzd) AS DISCOUNT_GUEST4_nzd , sum(sailapi.taxes_and_fees_eur) AS taxes_and_fees_eur , sum(sailapi.voyage_fare_guest1_eur) AS voyage_fare_guest1_eur , sum(sailapi.voyage_fare_guest2_eur) AS voyage_fare_guest2_eur , sum(sailapi.voyage_fare_guest3_eur) AS voyage_fare_guest3_eur , sum(sailapi.voyage_fare_guest4_eur) AS voyage_fare_guest4_eur , sum(sailapi.DISCOUNT_GUEST1_eur) AS DISCOUNT_GUEST1_eur , sum(sailapi.DISCOUNT_GUEST2_eur) AS DISCOUNT_GUEST2_eur , sum(sailapi.DISCOUNT_GUEST3_eur) AS DISCOUNT_GUEST3_eur , sum(sailapi.DISCOUNT_GUEST4_eur) AS DISCOUNT_GUEST4_eur FROM ( SELECT prc.sail_id AS prc_sail_id , prc.category AS prc_category , prc.promo_code AS promo_code , CASE WHEN prc.price_code = 'TAXES_FEES' AND currency = 'USD' AND prc.guestseq = 1 THEN prc.amount ELSE 0 END AS taxes_and_fees , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'USD' AND prc.guestseq = 1 THEN prc.amount ELSE 0 END AS voyage_fare_guest1 , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'USD' AND prc.guestseq = 2 THEN prc.amount ELSE 0 END AS voyage_fare_guest2 , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'USD' AND prc.guestseq = 3 THEN prc.amount ELSE 0 END AS voyage_fare_guest3 , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'USD' AND prc.guestseq = 4 THEN prc.amount ELSE 0 END AS voyage_fare_guest4 , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'USD' AND prc.guestseq = 1 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST1 , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'USD' AND prc.guestseq = 2 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST2 , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'USD' AND prc.guestseq = 3 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST3 , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'USD' AND prc.guestseq = 4 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST4 , CASE WHEN prc.price_code = 'TAXES_FEES' AND currency = 'GBP' AND prc.guestseq = 1 THEN prc.amount ELSE 0 END AS taxes_and_fees_gbp , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'GBP' AND prc.guestseq = 1 THEN prc.amount ELSE 0 END AS voyage_fare_guest1_gbp , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'GBP' AND prc.guestseq = 2 THEN prc.amount ELSE 0 END AS voyage_fare_guest2_gbp , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'GBP' AND prc.guestseq = 3 THEN prc.amount ELSE 0 END AS voyage_fare_guest3_gbp , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'GBP' AND prc.guestseq = 4 THEN prc.amount ELSE 0 END AS voyage_fare_guest4_gbp , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'GBP' AND prc.guestseq = 1 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST1_gbp , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'GBP' AND prc.guestseq = 2 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST2_gbp , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'GBP' AND prc.guestseq = 3 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST3_gbp , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'GBP' AND prc.guestseq = 4 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST4_gbp , CASE WHEN prc.price_code = 'TAXES_FEES' AND currency = 'AUD' AND prc.guestseq = 1 THEN prc.amount ELSE 0 END AS taxes_and_fees_AUD , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'AUD' AND prc.guestseq = 1 THEN prc.amount ELSE 0 END AS voyage_fare_guest1_AUD , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'AUD' AND prc.guestseq = 2 THEN prc.amount ELSE 0 END AS voyage_fare_guest2_AUD , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'AUD' AND prc.guestseq = 3 THEN prc.amount ELSE 0 END AS voyage_fare_guest3_AUD , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'AUD' AND prc.guestseq = 4 THEN prc.amount ELSE 0 END AS voyage_fare_guest4_AUD , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'AUD' AND prc.guestseq = 1 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST1_AUD , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'AUD' AND prc.guestseq = 2 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST2_AUD , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'AUD' AND prc.guestseq = 3 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST3_AUD , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'AUD' AND prc.guestseq = 4 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST4_AUD , CASE WHEN prc.price_code = 'TAXES_FEES' AND currency = 'CAD' AND prc.guestseq = 1 THEN prc.amount ELSE 0 END AS taxes_and_fees_CAD , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'CAD' AND prc.guestseq = 1 THEN prc.amount ELSE 0 END AS voyage_fare_guest1_CAD , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'CAD' AND prc.guestseq = 2 THEN prc.amount ELSE 0 END AS voyage_fare_guest2_CAD , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'CAD' AND prc.guestseq = 3 THEN prc.amount ELSE 0 END AS voyage_fare_guest3_CAD , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'CAD' AND prc.guestseq = 4 THEN prc.amount ELSE 0 END AS voyage_fare_guest4_CAD , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'CAD' AND prc.guestseq = 1 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST1_CAD , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'CAD' AND prc.guestseq = 2 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST2_CAD , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'CAD' AND prc.guestseq = 3 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST3_CAD , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'CAD' AND prc.guestseq = 4 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST4_CAD , CASE WHEN prc.price_code = 'TAXES_FEES' AND currency = 'NZD' AND prc.guestseq = 1 THEN prc.amount ELSE 0 END AS taxes_and_fees_NZD , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'NZD' AND prc.guestseq = 1 THEN prc.amount ELSE 0 END AS voyage_fare_guest1_NZD , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'NZD' AND prc.guestseq = 2 THEN prc.amount ELSE 0 END AS voyage_fare_guest2_NZD , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'NZD' AND prc.guestseq = 3 THEN prc.amount ELSE 0 END AS voyage_fare_guest3_NZD , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'NZD' AND prc.guestseq = 4 THEN prc.amount ELSE 0 END AS voyage_fare_guest4_NZD , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'NZD' AND prc.guestseq = 1 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST1_NZD , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'NZD' AND prc.guestseq = 2 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST2_NZD , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'NZD' AND prc.guestseq = 3 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST3_NZD , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'NZD' AND prc.guestseq = 4 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST4_NZD , CASE WHEN prc.price_code = 'TAXES_FEES' AND currency = 'EUR' AND prc.guestseq = 1 THEN prc.amount ELSE 0 END AS taxes_and_fees_EUR , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'EUR' AND prc.guestseq = 1 THEN prc.amount ELSE 0 END AS voyage_fare_guest1_EUR , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'EUR' AND prc.guestseq = 2 THEN prc.amount ELSE 0 END AS voyage_fare_guest2_EUR , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'EUR' AND prc.guestseq = 3 THEN prc.amount ELSE 0 END AS voyage_fare_guest3_EUR , CASE WHEN prc.price_code = 'VOYAGE_FARE' AND currency = 'EUR' AND prc.guestseq = 4 THEN prc.amount ELSE 0 END AS voyage_fare_guest4_EUR , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'EUR' AND prc.guestseq = 1 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST1_EUR , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'EUR' AND prc.guestseq = 2 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST2_EUR , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'EUR' AND prc.guestseq = 3 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST3_EUR , CASE WHEN prc.price_code = 'DISCOUNT' AND currency = 'EUR' AND prc.guestseq = 4 THEN prc.amount ELSE 0 END AS DISCOUNT_GUEST4_EUR FROM ( SELECT * FROM ( SELECT CASE WHEN max_available_capacity = 1 THEN 'Y' WHEN max_available_capacity >= 2 AND num_of_adults_passed = 2 THEN 'Y' ELSE 'N' END AS flag, * FROM vv_db.hvtb_nbx_core_pricing_detail WHERE guestseq IN (1, 2) )tab WHERE flag = 'Y' ) prc ) sailapi GROUP BY sailapi.prc_sail_id , sailapi.prc_category , sailapi.promo_code ) sailapi1 ON sailapi1.prc_sail_id = sd.sail_id AND c.cabin_category = sailapi1.prc_category LEFT JOIN vv_db.hvtb_nbx_core_sw_promotion_lkp swprlkp ON sailapi1.promo_code = swprlkp.promo_code"
			
			// resume the code of sail_cabin_detail_fact
			
			sparkSession.sql("%s".format(insert_hive_qry_dtl_scf))
			
		
            var insert_hive_qry_dtl = "insert overwrite table vv_db.hvtb_nbx_staging_sw_sail_cabin_detail_fact partition(snapshot_date) select * from tgttable_finaltbl"
			
            
            sparkSession.sql("%s".format(insert_hive_qry_dtl))
           
		  
		  // sparkSession.sql("  select * from temptable").show(100,false)
		  
            
			
          }
        }
        return startingDF2
      } catch {
        case e: Exception => { e.printStackTrace(); throw new Exception("General Exception..please check stacktrace ") }
      }
    }
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
     *
     * Function to check if a column is of Struct data type
     */
    def checkExplode(df: DataFrame, colname: String): Boolean = {

      df.schema(colname).dataType match {
        case StructType(_) => return true
        case _             => return false
      }
    }
  }

}