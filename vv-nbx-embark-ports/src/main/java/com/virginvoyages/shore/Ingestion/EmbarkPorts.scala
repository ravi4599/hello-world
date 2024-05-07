package com.virginvoyages.shore.Ingestion

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{ col, to_date, to_timestamp, monotonically_increasing_id }
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions.{ col, lit, when }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
//import com.virginvoyages.metadataframework.ManageMetadata
import java.sql.DriverManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ to_date, to_timestamp }
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.time.LocalDateTime
import org.apache.spark.SparkContext
import java.net.URI

object EmbarkPorts{
	def main(args: Array[String]) {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    spark.sqlContext.setConf("spark.sql.parquet.writeLegacyFormat", "true")
	
	//val EmptyDF = spark.emptyDataFrame
	println("---------------reading locations--------------------")
	
	val temploc = spark.sqlContext.sparkContext.getConf.get("spark.temp").trim()
	val targettableloc = spark.sparkContext.getConf.get("spark.target.tgtpath").trim()
	val targetcsvloc = spark.sqlContext.sparkContext.getConf.get("spark.target.location").trim()
	val src = new Path(spark.sqlContext.sparkContext.getConf.get("spark.temp").trim())
	
	val conf = spark.sparkContext.hadoopConfiguration
	val fs = src.getFileSystem(conf)
	
	println("------------Reading data from SrcEmbarkPortDF table--------------")
	
	val SrcEmbarkPortDF = spark.sql("""
	select distinct sail.sail_port_from as EMBARK_PORT,ship.ship as CRUISELINE_SHIPCODE,sail.is_active
	from vv_db.hvtb_nbx_core_sw_package_dim pkgdim
	join vv_db.hvtb_nbx_core_sw_sail_dim sail on pkgdim.src_sail_id  = sail.src_sail_id
	join vv_db.hvtb_nbx_core_sw_reservation_dim res on res.src_sail_id  = sail.src_sail_id and res.res_status in ('BK','CL','TM')
	join vv_db.hvtb_nbx_core_sw_res_guest_rel rel on res.res_id = rel.res_id
	join vv_db.hvtb_nbx_core_sw_guest_dim guest on guest.guest_id = rel.guest_id
	join vv_db.hvtb_nbx_core_sw_ship_dim ship on sail.ship_id = ship.ship_id
	where pkgdim.rec_end_dttm = '9999-12-31 00:00:00' and pkgdim.package_class = 'VOYAGE' and sail.is_active = 'Y'
	and sail.rec_end_dttm = '9999-12-31 00:00:00' 
	and (res.rec_end_dttm = '9999-12-31 00:00:00' or res.rec_end_dttm > sail.sail_date_from)
	and (guest.rec_end_dttm = '9999-12-31 00:00:00' or guest.rec_end_dttm > sail.sail_date_from)
	and guest.client_id is not null""")
	
	println("-----------printing counts from SrcEmbarkPortDF table")
	
	SrcEmbarkPortDF.printSchema()
	//ShorexCodeNameDF.count()
	
    SrcEmbarkPortDF.createOrReplaceTempView("embarktbldata")
	
	println("--------Reading data from target table")
	
	val TgtlkpDF = spark.sql("""select embark_port,cruiseline_shipcode,is_active from shipdw.hvtb_lkp_embark_ports""")
	
	println("---------printing schema for target table------------")
	
	TgtlkpDF.printSchema()

	TgtlkpDF.createOrReplaceTempView("lkptbldata")
	
	val DatajoinDF = spark.sql("""select lkpports.lkp_embark_port,lkpports.lkp_cruiseline_shipcode  ,embport.embark_port as src_embark_port,embport.cruiseline_shipcode as src_cruiseline_shipcode,embport.is_active,embport.cruiseline_shipcode from embarktbldata embport full outer join (select  embark_port as lkp_embark_port,cruiseline_shipcode as lkp_cruiseline_shipcode,is_active from lkptbldata) lkpports on embport.embark_port = lkpports.lkp_embark_port and embport.cruiseline_shipcode = lkpports.lkp_cruiseline_shipcode""") 
	DatajoinDF.dropDuplicates
	DatajoinDF.createOrReplaceTempView("DatajoinView")
	
	
	//Data not present in lookup but present in src mxp table (need to insert)
	val df1 = spark.sql("""select lkp_embark_port,src_embark_port,lkp_cruiseline_shipcode,src_cruiseline_shipcode,is_active from DatajoinView where lkp_embark_port is null and lkp_cruiseline_shipcode is null""")
	df1.createOrReplaceTempView("df1Temp")
	
	println("--------reading data from lkup table----------")
	df1.show(10,false)
	
	//Data which got deleted from MXP table and presnt in lookup
	val df2 = spark.sql("""select lkp_embark_port,src_embark_port,lkp_cruiseline_shipcode,src_cruiseline_shipcode,is_active from DatajoinView where src_embark_port is null and src_cruiseline_shipcode is null""")
	df2.createOrReplaceTempView("df2Temp")
	
	println("--------reading data deleted from src table----------")
	df2.show(10,false)
	
	if ((! df1.head(1).isEmpty) || (! df2.head(1).isEmpty) )
	{
		if (! df1.head(1).isEmpty)
		{
			println("-------inserting data to lkup table----------")
			spark.sql("insert into shipdw.hvtb_lkp_embark_ports select src_embark_port,src_cruiseline_shipcode,is_active,current_date() as etl_ld_dt from df1Temp")
		}

		if (!df2.head(1).isEmpty)
		{
			println("-------reading data not present in lkuptable----------")
			val deleteDF= spark.sql("""select * from lkptbldata where embark_port and cruiseline_shipcode not in (select lkp_embark_port,lkp_cruiseline_shipcode from df2Temp)""")
			deleteDF.write.mode("Overwrite").parquet(temploc)
			val ReadDF =spark.read.parquet(temploc)
			ReadDF.createOrReplaceTempView("finaltmp")
			ReadDF.write.mode("Overwrite").parquet(targettableloc)
			println("------refreshing table-------")
			spark.sql("""Refresh table shipdw.hvtb_lkp_embark_ports""")

 

		}
		//val finalDF= spark.sql("""select embark_port,cruiseline_shipcode,is_active from shipdw.hvtb_lkp_embark_ports""")
		val finalDF= spark.sql("""select distinct embark_port,is_active from shipdw.hvtb_lkp_embark_ports""")
		
		println("-----printing final df-------")
		finalDF.printSchema()
		//finalDF.count()
		
	
		//adding filename logic here
		finalDF.coalesce(1)
		.write.format("csv")
		.option("header", "true")
		.mode("overwrite")
		.save(targetcsvloc)

}
}
	
}