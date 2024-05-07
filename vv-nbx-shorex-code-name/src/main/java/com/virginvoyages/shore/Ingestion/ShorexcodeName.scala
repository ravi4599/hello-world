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

object ShorexcodeName {
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
	
	println("------------Reading data from activity table--------------")
	
	val ShorexCodeNameDF = spark.sql("""select code ,name,isdeleted from(select code ,name,isdeleted,row_number() over (partition by code order by lastmodifieddate desc) as rownum from shipdw.hvtb_parse_vxp_ars_activity where activitygroupcode = 'PA' and isdeleted = false) outerQry where rownum = 1""")
	
	println("-----------printing counts from activity table")
	
	ShorexCodeNameDF.printSchema()
	//ShorexCodeNameDF.count()
	
    ShorexCodeNameDF.createOrReplaceTempView("ShorexCodeNamedata")
	
	println("--------Reading data from target table")
	
	val TgtlkpDF = spark.sql("""select code,name,isdeleted from shipdw.hvtb_lkp_shorex_code_name""")
	
	println("---------printing schema for target table------------")
	
	TgtlkpDF.printSchema()

	TgtlkpDF.createOrReplaceTempView("lkptbldata")
	
	val DatajoinDF = spark.sql("""select lkpShorex.lkp_name,ShorexCode.name as shorex_name, ShorexCode.code,ShorexCode.isdeleted from ShorexCodeNamedata ShorexCode full outer join (select code as lkp_code,name as lkp_name,isdeleted as lkp_isdeleted from lkptbldata) lkpShorex on ShorexCode.name = lkpShorex.lkp_name""") 
	DatajoinDF.dropDuplicates
	DatajoinDF.createOrReplaceTempView("DatajoinView")
	
	
	//Data not present in lookup but present in src mxp table (need to insert)
	val df1 = spark.sql("""select lkp_name,shorex_name,code,isdeleted from DatajoinView where lkp_name is null""")
	df1.createOrReplaceTempView("df1Temp")
	
	println("--------reading data from lkup table----------")
	df1.show(10,false)
	
	//Data which got deleted from MXP table and presnt in lookup
	val df2 = spark.sql("""select lkp_name,shorex_name,code,isdeleted from DatajoinView where shorex_name is null""")
	df2.createOrReplaceTempView("df2Temp")
	
	println("--------reading data deleted from MXP table----------")
	df2.show(10,false)
	
	if ((! df1.head(1).isEmpty) || (! df2.head(1).isEmpty) )
	{
		if (! df1.head(1).isEmpty)
		{
			println("-------inserting data to lkup table----------")
			spark.sql("insert into shipdw.hvtb_lkp_shorex_code_name select shorex_name,code,isdeleted,current_date() as etl_ld_dt from df1Temp ")
		}

		if (!df2.head(1).isEmpty)
		{
			println("-------reading data not present in lkuptable----------")
			val deleteDF= spark.sql("""select * from lkptbldata where name not in (select lkp_name from df2Temp)""")
			deleteDF.write.mode("Overwrite").parquet(temploc)
			val ReadDF =spark.read.parquet(temploc)
			ReadDF.createOrReplaceTempView("finaltmp")
			ReadDF.write.mode("Overwrite").parquet(targettableloc)
			println("------refreshing table-------")
			spark.sql("""Refresh table shipdw.hvtb_lkp_shorex_code_name""")

 

		}
		val finalDF= spark.sql("""select name,code,isdeleted from shipdw.hvtb_lkp_shorex_code_name""")
		
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