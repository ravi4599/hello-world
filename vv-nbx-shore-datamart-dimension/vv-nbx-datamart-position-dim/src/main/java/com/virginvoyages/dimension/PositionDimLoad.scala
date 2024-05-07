package com.virginvoyages.dimension
import java.util.Date
//import com.virginvoyages.metadataframework.ManageMetadata
import com.virginvoyages.metadataframework.ManageMetadata
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact

import java.util.Properties
import java.sql.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StructType, StructField, TimestampType,StringType, IntegerType,DateType,LongType};
import org.apache.spark.sql.expressions.Window
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import scala.collection.JavaConversions._
//import com.mart.dim.ChangeDataCapture.slowlyChangingDimension
import org.apache.spark.sql.functions._
import scala.util.Try
import org.apache.spark.sql.DataFrame
import java.time.{ZonedDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{Column, DataFrame}
import scala.collection.mutable.ArrayBuffer
import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.sql.SaveMode

object PositionDimLoad {
    val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
 

  def main(args: Array[String]) {
   def getSparkSession() =
  {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
     
      spark

  }
  val spark = getSparkSession()
  val sc = spark.sparkContext
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  //
  //spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap) 
  val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
  /*************************************calling metadata framework*********************************************/  		
 
  
  val metadata=ManageMetadata.fetchBatchTime( sparkConfiguration,spark)        
 	metadata.productIterator.foreach(println) 
 	   val batch_id1=metadata._1
     val batch_instance_id1=metadata._2
     val batch_start_tme=metadata._3
     val batch_end_tme=metadata._4
     val part_read_start=metadata._5
     val part_read_end=metadata._6
     val start_execution_time=metadata._7
     val part_write_date=metadata._8 
  
   try{
  
  import spark.sqlContext.implicits._ 
  
  val whereClause=s""" where a.batchtime>= '$batch_start_tme' and a.batchtime<='$batch_end_tme' and a.part_date>='$part_read_start' and a.part_date<='$part_read_end'"""
  println(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
  log.info(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
  
  val positionDF = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").trim()+whereClause)//.where($"batchtime" >= lit(batch_start_tme).cast(TimestampType) && $"batchtime" <= lit(batch_end_tme).cast(TimestampType) && $"part_date" >= lit(part_read_start).cast(DateType) && $"part_date" <= lit(part_read_end).cast(DateType))
  
  val lookupitemsDF = spark.sql(spark.sparkContext.getConf.get("spark.source.looup.items.sql").trim())
  
  val positionjoinDF = positionDF.join(lookupitemsDF,(positionDF.col("position_dept_id") === lookupitemsDF.col("lookup_item_id") && lookupitemsDF.col("lookup_sub_category_id") === lit(62)),"left")
  
  var positionfinalDF = positionjoinDF.select("voyage_id","position_id","position_dept_id","position_name","lookup_item_name","position_code","src_active_flag","batchtime")
  
  positionfinalDF = positionfinalDF.withColumnRenamed("lookup_item_name", "position_dept_name")
  
  

  println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxx-------------------------------Source-Stage position------------------------------------xxxxxxxxxxxxxxxxxxxxxx")
  
  
  positionfinalDF.show(20,false)
  
  if(!positionfinalDF.take(1).isEmpty){
    
  loadDimFact(spark: SparkSession, positionfinalDF)}
   ManageMetadata.updateStatus(batch_instance_id1,batch_id1,"Successful",spark)
  
   }

  catch{  
     
     
  
    case e: SQLException => { ManageMetadata.updateStatus(batch_instance_id1,batch_id1,"Failed",spark);log.info("******************in the catch of sale_detail Dimension Load ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace",e) ;}  

    case e: Exception => { ManageMetadata.updateStatus(batch_instance_id1,batch_id1,"Failed",spark);log.info("******************in the catch of sale_detail Dimension Load ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace",e) ;}
    println("#----------------------------Process Has Failed---------------------------#")
    System.exit(1)
    //exit(1);
    }
	  

  
	  
   
   
   
    }
  
}