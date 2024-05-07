package com.virginvoyages.dimension
import java.util.Date


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

object DataQualityTest{
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
 

  
   try{
  
  import spark.sqlContext.implicits._ 
 
  
  
 
    val testCaseDF = spark.sql(s"""select * from (select * from     (select * , row_number() over(partition by testcase_id,testcase_name order by insert_timestamp  desc) as rn from shipdw.hvtb_nbx_dq_testcases)ttc where ttc.rn=1 and ttc.active_flag=true) tc inner join  (select connection_type as src_connection_type, connection_name as src_connection_name,connection_details as src_connection_details  from     (select * , row_number() over(partition by connection_id,connection_name order by insert_timestamp  desc) as rn from shipdw.hvtb_nbx_dq_connection)scc where scc.rn=1 and scc.active_flag=true) sc on(lower(tc.src_connection_name)==lower(sc.src_connection_name)) inner join (select connection_name as tgt_connection_name,connection_type as tgt_connection_type,connection_details as tgt_connection_details  from     (select * , row_number() over(partition by connection_id,connection_name order by insert_timestamp  desc) as rn from shipdw.hvtb_nbx_dq_connection)tcc where tcc.rn=1 and tcc.active_flag=true) tec  on(lower(tc.tgt_connection_name)==lower(tec.tgt_connection_name)) """)
    

  



  println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxx-------------------------------Data Quality------------------------------------xxxxxxxxxxxxxxxxxxxxxx")
  
  
  
  
   }

  catch{  
     
     
  
    case e: SQLException => {log.info("******************in the catch of sale_detail Dimension Load ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace",e) ;}  

    case e: Exception => {log.info("******************in the catch of sale_detail Dimension Load ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace",e) ;}
    println("#----------------------------Process Has Failed---------------------------#")
    System.exit(1)
    //exit(1);
    }
	  

  
	  
   
   
   
    }
  
}