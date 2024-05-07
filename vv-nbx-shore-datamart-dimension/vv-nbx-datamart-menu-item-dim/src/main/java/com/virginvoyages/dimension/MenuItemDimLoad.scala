package com.virginvoyages.dimension

import com.virginvoyages.metadataframework.ManageMetadata
import java.util.Properties

import java.sql.Timestamp

import org.apache.spark.SparkContext

import org.apache.spark.sql.types.{ StructType, StructField, TimestampType, StringType, IntegerType, DateType, LongType };

import org.apache.spark.sql.expressions.Window

import org.apache.spark.SparkConf

import org.apache.log4j.LogManager

import org.apache.log4j.Level

import org.apache.spark.sql.SparkSession

import java.sql.SQLException

import scala.collection.JavaConversions._

import org.apache.spark.sql.functions._

import scala.util.Try

import org.apache.spark.sql.DataFrame

import java.time.{ ZonedDateTime, ZoneId }

import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{ Column, DataFrame }

import scala.collection.mutable.ArrayBuffer

import java.sql.DriverManager

import java.sql.Connection

import org.apache.spark.sql.SaveMode

//XML validator imports
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import org.xml.sax.SAXException;
import java.io._
import java.nio.charset.StandardCharsets
import org.apache.spark.SparkFiles
import scala.util.Try
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

import com.virginvoyages.scd.ChangeDataCapture.loadDimFact

object MenuItemDimLoad {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    
    import spark.implicits._
    val sc = spark.sparkContext
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    
/*************************************calling metadata framework*********************************************/    
    val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
    metadata.productIterator.foreach(println)
    val batch_id1 = metadata._1
    val batch_instance_id1 = metadata._2
    val batch_start_tme = metadata._3
    val batch_end_tme = metadata._4
    val part_read_start = metadata._5
    val part_read_end = metadata._6
    val start_execution_time = metadata._7
    val part_write_date = metadata._8

    try {
      
      import spark.sqlContext.implicits._      
     
      //val whereClause = s""" where 1=1 """
      val whereClause = s""" where a.batchtime>= '$batch_start_tme' and a.batchtime<='$batch_end_tme' and a.part_date>='$part_read_start' and a.part_date<='$part_read_end'"""
      println(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
      log.info(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
   
      var menuItem_Dim = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").trim()+ whereClause )
      log.info(s"""#---------------------------Query execution menuItem_Dim ----------------#""")
     // menuItem_Dim.show(false)      

       
      val required_columns = spark.sparkContext.getConf.get("spark.source.columns").split(",")
      val missing_columns = ArrayBuffer[String]()
      val avaliable_columns = ArrayBuffer[String]()
      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
      
      for (col <- required_columns) {

        if (hasColumn(menuItem_Dim, col)) {
          println(col, "column exists", avaliable_columns.toString)
          println("column exists", avaliable_columns.length)
        } else {
          println(col, "column missing", missing_columns.length)
          println(missing_columns.length, "length")
        }

      }
      
      print(missing_columns, "Here are the missing columns")
      val stage_final_df = missing_columns.foldLeft(menuItem_Dim)((df, c) => df.withColumn(s"$c", lit(null)))
      //stage_final_df.show(false)
      
      loadDimFact(spark: SparkSession, stage_final_df)
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)

    } catch {
            case e: SQLException => { ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("******************inside catch for Dimension Load ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace", e); }
            case e: Exception =>  { ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("********************inside catch for Dimension Load ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace", e); }
            println("#----------------------------Process Has Failed---------------------------#")
        System.exit(1)      
    }

    //stage_final_df.show

  }

}
