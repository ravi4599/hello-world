package com.virginvoyages.fact
import java.util.Date
import com.virginvoyages.metadataframework.ManageMetadata

import com.virginvoyages.scd.ChangeDataCapture.loadDimFact

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
//import com.mart.dim.ChangeDataCapture.slowlyChangingDimension
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


object DashboardFactLoad {
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
  
  def main(args:Array[String]) ={
    def getSparkSession() = {
      val spark = SparkSession
        .builder()
        .enableHiveSupport()
        .getOrCreate()
        
        spark
    }
    val spark = getSparkSession()
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    
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
      println("#---------------------------Starting the Execution------------------#")

      
      val dashboardDF = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").trim())
      val required_columns = spark.sparkContext.getConf.get("spark.source.columns").split(",")
      val missing_columns = ArrayBuffer[String]()
      val avaliable_columns = ArrayBuffer[String]()
      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
      
            for (col <- required_columns) {

        if (hasColumn(dashboardDF, col)) {
          log.info(col, "column exists", avaliable_columns.toString, avaliable_columns.length)
          avaliable_columns.append(col)
        } else {
          println(col, "column missing", missing_columns.length)
          log.info(col, "column exists", missing_columns.toString, missing_columns.length)
          println("column missing", missing_columns)
          missing_columns.append(col)
        }
        
        }

      print(missing_columns, "Here are the missing columns")
      val stage_final_df = missing_columns.foldLeft(dashboardDF)((df, c) =>
        df.withColumn(s"$c", lit(null)))
        stage_final_df.printSchema()
        stage_final_df.show()
    println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxx-------------------------------Source-Stage Dashboard Fact------------------------------------xxxxxxxxxxxxxxxxxxxxxx")
    
 if(!stage_final_df.head(1).isEmpty){
      loadDimFact(spark: SparkSession, stage_final_df)}
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
    } catch {

      case e: SQLException => { ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("******************in the catch of Dashboard Fact Load ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace", e); }

      case e: Exception =>
        { ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("******************in the catch of Dashboard Fact Load ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace", e); }
        println("#----------------------------Process Has Failed---------------------------#")
        System.exit(1)
    }

  }

}