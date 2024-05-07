package com.virginvoyages.shore.fact
import com.virginvoyages.metadataframework.ManageMetadata
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import org.apache.spark.sql.types.{ StructType, StructField, TimestampType, StringType, IntegerType, DateType, LongType };
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact

object DeviceRevenueLoad {
  
   def main(args: Array[String]) {
    

    val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
  
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
    
    try{
      
      val whereClause = s""" where ems.batchtime>= '$batch_start_tme' and ems.batchtime<='$batch_end_tme' and ems.part_dt >='$part_read_start' and ems.part_dt <='$part_read_end'"""
      
     
      var deviceDf = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").trim()+whereClause)
      
      deviceDf.show(10,false)
      
      if(!deviceDf.head(1).isEmpty){
      
      val required_columns = spark.sparkContext.getConf.get("spark.source.columns").split(",")

      val missing_columns = ArrayBuffer[String]()
      val avaliable_columns = ArrayBuffer[String]()
      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
     
      
      for (col <- required_columns) {

        if (hasColumn(deviceDf, col)) {
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
      
      val finalDf = missing_columns.foldLeft(deviceDf)((df, c) =>
      df.withColumn(s"$c", lit(null)))
      
       
       log.info("Calling scd framework and loading fact table")
       loadDimFact(spark: SparkSession, finalDf)
     }
         
      log.info("Updating Metadata framework")
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
    }
    
    catch{
      
      case e: SQLException => { 
         ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
         log.info("******************in the catch of Device Revenue Fact Load ******************");
         e.printStackTrace(); 
         throw new Exception("SQL Exception..please check the stacktrace", e); 
         
       }

      
        println("#----------------------------Process Has Failed---------------------------#")
        System.exit(1)
        spark.stop()
      
      
    }
    
  }
  
}