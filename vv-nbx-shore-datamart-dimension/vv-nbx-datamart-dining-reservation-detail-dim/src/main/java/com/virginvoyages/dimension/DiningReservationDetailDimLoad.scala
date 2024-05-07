package com.virginvoyages.dimension

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
import org.apache.spark.sql.functions.unix_timestamp

object DiningReservationDetailDimLoad {
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
      
      val whereClause = s""" where a.batchtime>= '$batch_start_tme' and a.batchtime<='$batch_end_tme' and a.part_date >='$part_read_start' and a.part_date <='$part_read_end'"""
      
     
     var sourceDf = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").trim()+whereClause)   
     
   
     //sourceDf.show(10,false)
     
     //sourceDf.select("resstatus").distinct().show(10,false)
     
var resactionDf = spark.sql(spark.sparkContext.getConf.get("spark.resaction.sql").trim())
     

      if(!sourceDf.head(1).isEmpty){
       
      var res_created_pivot = resactionDf.select("reservation_id","resstatus","source").groupBy(col("reservation_id")).pivot("resstatus").agg(first(col("source"),ignoreNulls = true))
     // var res_created_utc_pivot = resactionDf.select("reservation_id","resstatus","reservation_created_utc").groupBy(col("reservation_id")).pivot("resstatus").agg(first(col("reservation_created_utc"),ignoreNulls = true))
     
      var joinedDf = sourceDf.join(res_created_pivot, sourceDf.col("dining_reservation_id") === res_created_pivot.col("reservation_id") ,"left")
                      .withColumnRenamed("upcoming", "reservation_upcoming_source")
                      .withColumnRenamed("waitlist", "reservation_waitlist_source")
                      .withColumnRenamed("seated", "reservation_seated_source")
                      .withColumnRenamed("completed", "reservation_completed_source")
                      .withColumnRenamed("cancelled", "reservation_canceled_source")
                      .drop(res_created_pivot.col("reservation_id"))  
     
      val colsToDrop = Seq("reservation_id","resstatus","source")
                      
 /*     var finalJoinedDf   = joinedDf.join(res_created_utc_pivot,joinedDf.col("reservation_id") === res_created_utc_pivot.col("reservation_id") ,"left")
                      .withColumnRenamed("upcoming", "reservation_upcoming_datetime_utc")
                      .withColumnRenamed("waitlist", "reservation_waitlist_datetime_utc")
                      .withColumnRenamed("seated", "reservation_seated_datetime_utc")
                      .withColumnRenamed("completed", "reservation_completed_datetime_utc")
                      .withColumnRenamed("canceled", "reservation_canceled_datetime_utc")
                      .drop(colsToDrop :_*)
                      .dropDuplicates() */
                      
     
                      
       val missingCols = "voyage_id,dining_reservation_id,booking_link_id,reservation_upcoming_source,reservation_waitlist_source,reservation_seated_source,reservation_completed_source,reservation_canceled_source,reservation_status,booking_source,booked_by_type,anon_guest_count".split(",")
       
       def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
       val selectColumns = joinedDf.columns.toSeq
       for (col <- missingCols) {
  
         
         if(!selectColumns.contains(col)){
          
          println("Missing cols")
          println(col)
          
           joinedDf= joinedDf.withColumn(s"$col",lit(null))
       }
       }
       
      println("joinedDf_Schema ")
      joinedDf.printSchema
                    
   /*    finalJoinedDf=   finalJoinedDf.withColumn("wait_time_in_seconds",when(unix_timestamp(col("reservation_seated_datetime")) > unix_timestamp(col("reservation_datetime")) , unix_timestamp(col("reservation_seated_datetime")) - unix_timestamp(col("reservation_datetime"))   ).otherwise(0))
                      .withColumn("table_turn_time_in_seconds",when(col("reservation_seated_datetime").isNotNull and col("reservation_completed_datetime").isNotNull , unix_timestamp(col("reservation_completed_datetime")) - unix_timestamp(col("reservation_seated_datetime")) ).otherwise(0))
                      .withColumn("total_party_size", col("anon_guest_count") + col("guest_count"))
                      .withColumnRenamed("guest_count", "reservation_number_of_guests") */
                      
  
      val diningDf = joinedDf.select("voyage_id","dining_reservation_id","booking_link_id","reservation_upcoming_source","reservation_waitlist_source","reservation_seated_source","reservation_completed_source","reservation_canceled_source","reservation_status","booking_source","booked_by_type","anon_guest_count","batchtime","part_date")
        
     
       
       log.info("Loading Dining Reservation Detail Dim table")
       loadDimFact(spark: SparkSession, diningDf)
     }
         

      log.info("Updating Metadata framework")
    ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
      
      
    }
    
    catch{
      
       case e: SQLException => { 
         ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
         log.info("******************in the catch of Dining Reservation Dim Load ******************");
         e.printStackTrace(); 
         throw new Exception("SQL Exception..please check the stacktrace", e); 
         
       }

      
        println("#----------------------------Process Has Failed---------------------------#")
        System.exit(1)
        spark.stop()
    }
  }
}