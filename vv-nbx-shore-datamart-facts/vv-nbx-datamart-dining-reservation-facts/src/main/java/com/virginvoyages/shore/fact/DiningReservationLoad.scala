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
import org.apache.spark.sql.functions.unix_timestamp

object DiningReservationLoad {
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
      
     val whereClause = s""" where res.batchtime>= '$batch_start_tme' and res.batchtime<='$batch_end_tme' and res.part_date >='$part_read_start' and res.part_date <='$part_read_end'"""
      
     
     var sourceDf = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").trim()+whereClause)   
     
   
     //sourceDf.show(10,false)
     
     var resactionDf = spark.sql(spark.sparkContext.getConf.get("spark.resaction.sql").trim())
     

      if(!sourceDf.head(1).isEmpty){
        
      
      
      var res_created_pivot = resactionDf.select("reservation_id","resstatus","reservation_created").groupBy(col("reservation_id")).pivot("resstatus").agg(first(col("reservation_created"),ignoreNulls = true))
      var res_created_utc_pivot = resactionDf.select("reservation_id","resstatus","reservation_created_utc").groupBy(col("reservation_id")).pivot("resstatus").agg(first(col("reservation_created_utc"),ignoreNulls = true))
      
      var joinedDf = sourceDf.join(res_created_pivot, sourceDf.col("reservation_id") === res_created_pivot.col("reservation_id") ,"left")
                      .withColumnRenamed("upcoming", "reservation_upcoming_datetime")
                      .withColumnRenamed("waitlist", "reservation_waitlist_datetime")
                      .withColumnRenamed("seated", "reservation_seated_datetime")
                      .withColumnRenamed("completed", "reservation_completed_datetime")
                      .withColumnRenamed("cancelled", "reservation_canceled_datetime")
                      .drop(res_created_pivot.col("reservation_id"))
     
      
      
      
      
      val colsToDrop = Seq("reservation_id","resstatus","deleted","reservation_created","reservation_created_utc")
                      
      var finalJoinedDf   = joinedDf.join(res_created_utc_pivot,joinedDf.col("reservation_id") === res_created_utc_pivot.col("reservation_id") ,"left")
                      .withColumnRenamed("upcoming", "reservation_upcoming_datetime_utc")
                      .withColumnRenamed("waitlist", "reservation_waitlist_datetime_utc")
                      .withColumnRenamed("seated", "reservation_seated_datetime_utc")
                      .withColumnRenamed("completed", "reservation_completed_datetime_utc")
                      .withColumnRenamed("cancelled", "reservation_canceled_datetime_utc")
                      .drop(colsToDrop :_*)
                      //.dropDuplicates()
                      
     
                      
       val missingCols = "reservation_upcoming_datetime,reservation_waitlist_datetime,reservation_seated_datetime,reservation_completed_datetime,reservation_canceled_datetime,reservation_upcoming_datetime_utc,reservation_waitlist_datetime_utc,reservation_seated_datetime_utc,reservation_completed_datetime_utc,reservation_canceled_datetime_utc".split(",") 
       def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
       val selectColumns = finalJoinedDf.columns.toSeq
       for (col <- missingCols) {
  
         
         if(!selectColumns.contains(col)){
          
          println("Missing cols")
          println(col)
          
           finalJoinedDf= finalJoinedDf.withColumn(s"$col",lit(null).cast(TimestampType))
       }
       }
       
                    
       finalJoinedDf=   finalJoinedDf.withColumn("wait_time_in_seconds",when(unix_timestamp(col("reservation_seated_datetime")) > unix_timestamp(col("reservation_datetime")) , unix_timestamp(col("reservation_seated_datetime")) - unix_timestamp(col("reservation_datetime"))   ).otherwise(0))
                      .withColumn("table_turn_time_in_seconds",when(col("reservation_seated_datetime").isNotNull and col("reservation_completed_datetime").isNotNull , unix_timestamp(col("reservation_completed_datetime")) - unix_timestamp(col("reservation_seated_datetime")) ).otherwise(0))
                      .withColumn("total_party_size", col("anon_guest_count") + col("guest_count"))
                      .withColumnRenamed("guest_count", "reservation_number_of_guests")
                      
  
      val diningDf = finalJoinedDf.select("voyage_id","voyage_skey","ship_code","dining_reservation_detail_skey","venue_skey",
          "reservation_booked_datetime_utc","reservation_datetime","reservation_upcoming_datetime",
          "reservation_waitlist_datetime","reservation_seated_datetime","reservation_completed_datetime",
          "reservation_canceled_datetime","reservation_upcoming_datetime_utc","reservation_waitlist_datetime_utc","reservation_seated_datetime_utc",
          "reservation_completed_datetime_utc","reservation_canceled_datetime_utc","wait_time_in_seconds","waitlist_delay","table_turn_time_in_seconds",
          "reservation_number_of_guests","total_party_size" ,"reservation_checkout_datetime_utc","party_size_seated","dining_session_skey","expected_duration","walk_ins","reservation_updated_datetime_utc", "batchtime" , "part_dt","public_id")
        
     
          
     

       
       log.info("Loading Dining Reservation fact table")
       loadDimFact(spark: SparkSession, diningDf)
     }
         

      log.info("Updating Metadata framework")
    ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
      
      
      
    }
    
    catch{
      
       case e: SQLException => { 
         ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
         log.info("******************in the catch of Dining Reservation Fact Load ******************");
         e.printStackTrace(); 
         throw new Exception("SQL Exception..please check the stacktrace", e); 
         
       }

      
        println("#----------------------------Process Has Failed---------------------------#")
        System.exit(1)
        spark.stop()
    }
  }
  
}