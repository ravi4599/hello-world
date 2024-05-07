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

object VirtualQFactLoad {
    
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
      
     val whereClause = s""" where vq.batchtime>= '$batch_start_tme' and vq.batchtime<='$batch_end_tme' and vq.part_date >='$part_read_start' and vq.part_date <='$part_read_end'"""
      
     var virtualqDf = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").trim()+whereClause) 

     
     
    if(!virtualqDf.head(1).isEmpty){

     var virtualHstryDf = spark.sql(spark.sparkContext.getConf.get("spark.virtual.history.sql").trim())
     
     
    var  virtualHstryDf_pivot =   virtualHstryDf.select("virtualqueueid", "status","statuschangeddate").groupBy(col("virtualqueueid")).pivot("status").agg(first(col("statuschangeddate"),ignoreNulls = true))
    
    
    val missingCols = "C,N,NS,R,".split(",") 
    
    def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
    
       val selectColumns = virtualHstryDf_pivot.columns.toSeq
       for (col <- missingCols) {
  
         
         if(!selectColumns.contains(col)){
        
          println("Missing cols")
          println(col)
          
           virtualHstryDf_pivot= virtualHstryDf_pivot.withColumn(s"$col",lit(null).cast(TimestampType))
       }
       }
    val colsToDrop = Seq("C","N","NS","R","W")  
    virtualHstryDf_pivot = virtualHstryDf_pivot.withColumnRenamed("C","queue_completed_datetime")
                                               .withColumnRenamed("N","queue_notified_datetime")
                                               .withColumn("queue_canceled_datetime", when(virtualHstryDf_pivot.col("NS").isNull , virtualHstryDf_pivot.col("R") ).otherwise(virtualHstryDf_pivot.col("NS")))
                                               .drop(colsToDrop :_*)
                                               
                                        
     
     var joinedVirtualDf =  virtualqDf.join(virtualHstryDf_pivot, virtualqDf.col("virtualqueueid") === virtualHstryDf_pivot.col("virtualqueueid"), "left")                                    
                                     .drop(virtualHstryDf_pivot.col("virtualqueueid"))
                                      .withColumn("queue_total_wait_time_seconds",when(virtualHstryDf_pivot.col("queue_notified_datetime").isNotNull,
   unix_timestamp(virtualHstryDf_pivot.col("queue_notified_datetime")) - unix_timestamp(virtualqDf.col("queue_joined_datetime")) ).otherwise(0))
   

   
   var finalVirtualDf = joinedVirtualDf.select("voyage_id","ship_code","voyage_skey","virtual_queue_detail_skey","virtual_queue_definition_skey","person_skey",
                                            "queue_joined_datetime","queue_canceled_datetime","queue_notified_datetime","queue_completed_datetime","queue_total_wait_time_seconds" ,"batchtime", "part_dt"  )
     
        log.info("Loading Virtual Queue fact table")
        loadDimFact(spark: SparkSession, finalVirtualDf)
        
      }
     
   log.info("Updating Metadata framework")
   ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
    }
     catch{
      
       case e: SQLException => { 
         ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
         log.info("******************in the catch of Virtual Queue Fact Load ******************");
         e.printStackTrace(); 
         throw new Exception("SQL Exception..please check the stacktrace", e); 
         
       }

      
        println("#----------------------------Process Has Failed---------------------------#")
        System.exit(1)
        spark.stop()
    }
   }
  
  
}