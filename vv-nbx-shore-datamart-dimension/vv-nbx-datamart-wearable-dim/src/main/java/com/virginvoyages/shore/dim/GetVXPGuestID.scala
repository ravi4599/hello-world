package com.virginvoyages.shore.dim
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
import com.virginvoyages.scd.GetXrefData.srcReferenceTypeTotgtReferenceType


object GetVXPGuestID {

def main(args: Array[String]): Unit = {
  
   val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
  
  val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
  
  val sc = spark.sparkContext
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    
  var sourceQueryDf = spark.sql(spark.sparkContext.getConf.get("spark.source.query").trim())  
  var reservationDf = spark.sql(spark.sparkContext.getConf.get("spark.reservation.query").trim())
  var resGuestDf = spark.sql(spark.sparkContext.getConf.get("spark.reservationguest.query").trim())
  var srcGuestRelDf = spark.sql(spark.sparkContext.getConf.get("spark.guestrel.query").trim())
  
  
  var incrementalDf = sourceQueryDf.select("src_guest_id","client_id","src_res_id").except(srcGuestRelDf.select("src_guest_id","client_id","src_res_id"))
  
  if(!incrementalDf.head(1).isEmpty){
  
  var sourceDf =  incrementalDf.join(sourceQueryDf,incrementalDf.col("src_guest_id") === sourceQueryDf("src_guest_id") && 
                                 incrementalDf.col("client_id") === sourceQueryDf("client_id") && incrementalDf.col("src_res_id") === sourceQueryDf("src_res_id"),"left")
                .select(incrementalDf.col("src_guest_id"),incrementalDf.col("client_id"),sourceQueryDf.col("sail_id"),
                 sourceQueryDf.col("sail_date_from"),sourceQueryDf.col("sail_date_to"),incrementalDf.col("src_res_id"))
  
  
  var getVxpResId = sourceDf.join(reservationDf, sourceDf.col("src_res_id") === reservationDf.col("reservationnumber"), "left")
                     .select(sourceDf.col("*"), reservationDf.col("reservationid"))
                     .withColumnRenamed("reservationid", "vxp_res_id")
                     .withColumnRenamed("client_id", "sourceID")
                     .withColumn("srcReferenceType", lit("Client"))
                     .withColumn("tgtReferenceType", lit("VXP-Guest"))
  
  val getVxpGuestId = srcReferenceTypeTotgtReferenceType(spark, getVxpResId)  
  
  
  val windowSpec = Window.orderBy("src_guest_id")
  
  val maxGuestRelId = srcGuestRelDf.select(coalesce(max("sw_vxp_guest_rel_id"), lit(0) )).collect()(0).getInt(0)   
  
  print(maxGuestRelId)
  
  var getVxpResGuestId = getVxpGuestId.join(resGuestDf , getVxpGuestId.col("vxp_res_id") === resGuestDf.col("reservationid") && 
                      getVxpGuestId.col("targetID") === resGuestDf.col("guestid") ,"left" )
                      .select(getVxpGuestId.col("*"),resGuestDf.col("reservationguestid"))
                      .withColumnRenamed("targetID", "vxp_guest_id")
                      .withColumnRenamed("reservationguestid", "vxp_reservation_guest_id")
                      .withColumnRenamed("sourceID", "client_id")                      
                      .withColumn("rowNumber", row_number() over windowSpec)
                      .withColumn("sw_vxp_guest_rel_id", col("rowNumber").cast(IntegerType) + maxGuestRelId)
                      .withColumn("loadtimestamp", current_timestamp())

    
   val tgtGuestRelDf = getVxpResGuestId.select("sw_vxp_guest_rel_id", "src_guest_id","client_id","vxp_guest_id","sail_date_from","sail_date_to","src_res_id","vxp_res_id","vxp_reservation_guest_id","loadtimestamp")
   
   tgtGuestRelDf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.table"))
   
  }
  
}
  
}