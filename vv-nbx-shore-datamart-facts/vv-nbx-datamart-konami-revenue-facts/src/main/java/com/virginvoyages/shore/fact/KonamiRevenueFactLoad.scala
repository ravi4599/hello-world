package com.virginvoyages.shore.fact
import java.util.Date

import com.virginvoyages.metadataframework.ManageMetadata
import java.net.UnknownHostException


import org.apache.spark.broadcast.Broadcast
import java.text.SimpleDateFormat

import scala.util.parsing.json._
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
import com.virginvoyages.scd.GetXrefData.srcReferenceTypeTotgtReferenceType

object KonamiRevenueFactLoad {

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
    //spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
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
      //val env = spark.sparkContext.getConf.get("spark.api.env")
      //val whereClause = s""" where 1=1 """
      val whereClause = s""" where konami_src.batchtime>= '$batch_start_tme' and konami_src.batchtime<='$batch_end_tme' and konami_src.part_date>='$part_read_start' and konami_src.part_date<='$part_read_end'"""
      println(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
      log.info(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")

      import spark.sqlContext.implicits._
      println("#---------------------------Starting the Execution------------------#")
      spark.conf.set("spark.sql.crossJoin.enabled", true)
      //val whereClause = s""" where batchtime>= '$batchStartTme' and batchtime<='$batchEndTme' and part_date>='$partReadStart' and part_date<='$partReadEnd'"""
      //println(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
      //log.info(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")

      import spark.sqlContext.implicits._
      println("#---------------------------Starting the Execution------------------#")
      val query = spark.sparkContext.getConf.get("spark.source.sql").replace("*whereclause*", whereClause)

      val stageRevenueDF = spark.sql(query).as("konami_fact")
      println(query)
      log.info(query)

      log.info("#######################################################")

      println("#-----------------------------------------------Stage XREF DF----------------------------------#")

      //val final_df = stageRevenueDF.join(client_df, col("cdf.seaware_id") === col("ptnid"), "left").as("finalVXPGuestDf")
      //final_df.show(20, false)
      val interimDF = srcReferenceTypeTotgtReferenceType(spark, stageRevenueDF)
        .selectExpr("targetID as vxp_guest_id", "'N/A' as booking_reference_number", "'KONAMI' as source_type", "ship_code", "voyage_skey", "voyage_id", "sale_detail_skey", "pos_item_skey", "folio_item_skey", "cast(ptnid as string)as ptnid", "itinerary_skey", "outlet_skey", "activity_skey", "sale_date_skey", "sale_time_skey", "device_skey", "-1 as charge_skey", "-1 as wearable_skey", "time_on_device_seconds", "avg_wager_amount", "casino_game_dph", "theoritical_winnings", "sale_quantity", "item_list_price_amount", "debit_amount", "credit_amount", "tax_amount", "discount_amount", "discount_value", "cost_of_goods", "manual_adjustment_amount", "total_collected_sales_amount", "total_shared_revenue_amount")
      val person_dim_df = spark.sql("select person_skey,person_guid from shipdw.hvtb_mart_dim_person")

      val stageKonamiFactDF = interimDF.join(person_dim_df, lower(col("person_guid")) === lower(col("vxp_guest_id")), "left")
        .select("vxp_guest_id", "person_skey", "booking_reference_number", "source_type", "ship_code", "konami_fact.voyage_skey", "voyage_id", "sale_detail_skey", "pos_item_skey", "folio_item_skey", "ptnid", "itinerary_skey", "outlet_skey", "activity_skey", "sale_date_skey", "sale_time_skey", "device_skey", "charge_skey", "wearable_skey", "time_on_device_seconds", "avg_wager_amount", "casino_game_dph", "theoritical_winnings", "sale_quantity", "item_list_price_amount", "debit_amount", "credit_amount", "tax_amount", "discount_amount", "discount_value", "cost_of_goods", "manual_adjustment_amount", "total_collected_sales_amount", "total_shared_revenue_amount")

      println("#-----------------------------------------------Stage person Dim----------------------------------#")
      val person_count_query = spark.sparkContext.getConf.get("spark.persondimsource.sql")
      val personcount_df = spark.sql(person_count_query).as("person_dim")

      val stagefinaldf = stageKonamiFactDF.join(personcount_df, col("person_dim.person_guid") === upper(col("vxp_guest_id")), "left").as("finalDF")
        .selectExpr("booking_reference_number", "source_type", "ship_code", "voyage_skey", "voyage_id", "sale_detail_skey", "pos_item_skey", "folio_item_skey", "ptnid", "itinerary_skey", "outlet_skey", "activity_skey", "sale_date_skey", "sale_time_skey", "device_skey", "person_skey", "charge_skey", "wearable_skey", "time_on_device_seconds", "avg_wager_amount", "casino_game_dph", "theoritical_winnings", "sale_quantity", "item_list_price_amount", "debit_amount", "credit_amount", "tax_amount", "discount_amount", "discount_value", "cost_of_goods", "manual_adjustment_amount", "total_collected_sales_amount", "total_collected_sales_amount/(guest_count*voyage_length) as total_collected_sales_ap", "total_shared_revenue_amount", "total_shared_revenue_amount/(guest_count*voyage_length) as total_shared_revenue_ap")

      println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxx-------------------------------Source-Stage Konami Revenue Fact------------------------------------xxxxxxxxxxxxxxxxxxxxxx")

      //stagefinaldf.show(20, false)
      loadDimFact(spark: SparkSession, stagefinaldf)
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)

    } catch {

      case e: SQLException => { ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("******************in the catch of Folio  Revenue Factn Load ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace", e); }

      case e: Exception =>
        { ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("******************in the catch of Folio  Revenue Factn Load ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace", e); }
        println("#----------------------------Process Has Failed---------------------------#")
        log.info("#----------------------------Process Has Failed---------------------------#")
        System.exit(1)

    }

  }

}