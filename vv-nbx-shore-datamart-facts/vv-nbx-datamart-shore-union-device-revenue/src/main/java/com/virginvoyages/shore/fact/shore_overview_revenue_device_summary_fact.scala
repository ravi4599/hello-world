package com.virginvoyages.shore.fact
import java.util.Date
import java.sql._;
import java.sql.SQLException;
import java.util.NoSuchElementException;
import org.apache.spark.sql.functions.hash
import java.util.Properties
import org.apache.spark.sql.SaveMode
import java.sql.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType, DateType, LongType };
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
import org.apache.spark.sql.functions._
//import scala.tools.scalap.Main
import scala.Array

object shore_overview_revenue_device_summary_fact {
  
    val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]) {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    try {

      if (args.length == 0) {

        val sc = spark.sparkContext
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
        sqlContext.setConf("hive.exec.dynamic.partition", "true")
        sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
        val log = LogManager.getRootLogger
        println("#--------------------------------------------In THE FACT----------------------------------------------#")
        log.setLevel(Level.INFO)

        import spark.sqlContext.implicits._

        val curr_date = current_date()
        val today = ZonedDateTime.now(ZoneId.of("UTC"))
        val dateformatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val curr_date_str = dateformatter format today
         val src_data =spark.sql("""SELECT voyage_id, ship_code, voyage_skey, itinerary_skey, revenue_type, revenue_group, revenue_type_skey, voyage_start_date, voyage_length, itinerary_description, day_of_voyage, sale_hour_of_day,count_sailors_booked, count_sailors_planned, planned_load_factor_pct, target_obr_gross_apd, target_obr_net_apd, count_bookings, sum_booking_guests, sum_listprice_plus_taxes, sum_vat_taxes, sum_list_prices,sum_debit_amount,sum_credit_amount , sum_promotions_discounts, sum_shared_manual_adjustments_comps, sum_total_collected_sales, sum_partner_gratuities, sum_retail_retail_cogs, sum_total_shared_revenue, sum_partner_revenue, vv_gross_onboard_revenue, sum_prevoyage_commissions, sum_vv_manual_adjustments_comps, sum_vv_cost_of_goods, sum_donations, vv_net_onboard_revenue,  source_type FROM shipdw.hvtb_temp_fact_ship_overview_device_revenue_summary union SELECT voyage_id, ship_code, voyage_skey, itinerary_skey, revenue_type, revenue_group, revenue_type_skey, voyage_start_date, voyage_length, itinerary_description, day_of_voyage, sale_hour_of_day,count_sailors_booked, count_sailors_planned, planned_load_factor_pct, target_obr_gross_apd, target_obr_net_apd, count_bookings, sum_booking_guests, sum_listprice_plus_taxes, sum_vat_taxes, sum_list_prices,sum_debit_amount,sum_credit_amount , sum_promotions_discounts , sum_shared_manual_adjustments_comps, sum_total_collected_sales, sum_partner_gratuities, sum_retail_retail_cogs, sum_total_shared_revenue, sum_partner_revenue, vv_gross_onboard_revenue, sum_prevoyage_commissions, sum_vv_manual_adjustments_comps, sum_vv_cost_of_goods, sum_donations, vv_net_onboard_revenue,   'revenue_fact' as source_type FROM shipdw.ship_overview_revenue_summary_fact_temp""")


        val insertFactDF = src_data.withColumn("upd_dt", current_timestamp()).withColumn("load_dt", current_timestamp())
        insertFactDF.createOrReplaceTempView("final_view")
        //insertFactDF.printSchema()

        /*spark.postgresql.database qanbx
spark.target.enablerops_table ship.HBTB_INGESTION_METADATA
spark.target.zkurl q-lab-hdp-ada1.virginvoyages.qa.dev:2181/hbase-secure:nbx.service@VIRGINVOYAGES.QA.DEV:/home/nbx.service/nbx.keytab
spark.table.type fact
spark.postgresql.url jdbc:postgresql://10.2.52.87:5432/qanbx
spark.postgresql.user nifiusr
spark.postgresql.password finiQA@123*/

        val finalColSQL = "Select * from final_view"
        val final_temp_df = spark.sql(finalColSQL)
        final_temp_df.printSchema()
final_temp_df.repartition(15).write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.location"))
        
      } else {
        log.info("This scripts does not require any parameters")
      }

    } catch {
      case e: SQLException => {
        e.printStackTrace(); log.info("Exception while executing the SQL command");

      }
      case e: Exception => {
        log.info("******************in the catch of Reservation Dim ******************");
        e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace")
      }
    }

    spark.stop()
  }
  
  
}