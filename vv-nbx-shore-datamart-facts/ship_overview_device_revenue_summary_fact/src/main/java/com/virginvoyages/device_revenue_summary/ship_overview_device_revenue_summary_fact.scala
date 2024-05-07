package com.virginvoyages.device_revenue_summary

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, to_date, to_timestamp,monotonically_increasing_id}
import java.sql.Timestamp
import org.apache.spark.sql.functions._

import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

object ship_overview_device_revenue_summary_fact {

  /**
   * Initilize thte logger
   */
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]) {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    try {

      /**
       * Reservation Dim
       */
      
        if (args.length == 0) {
        val pond_table = spark.sparkContext.getConf.get("spark.pond.database").trim() + "." + spark.sparkContext.getConf.get("spark.pond.table").trim()
        println("pond_table"+pond_table)
        println("REFRESH TABLE " + pond_table)
        val EmptyDF = spark.emptyDataFrame
        EmptyDF.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.table.loc").trim())
        spark.sql("REFRESH TABLE " + pond_table)
        
            spark.sql(""" insert overwrite table shipdw.hvtb_temp_fact_ship_overview_device_revenue_summary select  device_revenue_fact.voyage_id,
    voyage_dim.ship_code,
    voyage_dim.voyage_skey,
    itinerary_dim.itinerary_skey,
    'Casino Tables' as revenue_type,
    revenue_category_rel.revenue_area as revenue_group,
    revenue_category_rel.revenue_type_skey as revenue_type_skey,
    voyage_dim.voyage_start_date,
    voyage_dim.voyage_length,
    itinerary_dim.itinerary_name as itinerary_description,
    itinerary_dim.itinerary_day as day_of_voyage,
	hour(device_revenue_fact.game_date_time) as sale_hour_of_day,
    tmp_personcount.count_sailors as count_sailors_booked,
   voyage_obr_target_lkup.plan_sailors as count_sailors_planned,
   voyage_obr_target_lkup.plan_load_factor_pct,
 voyage_obr_target_lkup.gross_apd_target_amount,
 voyage_obr_target_lkup.net_apd_target_amount ,
    0 as count_bookings,
    0 as sum_booking_guests,
    0 as  sum_listprice_plus_taxes,
    0 as  sum_vat_taxes,
    0 as  sum_list_prices,
	0 as  sum_debit_amount,
	0 as  sum_credit_amount,
    0 as  sum_promotions_discounts_amount,
    0 as  sum_shared_manual_adjustments_comps,
    sum(device_revenue_fact.table_win_amount)  as sum_total_collected_sales,
    0 as  sum_partner_gratuities,
    0 as  sum_retail_cogs,
    sum(device_revenue_fact.table_win_amount) as   sum_total_shared_revenue,
    0 as  sum_partner_revenue,
    sum(device_revenue_fact.table_win_amount)  as vv_gross_onboard_revenue,
    0 as  sum_prevoyage_commissions,
    0 as  sum_vv_manual_adjustments_comps,
    0 as  sum_vv_cost_of_goods,
    0 as  sum_donations,
    sum(device_revenue_fact.table_win_amount)  as vv_net_onboard_revenue,
    current_date as load_dt,
    current_date as upd_dt,
    'device_revenue_fact' as source_type
    from (
       select device_revenue_fact.voyage_id,
           device_revenue_fact.ship_code,
		   device_revenue_fact.voyage_skey,
           'Casino Tables' as revenue_type,
           device_revenue_fact.game_date_time,
             device_revenue_fact.table_win_amount
       from shipdw.hvtb_mart_fact_device_revenue device_revenue_fact
) device_revenue_fact
inner join shipdw.hvtb_mart_dim_voyage voyage_dim 
       on device_revenue_fact.voyage_skey=voyage_dim.voyage_skey
       AND voyage_dim.src_active_flag = true 
       AND voyage_dim.src_deleted_flag = false
left outer join shipdw.hvtb_mart_dim_itinerary itinerary_dim 
       on voyage_dim.voyage_number=itinerary_dim.voyage_number 
       and date(itinerary_day_date)=date(device_revenue_fact.game_date_time)
cross JOIN shipdw.hvtb_mart_lkp_revenue_category_rel revenue_category_rel
       ON revenue_category_rel.revenue_type = device_revenue_fact.revenue_type
left outer join shipdw.hvtb_mart_voyage_obr_target_lkp voyage_obr_target_lkup
       on voyage_obr_target_lkup.voyage_number=voyage_dim.voyage_number and
	   voyage_obr_target_lkup.target_revenue_stream=revenue_category_rel.target_revenue_stream
left outer join (
SELECT pd1.voyage_skey,
	    count(*) AS count_sailors
	   FROM shipdw.hvtb_mart_dim_person pd1 where pd1.person_type='1' 
	   AND pd1.booking_status = '1'
	   AND pd1.booking_manifest_type = 'P'
	  	group by pd1.voyage_skey 
		) tmp_personcount
       on device_revenue_fact.voyage_skey=tmp_personcount.voyage_skey
       group by device_revenue_fact.voyage_id,
    voyage_dim.ship_code,
    voyage_dim.voyage_skey,
    itinerary_dim.itinerary_skey,
    'Casino Tables',
    revenue_category_rel.revenue_area ,
    revenue_category_rel.revenue_type_skey ,
    voyage_dim.voyage_start_date,
    voyage_dim.voyage_length,
    itinerary_dim.itinerary_name ,
    itinerary_dim.itinerary_day ,
    tmp_personcount.count_sailors ,
	hour(device_revenue_fact.game_date_time) ,
voyage_obr_target_lkup.plan_sailors,
voyage_obr_target_lkup.plan_load_factor_pct,
 voyage_obr_target_lkup.gross_apd_target_amount,
 voyage_obr_target_lkup.net_apd_target_amount """)

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
