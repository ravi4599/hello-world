package com.virginvoyages.shore.fact
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{ col, to_date, to_timestamp, monotonically_increasing_id }
import java.sql.Timestamp
import org.apache.spark.sql.functions._

import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions.{ col, lit, when }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

object shore_overview_revenue_summary_fact {

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

        //dfPos:

        val df_pos = spark.sql("SELECT pos_item_dim.item_skey, revenue_category_rel.revenue_type AS catg_pos_revenue_type, revenue_category_rel.revenue_type AS catg_pos_revenue_area, revenue_category_rel.revenue_type_skey AS catg_pos_revenue_stream_skey, revenue_category_rel.type AS category_type, revenue_category_rel.description as target_revenue_stream FROM shipdw.hvtb_mart_dim_pos_item pos_item_dim LEFT JOIN shipdw.hvtb_mart_lkp_revenue_category_rel revenue_category_rel ON revenue_category_rel.code = pos_item_dim.item_plu_category_number AND revenue_category_rel.type = 'PLU Category'");

        //val df_pos=spark.sql("select pos_item_dim.item_skey, revenue_category_rel.revenue_type as catg_pos_revenue_type, revenue_category_rel.revenue_area as catg_pos_revenue_area, revenue_category_rel.revenue_type_skey as catg_pos_revenue_stream_skey from shipdw.hvtb_mart_dim_pos_item  pos_item_dim left join shipdw.hvtb_mart_lkp_revenue_category_rel revenue_category_rel on revenue_category_rel.category_value=pos_item_dim.item_plu_category_name and revenue_category_rel.category_type='POS_ITEM_CATEG_NUM'");

        df_pos.createOrReplaceTempView("dfPos");

        //dfFolio:
        val df_Folio = spark.sql("SELECT revenue_category_rel.revenue_type AS catg_folio_revenue_type , revenue_category_rel.revenue_type AS catg_folio_revenue_area , folio_item_dim.folio_item_skey , revenue_category_rel.revenue_type_skey AS catg_folio_revenue_stream_skey, revenue_category_rel.type AS category_type, revenue_category_rel.description as target_revenue_stream FROM shipdw.hvtb_mart_dim_folio_item folio_item_dim JOIN shipdw.hvtb_mart_lkp_revenue_category_rel revenue_category_rel ON revenue_category_rel.code = folio_item_dim.folio_item_quickcode AND revenue_category_rel.type = 'Quick Code'");

        //val df_Folio = spark.sql("select revenue_category_rel.revenue_type as catg_folio_revenue_type ,revenue_category_rel.revenue_area as catg_folio_revenue_area , folio_item_dim.folio_item_skey ,revenue_category_rel.revenue_type_skey as catg_folio_revenue_stream_skey from shipdw.hvtb_mart_dim_folio_item folio_item_dim JOIN  shipdw.hvtb_mart_lkp_revenue_category_rel revenue_category_rel ON revenue_category_rel.category_value=folio_item_dim.folio_item_quickcode");

        df_Folio.createOrReplaceTempView("dfFolio");

        /*dFPersonDim:*/
        val dF_PersonDim = spark.sql("SELECT person_dim.voyage_skey, count(person_skey) AS count_sailors_booked FROM shipdw.hvtb_mart_dim_person person_dim LEFT JOIN shipdw.hvtb_mart_dim_voyage voyage_dim ON voyage_dim.voyage_number = person_dim.booking_cruise_number WHERE person_dim.person_type = '1' AND person_dim.booking_status = '1' AND person_dim.booking_manifest_type = 'P' GROUP BY person_dim.voyage_skey");

        dF_PersonDim.createOrReplaceTempView("dFPersonDim");

        val DF_Gratutity = spark.sql("select   ship_revn_partner_gratuity.revenue_type	,ship_revn_partner_gratuity.partner_gratuity_pct, pos_item_dim.item_skey , pos_item_dim.item_plu_group_number FROM shipdw.hvtb_mart_lkp_partner_gratuity_lkup ship_revn_partner_gratuity JOIN shipdw.hvtb_mart_dim_pos_item pos_item_dim  ON ship_revn_partner_gratuity.item_plu_group_num = pos_item_dim.item_plu_group_number ");

        DF_Gratutity.createOrReplaceTempView("DFGratutity");

        //DF_Share:

        val DF_Share = spark.sql("Select pos_item_dim.item_skey,ship_revn_partner_share.revenue_type,ship_revn_partner_share.item_plu_group_num,ship_revn_partner_share.partner_share_pct   ,ship_revn_partner_share.vv_share_pct,ship_revn_partner_share.share_type from shipdw.hvtb_mart_lkp_partner_share_lkup ship_revn_partner_share left JOIN shipdw.hvtb_mart_dim_pos_item pos_item_dim ON ship_revn_partner_share.item_plu_group_num = pos_item_dim.item_plu_group_number");

        DF_Share.createOrReplaceTempView("DFShare");

        //DF_manual_adjustments_comps

        val DF_manual_adjustments_comps = spark.sql("select ship_internal_accounts.revenue_type,person_dim.person_skey  FROM shipdw.hvtb_mart_lkp_internal_accounts_lkup ship_internal_accounts JOIN  shipdw.hvtb_mart_dim_person person_dim ON ship_internal_accounts.account_nbr = person_dim.account_number");
        DF_manual_adjustments_comps.createOrReplaceTempView("DFManualAdjustmentsComps");

        //DF_pos_sum_donations

        val DF_pos_sum_donations = spark.sql("select pos_item_dim.item_skey as item_skey, donation_item_rel.category_type as category_type_pos,revenue_category_rel.revenue_type from shipdw.hvtb_mart_dim_pos_item pos_item_dim JOIN shipdw.hvtb_mart_lkp_donation_item_rel  donation_item_rel ON donation_item_rel.category_value=pos_item_dim.item_plu_number join shipdw.hvtb_mart_lkp_revenue_category_rel revenue_category_rel on revenue_category_rel.revenue_type=donation_item_rel.donation_type and donation_item_rel.category_type='POS_ITEM_PLU_NUM'");

        DF_pos_sum_donations.createOrReplaceTempView("DFPosSumDonations");

        //DF_FOLIO_sum_donations

        val DF_FOLIO_sum_donations = spark.sql("select folio_item_dim.folio_item_skey, donation_item_rel.category_type as category_type_folio, revenue_category_rel.revenue_type  from shipdw.hvtb_mart_dim_folio_item folio_item_dim JOIN shipdw.hvtb_mart_lkp_donation_item_rel donation_item_rel ON donation_item_rel.category_value=folio_item_dim.folio_item_quickcode JOIN shipdw.hvtb_mart_lkp_revenue_category_rel revenue_category_rel on revenue_category_rel.revenue_type=donation_item_rel.donation_type and donation_item_rel.category_type='POS_ITEM_PLU_NUM'")

        DF_FOLIO_sum_donations.createOrReplaceTempView("DFFOLIOSumDonations");

        //DF_BOOKING
        val DF_BOOKING = spark.sql("SELECT booking_fact.voyage_skey, itinerary_dim.itinerary_skey, activity_group_revenue_rel.revenue_type, count(DISTINCT booking_detail_dim.booking_link_id) AS count_bookings, count(DISTINCT booking_fact.person_skey) AS sum_booking_guests FROM shipdw.hvtb_mart_lkp_activity_group_revenue_rel activity_group_revenue_rel JOIN shipdw.hvtb_mart_lkp_revenue_category_rel revenue_category_rel ON activity_group_revenue_rel.revenue_Type = revenue_category_rel.revenue_Type JOIN shipdw.hvtb_mart_dim_activity activity_dim ON activity_dim.activity_group_code = activity_group_revenue_rel.activity_group_code JOIN shipdw.hvtb_mart_fact_booking booking_fact ON booking_fact.activity_skey = activity_dim.activity_skey JOIN shipdw.hvtb_mart_dim_voyage voyage_dim ON booking_fact.voyage_skey = voyage_dim.voyage_skey JOIN shipdw.hvtb_mart_dim_booking_detail booking_detail_dim ON booking_detail_dim.booking_detail_skey = booking_fact.booking_detail_skey JOIN shipdw.hvtb_mart_dim_itinerary itinerary_dim ON itinerary_dim.voyage_number = voyage_dim.voyage_number WHERE booking_detail_dim.status NOT IN ('CANCELLED','RESERVATION_CANCELLED') GROUP BY 1, 2, 3")

        DF_BOOKING.createOrReplaceTempView("DFBOOKING");

        spark.sql("INSERT overwrite TABLE shipdw.ship_overview_revenue_summary_fact_temp ( SELECT voyage_id, ship_code, reve_fct.voyage_skey, reve_fct.itinerary_skey, CASE WHEN reve_fct.revenue_type IS NULL THEN 'Uncategorized' ELSE reve_fct.revenue_type END AS revenue_type , CASE WHEN reve_fct.revenue_group IS NULL THEN 'Uncategorized' ELSE reve_fct.revenue_group END AS revenue_group , revenue_type_skey, voyage_start_date, voyage_length, itinerary_description , day_of_voyage , sale_hour_of_day, count_sailors_booked, count_sailors_planned, planned_load_factor_pct, target_obr_gross_apd, target_obr_net_apd, DFBOOKING.count_bookings, DFBOOKING.sum_booking_guests, sum_listprice_plus_taxes, sum_vat_taxes, sum_list_prices, sum_debit_amount, sum_credit_amount, sum_promotions_discounts_amount, sum_shared_manual_adjustments_comps, sum_total_collected_sales, sum_partner_gratuities, sum_retail_retail_cogs , (sum_total_collected_sales - sum_partner_gratuities - sum_retail_retail_cogs) AS sum_total_shared_revenue, partner_share_pct * (sum_total_collected_sales - sum_partner_gratuities -sum_retail_retail_cogs) AS sum_partner_revenue, ((sum_total_collected_sales - sum_partner_gratuities - sum_retail_retail_cogs) - (partner_share_pct * (sum_total_collected_sales - sum_partner_gratuities -sum_retail_retail_cogs))) AS vv_gross_onboard_revenue, CASE WHEN reve_fct.revenue_type IN ('Shore Things', 'Entertainment', 'Spa Services', 'Tattoo', 'Premium Wifi') THEN sum_list_prices * 0.10 ELSE 0 END AS sum_prevoyage_commissions, sum_vv_manual_adjustments_comps, sum_vv_cost_of_goods, sum_donations, ((sum_total_collected_sales - sum_partner_gratuities - sum_retail_retail_cogs) - (partner_share_pct * (sum_total_collected_sales - sum_partner_gratuities -sum_retail_retail_cogs)) - sum_vv_manual_adjustments_comps - sum_vv_cost_of_goods - sum_donations) AS vv_net_onboard_revenue , current_date AS load_dt, current_date AS upd_dt, 'revenue_fact' AS source_type FROM ( SELECT voyage_id, ship_code, voyage_skey, itinerary_skey, revenue_type, revenue_group, revenue_type_skey, voyage_start_date, voyage_length, itinerary_description , day_of_voyage , sale_hour_of_day, count_sailors_booked, count_sailors_planned, planned_load_factor_pct, target_obr_gross_apd, target_obr_net_apd, sum(item_list_price_amount*sale_quantity) + sum(tax_amount) AS sum_listprice_plus_taxes, sum(tax_amount) AS sum_vat_taxes, sum(item_list_price_amount * sale_quantity) AS sum_list_prices, sum(discount_amount) AS sum_promotions_discounts_amount, sum(manual_adjustment_amount) AS sum_shared_manual_adjustments_comps, sum(total_collected_sales_amount) AS sum_total_collected_sales, CASE WHEN partner_gratuity_pct IS NOT NULL AND partner_gratuity_pct != 0.0 THEN (partner_gratuity_pct / 100) * sum(total_collected_sales_amount) ELSE 0.0 END AS sum_partner_gratuities, CASE WHEN revenue_type IN ('Retail') THEN sum(cost_of_goods) ELSE 0.0 END AS sum_retail_retail_cogs , CASE WHEN revenue_type NOT IN ('Retail') THEN sum(cost_of_goods) ELSE 0.0 END AS sum_vv_cost_of_goods, CASE WHEN partner_share_pct IS NOT NULL AND partner_share_pct != 0 THEN partner_share_pct / 100 ELSE partner_share_pct END AS partner_share_pct , CASE WHEN sum_dontaion_flag = 'donation' THEN sum(total_collected_sales_amount) ELSE 0.0 END AS sum_donations , CASE WHEN manual_adjustment_flag = 'manual_adjustment' THEN sum(total_collected_sales_amount) ELSE 0.0 END AS sum_vv_manual_adjustments_comps, sum(debit_amount) AS sum_debit_amount, sum(credit_amount) AS sum_credit_amount FROM ( SELECT rev_fct.voyage_id, rev_fct.ship_code , rev_fct.voyage_skey, rev_fct.itinerary_skey, rev_fct.revenue_Type, rev_fct.revenue_group, rev_fct.revenue_type_skey, rev_fct.voyage_start_date, rev_fct.voyage_length , rev_fct.itinerary_description, rev_fct.day_of_voyage , rev_fct.sale_hour_of_day, count_sailors_booked, obr_target_lkup.plan_sailors AS count_sailors_planned, obr_target_lkup.plan_load_factor_pct AS planned_load_factor_pct, obr_target_lkup.gross_apd_target_amount AS target_obr_gross_apd, obr_target_lkup.net_apd_target_amount AS target_obr_net_apd, rev_fct.item_list_price_amount, rev_fct.tax_amount, rev_fct.sale_quantity, rev_fct.discount_amount, rev_fct.manual_adjustment_amount, rev_fct.total_collected_sales_amount, COALESCE(DFGratutity.partner_gratuity_pct, DFGratutity1.partner_gratuity_pct, 0 ) AS partner_gratuity_pct, rev_fct.cost_of_goods, COALESCE(DFShare.partner_share_pct, DFShare1.partner_share_pct, 0 ) AS partner_share_pct , rev_fct.sum_dontaion_flag , rev_fct.manual_adjustment_flag, rev_fct.debit_amount, rev_fct.credit_amount FROM ( SELECT ship_revenue_fact.voyage_id, voyage_dim.ship_code AS ship_code , ship_revenue_fact.voyage_skey AS voyage_skey, ship_revenue_fact.itinerary_skey, CASE WHEN ship_revenue_fact.source_type = 'KONAMI' AND device_dim.device_type_id != 3 THEN 'Casino Slots' WHEN ship_revenue_fact.pos_item_skey IS NOT NULL AND ship_revenue_fact.pos_item_skey != -1 THEN dfPos.catg_pos_revenue_type WHEN ship_revenue_fact.folio_item_skey IS NOT NULL AND ship_revenue_fact.folio_item_skey != -1 THEN dfFolio.catg_folio_revenue_type ELSE 'Uncategorized' END AS revenue_Type, NULL  AS revenue_group, NULL AS revenue_type_skey, voyage_dim.voyage_start_date, voyage_dim.voyage_length , itinerary_dim.itinerary_name AS itinerary_description, itinerary_dim.itinerary_day AS day_of_voyage , dim_time.hours_of_day AS sale_hour_of_day, dFPersonDim.count_sailors_booked, CASE WHEN dfPos.category_type = 'POS_ITEM_CATEG_NUM' THEN dfPos.target_revenue_stream WHEN dfFolio.category_type = 'FOLIO_QUICKCODE' THEN dfFolio.target_revenue_stream ELSE NULL END AS target_revenue_stream, ship_revenue_fact.item_list_price_amount, ship_revenue_fact.tax_amount, ship_revenue_fact.sale_quantity, ship_revenue_fact.discount_amount, ship_revenue_fact.manual_adjustment_amount, ship_revenue_fact.total_collected_sales_amount, cost_of_goods, CASE WHEN DFFOLIOSumDonations.folio_item_skey IS NOT NULL THEN 'donation' WHEN DFPosSumDonations.item_skey IS NOT NULL THEN 'donation' ELSE 'no donation' END AS sum_dontaion_flag , CASE WHEN DFManualAdjustmentsComps.person_skey IS NOT NULL THEN 'manual_adjustment' ELSE 'na' END AS manual_adjustment_flag, voyage_dim.voyage_number, ship_revenue_fact.pos_item_skey, ship_revenue_fact.debit_amount, ship_revenue_fact.credit_amount FROM shipdw.hvtb_mart_fact_revenue ship_revenue_fact LEFT JOIN shipdw.hvtb_mart_dim_voyage voyage_dim ON ship_revenue_fact.voyage_skey = voyage_dim.voyage_skey LEFT JOIN shipdw.hvtb_mart_dim_device device_dim ON ship_revenue_fact.device_skey = device_dim.device_skey LEFT JOIN dfPos ON dfPos.item_skey = ship_revenue_fact.pos_item_skey LEFT JOIN dfFolio ON dfFolio.folio_item_skey = ship_revenue_fact.folio_item_skey LEFT JOIN shipdw.hvtb_mart_dim_itinerary itinerary_dim ON ship_revenue_fact.itinerary_skey = itinerary_dim.itinerary_skey LEFT JOIN shipdw.hvtb_nbx_core_time_dim dim_time ON (ship_revenue_fact.sale_time_skey = dim_time.time_skey) LEFT JOIN DFGratutity ON ship_revenue_fact.pos_item_skey = DFGratutity.item_skey LEFT JOIN DFManualAdjustmentsComps ON ship_revenue_fact.person_skey = DFManualAdjustmentsComps.person_skey LEFT JOIN DFPosSumDonations ON ship_revenue_fact.pos_item_skey = DFPosSumDonations.item_skey LEFT JOIN DFFOLIOSumDonations ON ship_revenue_fact.folio_item_skey = DFFOLIOSumDonations.folio_item_skey LEFT JOIN dFPersonDim ON dFPersonDim.voyage_skey = ship_revenue_fact.voyage_skey)rev_fct LEFT JOIN DFShare ON rev_fct.revenue_Type = DFShare.revenue_Type AND rev_fct.pos_item_skey = DFShare.item_skey LEFT JOIN shipdw.hvtb_mart_lkp_partner_share_lkup DFShare1 ON rev_fct.revenue_Type = DFShare1.revenue_Type LEFT JOIN DFGratutity ON rev_fct.revenue_Type = DFGratutity.revenue_Type AND rev_fct.pos_item_skey = DFGratutity.item_skey LEFT JOIN shipdw.hvtb_mart_lkp_partner_gratuity_lkup DFGratutity1 ON rev_fct.revenue_Type = DFGratutity1.revenue_Type AND DFGratutity.item_plu_group_number IS NULL LEFT JOIN shipdw.hvtb_mart_voyage_obr_target_lkp obr_target_lkup ON obr_target_lkup.voyage_number = rev_fct.voyage_number AND obr_target_lkup.target_revenue_stream = rev_fct.revenue_Type ) GROUP BY voyage_id, ship_code, voyage_skey, itinerary_skey, revenue_type, revenue_group, revenue_type_skey, voyage_start_date, voyage_length, itinerary_description , day_of_voyage , sale_hour_of_day, count_sailors_booked, count_sailors_planned, planned_load_factor_pct, target_obr_gross_apd, partner_gratuity_pct, target_obr_net_apd , partner_share_pct, sum_dontaion_flag, manual_adjustment_flag) reve_fct LEFT JOIN DFBOOKING ON reve_fct.voyage_skey = DFBOOKING.voyage_skey AND reve_fct.itinerary_skey = DFBOOKING.itinerary_skey AND reve_fct.revenue_type = DFBOOKING.revenue_type )");

      } else {
        log.info("This scripts does not require any parameters")
      }

    } catch {
      case e: SQLException => {
        e.printStackTrace(); log.info("Exception while executing the SQL command");

      }
      case e: Exception => {
        log.info("******************in the catch of Revenue Fact ******************");
        e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace")
      }
    }

    spark.stop()
  }

}