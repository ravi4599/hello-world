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

//import com.virginvoyages.metadataframework.ManageMetadata

object ShoreActivityRevenueSummaryFact {

 

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

    import spark.implicits._

    val spark = getSparkSession()

    val sc = spark.sparkContext

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)

 

    try {

      /*val whereClause = s""" where batchtime>= '$batch_start_tme' and batchtime<='$batch_end_tme' and part_date>='$part_read_start' and part_date<='$part_read_end'"""

      log.info(s"""whereClause : $whereClause """)*/

 

      import spark.sqlContext.implicits._

      log.info(s"""Starting the Execution""")

 

      val bookingdetaildf = spark.sql("select booking_link_id,booking_detail_skey from shipdw.hvtb_mart_dim_booking_detail")

      val bookingfactdf = spark.sql("select booking_detail_skey,activity_skey as bookingfact_activity_skey from shipdw.hvtb_mart_fact_booking")

      val bookdtldimfact = bookingfactdf.join(bookingdetaildf, bookingfactdf("booking_detail_skey") === bookingdetaildf("booking_detail_skey"), "inner")

      val activitydf = spark.sql("select activity_skey,activity_name,activity_group_code from shipdw.hvtb_mart_dim_activity")

      val bookactivitydf = activitydf.join(bookdtldimfact, activitydf("activity_skey") === bookdtldimfact("bookingfact_activity_skey"), "inner")

      val bookgactvdf = bookactivitydf.withColumn("finalRank", row_number().over(Window.partitionBy($"activity_skey", $"activity_name", $"activity_group_code", $"booking_link_id").orderBy($"activity_skey".desc))).filter($"finalRank" === 1).select("activity_skey", "activity_name", "activity_group_code", "booking_link_id")

      bookgactvdf.createOrReplaceTempView("bookingactivitytable")

 

      //dfPos:

      val df_pos = spark.sql("select pos_item_dim.item_skey, revenue_category_rel.revenue_type as catg_pos_revenue_type, revenue_category_rel.revenue_area as catg_pos_revenue_area, revenue_category_rel.revenue_type_skey as catg_pos_revenue_stream_skey, revenue_category_rel.category_type, revenue_category_rel.target_revenue_stream from shipdw.hvtb_mart_dim_pos_item  pos_item_dim left join shipdw.hvtb_mart_lkp_revenue_category_rel revenue_category_rel on revenue_category_rel.category_value=pos_item_dim.item_plu_category_name and revenue_category_rel.category_type='POS_ITEM_CATEG_NUM'");

 

      df_pos.createOrReplaceTempView("dfPos");

 

      //dfFolio:

      val df_Folio = spark.sql("select revenue_category_rel.revenue_type as catg_folio_revenue_type ,revenue_category_rel.revenue_area as catg_folio_revenue_area , folio_item_dim.folio_item_skey ,revenue_category_rel.revenue_type_skey as catg_folio_revenue_stream_skey, revenue_category_rel.category_type, revenue_category_rel.target_revenue_stream from shipdw.hvtb_mart_dim_folio_item folio_item_dim JOIN  shipdw.hvtb_mart_lkp_revenue_category_rel revenue_category_rel ON revenue_category_rel.category_value=folio_item_dim.folio_item_quickcode and revenue_category_rel.category_type='FOLIO_QUICKCODE'");

 

      df_Folio.createOrReplaceTempView("dfFolio");

 

      // dFPersonDim:

      val dF_PersonDim = spark.sql("select person_dim.voyage_skey, count(person_skey) as count_sailors_booked  from shipdw.hvtb_mart_dim_person person_dim where person_dim.person_type='1' and person_dim.booking_status = 1 and  person_dim.booking_manifest_type='P' group by person_dim.voyage_skey");

 

      dF_PersonDim.createOrReplaceTempView("dFPersonDim");

 

      val DF_Gratutity = spark.sql("select   ship_revn_partner_gratuity.revenue_type ,ship_revn_partner_gratuity.partner_gratuity_pct, pos_item_dim.item_skey, pos_item_dim.item_plu_group_number  FROM shipdw.hvtb_mart_lkp_partner_gratuity_lkup ship_revn_partner_gratuity JOIN shipdw.hvtb_mart_dim_pos_item pos_item_dim  ON ship_revn_partner_gratuity.item_plu_group_num = pos_item_dim.item_plu_group_number ");

 

      DF_Gratutity.createOrReplaceTempView("DFGratutity");

 

      //DF_Share:

 

      val DF_Share = spark.sql("Select pos_item_dim.item_skey,ship_revn_partner_share.revenue_type,ship_revn_partner_share.item_plu_group_num,ship_revn_partner_share.partner_share_pct   ,ship_revn_partner_share.vv_share_pct,ship_revn_partner_share.share_type from shipdw.hvtb_mart_lkp_partner_share_lkup ship_revn_partner_share left JOIN shipdw.hvtb_mart_dim_pos_item pos_item_dim ON ship_revn_partner_share.item_plu_group_num = pos_item_dim.item_plu_group_number");

 

      DF_Share.createOrReplaceTempView("DFShare");

 

      //DF_manual_adjustments_comps

 

      val DF_manual_adjustments_comps = spark.sql("select ship_internal_accounts.revenue_type,person_dim.person_skey  FROM shipdw.hvtb_mart_lkp_internal_accounts_lkup ship_internal_accounts JOIN  shipdw.hvtb_mart_dim_person person_dim ON ship_internal_accounts.account_nbr = person_dim.account_number");

      DF_manual_adjustments_comps.createOrReplaceTempView("DFManualAdjustmentsComps");

 

      //DF_pos_sum_donations

 

      val DF_pos_sum_donations = spark.sql("select pos_item_dim.item_skey as item_skey, donation_item_rel.category_type as category_type_pos,revenue_category_rel.revenue_type from shipdw.hvtb_mart_dim_pos_item pos_item_dim JOIN shipdw.hvtb_mart_lkp_donation_item_rel  donation_item_rel ON donation_item_rel.category_value=pos_item_dim.item_plu_number join shipdw.hvtb_mart_lkp_revenue_category_rel revenue_category_rel on revenue_category_rel.revenue_type=donation_item_rel.donation_type and donation_item_rel.category_type='POS_ITEM_PLU_NUM'")

 

      DF_pos_sum_donations.createOrReplaceTempView("DFPosSumDonations");

 

      //DF_FOLIO_sum_donations

 

      val DF_FOLIO_sum_donations = spark.sql("select folio_item_dim.folio_item_skey, donation_item_rel.category_type as category_type_folio, revenue_category_rel.revenue_type  from shipdw.hvtb_mart_dim_folio_item folio_item_dim JOIN shipdw.hvtb_mart_lkp_donation_item_rel donation_item_rel ON donation_item_rel.category_value=folio_item_dim.folio_item_quickcode JOIN shipdw.hvtb_mart_lkp_revenue_category_rel revenue_category_rel on revenue_category_rel.revenue_type=donation_item_rel.donation_type and donation_item_rel.category_type='FOLIO_QUICKCODE'")

 

      DF_FOLIO_sum_donations.createOrReplaceTempView("DFFOLIOSumDonations");

 

      //DF_BOOKING

 

      val DF_BOOKING = spark.sql("select booking_fact.voyage_skey,itinerary_dim.itinerary_skey,activity_group_revenue_rel.revenue_type, count(distinct booking_detail_dim.booking_link_id) as count_bookings,  count(distinct booking_detail_dim.booking_detail_skey) as sum_booking_guests    from   shipdw.hvtb_mart_lkp_activity_group_revenue_rel activity_group_revenue_rel join shipdw.hvtb_mart_lkp_revenue_category_rel revenue_category_rel ON  activity_group_revenue_rel.revenue_Type=revenue_category_rel.revenue_Type join shipdw.hvtb_mart_dim_activity activity_dim ON activity_dim.activity_group_code = activity_group_revenue_rel.activity_group_code join shipdw.hvtb_mart_fact_booking booking_fact ON booking_fact.activity_skey= activity_dim.activity_skey join shipdw.hvtb_mart_dim_voyage voyage_dim ON booking_fact.voyage_skey=voyage_dim.voyage_skey join shipdw.hvtb_mart_dim_booking_detail booking_detail_dim ON  booking_detail_dim.booking_detail_skey =booking_fact.booking_detail_skey join shipdw.hvtb_mart_dim_itinerary itinerary_dim ON itinerary_dim.voyage_number = voyage_dim.voyage_number where booking_detail_dim.status<>'CANCELLED' OR booking_detail_dim.status<>'RESERVATION_CANCELLED' group by 1,2,3")

 

      DF_BOOKING.createOrReplaceTempView("DFBOOKING");

 

      val src_data = spark.sql("""

select

   voyage_id,

   ship_code,

   reve_fct.voyage_skey,

   reve_fct.itinerary_skey,  

   activity_skey,

   activity_name,

   activity_group_code,

   CASE

      WHEN

         reve_fct.revenue_type is null

      then

         'Uncategorized'

      else

         reve_fct.revenue_type

   end

   as revenue_type ,

   CASE

      WHEN

         reve_fct.revenue_group is null

      then

         'Uncategorized'

      else

         reve_fct.revenue_group

   end

   as revenue_group , revenue_type_skey, voyage_start_date, voyage_length, itinerary_description , day_of_voyage , count_sailors_booked, count_sailors_planned, planned_load_factor_pct, target_obr_gross_apd, target_obr_net_apd, DFBOOKING.count_bookings, DFBOOKING.sum_booking_guests, sum_listprice_plus_taxes, sum_vat_taxes, sum_list_prices,sum_debit_amount ,sum_credit_amount,sum_promotions_discounts_amount, sum_shared_manual_adjustments_comps, sum_total_collected_sales, sum_partner_gratuities, sum_retail_retail_cogs ,

   (

      sum_total_collected_sales - sum_partner_gratuities - sum_retail_retail_cogs

   )

   as sum_total_shared_revenue, partner_share_pct * (sum_total_collected_sales - sum_partner_gratuities - sum_retail_retail_cogs) as sum_partner_revenue,

   (

(sum_total_collected_sales - sum_partner_gratuities - sum_retail_retail_cogs) - (partner_share_pct * (sum_total_collected_sales - sum_partner_gratuities - sum_retail_retail_cogs))

   )

   as vv_gross_onboard_revenue,

   CASE

      WHEN

         revenue_group in

         (

            'Shore Things', 'Entertainment', 'Spa Services', 'Tattoo', 'Premium Wifi'

         )

         and reve_fct.revenue_type not in

         (

            'Entertainment - Drag Brunch'

         )

      then

         sum_list_prices * 0.10

      ELSE

         0

   end

   as sum_prevoyage_commissions, sum_vv_manual_adjustments_comps, sum_vv_cost_of_goods, sum_donations,

   (

(sum_total_collected_sales - sum_partner_gratuities - sum_retail_retail_cogs) - (partner_share_pct * (sum_total_collected_sales - sum_partner_gratuities - sum_retail_retail_cogs)) - sum_vv_manual_adjustments_comps - sum_vv_cost_of_goods - sum_donations

   )

   as vv_net_onboard_revenue

from

   (

      select

         voyage_id,

         ship_code,

         voyage_skey,

         itinerary_skey,

                                  activity_skey,

                                  activity_name,

                     activity_group_code,

         revenue_type,

         revenue_group,

         revenue_type_skey,

         voyage_start_date,

         voyage_length,

         itinerary_description,

         day_of_voyage,

         count_sailors_booked,

         count_sailors_planned,

         planned_load_factor_pct,

         target_obr_gross_apd,

         target_obr_net_apd,

         sum(item_list_price_amount * sale_quantity) + sum(tax_amount) as sum_listprice_plus_taxes,

         sum(tax_amount) as sum_vat_taxes,

         sum(item_list_price_amount * sale_quantity) as sum_list_prices,

                             sum(debit_amount) as sum_debit_amount,

                             sum(credit_amount) as sum_credit_amount,

         sum(discount_amount) as sum_promotions_discounts_amount,

         sum(manual_adjustment_amount) as sum_shared_manual_adjustments_comps,

         sum(total_collected_sales_amount) as sum_total_collected_sales,

                            

         CASE

            when

               partner_gratuity_pct is not null

               and partner_gratuity_pct != 0.0

            then

               sum((partner_gratuity_pct / 100) * total_collected_sales_amount)

            else

               0.0

         end

         as sum_partner_gratuities,

         CASE

            WHEN

               revenue_type in

               (

                  'Retail', 'Tattoo'

               )

            then

               sum(cost_of_goods)

            ELSE

               0.0

         end

         as sum_retail_retail_cogs ,

         CASE

            WHEN

               revenue_type not in

               (

                  'Retail', 'Tattoo'

               )

            then

               sum(cost_of_goods)

            ELSE

               0.0

         end

         as sum_vv_cost_of_goods,

         Case

            when

               partner_share_pct is not null

               and partner_share_pct != 0

            then

               partner_share_pct / 100

            else

               partner_share_pct

         end

         as partner_share_pct ,

         case

            when

               sum_dontaion_flag = 'donation'

            then

               sum(total_collected_sales_amount)

            else

               0.0

         end

         as sum_donations ,

         case

            when

               manual_adjustment_flag = 'manual_adjustment'

            then

               sum(total_collected_sales_amount)

            else

               0.0

         end

         as sum_vv_manual_adjustments_comps

      from

         (

            select

               rev_fct.voyage_id,

               rev_fct.ship_code,

               rev_fct.voyage_skey,

               rev_fct.itinerary_skey,

                                                    rev_fct.activity_skey,

                                                    rev_fct.activity_name,

                                                    rev_fct.activity_group_code,             

               rev_fct.revenue_Type,

               rev_fct.revenue_group,

               rev_fct.revenue_type_skey,

               rev_fct.voyage_start_date,

               rev_fct.voyage_length,

               rev_fct.itinerary_description,

               rev_fct.day_of_voyage,

               count_sailors_booked,

               cast(obr_target_lkup.plan_sailors as double) as count_sailors_planned,

               cast(obr_target_lkup.plan_load_factor_pct as double) as planned_load_factor_pct,

               cast(obr_target_lkup.gross_apd_target_amount as double) as target_obr_gross_apd,

               cast(obr_target_lkup.net_apd_target_amount as double)as target_obr_net_apd,                   

                                              rev_fct.credit_amount,

                                              rev_fct.debit_amount,

               rev_fct.item_list_price_amount,

               rev_fct.tax_amount,

               rev_fct.sale_quantity,

               rev_fct.discount_amount,

               rev_fct.manual_adjustment_amount,

               rev_fct.total_collected_sales_amount,

               coalesce(DFGratutity.partner_gratuity_pct, DFGratutity1.partner_gratuity_pct, 0 ) as partner_gratuity_pct,

               rev_fct.cost_of_goods,

               coalesce(DFShare.partner_share_pct, DFShare1.partner_share_pct, 0 ) as partner_share_pct,

               rev_fct.sum_dontaion_flag,

               rev_fct.manual_adjustment_flag

            from

               (

                  select

                     ship_revenue_fact.voyage_id,

                     voyage_dim.ship_code as ship_code,

                     ship_revenue_fact.voyage_skey as voyage_skey,

                     ship_revenue_fact.itinerary_skey,

                                                                         bookingactivitytable.activity_skey,

                                                                         bookingactivitytable.activity_name,

                                                                         bookingactivitytable.activity_group_code,                   

                     case

                        when

                           ship_revenue_fact.source_type = 'KONAMI'

                           and device_dim.device_type_id != 3

                        then

                           'Casino Slots'

                        when

                           ship_revenue_fact.pos_item_skey is not null

                           and ship_revenue_fact.pos_item_skey != - 1

                        then

                           dfPos.catg_pos_revenue_type

                        when

                           ship_revenue_fact.folio_item_skey is not null

                           and ship_revenue_fact.folio_item_skey != - 1

                        then

                           dfFolio.catg_folio_revenue_type

                        else

                           'Uncategorized'

                     end

                     as revenue_Type,

                     case

                        when

                           ship_revenue_fact.source_type = 'KONAMI'

                           and device_dim.device_type_id != 3

                        then

                           'Casino Slots'

                        when

                           ship_revenue_fact.pos_item_skey is not null

                           and ship_revenue_fact.pos_item_skey != - 1

                        then

                           dfPos.catg_pos_revenue_area

                        when

                           ship_revenue_fact.folio_item_skey is not null

                           and ship_revenue_fact.folio_item_skey != - 1

                        then

                           dfFolio.catg_folio_revenue_area

                        else

                           'Uncategorized'

                     end

                     as revenue_group,

                     CASE

                        WHEN

                           ship_revenue_fact.source_type = 'KONAMI'

                        then

                           0

                        WHEN

                           ship_revenue_fact.pos_item_skey is not null

                           and ship_revenue_fact.pos_item_skey != - 1

                           and dfPos.catg_pos_revenue_stream_skey is not null

                        then

                           dfPos.catg_pos_revenue_stream_skey

                        WHEN

                           ship_revenue_fact.folio_item_skey is not null

                           and ship_revenue_fact.folio_item_skey != - 1

                           and dfFolio.catg_folio_revenue_stream_skey is not null

                        then

                           dfFolio.catg_folio_revenue_stream_skey

                        else

                           - 1

                     end

                     as revenue_type_skey, voyage_dim.voyage_start_date, voyage_dim.voyage_length , itinerary_dim.itinerary_name as itinerary_description, itinerary_dim.itinerary_day as day_of_voyage , dFPersonDim.count_sailors_booked,

                     CASE

                        WHEN

                           dfPos.category_type = 'POS_ITEM_CATEG_NUM'

                        then

                           dfPos.target_revenue_stream

                        when

                           dfFolio.category_type = 'FOLIO_QUICKCODE'

                        then

                           dfFolio.target_revenue_stream

                        else

                           null

                     end

                     as target_revenue_stream,ship_revenue_fact.credit_amount,ship_revenue_fact.debit_amount, ship_revenue_fact.item_list_price_amount, ship_revenue_fact.tax_amount, ship_revenue_fact.sale_quantity, ship_revenue_fact.discount_amount, ship_revenue_fact.manual_adjustment_amount, ship_revenue_fact.total_collected_sales_amount, cost_of_goods,

                     CASE

                        when

                           DFFOLIOSumDonations.folio_item_skey is not null

                        then

                           'donation'

                        when

                           DFPosSumDonations.item_skey is not null

                        then

                           'donation'

                        else

                           'no donation'

                     end

                     as sum_dontaion_flag ,

                     case

                        when

                           DFManualAdjustmentsComps.person_skey is not null

                        then

                           'manual_adjustment'

                        else

                           'na'

                     end

                     as manual_adjustment_flag, voyage_dim.voyage_number, ship_revenue_fact.pos_item_skey, sale_detail_dim.sale_id

                  from

                     shipdw.hvtb_mart_fact_revenue ship_revenue_fact

                     LEFT join

                        shipdw.hvtb_mart_dim_voyage voyage_dim

                        ON ship_revenue_fact.voyage_skey = voyage_dim.voyage_skey

                                                                                   INNER JOIN

                                                                                                  bookingactivitytable

                                                                                                  ON ship_revenue_fact.booking_reference_number = bookingactivitytable.booking_link_id

                     LEFT join

                        shipdw.hvtb_mart_dim_device device_dim

                        on ship_revenue_fact.device_skey = device_dim.device_skey

                     LEFT join

                        dfPos

                        on dfPos.item_skey = ship_revenue_fact.pos_item_skey

                     LEFT join

                        dfFolio

                        on dfFolio.folio_item_skey = ship_revenue_fact.folio_item_skey

                     LEFT JOIN

                        shipdw.hvtb_mart_dim_itinerary itinerary_dim

                        ON ship_revenue_fact.itinerary_skey = itinerary_dim.itinerary_skey                     

                     LEFT JOIN

                        DFManualAdjustmentsComps

                        ON ship_revenue_fact.person_skey = DFManualAdjustmentsComps.person_skey

                     LEFT JOIN

                        DFPosSumDonations

                        ON ship_revenue_fact.pos_item_skey = DFPosSumDonations.item_skey

                     LEFT JOIN

                        DFFOLIOSumDonations

                        ON ship_revenue_fact.folio_item_skey = DFFOLIOSumDonations.folio_item_skey

                     LEFT JOIN

                        shipdw.hvtb_mart_dim_sale_detail sale_detail_dim

                        ON sale_detail_dim.sale_detail_skey = ship_revenue_fact.sale_detail_skey

                     LEFT JOIN

                        dFPersonDim

                        on dFPersonDim.voyage_skey = ship_revenue_fact.voyage_skey

               )

               rev_fct

               LEFT JOIN

                  DFShare

                  ON rev_fct.revenue_Type = DFShare.revenue_Type

                  and rev_fct.pos_item_skey = DFShare.item_skey

               LEFT JOIN

                  shipdw.hvtb_mart_lkp_partner_share_lkup DFShare1

                  on rev_fct.revenue_Type = DFShare1.revenue_Type

               LEFT JOIN

                  DFGratutity

                  on rev_fct.revenue_Type = DFGratutity.revenue_Type

                  and rev_fct.pos_item_skey = DFGratutity.item_skey

               LEFT JOIN

                  shipdw.hvtb_mart_lkp_partner_gratuity_lkup DFGratutity1

                  ON rev_fct.revenue_Type = DFGratutity1.revenue_Type

                  and DFGratutity.item_plu_group_number is null

               LEFT JOIN

                  shipdw.hvtb_mart_voyage_obr_target_lkp obr_target_lkup

                  ON obr_target_lkup.voyage_number = rev_fct.voyage_number

                  and obr_target_lkup.target_revenue_stream = rev_fct.target_revenue_stream

         )

      group by

         voyage_id, ship_code, voyage_skey, itinerary_skey,activity_skey, activity_name, activity_group_code,revenue_type, revenue_group, revenue_type_skey, voyage_start_date, voyage_length, itinerary_description , day_of_voyage , count_sailors_booked, count_sailors_planned, planned_load_factor_pct, target_obr_gross_apd, partner_gratuity_pct, target_obr_net_apd , partner_share_pct, sum_dontaion_flag, manual_adjustment_flag                  

   )

   reve_fct

   LEFT JOIN

      DFBOOKING

      ON reve_fct.voyage_skey = DFBOOKING.voyage_skey

      and reve_fct.itinerary_skey = DFBOOKING.itinerary_skey

      and reve_fct.revenue_type = DFBOOKING.revenue_type """)

 
      val final_df = src_data.withColumn("upd_dt", current_timestamp()).withColumn("load_dt", current_timestamp());
    final_df.repartition(15).write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.location"))

    } catch {

 

      case e: SQLException =>

        {

          //   ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);

          log.info("******************in the catch of Activity Revenue Summary Fact Load ******************");

          e.printStackTrace();

          throw new Exception("SQL Exception..please check the stacktrace", e);

 

        }

        println("#----------------------------Process Has Failed---------------------------#")

        System.exit(1)

        spark.stop()

 

    }

  }

}