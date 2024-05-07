package com.virginvoyages.shore.fact
import java.util.Date
import com.virginvoyages.metadataframework.ManageMetadata

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

object PosRevenueFactLoad {
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
      //val whereClause = s""" where 1=1 """
      val whereClause = s""" where batchtime>= '$batch_start_tme' and batchtime<='$batch_end_tme' and part_date>='$part_read_start' and part_date<='$part_read_end'"""
      println(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
      log.info(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")

      import spark.sqlContext.implicits._
      println("#---------------------------Starting the Execution------------------#")
      val query = spark.sparkContext.getConf.get("spark.source.sql").replace("*whereclause*", whereClause)

      val stageRevenueDF = spark.sql(query).as("sale_fact")
      println(query)
      log.info(query)
      //val stageRevenueDF= spark.sql
      /*println(s"""Select part_date,batchtime, pos_include_flg, sales_checkstatus,cast(sale_time as date),sale_id,"N/A" as booking_reference_number,-1 as device_skey,wearable_skey,charge_skey,0 as time_on_device_seconds,0 as avg_wager_amount,0 as casino_game_dph,0 as theoritical_winnings,'POS' as source_type,revenue_type,sales.voyage_id ,dated.date_id as sale_date_skey,timed.s_key as sale_time_skey,  sale_detail_skey  ,  item_skey as pos_item_skey  ,  '-1' as folio_item_skey  , person_skey  ,  itinerary_skey  ,  outlet_skey  ,  '-1'activity_skey  , saleqty as sale_quantity  ,  unitprice as item_list_price_amount  ,  0.0 as debit_amount ,   0.0 as credit_amount ,  checkdetailtaxamount as tax_amount  ,  discount_amount  ,  discount_value  ,  cost_of_goods  ,      manual_adjustment_amount  ,      total_collected_sales_amount ,     NVL(total_collected_sales_amount/(guest_count*voyage_length),0) as total_collected_sales_ap   ,       total_collected_sales_amount - NVL(cost_of_goods,0) as total_shared_revenue_amount , NVL((total_collected_sales_amount - cost_of_goods)/(guest_count*voyage_length),0)  as total_shared_revenue_ap ,sale_date from
    (select * ,         NVL((unitprice*saleqty)-checkdetailtaxamount-discount_amount ,0)as total_collected_sales_amount  ,CASE when (UPPER(person_type)=='COMPANY ACCOUNT') THEN NVL(((unitprice*saleqty)-checkdetailtaxamount-discount_amount),0) ELSE 0  end as manual_adjustment_amount,sales_trans.cruise_number_sales as cruise_no from
   (   select batchtime,part_date,outlet_name,pos_include_flg,sales_checkstatus,voyage_dim.voyage_number as cruise_number_sales,person.person_type, CASE   WHEN (UPPER(revenue_type)=='BEVERAGES') THEN    NVL(apollo_pos_change.costofgoods,0)
   +NVL(sestra.cost,0

   )  WHEN (UPPER(revenue_type)=='RETAIL') THEN NVL(cog.price,0)ELSE   0   END AS cost_of_goods,revenue_rel.revenue_type,revenue_rel.category_value,NVL(itinerary_skey,-1) as itinerary_skey,sale_detail_skey,nvl(item_skey,-1)as item_skey ,person_skey,wearable_skey,charge_skey,nvl(outlet_skey,-1) as outlet_skey , sale.voyageid as voyage_id,sale.sales_id as sale_id,checkd.checkdetailsplunr as plunr, sale.sales_chargeid as charge_id,Cast(split(sales_date,'T')[0] as date)as sale_date,from_unixtime(UNIX_TIMESTAMP(Cast(concat(split(sales_time,'T')[0]," ",split(sales_time,'T')[1])as timestamp)), 'YYYY-MM-dd HH:mm:ss')as sale_time ,  sale.sales_outletid as outlet_id,     NVL(checkd.checkdetailsalesqty,0)     as saleqty  ,    NVL(checkd.checkdetailunitprice,0)   as unitprice,    NVL(checkd.checkdetailtaxamount,0)   as checkdetailtaxamount,  NVL(checkd.discount_amount,0) as discount_amount,  NVL(checkd.discount_value,0)  as discount_value,  NVL(cog.price,0)   as price        from
    (select * from
    (select  checkdetailsguid,price, ROW_NUMBER() OVER (Partition by checkdetailsguid ORDER by datediff(valid_to,valid_from
   ) asc) as rn  from
   ( select NVL(Cast(split(validfrom
   ,'T')[0] as date),'1999-01-01') as valid_from
    ,NVL(Cast(split(validto,'T')[0] as date),'2099-12-31') as valid_to,checkdetailsguid,price from
    shipdw.hvtb_parse_mxp_pos_sales_costofgood       )a where CURRENT_DATE BETWEEN a.valid_from
    AND a.valid_to)b where b.rn=1) cog left join (select batchtime,part_date,discount_value, discount_amount,checkdetailtaxamount,checkdetailunitprice,checkdetailsalesqty,checkdetailsplunr, checkdetailsguid,sales_guid from
    shipdw.hvtb_parse_mxp_pos_sales_checkdetails group by batchtime,part_date,discount_value, discount_amount,checkdetailtaxamount,checkdetailunitprice,checkdetailsalesqty,checkdetailsplunr, checkdetailsguid,sales_guid  ) checkd on(cog.checkdetailsguid==checkd.checkdetailsguid) left join ((select * from
   (select sales_checkstatus,voyageid,sales_chargeid,sales_shipcode, sales_time,sales_date,sales_id,sales_guid, sales_outletid ,row_number() over( partition by sales_id,sales_guid, sales_outletid ,sales_shipcode order by sales_date desc,sales_time desc)  as rn from
            shipdw.hvtb_parse_mxp_pos_sales where ((sales_checkstatus='V' and sales_voidedparent is not null) or (sales_checkstatus='C'))   	 	)sales_check where sales_check.rn=1      ) ) sale on(checkd.sales_guid==sale.sales_guid)  left join shipdw.hvtb_mart_dim_voyage voyage_dim on (sale.sales_shipcode == voyage_dim.ship_code and Cast(split(sales_date,'T')[0] as date) between voyage_dim.voyage_start_date and voyage_dim.voyage_end_date) left join (   select * from
    (select voyage_number,itinerary_day_date,itinerary_skey ,row_number() over(partition by voyage_number,itinerary_day_date order by  itinerary_day_date desc ) as rn from
    shipdw.hvtb_mart_dim_itinerary )iti where iti.rn=1  ) dim_itinerary
	on(voyage_dim.voyage_number==dim_itinerary.voyage_number and Cast(split(sales_date,'T')[0] as date)==date(dim_itinerary.itinerary_day_date))
	left join shipdw.hvtb_mart_dim_sale_detail as sale_detail on(sale_detail_src='POS' and sale.sales_id==sale_detail.sale_id )
	left join shipdw.hvtb_mart_dim_pos_item as item on(checkd.checkdetailsplunr==item.item_plu_number)
	left join shipdw.hvtb_mart_lkp_revenue_category_rel as revenue_rel 	on( (item.item_plu_category_name==revenue_rel.category_value))
	left join

	(  select person_type,person_charge_id,booking_cruise_number, person_skey,wearable_skey,
charge_skey from(select dim_person.person_type,person_charge_id,booking_cruise_number, dim_person.person_skey,wearable_skey,
charge_skey,
row_number() over(partition by dim_person.person_charge_id order by dim_person.load_dt desc) as p_rn
from
    shipdw.hvtb_mart_dim_person dim_person  left join   shipdw.hvtb_mart_dim_wearable wearable_dim
	on(dim_person.guest_wearable_id == wearable_dim.wearable_id)
	left join shipdw.hvtb_mart_dim_charge charge_dim on
	(dim_person.person_charge_id == charge_dim.charge_id)
	where person_charge_id is not null and  charge_dim.charge_id is not null )person_charge
	where person_charge.p_rn =1)
	as  person on(sale.sales_chargeid ==person.person_charge_id  )  	left join  shipdw.hvtb_mart_dim_outlet as outlet on(sale.sales_outletid==outlet.outlet_id)
	left join ( select * from (select plunr,costofgoods,row_number() over(partition by plunr order by  batchtime desc) as rn from shipdw.hvtb_parse_apollo_poschange)apollo_dedup where apollo_dedup.rn=1) as apollo_pos_change on((checkd.checkdetailsplunr==apollo_pos_change.plunr)
	)
	left join shipdw.hvtb_mart_lkp_sestra_cogs as sestra on(cast(checkd.checkdetailsplunr as bigint)==sestra.plunbr)
	--left join shipdw.hvtb_mart_lkp_revenue_category_rel as revenue_rel on(item.item_plu_category_name==revenue_rel.item_plu_category_name)
	where --NVL(pos_include_flg,'Y')!='N'
	trim(outlet.outlet_name)!='Casino'
	)sales_trans   $whereClause
	)sales left join shipdw.hvtb_mart_dim_date dated on(dated.`date`==sales.sale_date) left join shipdw.hvtb_mart_dim_time timed on(timed.second_of_day==hour(sale_time)*60*60+minute(sale_time)*60+second(sale_time) ) left join (select count(*) as guest_count, voyage_number ,abs(voyage_length) as voyage_length  from
    shipdw.hvtb_mart_dim_voyage voyage inner join shipdw.hvtb_mart_dim_person person on (voyage.voyage_number==person.booking_cruise_number) where person_type="Guest"  group by voyage_number,abs(voyage_length))guest_count on(guest_count.voyage_number==sales.cruise_no)
	""").as("sale_fact")
  println(s"""Select cast(sale_time as date),sale_id,"N/A" as booking_reference_number,-1 as device_skey,wearable_skey,charge_skey,0 as time_on_device_seconds,0 as avg_wager_amount,0 as casino_game_dph,0 as theoritical_winnings,'POS' as source_type,revenue_type,sales.voyage_id ,dated.date_id as sale_date_skey,timed.s_key as sale_time_skey,  sale_detail_skey  ,  item_skey as pos_item_skey  ,  '-1' as folio_item_skey  , person_skey  ,  itinerary_skey  ,  outlet_skey  ,  '-1'activity_skey  , saleqty as sale_quantity  ,  unitprice as item_list_price_amount  ,  0.0 as debit_amount ,   0.0 as credit_amount ,  checkdetailtaxamount as tax_amount  ,  discount_amount  ,  discount_value  ,  cost_of_goods  ,  manual_adjustment_amount  ,  total_collected_sales_amount  ,    NVL(total_collected_sales_amount/(guest_count*voyage_length),0) as total_collected_sales_ap   , total_collected_sales_amount - NVL(cost_of_goods,0) as total_shared_revenue_amount , NVL((total_collected_sales_amount - cost_of_goods)/(guest_count*voyage_length),0)  as total_shared_revenue_ap ,sale_date from (select * ,(unitprice-checkdetailtaxamount-discount_amount)*saleqty as  total_collected_sales_amount,CASE when (UPPER(person_type)=='COMPANY ACCOUNT') THEN NVL(((unitprice-checkdetailtaxamount-discount_amount)*saleqty),0) ELSE 0  end as manual_adjustment_amount,sales_trans.cruise_number_sales as cruise_no from(   select voyage_dim.voyage_number as cruise_number_sales,person.person_type, CASE   WHEN (UPPER(revenue_type)=='BEVERAGES') THEN    NVL(apollo_pos_change.costofgoods,0)+NVL(sestra.cost,0) ELSE   NVL(cog.price,0)   END AS cost_of_goods,revenue_rel.revenue_type,revenue_rel.category_value,NVL(itinerary_skey,-1) as itinerary_skey,sale_detail_skey,nvl(item_skey,-1)as item_skey ,person_skey,wearable_skey,charge_skey,nvl(outlet_skey,-1) as outlet_skey , sale.voyageid as voyage_id,sale.sales_id as sale_id,checkd.checkdetailsplunr as plunr, sale.sales_chargeid as charge_id,Cast(split(sales_date,'T')[0] as date)as sale_date,from_unixtime(UNIX_TIMESTAMP(Cast(concat(split(sales_time,'T')[0]," ",split(sales_time,'T')[1])as timestamp)), 'YYYY-MM-dd HH:mm:ss')as sale_time ,  sale.sales_outletid as outlet_id, checkd.checkdetailsalesqty as saleqty  ,   case when sale.sales_checkstatus == 'V' then ABS(NVL(checkd.checkdetailunitprice,0))  else NVL(checkd.checkdetailunitprice,0)  end as unitprice,  case when sale.sales_checkstatus == 'V' then ABS(NVL(checkd.checkdetailtaxamount,0))  else NVL(checkd.checkdetailtaxamount,0)  end as checkdetailtaxamount,  case when sale.sales_checkstatus == 'V' then ABS(NVL(checkd.discount_amount,0))  else NVL(checkd.discount_amount,0)  end as discount_amount,  case when sale.sales_checkstatus == 'V' then ABS(NVL(checkd.discount_value,0))  else NVL(checkd.discount_value,0)  end as discount_value,  case when sale.sales_checkstatus == 'V' then ABS(NVL(cog.price ,0))  else NVL(cog.price,0)  end as price        from (select * from (select  checkdetailsguid,price, ROW_NUMBER() OVER (Partition by checkdetailsguid ORDER by datediff(valid_to,valid_from) asc) as rn  from( select Cast(split(validfrom,'T')[0] as date) as valid_from ,Cast(split(validto,'T')[0] as date) as valid_to,checkdetailsguid,price FROM shipdw.hvtb_parse_mxp_pos_sales_costofgood where validfrom is not null)a where CURRENT_DATE BETWEEN a.valid_from AND a.valid_to)b where b.rn=1) cog left join (select discount_value, discount_amount,checkdetailtaxamount,checkdetailunitprice,checkdetailsalesqty,checkdetailsplunr, checkdetailsguid,sales_guid from shipdw.hvtb_parse_mxp_pos_sales_checkdetails group by discount_value, discount_amount,checkdetailtaxamount,checkdetailunitprice,checkdetailsalesqty,checkdetailsplunr, checkdetailsguid,sales_guid  ) checkd on(cog.checkdetailsguid==checkd.checkdetailsguid) left join ((select * from(select sales_checkstatus,voyageid,sales_chargeid,sales_shipcode, sales_time,sales_date,sales_id,sales_guid, sales_outletid ,row_number() over( partition by sales_id,sales_guid, sales_outletid ,sales_shipcode order by sales_date desc,sales_time desc)  as rn from shipdw.hvtb_parse_mxp_pos_sales)sales_check where sales_check.rn=1      ) ) sale on(checkd.sales_guid==sale.sales_guid)  left join shipdw.hvtb_mart_dim_voyage voyage_dim on (sale.sales_shipcode == voyage_dim.ship_code and Cast(split(sales_date,'T')[0] as date) between voyage_dim.voyage_start_date and voyage_dim.voyage_end_date) left join (   select * from (select voyage_number,itinerary_day_date,itinerary_skey ,row_number() over(partition by voyage_number,itinerary_day_date order by  itinerary_day_date desc ) as rn from shipdw.hvtb_mart_dim_itinerary )iti where iti.rn=1  ) dim_itinerary on(voyage_dim.voyage_number==dim_itinerary.voyage_number and Cast(split(sales_date,'T')[0] as date)==dim_itinerary.itinerary_day_date)   left join shipdw.hvtb_mart_dim_sale_detail as sale_detail on(sale_detail_src='POS' and sale.sales_id==sale_detail.sale_id ) left join shipdw.hvtb_mart_dim_pos_item as item on(checkd.checkdetailsplunr==item.item_plu_number) left join (  select person_type,person_charge_id,booking_cruise_number, dim_person.person_skey,wearable_skey,charge_skey   from shipdw.hvtb_mart_dim_person dim_person  left join   shipdw.hvtb_mart_dim_wearable wearable_dim   on(dim_person.guest_wearable_id == wearable_dim.wearable_id)  left join shipdw.hvtb_mart_dim_charge charge_dim on(dim_person.person_charge_id == charge_dim.charge_id)   ) as  person on(sale.sales_chargeid ==person.person_charge_id) left join  shipdw.hvtb_mart_dim_outlet as outlet on(sale.sales_outletid==outlet.outlet_id)      left join shipdw.hvtb_parse_apollo_poschange as apollo_pos_change on(checkd.checkdetailsplunr==apollo_pos_change.plunr) left join shipdw.hvtb_mart_lkp_sestra_cogs as sestra on(cast(checkd.checkdetailsplunr as bigint)==sestra.plunbr)left join shipdw.hvtb_mart_lkp_revenue_category_rel as revenue_rel on(item.item_plu_category_name==revenue_rel.category_value and revenue_rel.category_type='POS_SALESOUTLET_NAME')    where NVL(revenue_rel.pos_include_flg,'Y')!='N'  )sales_trans      )sales left join shipdw.hvtb_mart_dim_date dated on(dated.`date`==sales.sale_date) left join shipdw.hvtb_mart_dim_time timed on(timed.second_of_day==hour(sale_time)*60*60+minute(sale_time)*60+second(sale_time) ) left join (select count(*) as guest_count, voyage_number ,abs(voyage_length) as voyage_length  from shipdw.hvtb_mart_dim_voyage voyage inner join shipdw.hvtb_mart_dim_person person on (voyage.voyage_number==person.booking_cruise_number) where person_type="Guest"  group by voyage_number,abs(voyage_length))guest_count on(guest_count.voyage_number==sales.cruise_no)  """)
  log.info(s"""Select cast(sale_time as date),sale_id,"N/A" as booking_reference_number,-1 as device_skey,wearable_skey,charge_skey,0 as time_on_device_seconds,0 as avg_wager_amount,0 as casino_game_dph,0 as theoritical_winnings,'POS' as source_type,revenue_type,sales.voyage_id ,dated.date_id as sale_date_skey,timed.s_key as sale_time_skey,  sale_detail_skey  ,  item_skey as pos_item_skey  ,  '-1' as folio_item_skey  , person_skey  ,  itinerary_skey  ,  outlet_skey  ,  '-1'activity_skey  , saleqty as sale_quantity  ,  unitprice as item_list_price_amount  ,  0.0 as debit_amount ,   0.0 as credit_amount ,  checkdetailtaxamount as tax_amount  ,  discount_amount  ,  discount_value  ,  cost_of_goods  ,  manual_adjustment_amount  ,  total_collected_sales_amount  ,    NVL(total_collected_sales_amount/(guest_count*voyage_length),0) as total_collected_sales_ap   , total_collected_sales_amount - NVL(cost_of_goods,0) as total_shared_revenue_amount , NVL((total_collected_sales_amount - cost_of_goods)/(guest_count*voyage_length),0)  as total_shared_revenue_ap ,sale_date from (select * ,(unitprice-checkdetailtaxamount-discount_amount)*saleqty as  total_collected_sales_amount,CASE when (UPPER(person_type)=='COMPANY ACCOUNT') THEN NVL(((unitprice-checkdetailtaxamount-discount_amount)*saleqty),0) ELSE 0  end as manual_adjustment_amount,sales_trans.cruise_number_sales as cruise_no from(   select voyage_dim.voyage_number as cruise_number_sales,person.person_type, CASE   WHEN (UPPER(revenue_type)=='BEVERAGES') THEN    NVL(apollo_pos_change.costofgoods,0)+NVL(sestra.cost,0) ELSE   NVL(cog.price,0)   END AS cost_of_goods,revenue_rel.revenue_type,revenue_rel.category_value,NVL(itinerary_skey,-1) as itinerary_skey,sale_detail_skey,nvl(item_skey,-1)as item_skey ,person_skey,wearable_skey,charge_skey,nvl(outlet_skey,-1) as outlet_skey , sale.voyageid as voyage_id,sale.sales_id as sale_id,checkd.checkdetailsplunr as plunr, sale.sales_chargeid as charge_id,Cast(split(sales_date,'T')[0] as date)as sale_date,from_unixtime(UNIX_TIMESTAMP(Cast(concat(split(sales_time,'T')[0]," ",split(sales_time,'T')[1])as timestamp)), 'YYYY-MM-dd HH:mm:ss')as sale_time ,  sale.sales_outletid as outlet_id, checkd.checkdetailsalesqty as saleqty  ,   case when sale.sales_checkstatus == 'V' then ABS(NVL(checkd.checkdetailunitprice,0))  else NVL(checkd.checkdetailunitprice,0)  end as unitprice,  case when sale.sales_checkstatus == 'V' then ABS(NVL(checkd.checkdetailtaxamount,0))  else NVL(checkd.checkdetailtaxamount,0)  end as checkdetailtaxamount,  case when sale.sales_checkstatus == 'V' then ABS(NVL(checkd.discount_amount,0))  else NVL(checkd.discount_amount,0)  end as discount_amount,  case when sale.sales_checkstatus == 'V' then ABS(NVL(checkd.discount_value,0))  else NVL(checkd.discount_value,0)  end as discount_value,  case when sale.sales_checkstatus == 'V' then ABS(NVL(cog.price ,0))  else NVL(cog.price,0)  end as price        from (select * from (select  checkdetailsguid,price, ROW_NUMBER() OVER (Partition by checkdetailsguid ORDER by datediff(valid_to,valid_from) asc) as rn  from( select Cast(split(validfrom,'T')[0] as date) as valid_from ,Cast(split(validto,'T')[0] as date) as valid_to,checkdetailsguid,price FROM shipdw.hvtb_parse_mxp_pos_sales_costofgood where validfrom is not null)a where CURRENT_DATE BETWEEN a.valid_from AND a.valid_to)b where b.rn=1) cog left join (select discount_value, discount_amount,checkdetailtaxamount,checkdetailunitprice,checkdetailsalesqty,checkdetailsplunr, checkdetailsguid,sales_guid from shipdw.hvtb_parse_mxp_pos_sales_checkdetails group by discount_value, discount_amount,checkdetailtaxamount,checkdetailunitprice,checkdetailsalesqty,checkdetailsplunr, checkdetailsguid,sales_guid  ) checkd on(cog.checkdetailsguid==checkd.checkdetailsguid) left join ((select * from(select sales_checkstatus,voyageid,sales_chargeid,sales_shipcode, sales_time,sales_date,sales_id,sales_guid, sales_outletid ,row_number() over( partition by sales_id,sales_guid, sales_outletid ,sales_shipcode order by sales_date desc,sales_time desc)  as rn from shipdw.hvtb_parse_mxp_pos_sales)sales_check where sales_check.rn=1      ) ) sale on(checkd.sales_guid==sale.sales_guid)  left join shipdw.hvtb_mart_dim_voyage voyage_dim on (sale.sales_shipcode == voyage_dim.ship_code and Cast(split(sales_date,'T')[0] as date) between voyage_dim.voyage_start_date and voyage_dim.voyage_end_date) left join (   select * from (select voyage_number,itinerary_day_date,itinerary_skey ,row_number() over(partition by voyage_number,itinerary_day_date order by  itinerary_day_date desc ) as rn from shipdw.hvtb_mart_dim_itinerary )iti where iti.rn=1  ) dim_itinerary on(voyage_dim.voyage_number==dim_itinerary.voyage_number and Cast(split(sales_date,'T')[0] as date)==dim_itinerary.itinerary_day_date)   left join shipdw.hvtb_mart_dim_sale_detail as sale_detail on(sale_detail_src='POS' and sale.sales_id==sale_detail.sale_id ) left join shipdw.hvtb_mart_dim_pos_item as item on(checkd.checkdetailsplunr==item.item_plu_number) left join (  select person_type,person_charge_id,booking_cruise_number, dim_person.person_skey,wearable_skey,charge_skey   from shipdw.hvtb_mart_dim_person dim_person  left join   shipdw.hvtb_mart_dim_wearable wearable_dim   on(dim_person.guest_wearable_id == wearable_dim.wearable_id)  left join shipdw.hvtb_mart_dim_charge charge_dim on(dim_person.person_charge_id == charge_dim.charge_id)   ) as  person on(sale.sales_chargeid ==person.person_charge_id) left join  shipdw.hvtb_mart_dim_outlet as outlet on(sale.sales_outletid==outlet.outlet_id)      left join shipdw.hvtb_parse_apollo_poschange as apollo_pos_change on(checkd.checkdetailsplunr==apollo_pos_change.plunr) left join shipdw.hvtb_mart_lkp_sestra_cogs as sestra on(cast(checkd.checkdetailsplunr as bigint)==sestra.plunbr)left join shipdw.hvtb_mart_lkp_revenue_category_rel as revenue_rel on(item.item_plu_category_name==revenue_rel.category_value and revenue_rel.category_type='POS_SALESOUTLET_NAME')    where NVL(revenue_rel.pos_include_flg,'Y')!='N'  )sales_trans      )sales left join shipdw.hvtb_mart_dim_date dated on(dated.`date`==sales.sale_date) left join shipdw.hvtb_mart_dim_time timed on(timed.second_of_day==hour(sale_time)*60*60+minute(sale_time)*60+second(sale_time) ) left join (select count(*) as guest_count, voyage_number ,abs(voyage_length) as voyage_length  from shipdw.hvtb_mart_dim_voyage voyage inner join shipdw.hvtb_mart_dim_person person on (voyage.voyage_number==person.booking_cruise_number) where person_type="Guest"  group by voyage_number,abs(voyage_length))guest_count on(guest_count.voyage_number==sales.cruise_no)  """)
  */
      stageRevenueDF.printSchema()

      val required_columns = spark.sparkContext.getConf.get("spark.source.columns").split(",")

      val missing_columns = ArrayBuffer[String]()
      val avaliable_columns = ArrayBuffer[String]()
      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

      for (col <- required_columns) {

        if (hasColumn(stageRevenueDF, col)) {
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

      print(missing_columns, "Here are the missing columns")
      val stage_final_df = missing_columns.foldLeft(stageRevenueDF)((df, c) =>
        df.withColumn(s"$c", lit("N/A")))

      println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxx-------------------------------Source-Stage Pos Revenue Fact------------------------------------xxxxxxxxxxxxxxxxxxxxxx")

      loadDimFact(spark: SparkSession, stage_final_df)
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)

    } catch {

      case e: SQLException => { ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("******************in the catch of POS Revenue Factn Load ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace", e); }

      case e: Exception =>
        {
          ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("******************in the catch of POS Revenue Factn Load ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace", e);
          println("#--ERRoR--#")
        }
        println("#----------------------------Process Has Failed---------------------------#")
        log.info("#----------------------------Process Has Failed---------------------------#")
        System.exit(1)

    }

  }

}