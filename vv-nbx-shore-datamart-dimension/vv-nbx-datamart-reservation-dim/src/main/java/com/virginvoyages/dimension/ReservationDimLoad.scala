package com.virginvoyages.dimension

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

object ReservationDim {

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
        
        

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
val tempTable = spark.sparkContext.getConf.get("spark.target.temptablename").trim()
val tgtTable =  spark.sparkContext.getConf.get("spark.target.tgttablename").trim()

import spark.implicits._
/* set the load and update to the same time in the batch */
val currtime = Calendar.getInstance().getTime()
val outputFormat = "yyyy-MM-dd HH:mm:ss"
val currts = new SimpleDateFormat(outputFormat).format(currtime)

val accTransDtl = spark.sql("""
select trans_res_id, ts_ms as accTransDtlTs_ms from (
select CASE WHEN source_entity_type = 'RES' THEN source_entity_id WHEN dest_entity_type = 'RES' THEN dest_entity_id END as trans_res_id, ts_ms, row_number() OVER ( PARTITION BY CASE WHEN source_entity_type = 'RES' THEN source_entity_id WHEN dest_entity_type = 'RES' THEN dest_entity_id END ORDER BY ts_ms DESC,lsn desc) as rn
from vv_db.hvtb_parse_sw_rpl_acc_trans_detail where ( source_entity_type = 'RES' OR dest_entity_type = 'RES' ) and form_of_trans = 'CPN' and trans_status IN ( 'OK') and trans_type IN ( 'PMNT' ,'REFUND','MANUAL REFUND' ) ) OuterQry where rn = 1
""")
      accTransDtl.createOrReplaceTempView("accTransDtlTbl")
	  
	  val rlpAgency = spark.sql("""
select agency_id, is_internal , ts_ms as rlpAgencyTs_ms from (select agency_id, is_internal, ts_ms, row_number() OVER ( PARTITION BY agency_id ORDER BY ts_ms DESC,lsn desc ) as rn from vv_db.hvtb_parse_sw_rpl_agency ) outerQry where rn = 1 
""")
      rlpAgency.createOrReplaceTempView("rlpAgencyTbl")
	  
	  val resEvent = spark.sql("""
SELECT min(event_timestamp) as first_cancellation_date,max(event_timestamp) cancellation_date, res_id FROM vv_db.hvtb_parse_sw_rpl_res_event WHERE new_status = 'CX' GROUP BY res_id
""")
      resEvent.createOrReplaceTempView("resEventTbl")
	  
	  val resEvent_bk = spark.sql("""
SELECT min(event_timestamp) as bk_date, res_id FROM vv_db.hvtb_parse_sw_rpl_res_event WHERE new_status = 'BK' GROUP BY res_id
""")
      resEvent_bk.createOrReplaceTempView("resEventTbl_bk")
	  
	  val resHistory = spark.sql(""" Select operator, res_id, trans_timestamp from ( SELECT performer as operator, res_id, trans_timestamp, row_number() OVER ( PARTITION BY res_id ORDER BY trans_timestamp DESC,lsn desc ) as rn FROM vv_db.hvtb_parse_sw_rpl_res_history) outerQry where rn = 1 """)
      resHistory.createOrReplaceTempView("resHistoryTbl")
	  
	  val resPackage = spark.sql("""
SELECT max(effective_date) AS effective_date, res_id FROM vv_db.hvtb_parse_sw_rpl_res_package WHERE package_class = 'VOYAGE' GROUP BY res_id 
""")
      resPackage.createOrReplaceTempView("resPackageTbl")
	  
	  val CancellCase = spark.sql("""
select cancellation_case, ts_ms as CancellCaseTs_ms, case_id from (
select case_name as cancellation_case, case_id, ts_ms, row_number() OVER ( PARTITION BY case_id ORDER BY ts_ms DESC,lsn desc) as rn FROM vv_db.hvtb_parse_sw_rpl_cancellation_case
) where rn = 1
""")
      CancellCase.createOrReplaceTempView("CancellCaseTbl")
	  
	  val HotelReq = spark.sql("""
SELECT res_id, ts_ms as HotelReqTs_ms from (
SELECT res_id ,ts_ms,  row_number() OVER ( PARTITION BY res_id ORDER BY ts_ms DESC,lsn desc ) rn FROM vv_db.hvtb_parse_sw_rpl_res_hotel_request
) OuterQry where rn = 1
""")
      HotelReq.createOrReplaceTempView("HotelReqTbl")
	  
	   val SailHdr = spark.sql("""
SELECT ship_code, dep_ref_id, arr_ref_id, src_sail_id, ts_ms as SailHdrTs_ms from (
SELECT ship_code, dep_ref_id, arr_ref_id ,sail_id AS src_sail_id, ts_ms, row_number() OVER ( PARTITION BY sail_id ORDER BY ts_ms DESC,lsn desc) as rn FROM vv_db.hvtb_parse_sw_rpl_sail_header) OuterQry where rn = 1
""")
      SailHdr.createOrReplaceTempView("SailHdrTbl")
	  
val recResHeaderSrc_int = spark.sql("""select distinct case when accTransDtlTbl.trans_res_id is not null then 'Y' else 'N' end as opted_fvc ,resEventTbl_bk.bk_date,rh.group_id,rh.res_id, rh.res_init_date, rh.res_guest_count, rh.res_mode, resEventTbl.cancellation_date,resEventTbl.first_cancellation_date, rh.res_status, resHistoryTbl.operator, rh.res_type, rlpAgencyTbl.is_internal, rh.source_code,resPackageTbl.effective_date, sh.src_sail_id,rh.ship_code, cc.cancellation_case, rh.currency_code as currency, rh.currency_rate as currency_rate,case when row_number() OVER (PARTITION BY rh.res_id ORDER BY rh.last_updated_at, rh.ts_ms DESC) = count(rh.res_id) OVER (PARTITION BY rh.res_id) then greatest(rh.ts_ms, resEventTbl.cancellation_date,resEventTbl.first_cancellation_date, resHistoryTbl.trans_timestamp, resPackageTbl.effective_date, accTransDtlTbl.accTransDtlTs_ms, sh.SailHdrTs_ms, rlpAgencyTbl.rlpAgencyTs_ms, cc.CancellCaseTs_ms) else greatest(rh.last_updated_at,rh.ts_ms) end as src_date,case when rh.ship_code <> 'XE' then 'Y' when rh.ship_code = 'XE' and rh.res_init_date >= cast('2022-04-22 00:00:00' as timestamp) then 'Y' else 'N' end load_flag,rh.agency_id,rh.agent_id,rh.sec_agency_id,rh.sec_agent_id from
(select * from ( select row_number() over (partition by res_id,ts_ms order by last_updated_at,lsn desc) r , * from vv_db.hvtb_parse_sw_rpl_res_header)t where r = 1 ) rh 
LEFT JOIN 
accTransDtlTbl on rh.res_id = accTransDtlTbl.trans_res_id 
left join CancellCaseTbl cc on rh.cancellation_case = cc.case_id
left join resEventTbl on rh.res_id = resEventTbl.res_id
left join resEventTbl_bk on resEventTbl_bk.res_id = rh.res_id 
left join resHistoryTbl on rh.res_id = resHistoryTbl.res_id
left join resPackageTbl on rh.res_id = resPackageTbl.res_id
left join rlpAgencyTbl on rh.agency_id = rlpAgencyTbl.agency_id
left join SailHdrTbl sh on  rh.ship_code = sh.ship_code and rh.dep_ref_id = sh.dep_ref_id and rh.arr_ref_id = sh.arr_ref_id
""")
recResHeaderSrc_int.filter("load_flag = 'Y'").createOrReplaceTempView("recResHeaderSrc_intTbl")

val recResHeaderSrc = spark.sql("""select bk_date,cancellation_case,group_id,opted_fvc, src_date, i.res_id, res_init_date, res_guest_count, res_mode, cancellation_date,first_cancellation_date, res_status, operator, res_type, is_internal, source_code,effective_date,src_sail_id,ship_code,currency,currency_rate,agency_id,agent_id,sec_agency_id,sec_agent_id from  recResHeaderSrc_intTbl i """)
recResHeaderSrc.createOrReplaceTempView("recResHeaderSrcTbl")

/* Get records from target table */
val md5_scdTgt = """ md5(concat(coalesce(bk_date,'1990-01-01 00:00:00'),coalesce(src_res_id, 0),coalesce(cancellation_case,'~'),coalesce(currency,'~'),coalesce(currency_rate,0),coalesce(src_group_id,0), coalesce(res_init_date, '1990-01-01 00:00:00'), coalesce(res_guest_count, 0), coalesce(res_mode, '~'), coalesce(cancellation_date, '1990-01-01 00:00:00'),coalesce(first_cancellation_date, '1990-01-01 00:00:00'), coalesce(res_status, '~'), coalesce(operator, '~'), coalesce(res_type, '~'), coalesce(is_internal, '~'), coalesce(source_code, '~'), coalesce(effective_date, '1990-01-01 00:00:00'), coalesce(vip_status, '~'), coalesce(stage, '~'), coalesce(booking_source, '~'), coalesce(last_modifiedby_id, '~'), coalesce(src_sail_id, '~') ,coalesce(seaware_agency_id, 0),coalesce(seaware_agent_id, 0), coalesce(opted_fvc,'~'),coalesce(agency_id,0),coalesce(agent_id,0),coalesce(sec_agency_id,0),coalesce(sec_agent_id,0) )) """

val md5_dwhTgt = """ md5(concat(coalesce(bk_date,'1990-01-01 00:00:00'),coalesce(src_res_id, 0),coalesce(cancellation_case,'~'),coalesce(currency,'~'),coalesce(currency_rate,0),coalesce(src_group_id,0), coalesce(res_init_date, '1990-01-01 00:00:00'), coalesce(res_guest_count, 0), coalesce(res_mode, '~'), coalesce(cancellation_date, '1990-01-01 00:00:00'),coalesce(first_cancellation_date, '1990-01-01 00:00:00'), coalesce(res_status, '~'), coalesce(operator, '~'), coalesce(res_type, '~'), coalesce(is_internal, '~'), coalesce(source_code, '~'), coalesce(effective_date, '1990-01-01 00:00:00'), coalesce(hotel_flag, '~'), coalesce(src_sail_id, '~'), coalesce(opted_fvc,'~') ,coalesce(agency_id,0),coalesce(agent_id,0),coalesce(sec_agency_id,0),coalesce(sec_agent_id,0) )) """

val md5_crmTgt = """ md5(concat(coalesce(vip_status, '~'), coalesce(stage, '~'), coalesce(booking_source, '~'), coalesce(last_modifiedby_id, '~'),coalesce(seaware_agency_id, 0),coalesce(seaware_agent_id, 0) )) """

val resDimTgt = spark.sql("""
select res_id, rec_start_dttm, rec_end_dttm, src_res_id, res_init_date, res_guest_count, res_mode, cancellation_date, first_cancellation_date,res_status, operator, res_type, is_internal, source_code, effective_date, md5_dim, etl_ld_status	, load_dt, upd_dt, vip_status, stage, booking_source, last_modifiedby_id, hotel_flag, src_sail_id,seaware_agency_id,seaware_agent_id ,opted_fvc,cancellation_case,src_group_id,currency,currency_rate,bk_date,"""+md5_scdTgt +""" as md5_scdTgt, """+md5_dwhTgt +""" as md5_dwhTgt, """+md5_crmTgt +""" as md5_crmTgt,agency_id,agent_id,sec_agency_id,sec_agent_id
from """+tgtTable +""" where rec_end_dttm = '9999-12-31 00:00:00'
""")

resDimTgt.createOrReplaceTempView("resDimTgtTbl")

/* Process only those records from source which have not been processed in the target or the last record from target (there could be change from other source, hence last record has to be retireved */
val recResHdrSrcProc = spark.sql(""" select a.bk_date src_bk_date,b.bk_date,a.agency_id src_agency_id,a.agent_id src_agent_id,a.sec_agency_id src_sec_agency_id,a.sec_agent_id src_sec_agent_id,b.agency_id,b.agent_id,b.sec_agency_id,b.sec_agent_id,b.currency,b.currency_rate,a.currency src_currency,a.currency_rate src_currency_rate,b.cancellation_case, a.cancellation_case src_cancellation_case, a.group_id src_src_group_id, a.opted_fvc as src_opted_fvc ,a.src_date as src_date, a.res_id as src_src_res_id, a.res_init_date as src_res_init_date, a.res_guest_count as src_res_guest_count, a.res_mode as src_res_mode, a.cancellation_date as src_cancellation_date,a.first_cancellation_date as src_first_cancellation_date, a.res_status as src_res_status, a.operator as src_operator, a.res_type as src_res_type, a.is_internal as src_is_internal, a.source_code as src_source_code,a.effective_date as src_effective_date,a.src_sail_id as src_src_sail_id,a.ship_code,b.src_group_id, b.opted_fvc, b.res_id, b.rec_start_dttm as rec_start_dttm, b.rec_end_dttm, b.src_res_id as src_res_id, b.res_init_date, b.res_guest_count, b.res_mode, b.cancellation_date,b.first_cancellation_date, b.res_status, b.operator, b.res_type, b.is_internal, b.source_code, b.effective_date, b.md5_dim, b.etl_ld_status	, b.load_dt, b.upd_dt, b.vip_status, b.stage, b.booking_source, b.last_modifiedby_id, b.hotel_flag, b.src_sail_id, b.seaware_agency_id,b.seaware_agent_id,b.md5_scdTgt, b.md5_dwhTgt, b.md5_crmTgt from recResHeaderSrcTbl a  full join resDimTgtTbl b on a.res_id = b.src_res_id where src_date >= rec_start_dttm or src_res_id is null""").withColumn("src_src_res_id",$"src_src_res_id".cast("Int")).withColumn("src_res_guest_count",$"src_res_guest_count".cast("Int"))
recResHdrSrcProc.createOrReplaceTempView("recResHdrSrcProcTbl")

val md5_landing = """ md5(concat(coalesce(a.src_bk_date,'1990-01-01 00:00:00'),coalesce(a.src_src_res_id, 0),coalesce(a.src_currency,'~'),coalesce(a.src_currency_rate,0),coalesce(a.src_cancellation_case, '~'), coalesce(a.src_src_group_id, 0), coalesce(a.src_res_init_date, '1990-01-01 00:00:00'), coalesce(a.src_res_guest_count, 0), coalesce(a.src_res_mode, '~'), coalesce(a.src_cancellation_date, '1990-01-01 00:00:00'), coalesce(a.src_first_cancellation_date, '1990-01-01 00:00:00'),coalesce(a.src_res_status, '~'), coalesce(a.src_operator, '~'), coalesce(a.src_res_type, '~'), coalesce(a.src_is_internal, '~'), coalesce(a.src_source_code, '~'), coalesce(a.src_effective_date, '1990-01-01 00:00:00'), coalesce(op.vip_status__c, '~'), coalesce(op.stagename, '~'), coalesce(op.bookingsource__c, '~'), coalesce(op.lastmodifiedbyid, '~'), coalesce(CASE WHEN rhtl.res_id is not null AND a.res_status = 'BK' THEN 'Y' ELSE 'N' END,'~'), coalesce(a.src_src_sail_id, '~'),coalesce(seaware_agency_id__c, 0),coalesce(seaware_agent_id__c, 0), coalesce(src_opted_fvc,'~'),coalesce(src_agency_id,0),coalesce(src_agent_id,0),coalesce(src_sec_agency_id,0),coalesce(src_sec_agent_id,0) )) """

val md5_scdSrc = """ md5(concat(coalesce(a.src_bk_date,'1990-01-01 00:00:00'),coalesce(a.src_src_res_id, 0),coalesce(a.src_currency,'~'),coalesce(a.src_currency_rate,0),coalesce(a.src_cancellation_case, '~'),coalesce(a.src_src_group_id, 0), coalesce(a.src_res_init_date, '1990-01-01 00:00:00'), coalesce(a.src_res_guest_count, 0), coalesce(a.src_res_mode, '~'), coalesce(a.src_cancellation_date, '1990-01-01 00:00:00'),coalesce(a.src_first_cancellation_date, '1990-01-01 00:00:00'), coalesce(a.src_res_status, '~'), coalesce(a.src_operator, '~'), coalesce(a.src_res_type, '~'), coalesce(a.src_is_internal, '~'), coalesce(a.src_source_code, '~'), coalesce(a.src_effective_date, '1990-01-01 00:00:00'), coalesce(op.vip_status__c, '~'), coalesce(op.stagename, '~'), coalesce(op.bookingsource__c, '~'), coalesce(op.lastmodifiedbyid, '~'), coalesce(CASE WHEN rhtl.res_id is not null AND a.res_status = 'BK' THEN 'Y' ELSE 'N' END,'~'), coalesce(a.src_src_sail_id, '~'),coalesce(seaware_agency_id__c, 0),coalesce(seaware_agent_id__c, 0), coalesce(src_opted_fvc,'~'),coalesce(src_agency_id,0),coalesce(src_agent_id,0),coalesce(src_sec_agency_id,0),coalesce(src_sec_agent_id,0) )) """

val md5_dwhSrc = """ md5(concat(coalesce(a.src_bk_date,'1990-01-01 00:00:00'),coalesce(a.src_src_res_id, 0),coalesce(a.src_currency,'~'),coalesce(a.src_currency_rate,0),coalesce(a.src_cancellation_case, '~'),coalesce(a.src_first_cancellation_date, '1990-01-01 00:00:00'),coalesce(a.src_src_group_id, 0), coalesce(a.src_res_init_date, '1990-01-01 00:00:00'), coalesce(a.src_res_guest_count, 0), coalesce(a.src_res_mode, '~'), coalesce(a.src_cancellation_date, '1990-01-01 00:00:00'), coalesce(a.src_res_status, '~'), coalesce(a.src_operator, '~'), coalesce(a.src_res_type, '~'), coalesce(a.src_is_internal, '~'), coalesce(a.src_source_code, '~'), coalesce(a.src_effective_date, '1990-01-01 00:00:00'), coalesce(CASE WHEN rhtl.res_id is not null AND a.res_status = 'BK' THEN 'Y' ELSE 'N' END,'~'), coalesce(a.src_src_sail_id, '~'), coalesce(src_opted_fvc,'~'),coalesce(src_agency_id,0),coalesce(src_agent_id,0),coalesce(src_sec_agency_id,0),coalesce(src_sec_agent_id,0) )) """

val md5_crmSrc = """ md5(concat(coalesce(op.vip_status__c, '~'), coalesce(op.stagename, '~'), coalesce(op.bookingsource__c, '~'), coalesce(op.lastmodifiedbyid, '~'),coalesce(seaware_agency_id__c, 0),coalesce(seaware_agent_id__c, 0)) ) """


val recResHeader = spark.sql("""
select a.src_bk_date,a.bk_date,a.src_agency_id,a.src_agent_id,a.src_sec_agency_id,a.src_sec_agent_id,a.agency_id,a.agent_id,a.sec_agency_id,a.sec_agent_id,a.currency,a.currency_rate,a.src_currency,a.src_currency_rate,a.src_cancellation_case,a.cancellation_case,a.src_src_group_id,a.src_group_id,a.src_opted_fvc,a.src_date,	a.src_src_res_id,	a.src_res_init_date,	a.src_res_guest_count,	a.src_res_mode,	a.src_cancellation_date,a.src_first_cancellation_date,	a.src_res_status,	a.src_operator,	a.src_res_type,	a.src_is_internal,	a.src_source_code, a.opted_fvc,	a.res_id,	a.rec_start_dttm,	a.rec_end_dttm,	a.src_res_id,	a.res_init_date,	a.res_guest_count,	a.res_mode,	a.cancellation_date,a.first_cancellation_date,	a.res_status,a.operator,	a.res_type,	a.is_internal,	a.source_code,	a.src_effective_date,	a.md5_dim,	a.etl_ld_status, a.load_dt,	a.upd_dt,	a.vip_status,	a.stage,	a.booking_source,	a.last_modifiedby_id,	a.hotel_flag,	a.src_src_sail_id,	a.seaware_agency_id,a.seaware_agent_id,a.md5_scdTgt, a.md5_dwhTgt, a.md5_crmTgt, a.ship_code,a.effective_date,op.vip_status__c as src_vip_status, op.stagename as src_stage, op.bookingsource__c as src_booking_source, op.lastmodifiedbyid as src_last_modifiedby_id, op.lastmodifieddate, CASE WHEN rhtl.res_id is not null AND a.res_status = 'BK' THEN 'Y' ELSE 'N' END as src_hotel_flag, a.src_sail_id,seaware_agency_id__c as src_seaware_agency_id , seaware_agent_id__c as src_seaware_agent_id, """+ md5_landing + """ as md5_landing , """+ md5_scdSrc + """ as md5_scdSrc, """+ md5_dwhSrc + """ as md5_dwhSrc, """+ md5_crmSrc + """ as md5_crmSrc 
from recResHdrSrcProcTbl a
left join HotelReqTbl rhtl on a.res_id = rhtl.res_id 
left join vv_db.hvtb_parse_sfdc_opportunity op on a.src_src_res_id = op.reservation_number__c and isdeleted = false
left join vv_db.hvtb_nbx_mart_crm_agent_master_dim at on op.agentname__c = at.id and at.rec_end_dttm like '999%'
left join vv_db.hvtb_nbx_mart_crm_agency_master_dim ac on op.agencyname__c = ac.id and ac.rec_end_dttm like '999%'
""")
recResHeader.createOrReplaceTempView("recResHeaderTbl")

/* remove duplicates from landing table as seaware Oracle DWH may have data which does not change*/
/* scdLagGrp = set 1 for the first record (this will be used for start and end) */
/* scdLeadGrp = set 1 for the last record (this will be the actual record) */
val recResHeaderGrp = spark.sql("""
Select src_bk_date,bk_date,src_agency_id,src_agent_id,src_sec_agency_id,src_sec_agent_id,agency_id,agent_id,sec_agency_id,sec_agent_id,currency,currency_rate,src_currency,src_currency_rate,src_cancellation_case,cancellation_case,src_src_group_id,src_group_id,src_opted_fvc,src_date,	src_src_res_id,	src_res_init_date,	src_res_guest_count,	src_res_mode,	src_cancellation_date,src_first_cancellation_date,	src_res_status,	src_operator,	src_res_type,	src_is_internal,	src_source_code,	src_effective_date,	src_vip_status,	src_stage,	src_booking_source,	src_last_modifiedby_id,	src_hotel_flag,	src_src_sail_id,	md5_landing,opted_fvc,	res_id,	rec_start_dttm,	rec_end_dttm,	etl_ld_status,	load_dt,	upd_dt,	src_res_id,	res_init_date,	res_guest_count,	res_mode,	cancellation_date,first_cancellation_date,	res_status,	operator,	res_type,	is_internal,	source_code,	effective_date,	md5_dim,	vip_status,	stage,	booking_source,	last_modifiedby_id,	hotel_flag,	src_sail_id,	seaware_agency_id,seaware_agent_id,src_seaware_agency_id,src_seaware_agent_id,	md5_scdTgt,	md5_dwhTgt, md5_crmTgt, md5_scdSrc, md5_dwhSrc, md5_crmSrc, lastmodifieddate,
case when md5_scdSrc = lag(md5_scdSrc) over (partition by src_src_res_id order BY src_date) then 0 else row_number() over (partition by src_src_res_id, md5_scdSrc order by src_date) end as scdLagGrp,
case when md5_scdSrc = lead(md5_scdSrc) over (partition by src_src_res_id order BY src_date) then 0 else row_number() over (partition by src_src_res_id, md5_scdSrc order by src_date) end as scdLeadGrp
from
(select src_bk_date,bk_date,src_agency_id,src_agent_id,src_sec_agency_id,src_sec_agent_id,agency_id,agent_id,sec_agency_id,sec_agent_id,currency,currency_rate,src_currency,src_currency_rate,src_cancellation_case,cancellation_case,src_src_group_id,src_group_id,src_opted_fvc,src_date,	src_src_res_id,	src_res_init_date,	src_res_guest_count,	src_res_mode,	src_cancellation_date,src_first_cancellation_date,	src_res_status,	src_operator,	src_res_type,	src_is_internal,	src_source_code,	src_effective_date,	src_vip_status,	src_stage,	src_booking_source,	src_last_modifiedby_id,	src_hotel_flag,	src_src_sail_id,	md5_landing,opted_fvc,	res_id,	rec_start_dttm,	rec_end_dttm,	etl_ld_status,	load_dt,	upd_dt,	src_res_id,	res_init_date,	res_guest_count,	res_mode,	cancellation_date,first_cancellation_date,	res_status,	operator,	res_type,	is_internal,	source_code,	effective_date,	md5_dim,	vip_status,	stage,	booking_source,	last_modifiedby_id,	hotel_flag,	src_sail_id,seaware_agency_id,seaware_agent_id,src_seaware_agency_id,src_seaware_agent_id,	md5_scdTgt,	md5_dwhTgt, md5_crmTgt, md5_scdSrc, md5_dwhSrc, md5_crmSrc, lastmodifieddate,
case when temp.md5_landing = lag(temp.md5_landing) over (partition by temp.src_src_res_id order BY temp.src_date) then 'dup' 
when temp.src_date = lag(temp.src_date) over (partition by temp.src_src_res_id order BY temp.src_date,src_res_init_date,src_effective_date ,src_cancellation_date) then 'dup' 
else 'nodup' end as  dup_check   
from recResHeaderTbl temp) temp2
where dup_check = 'nodup'
""")

recResHeaderGrp.createOrReplaceTempView("recResHeaderGrpTbl")

val recResHeaderGrpLag = spark.sql("""
select src_date,  src_src_res_id, md5_landing, md5_scdSrc, scdLagGrp
from recResHeaderGrpTbl
where scdLagGrp != 0
""")

recResHeaderGrpLag.createOrReplaceTempView("recResHeaderGrpLagTbl")

val recResHeaderGrpLag1 = spark.sql("""
select  src_date,  src_src_res_id, md5_landing, md5_scdSrc, scdLagGrp, 
lead(scdLagGrp) over (partition by src_src_res_id, md5_scdSrc order BY src_date) as scdLagGrpNext
from recResHeaderGrpLagTbl
""")
recResHeaderGrpLag1.createOrReplaceTempView("recResHeaderGrpLag1Tbl")

/*separate out the Lag(first) and the lead(last) record  -- Lead*/
val recResHeaderGrpLead = spark.sql("""
select src_bk_date,bk_date,src_agency_id,src_agent_id,src_sec_agency_id,src_sec_agent_id,agency_id,agent_id,sec_agency_id,sec_agent_id,currency,currency_rate,src_currency,src_currency_rate,src_cancellation_case,cancellation_case,src_src_group_id,src_group_id,src_opted_fvc, src_date,	src_src_res_id,	src_res_init_date,	src_res_guest_count,	src_res_mode,	src_cancellation_date,src_first_cancellation_date,	src_res_status,	src_operator,	src_res_type,	src_is_internal,	src_source_code,	src_effective_date,	src_vip_status,	src_stage,	src_booking_source,	src_last_modifiedby_id,	src_hotel_flag,	src_src_sail_id,	md5_landing,	res_id,	rec_start_dttm,	rec_end_dttm,	etl_ld_status,	load_dt,	upd_dt,	src_res_id,	res_init_date,	res_guest_count,	res_mode,	cancellation_date,first_cancellation_date,	res_status,	operator,	res_type,	is_internal,	source_code,	effective_date,	md5_dim,	vip_status,	stage,	booking_source,	last_modifiedby_id,	hotel_flag,	src_sail_id,src_seaware_agency_id,src_seaware_agent_id,seaware_agency_id,seaware_agent_id,	md5_scdTgt,	md5_dwhTgt, md5_crmTgt, md5_scdSrc, md5_dwhSrc, md5_crmSrc, lastmodifieddate, scdLeadGrp,opted_fvc
from recResHeaderGrpTbl
where scdLeadGrp != 0
""")
recResHeaderGrpLead.createOrReplaceTempView("recResHeaderGrpLeadTbl")

/*Get the final records from source*/
val  NoDup = spark.sql("""select  lead.src_bk_date,lead.bk_date,lead.src_agency_id,lead.src_agent_id,lead.src_sec_agency_id,lead.src_sec_agent_id,lead.agency_id,lead.agent_id,lead.sec_agency_id,lead.sec_agent_id,lead.currency,lead.currency_rate,lead.src_currency,lead.src_currency_rate,lead.src_cancellation_case,lead.cancellation_case, lag.src_date as min_src_date, lag.src_date as lastChanged, lag.scdLagGrp, lag.scdLagGrpNext,lead.src_date,lead.src_src_group_id,lead.src_group_id,	lead.src_opted_fvc,lead.src_src_res_id,	lead.src_res_init_date,	lead.src_res_guest_count,	lead.src_res_mode,	lead.src_cancellation_date,lead.src_first_cancellation_date,	lead.src_res_status,	lead.src_operator,	lead.src_res_type,	lead.src_is_internal,	lead.src_source_code,	lead.src_effective_date,	lead.src_vip_status,	lead.src_stage,	lead.src_booking_source,	lead.src_last_modifiedby_id,	lead.src_hotel_flag,	lead.src_src_sail_id,	lead.md5_landing,	lead.res_id,	lead.rec_start_dttm,	lead.rec_end_dttm,	lead.etl_ld_status,	lead.load_dt,	lead.upd_dt,	lead.src_res_id,	lead.res_init_date,	lead.res_guest_count,	lead.res_mode,	lead.cancellation_date,lead.first_cancellation_date,	lead.res_status,	lead.operator,	lead.res_type,	lead.is_internal,	lead.source_code,	lead.effective_date,	lead.md5_dim,	lead.vip_status,	lead.stage,	lead.booking_source,	lead.last_modifiedby_id,	lead.hotel_flag,lead.opted_fvc,	lead.src_sail_id,	lead.md5_scdTgt,	lead.md5_dwhTgt, lead.md5_crmTgt,  lead.md5_scdSrc, lead.md5_dwhSrc, lead.md5_crmSrc, lead.lastmodifieddate, lead.scdLeadGrp, seaware_agency_id,seaware_agent_id,src_seaware_agency_id,src_seaware_agent_id
from recResHeaderGrpLag1Tbl lag join recResHeaderGrpLeadTbl lead
where lag.src_src_res_id = lead.src_src_res_id
and lag.md5_scdSrc = lead.md5_scdSrc
and lead.scdLeadGrp >= lag.scdLagGrp
and( (lead.scdLeadGrp < lag.scdLagGrpNext) or (lag.scdLagGrpNext is null) )
""")
NoDup.createOrReplaceTempView("NoDupTbl") 

/* set end date for all records except the last one from source. The last record should be compared against the target*/
/* set the min_src_date (start date) last next load date (end date) based on whether the change is from CRM or DWH */ 
val multiSrc = spark.sql("""
select min_src_date as original_min_src_date,
/*case 
when (md5_dwhSrc != md5_dwhTgt) then min_src_date
when (md5_dwhSrc = md5_dwhTgt) and (md5_crmSrc != md5_crmTgt) and (lastmodifieddate > min_src_date) then lastmodifieddate
else */min_src_date
/*end as*/ min_src_date,	
lastChanged,	scdLagGrp,	scdLagGrpNext,src_agency_id,src_bk_date,bk_date,src_agent_id,src_sec_agency_id,src_sec_agent_id,agency_id,agent_id,sec_agency_id,sec_agent_id,currency,currency_rate,src_currency,src_currency_rate,	src_date,src_cancellation_case,cancellation_case,src_src_group_id,src_group_id,src_opted_fvc,	src_src_res_id,	src_res_init_date,	src_res_guest_count,	src_res_mode,	src_cancellation_date,src_first_cancellation_date,	src_res_status,	src_operator,	src_res_type,	src_is_internal,	src_source_code,	src_effective_date,	src_vip_status,	src_stage,	src_booking_source,	src_last_modifiedby_id,	src_hotel_flag,	src_src_sail_id,	md5_landing,opted_fvc,	res_id,	rec_start_dttm,	rec_end_dttm,	etl_ld_status,	load_dt,	upd_dt,	src_res_id,	res_init_date,	res_guest_count,	res_mode,	cancellation_date,first_cancellation_date,	res_status,	operator,	res_type,	is_internal,	source_code,	effective_date,	md5_dim,	vip_status,	stage,	booking_source,	last_modifiedby_id,	hotel_flag,	src_sail_id, md5_scdTgt, md5_dwhTgt, md5_crmTgt, md5_scdSrc,	md5_dwhSrc, md5_crmSrc, lastmodifieddate, scdLeadGrp,  seaware_agency_id,seaware_agent_id,src_seaware_agency_id,src_seaware_agent_id,
cast(count(src_src_res_id) over (partition by src_src_res_id) as BIGINT) as tot_rec_cnt,
row_number()  over (partition by src_src_res_id order by src_date)  as curr_rec_cnt,
lead(min_src_date) over (partition by src_src_res_id order BY src_date) as original_next_src_date,
/*case 
when (md5_dwhSrc != md5_dwhTgt)   then lead(min_src_date) over (partition by src_src_res_id order BY src_date)
when  (md5_dwhSrc = md5_dwhTgt) and (md5_crmSrc != md5_crmTgt) and (lastmodifieddate > min_src_date) then lead(lastmodifieddate) over (partition by src_src_res_id order BY src_date) 
else */lead(min_src_date) over (partition by src_src_res_id order BY src_date) 
/*end*/ as next_src_date
from NoDupTbl
""")
multiSrc.createOrReplaceTempView("multiSrcTbl") 

/*Compare source and target so last record will always be there in both source and target*/
/* MULTISRC --> one with not null next_src_date should be taken - in case of single record this cond. will not satisfy */
/* SCDU --> will always be multiple records so the first record in multisrc is actual SCDU record */
/* NC --> in case of no change there will always be one record (tot_rec_cnt = 1) source is compared with target and there will always be one record */
/* I --> when there is no target, if multiple records from source then only last has to be inserted, earlier records will be MULTISRC */
/* SCDI --> last record is always SCD since there will be only one record with next_src_date is null */
/* UPD --> only when tot_rec_cnt = 1 and last record and the landing does not match */
val compareSrcTgt = spark.sql("""
select monotonically_increasing_id() as id, 
case when next_src_date is null then to_timestamp('9999-12-31 00:00:00', 'yyyy-MM-dd HH:mm:ss')
else (next_src_date - INTERVAL 1 seconds) 
end as src_rec_end_dttm,
min_src_date,	lastChanged,	scdLagGrp,	scdLagGrpNext,	src_date,	src_src_res_id,	src_res_init_date,	src_res_guest_count,	src_res_mode,	src_cancellation_date,src_first_cancellation_date,src_res_status,	src_operator,	src_res_type,	src_is_internal,	src_source_code,	src_effective_date,	src_vip_status,	src_stage,	src_booking_source,	src_last_modifiedby_id,	src_hotel_flag,	src_src_sail_id,	md5_landing,	res_id,	rec_start_dttm,	rec_end_dttm,	etl_ld_status,	load_dt,	upd_dt,	src_res_id,	res_init_date,	res_guest_count,	res_mode,	cancellation_date,first_cancellation_date,	res_status,	operator,	res_type,	is_internal,	source_code,	effective_date,	md5_dim,	vip_status,	stage,	booking_source,	last_modifiedby_id,	hotel_flag,	src_sail_id,	md5_scdTgt,	md5_scdSrc,	scdLeadGrp, tot_rec_cnt, curr_rec_cnt, next_src_date, md5_dwhSrc, md5_crmSrc, md5_dwhTgt, md5_crmTgt, lastmodifieddate,seaware_agency_id,seaware_agent_id,src_seaware_agency_id,src_seaware_agent_id,opted_fvc,src_opted_fvc,src_src_group_id,src_group_id,src_cancellation_case,cancellation_case,currency,currency_rate,src_currency,src_currency_rate,src_agency_id,src_agent_id,src_sec_agency_id,src_sec_agent_id,agency_id,agent_id,sec_agency_id,sec_agent_id,src_bk_date,bk_date,
case 
when (next_src_date is not null) and ( (res_id is null) or (curr_rec_cnt > 1) ) then 'MULTISRC'
when (res_id = -1) or ( (src_src_res_id is not null) and (src_res_id is not null) and (md5_landing =  md5_dim) and (next_src_date is null) and (tot_rec_cnt =1) ) then 'NC'
when (next_src_date is not null) and (res_id is not null) and (curr_rec_cnt = 1)  then 'SCDU'
when (res_id is null) and (next_src_date is null) then 'I'
when (src_src_res_id is not null) and (src_res_id is not null) and (min_src_date > rec_start_dttm) and (next_src_date is null) then 'SCDI'
when (src_src_res_id is not null) and (src_res_id is not null) and (min_src_date >= rec_start_dttm) and (md5_landing !=  md5_dim) and (next_src_date is null) and (tot_rec_cnt =1)  then 'UPD'
else 'IGNORE'
end as RecordStatus
from multiSrcTbl
""")
compareSrcTgt.persist(StorageLevel.MEMORY_AND_DISK)
compareSrcTgt.createOrReplaceTempView("compareSrcTgtTbl") 

/*get the last value of surrogate key from target */
/*val maxResId = spark.sql(""" select max(res_id) as max_cnt from vv_db.hvtb_nbx_core_sw_reservation_dim""").rdd.map(x=>x.mkString).collect*/
val maxResId = spark.sql("""select coalesce(max(res_id),0) as max_cnt from """+ tgtTable).rdd.map(x=>x.mkString).collect
val maxVal = maxResId(0).toInt


//compareSrcTgt.where($"src_res_id" === 282107).show(false)

/* Multiple records from source which have to be appended to the target */
val tgtMULTISRC = spark.sql("""
select id,
min_src_date as rec_start_dttm, src_rec_end_dttm as rec_end_dttm, src_src_res_id as src_res_id, src_res_init_date as res_init_date, src_res_guest_count as res_guest_count, src_res_mode as res_mode, src_cancellation_date as cancellation_date,src_first_cancellation_date as first_cancellation_date, src_res_status as res_status, src_operator as operator, src_res_type as res_type, src_is_internal as is_internal, src_source_code as source_code, src_effective_date as effective_date, md5_landing as md5_dim, RecordStatus as etl_ld_status,
cast('"""+ currts +"""' as timestamp) as load_dt,  cast('"""+ currts + """' as timestamp) as upd_dt, src_vip_status as vip_status, src_stage as stage, src_booking_source as booking_source, src_last_modifiedby_id as last_modifiedby_id, src_hotel_flag as hotel_flag, src_src_sail_id as src_sail_id,src_seaware_agency_id as seaware_agency_id,src_seaware_agent_id as seaware_agent_id,src_opted_fvc as opted_fvc,src_cancellation_case cancellation_case,src_src_group_id as src_group_id,src_currency currency, src_currency_rate currency_rate,src_agency_id agency_id,src_agent_id agent_id,src_sec_agency_id sec_agency_id,src_sec_agent_id sec_agent_id,src_bk_date as bk_date
from compareSrcTgtTbl
where RecordStatus = 'MULTISRC'
""").withColumn("res_id",(row_number().over(Window.orderBy("id"))+ maxVal)).select("res_id", "rec_start_dttm","rec_end_dttm", "src_res_id", "res_init_date", "res_guest_count", "res_mode", "cancellation_date","first_cancellation_date", "res_status", "operator", "res_type", "is_internal", "source_code", "effective_date", "md5_dim", "etl_ld_status", "load_dt", "upd_dt", "vip_status", "stage", "booking_source", "last_modifiedby_id", "hotel_flag", "src_sail_id","seaware_agency_id","seaware_agent_id","opted_fvc","cancellation_case","src_group_id","currency","currency_rate","agency_id","agent_id","sec_agency_id","sec_agent_id","bk_date")

/* Insert records to target - records as source , SCD insert records also same as source*/
/*******************************=======================***********/
val cntIns = tgtMULTISRC.count.toInt + maxVal
/*val InsCnt = """row_number() over (order by src_src_res_id,src_rec_start_dttm)  + """ +lit(cntIns)*/
val tgtI = spark.sql("""
select id,
min_src_date as rec_start_dttm, 
src_rec_end_dttm as rec_end_dttm, 
src_src_res_id as src_res_id, src_res_init_date as res_init_date, src_res_guest_count as res_guest_count, src_res_mode as res_mode, src_cancellation_date as cancellation_date,src_first_cancellation_date as first_cancellation_date, src_res_status as res_status, src_operator as operator, src_res_type as res_type, src_is_internal as is_internal, src_source_code as source_code, src_effective_date as effective_date, md5_landing as md5_dim, 
RecordStatus as etl_ld_status,
cast('"""+ currts +"""' as timestamp) as load_dt, cast('"""+ currts + """' as timestamp) as upd_dt, src_vip_status as vip_status, src_stage as stage, src_booking_source as booking_source, src_last_modifiedby_id as last_modifiedby_id, src_hotel_flag as hotel_flag, src_src_sail_id as src_sail_id ,src_seaware_agency_id as seaware_agency_id,src_seaware_agent_id as seaware_agent_id,src_opted_fvc as opted_fvc,src_cancellation_case cancellation_case,src_src_group_id as src_group_id,src_currency currency, src_currency_rate currency_rate,src_agency_id agency_id,src_agent_id agent_id,src_sec_agency_id sec_agency_id,src_sec_agent_id sec_agent_id,src_bk_date as bk_date
from compareSrcTgtTbl
where (RecordStatus = 'I') or (RecordStatus = 'SCDI')
""").withColumn("res_id",(row_number().over(Window.orderBy("id"))+ cntIns)).select("res_id", "rec_start_dttm","rec_end_dttm", "src_res_id", "res_init_date", "res_guest_count", "res_mode", "cancellation_date","first_cancellation_date","res_status", "operator", "res_type", "is_internal", "source_code", "effective_date", "md5_dim", "etl_ld_status", "load_dt", "upd_dt", "vip_status", "stage", "booking_source", "last_modifiedby_id", "hotel_flag", "src_sail_id","seaware_agency_id","seaware_agent_id","opted_fvc","cancellation_case","src_group_id","currency","currency_rate","agency_id","agent_id","sec_agency_id","sec_agent_id","bk_date")

/* Update end date of existing records for SCD */
val tgtSCD = spark.sql("""
Select tgt.res_id, tgt.rec_start_dttm, 
case when RecordStatus = 'SCDI' then (rec_start_dttm - INTERVAL 1 seconds)
else src_rec_end_dttm end as rec_end_dttm, 
tgt.src_res_id, tgt.res_init_date, tgt.res_guest_count, tgt.res_mode, tgt.cancellation_date,tgt.first_cancellation_date, tgt.res_status, tgt.operator, tgt.res_type, tgt.is_internal, tgt.source_code, tgt.effective_date, tgt.md5_dim, RecordStatus as etl_ld_status	, tgt.load_dt, cast('"""+ currts + """' as timestamp) as upd_dt, tgt.vip_status, tgt.stage, tgt.booking_source, tgt.last_modifiedby_id, tgt.hotel_flag, tgt.src_sail_id,seaware_agency_id as seaware_agency_id,seaware_agent_id as seaware_agent_id,opted_fvc as opted_fvc,cancellation_case,src_group_id,currency,currency_rate,agency_id,agent_id,sec_agency_id,sec_agent_id,bk_date
from compareSrcTgtTbl tgt 
where RecordStatus = 'SCDU'
""")

/* when CRM update only - then only one record both for SCDI and SCDU hence SCDU has to be added spearately*/
val tgtSCDCRMU = spark.sql("""
Select tgt.res_id, tgt.rec_start_dttm, (min_src_date - INTERVAL 1 seconds) as rec_end_dttm, 
tgt.src_res_id, tgt.res_init_date, tgt.res_guest_count, tgt.res_mode, tgt.cancellation_date,tgt.first_cancellation_date, tgt.res_status, tgt.operator, tgt.res_type, tgt.is_internal, tgt.source_code, tgt.effective_date, tgt.md5_dim, 'CRMU' as etl_ld_status	, tgt.load_dt, cast('"""+ currts + """' as timestamp) as upd_dt, tgt.vip_status, tgt.stage, tgt.booking_source, tgt.last_modifiedby_id, tgt.hotel_flag, tgt.src_sail_id,seaware_agency_id as seaware_agency_id,seaware_agent_id as seaware_agent_id,opted_fvc as opted_fvc,cancellation_case,src_group_id as src_group_id,currency,currency_rate,agency_id,agent_id,sec_agency_id,sec_agent_id,bk_date
from compareSrcTgtTbl tgt 
where RecordStatus = 'SCDI' and tot_rec_cnt = 1
""")

/* No change records 'NC' - remain same as target */
val tgtNC = spark.sql("""
Select tgt.res_id, tgt.rec_start_dttm, tgt.rec_end_dttm, tgt.src_res_id, tgt.res_init_date, tgt.res_guest_count, tgt.res_mode, tgt.cancellation_date,tgt.first_cancellation_date, tgt.res_status, tgt.operator, tgt.res_type, tgt.is_internal, tgt.source_code, tgt.effective_date, tgt.md5_dim, RecordStatus as etl_ld_status	, tgt.load_dt, tgt.upd_dt, tgt.vip_status, tgt.stage, tgt.booking_source, tgt.last_modifiedby_id, tgt.hotel_flag, tgt.src_sail_id,seaware_agency_id as seaware_agency_id,seaware_agent_id as seaware_agent_id,opted_fvc as opted_fvc,cancellation_case,src_group_id as src_group_id,currency,currency_rate,agency_id,agent_id,sec_agency_id,sec_agent_id,bk_date
from compareSrcTgtTbl tgt where RecordStatus = 'NC' 
""")

/* Update target record from source keep res_id, start and end of the target and update everything else from source*/
val tgtUPD = spark.sql("""
select res_id, rec_start_dttm,	 rec_end_dttm,
src_src_res_id as src_res_id, src_res_init_date as res_init_date, src_res_guest_count as res_guest_count, src_res_mode as res_mode, src_cancellation_date as cancellation_date,src_first_cancellation_date as first_cancellation_date, src_res_status as res_status, src_operator as operator, src_res_type as res_type, src_is_internal as is_internal, src_source_code as source_code, src_effective_date as effective_date, md5_landing as md5_dim, RecordStatus as etl_ld_status,
load_dt,  cast('"""+ currts + """' as timestamp) as upd_dt, src_vip_status as vip_status, src_stage as stage, src_booking_source as booking_source, src_last_modifiedby_id as last_modifiedby_id, src_hotel_flag as hotel_flag, src_src_sail_id as src_sail_id ,src_seaware_agency_id as seaware_agency_id,src_seaware_agent_id as seaware_agent_id,src_opted_fvc as opted_fvc,src_cancellation_case cancellation_case,src_src_group_id as src_group_id ,src_currency currency, src_currency_rate currency_rate,src_agency_id agency_id,src_agent_id agent_id,src_sec_agency_id sec_agency_id,src_sec_agent_id sec_agent_id,src_bk_date bk_date
from compareSrcTgtTbl  where (RecordStatus = 'UPD')
""")

/* historical data from target which has to be appended */
val tgtHIST = spark.sql("""
select res_id, rec_start_dttm, rec_end_dttm, src_res_id, res_init_date, res_guest_count, res_mode, cancellation_date,first_cancellation_date, res_status, operator, res_type, is_internal, source_code, effective_date, md5_dim, 'HIST' as etl_ld_status, load_dt, upd_dt, vip_status, stage, booking_source, last_modifiedby_id, hotel_flag, src_sail_id,seaware_agency_id as seaware_agency_id,seaware_agent_id as seaware_agent_id,opted_fvc as opted_fvc, cancellation_case,src_group_id as src_group_id,currency,currency_rate,agency_id,agent_id,sec_agency_id,sec_agent_id,bk_date
from """ + tgtTable  +""" where rec_end_dttm != '9999-12-31 00:00:00'
""")


val resDimtgtAll = tgtHIST union tgtMULTISRC union tgtSCD union tgtI union tgtUPD union tgtNC union tgtSCDCRMU
resDimtgtAll.createOrReplaceTempView("resDimtgtAlltbl")

val finaldf  = spark.sql("""
select res_id, rec_start_dttm, rec_end_dttm, src_res_id, res_init_date, res_guest_count, res_mode, cancellation_date, first_cancellation_date,res_status, operator, res_type, is_internal, source_code, effective_date, vip_status, stage, booking_source, last_modifiedby_id, hotel_flag, src_sail_id, seaware_agency_id, seaware_agent_id, agency_id, agent_id, sec_agency_id, sec_agent_id, opted_fvc, cancellation_case, src_group_id, currency, currency_rate, bk_date, etl_ld_status, load_dt, upd_dt, md5_dim from resDimtgtAlltbl """)

val tgtTblTmpAll = tempTable
val coalesceval = 4
/*spark.sparkContext.getConf.get("spark.target.coalesceval").trim()*/
finaldf.coalesce( coalesceval.toInt ).write.mode("Overwrite").insertInto(tgtTblTmpAll)

//finaldf.where($"src_res_id" ===282107).show(false)


compareSrcTgt.unpersist()
val tgtTblOriginal = tgtTable
val finaDF = spark.sql("""select * from  """+tgtTblTmpAll)
finaDF.coalesce(coalesceval.toInt).write.mode("Overwrite").insertInto(tgtTblOriginal)
   	
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