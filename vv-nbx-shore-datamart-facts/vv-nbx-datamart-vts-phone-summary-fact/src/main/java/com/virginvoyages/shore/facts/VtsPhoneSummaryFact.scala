package com.virginvoyages.shore.facts

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{ col, to_date, monotonically_increasing_id }
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions.{ col, lit, when }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import com.virginvoyages.metadataframework.ManageMetadata
import java.sql.DriverManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryException
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.Job
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.QueryJobConfiguration
import org.apache.spark.sql.SparkSession

object VtsPhoneSummaryFact {

  /**
   * Initialize the logger
   */
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]) {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    val coalesceval = 4 /*spark.sparkContext.getConf.get("spark.target.coalesceval").trim()*/
    spark.sqlContext.setConf("spark.sql.parquet.writeLegacyFormat", "true")
    //spark.sqlContext.setConf("spark.sql.parquet.mergeSchema", "true")
    var batch_instance_id1: String = null
    var batch_id1: String = null
    try {
      if (args.length == 0) {
        val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)

        batch_instance_id1 = metadata._2
        batch_id1 = metadata._1
        //val tgtTempTbl = "" /*spark.sparkContext.getConf.get("spark.target.temp").trim()*/
        //val tgtTable = "" /*spark.sparkContext.getConf.get("spark.target.tgttablename").trim()*/
		
//        val phoneSummaryDf = spark.sql("""select 
//                            	  	cti_team_dim.team_name as team_name
//                            	,  'CTI' as source
//                            	,  'Phone' as origin
//                            	,  vts_performance_targets_lkp.target_queue_name as queue_name  
//                            	,  cti_campaign_dim.campaign_name
//                            	,  cti_skill_dim.skill_name as skill_name
//                            	,  core_crm_task_vw.callreason__c as reason
//                            	,  core_crm_task_vw.call_resolution__c as resolution
//                            	,  core_crm_task_vw.call_sub_resolution__c as sub_resolution
//                            	,  date_dim.`date` as phone_date
//                            	,  time_dim.hours_of_day as phone_hour
//                            	,  count(distinct(case when cti_call_fact.outbound_call_flg = false 
//                            		and cti_call_fact.transferindicator_flg = false 
//                            		and cti_call_state_history_fact.contact_state_name in ('Prequeue','Spawned','PlaceCall') 
//                            		Then cti_call_fact.src_contact_id else null end)) as count_incoming_calls
//                            	,  count(distinct(case when cti_call_fact.outbound_call_flg = false 
//                            		and cti_call_state_history_fact.contact_state_name in ('Inqueue') 
//                            		Then cti_call_fact.src_contact_id else null end)) as count_queued_calls
//                            	,  sum(coalesce(case when cti_call_state_history_fact.contact_state_name in ('Inqueue','Routing','CallBack') 
//                            		Then cti_call_state_history_fact.duration_in_seconds else null end,0)) as total_call_queue_time_seconds 
//                            	,  sum(coalesce(case when cti_call_state_history_fact.contact_state_name in ('Hold') 
//                            		Then cti_call_state_history_fact.duration_in_seconds else null end,0)) as total_call_hold_time
//                            	,  count(distinct(case when cti_call_state_history_fact.contact_state_name in ('Hold') 
//                            		Then cti_call_fact.src_contact_id else null end)) as  count_held_calls	
//                            	,  count(distinct(case when cti_call_state_history_fact.contact_state_name in ('Transfer') 
//                            		Then cti_call_fact.src_contact_id else null end)) as count_transfers
//                            	,  count(distinct(case when cti_call_fact.outbound_call_flg = false 
//                            		Then cti_call_fact.src_contact_id else null end)) as count_inbound_calls
//                            	,  count(distinct(case when cti_call_fact.outbound_call_flg = false 
//                            		and cti_call_state_history_fact.contact_state_name in ('Active') 
//                            		Then cti_call_fact.src_contact_id else null end)) as count_inbound_handled_calls
//                            	,  count(distinct(case when cti_call_fact.outbound_call_flg = TRUE 
//                            		and cti_call_state_history_fact.contact_state_name in ('Active') 
//                            		Then cti_call_fact.src_contact_id else null end)) as count_outbound_handled_calls
//                            	,  count(distinct(case when cti_call_state_history_fact.contact_state_name in ('Active') 
//                            		Then cti_call_fact.src_contact_id else null end)) as count_total_handled_calls
//                            	, 	vts_performance_targets_lkp.target_pct_calls_handled_total as target_pct_handled 
//                            	,  sum(coalesce(case when cti_call_state_history_fact.contact_state_name in ('Agent', 'Hold', 'Conference', 'ACW') 
//                            		Then cti_call_state_history_fact.duration_in_seconds else null end,0)) as total_handle_time
//                            	,  vts_performance_targets_lkp.target_pct_inbound_calls_handled as target_pct_inbound_calls_handled		
//                            	,  count(distinct(case when cti_call_fact.outbound_call_flg = false 
//                            		and cti_call_state_history_fact.contact_state_name in ('Abandoned') 
//                            		Then cti_call_fact.src_contact_id else null end)) as count_abandoned_calls
//                            	, 	vts_performance_targets_lkp.target_avg_handle_time_hrs as target_avg_handle_time_hrs 
//                            	, vts_performance_targets_lkp.target_phone_service_level as target_phone_service_level
//                            	, vts_performance_targets_lkp.target_pct_call_abandon as target_pct_call_abandon
//                            	, vts_performance_targets_lkp.target_count_outbound_calls as target_count_outbound_calls 
//                            from 
//                            	vv_db.hvtb_nbx_core_cti_call_fact cti_call_fact 
//                            	left join vv_db.hvtb_nbx_core_cti_call_state_history_fact cti_call_state_history_fact on cti_call_fact.src_contact_id = cti_call_state_history_fact.src_contact_id 
//                            	left join vv_db.hvtb_nbx_core_cti_team_dim cti_team_dim on  cti_call_fact.cti_team_skey = cti_team_dim.cti_team_skey 
//                            	left join vv_db.hvtb_nbx_core_cti_campaign_dim cti_campaign_dim on  cti_call_fact.cti_campaign_skey = cti_campaign_dim.cti_campaign_skey 
//                            	left join vv_db.hvtb_nbx_core_cti_skill_dim cti_skill_dim on cti_call_fact.cti_skill_skey = cti_skill_dim.cti_skill_skey 
//                            	left join vv_db.hvtb_nbx_core_date_dim date_dim on  cti_call_fact.start_date_skey = date_dim.date_id 
//                            	left join vv_db.hvtb_nbx_core_time_dim time_dim on cti_call_fact.start_time_skey = time_dim.time_skey 
//                            	left join vv_db.hvtb_nbx_core_crm_task_dim core_crm_task_vw 
//                            		on core_crm_task_vw.callobject = cti_call_fact.src_contact_id 
//                            		and DATE(core_crm_task_vw.rec_end_dttm) = '9999-12-31'
//                            		AND core_crm_task_vw.type = 'Call'
//                            	left join vv_db.hvtb_mart_vts_performance_targets_lkp vts_performance_targets_lkp 
//                            		on cti_team_dim.team_name=vts_performance_targets_lkp.target_team_name
//                            		and date_dim.`date` between vts_performance_targets_lkp.target_range_start_date and vts_performance_targets_lkp.target_range_end_date
//                            group by 
//                            cti_team_dim.team_name,
//                            vts_performance_targets_lkp.target_queue_name,
//                            cti_campaign_dim.campaign_name,
//                            cti_skill_dim.skill_name,
//                            core_crm_task_vw.callreason__c,
//                            core_crm_task_vw.call_resolution__c,
//                            core_crm_task_vw.call_sub_resolution__c,
//                            date_dim.`date`,
//                            time_dim.hours_of_day,
//                            vts_performance_targets_lkp.target_pct_calls_handled_total,
//                            vts_performance_targets_lkp.target_pct_inbound_calls_handled,
//                            vts_performance_targets_lkp.target_avg_handle_time_hrs,
//                            vts_performance_targets_lkp.target_phone_service_level,
//                            vts_performance_targets_lkp.target_pct_call_abandon,
//                            vts_performance_targets_lkp.target_count_outbound_calls 
//                            """)
                            
        val phoneSummaryDf = spark.sql("""select 
                            	  	cti_team_dim.team_name as team_name
                            	,  'CTI' as source
                            	,  'Phone' as origin
                            	,  vts_performance_targets_lkp.target_queue_name as queue_name  
                            	,  cti_campaign_dim.campaign_name
                            	,  cti_skill_dim.skill_name as skill_name
                            	,  core_crm_task_vw.callreason__c as reason
                            	,  core_crm_task_vw.call_resolution__c as resolution
                            	,  core_crm_task_vw.call_sub_resolution__c as sub_resolution
                            	,  date_dim.`date` as phone_date
                            	,  time_dim.hours_of_day as phone_hour
                            	,  count(distinct(case when cti_call_fact.outbound_call_flg = false 
                            		and cti_call_fact.transferindicator_flg = false 
                            		and cti_call_state_history_fact.contact_state_name in ('Prequeue','Spawned','PlaceCall') 
                            		Then cti_call_fact.src_contact_id else null end)) as count_incoming_calls
                            	,  count(distinct(case when cti_call_fact.outbound_call_flg = false 
                            		and cti_call_state_history_fact.contact_state_name in ('Inqueue') 
                            		Then cti_call_fact.src_contact_id else null end)) as count_queued_calls
                            	,  sum(coalesce(case when cti_call_state_history_fact.contact_state_name in ('Inqueue','Routing','CallBack') 
                            		Then cti_call_state_history_fact.duration_in_seconds else null end,0)) as total_call_queue_time_seconds 
                            	,  sum(coalesce(case when cti_call_state_history_fact.contact_state_name in ('Hold') 
                            		Then cti_call_state_history_fact.duration_in_seconds else null end,0)) as total_call_hold_time
                            	,  count(distinct(case when cti_call_state_history_fact.contact_state_name in ('Hold') 
                            		Then cti_call_fact.src_contact_id else null end)) as  count_held_calls	
                            	,  count(distinct(case when cti_call_state_history_fact.contact_state_name in ('Transfer') 
                            		Then cti_call_fact.src_contact_id else null end)) as count_transfers
                            	,  count(distinct(case when cti_call_fact.outbound_call_flg = false 
                            		Then cti_call_fact.src_contact_id else null end)) as count_inbound_calls
                            	,  count(distinct(case when cti_call_fact.outbound_call_flg = false 
                            		and cti_call_state_history_fact.contact_state_name in ('Active') 
                            		Then cti_call_fact.src_contact_id else null end)) as count_inbound_handled_calls
                            	,  count(distinct(case when cti_call_fact.outbound_call_flg = TRUE 
                            		and cti_call_state_history_fact.contact_state_name in ('Active') 
                            		Then cti_call_fact.src_contact_id else null end)) as count_outbound_handled_calls
                            	,  count(distinct(case when cti_call_state_history_fact.contact_state_name in ('Active') 
                            		Then cti_call_fact.src_contact_id else null end)) as count_total_handled_calls
                            	, 	vts_performance_targets_lkp.target_pct_calls_handled_total as target_pct_handled 
                            	,  sum(coalesce(case when cti_call_state_history_fact.contact_state_name in ('Agent', 'Hold', 'Conference', 'ACW') 
                            		Then cti_call_state_history_fact.duration_in_seconds else null end,0)) as total_handle_time
                            	,  vts_performance_targets_lkp.target_pct_inbound_calls_handled as target_pct_inbound_calls_handled		
                            	,  count(distinct(case when cti_call_fact.outbound_call_flg = false 
                            		and cti_call_state_history_fact.contact_state_name in ('Abandoned') 
                            		Then cti_call_fact.src_contact_id else null end)) as count_abandoned_calls
                            	, 	vts_performance_targets_lkp.target_avg_handle_time_hrs as target_avg_handle_time_hrs 
                            	, vts_performance_targets_lkp.target_phone_service_level as target_phone_service_level
                            	, vts_performance_targets_lkp.target_pct_call_abandon as target_pct_call_abandon
                            	, vts_performance_targets_lkp.target_count_outbound_calls as target_count_outbound_calls 
								, count(distinct core_crm_task_vw.task_id) as count_total_tasks 
                            from 
                            	vv_db.hvtb_nbx_core_cti_call_fact cti_call_fact 
                            	left join vv_db.hvtb_nbx_core_cti_call_state_history_fact cti_call_state_history_fact on cti_call_fact.src_contact_id = cti_call_state_history_fact.src_contact_id 
                            	left join vv_db.hvtb_nbx_core_cti_team_dim cti_team_dim on  cti_call_fact.cti_team_skey = cti_team_dim.cti_team_skey 
                            	left join vv_db.hvtb_nbx_core_cti_campaign_dim cti_campaign_dim on  cti_call_fact.cti_campaign_skey = cti_campaign_dim.cti_campaign_skey 
                            	left join vv_db.hvtb_nbx_core_cti_skill_dim cti_skill_dim on cti_call_fact.cti_skill_skey = cti_skill_dim.cti_skill_skey 
                            	left join vv_db.hvtb_nbx_core_date_dim date_dim on  cti_call_fact.start_date_skey = date_dim.date_id 
                            	left join vv_db.hvtb_nbx_core_time_dim time_dim on cti_call_fact.start_time_skey = time_dim.time_skey 
                            	left join vv_db.hvtb_nbx_core_crm_task_dim core_crm_task_vw 
                            		on core_crm_task_vw.callobject = cti_call_fact.src_contact_id 
                            		and DATE(core_crm_task_vw.rec_end_dttm) = '9999-12-31'
                            		AND core_crm_task_vw.type = 'Call'
                            	left join vv_db.hvtb_mart_vts_performance_targets_lkp vts_performance_targets_lkp 
                            		on cti_team_dim.team_name=vts_performance_targets_lkp.target_team_name
                            		and date_dim.`date` between vts_performance_targets_lkp.target_range_start_date and vts_performance_targets_lkp.target_range_end_date
                            group by 
                            cti_team_dim.team_name,
                            vts_performance_targets_lkp.target_queue_name,
                            cti_campaign_dim.campaign_name,
                            cti_skill_dim.skill_name,
                            core_crm_task_vw.callreason__c,
                            core_crm_task_vw.call_resolution__c,
                            core_crm_task_vw.call_sub_resolution__c,
                            date_dim.`date`,
                            time_dim.hours_of_day,
                            vts_performance_targets_lkp.target_pct_calls_handled_total,
                            vts_performance_targets_lkp.target_pct_inbound_calls_handled,
                            vts_performance_targets_lkp.target_avg_handle_time_hrs,
                            vts_performance_targets_lkp.target_phone_service_level,
                            vts_performance_targets_lkp.target_pct_call_abandon,
                            vts_performance_targets_lkp.target_count_outbound_calls 
                            """)
                            
       

        val targetDf = phoneSummaryDf.withColumnRenamed("team_name", "team").withColumnRenamed("queue_name", "queue")
          //.withColumnRenamed("chat_date", "date").withColumnRenamed("chat_hour","hour")
          .withColumnRenamed("campaign_name", "campaign")
          .withColumnRenamed("count_incoming_calls", "incoming_calls_cnt")
          .withColumnRenamed("count_queued_calls", "queued_calls_cnt")
          .withColumnRenamed("total_call_queue_time_seconds", "total_call_queued_time")
          .withColumnRenamed("count_held_calls", "held_calls_cnt")
          .withColumnRenamed("count_transfers", "transfers_cnt")
          .withColumnRenamed("count_inbound_calls", "inbound_calls_cnt")
          .withColumnRenamed("count_inbound_handled_calls", "inbound_handled_calls_cnt")
          .withColumnRenamed("count_outbound_handled_calls", "outbound_handled_calls_cnt")
          .withColumnRenamed("count_total_handled_calls", "total_handled_calls")
          .withColumnRenamed("target_pct_handled", "target_handled_pct")
          .withColumnRenamed("total_handle_time", "handle_time")
          .withColumnRenamed("target_pct_inbound_calls_handled", "target_inbound_calls_handled_pct")
          .withColumnRenamed("count_abandoned_calls", "abandoned_calls_cnt")
          .withColumn("etl_ld_dt", current_timestamp())
          .withColumn("etl_upd_dt", current_timestamp())
          
          val changetype = targetDf.withColumn("incoming_calls_cnt", targetDf.col("incoming_calls_cnt").cast(IntegerType)).withColumn("queued_calls_cnt", targetDf.col("queued_calls_cnt").cast(IntegerType)).withColumn("total_call_queued_time", targetDf.col("total_call_queued_time").cast(FloatType)).withColumn("total_call_hold_time", targetDf.col("total_call_hold_time").cast(FloatType)).withColumn("held_calls_cnt", targetDf.col("held_calls_cnt").cast(IntegerType)).withColumn("transfers_cnt", targetDf.col("transfers_cnt").cast(IntegerType)).withColumn("inbound_calls_cnt", targetDf.col("inbound_calls_cnt").cast(IntegerType)).withColumn("inbound_handled_calls_cnt", targetDf.col("inbound_handled_calls_cnt").cast(IntegerType)).withColumn("outbound_handled_calls_cnt", targetDf.col("outbound_handled_calls_cnt").cast(IntegerType)).withColumn("total_handled_calls", targetDf.col("total_handled_calls").cast(IntegerType)).withColumn("handle_time", targetDf.col("handle_time").cast(FloatType)).withColumn("target_inbound_calls_handled_pct", targetDf.col("target_inbound_calls_handled_pct").cast(FloatType)).withColumn("abandoned_calls_cnt", targetDf.col("abandoned_calls_cnt").cast(IntegerType)).withColumn("count_total_tasks", targetDf.col("count_total_tasks").cast(IntegerType)).select("team", "source", "origin", "queue", "campaign", "skill_name", "reason", "resolution", "sub_resolution", "phone_date", "phone_hour", "incoming_calls_cnt", "queued_calls_cnt", "total_call_queued_time", "total_call_hold_time", "held_calls_cnt", "transfers_cnt", "inbound_calls_cnt", "inbound_handled_calls_cnt", "outbound_handled_calls_cnt", "total_handled_calls", "target_handled_pct", "handle_time", "target_inbound_calls_handled_pct", "abandoned_calls_cnt", "target_avg_handle_time_hrs", "target_phone_service_level", "target_pct_call_abandon", "target_count_outbound_calls","count_total_tasks", "etl_ld_dt", "etl_upd_dt")

          
        log.info("We are entering the bigquery")
        tempToRSTableLoad(spark, changetype)
        
        log.info("BigQuery Connection  Succesful")
        ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Succesful", spark)
      } else {
        log.info("This scripts does not require any parameters")
      }
    } catch {
      case e: SQLException => {
        e.printStackTrace();
        log.info("Exception while executing the SQL command");
      }
      case e: Exception => {
        log.info("******************in the catch of VtsPhoneSummaryFact Dim ******************");
        e.printStackTrace();
        throw new Exception("General Exception..please check the stacktrace")
      }
    }

    spark.stop()
  }

  def tempToRSTableLoad(spark: SparkSession, targetDf: DataFrame) = {
    //val redshift_table = spark.sparkContext.getConf.get("spark.pond.database").trim() + "." + spark.sparkContext.getConf.get("spark.pond.table").trim()
    val EmptyDF = spark.emptyDataFrame
   // EmptyDF.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())

    targetDf.repartition(15).write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
    //log.info("Alter RS TABLE to point to temp location" + redshift_table)
    log.info("First alter table")

    val tempLocation = spark.sparkContext.getConf.get("spark.target.temp").trim()
    val BQTempExtnTbl = spark.sparkContext.getConf.get("spark.bq.dataset").trim() + "." + spark.sparkContext.getConf.get("spark.bq.temptblname").trim()
    val BQViewName = spark.sparkContext.getConf.get("spark.bq.viewname").trim()
    log.info(s"Alter View $BQViewName to point $BQTempExtnTbl ")
    val temp_qry=s"CREATE OR REPLACE VIEW $BQViewName AS SELECT * FROM $BQTempExtnTbl"	

    try {
       val bigquery = BigQueryOptions.getDefaultInstance().getService()
			 val config = QueryJobConfiguration.newBuilder(temp_qry).build()
			 val job = bigquery.create(JobInfo.of(config))
			 log.info("**Bigquery Phone Summary Fact Load**" + "job val= " + job);
                                            if (job.getStatus().getError() != null)
                                         {  println("Job create view failed ..."+ job.getStatus().getError())
                              throw new RuntimeException(String.format("Job %s ended with error %s", job.getJobId(),
                   job.getStatus().getError().getMessage()))    }
                                      else println("view executed ")
    } catch {
      //Handle errors for JDBC
      case e: BigQueryException =>
        {
          log.info("******************in the catch of Bigquery Dimension Load ******************");
          e.printStackTrace();
        }
      case e: Exception =>
        {
          log.info("******************in the catch of Bigquery Dimension Load ******************");
          e.printStackTrace();
          throw new Exception("SQL Exception..please check the stacktrace", e);
        }
        print("Bigquery Query Failed")
    }
   val final_df = spark.read.parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
					final_df.repartition(15).write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.location").trim())
					val finalLocation = spark.sparkContext.getConf.get("spark.target.location").trim()
					val BQPermExtnTbl = spark.sparkContext.getConf.get("spark.bq.dataset").trim() + "." + spark.sparkContext.getConf.get("spark.bq.permtblname").trim()
          val BQErrorTableName = spark.sparkContext.getConf.get("spark.bq.dataset").trim() + "." + spark.sparkContext.getConf.get("spark.bq.permtblname").trim() + "_err"
          log.info(s"Alter View $BQViewName to point $BQPermExtnTbl ")
          val perm_qry=s"CREATE OR REPLACE VIEW $BQViewName AS SELECT * FROM $BQPermExtnTbl"
    //log.info(s"Alter Table $redShiftTableName SET LOCATION '$finalLocation' ")
    try {
      val bigquery = BigQueryOptions.getDefaultInstance().getService()
      val config = QueryJobConfiguration.newBuilder(perm_qry).build()
      val job = bigquery.create(JobInfo.of(config))
      
       log.info("**Bigquery Phone Summary Facts Load**" + "job val= " + job);
                                            if (job.getStatus().getError() != null)
                                         {  println("Job create view failed ..."+ job.getStatus().getError())
                              throw new RuntimeException(String.format("Job %s ended with error %s", job.getJobId(),
                   job.getStatus().getError().getMessage()))    }
                                      else println("view executed ")
      
    } catch {
      //Handle errors for JDBC
      case e: BigQueryException =>
        {
          log.info("******************in the catch of Bigquery Dimension Load ******************");
          e.printStackTrace();
        }
      case e: Exception =>
        {
          log.info("******************in the catch of Bigquery Dimension Load******************");
          e.printStackTrace();
          throw new Exception("SQL Exception..please check the stacktrace", e);
        }
        print("BigQuery Query Failed")
    }
    //sqlConnection.close()
  }

}