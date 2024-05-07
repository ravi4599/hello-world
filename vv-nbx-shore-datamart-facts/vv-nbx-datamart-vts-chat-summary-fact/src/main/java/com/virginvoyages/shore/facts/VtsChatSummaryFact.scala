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
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryException
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.Job
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.QueryJobConfiguration
import org.apache.spark.sql.SparkSession

object VtsChatSummaryFact {
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
var batch_instance_id1: String = null
var batch_id1: String = null
try {
if (args.length == 0) {
val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)

batch_instance_id1 = metadata._2
batch_id1 = metadata._1
//val tgtTempTbl = "" /*spark.sparkContext.getConf.get("spark.target.temp").trim()*/
//val tgtTable = "" /*spark.sparkContext.getConf.get("spark.target.tgttablename").trim()*/
val chatSummaryDf = spark.sql(""" select
vts_performance_targets_lkp.target_team_name as team_name
,  'CRM' as source
,  'Chat' as origin
,  core_crm_case_dim.Case_Owner_Queue__c as queue_name
,  case when core_crm_case_dim.reason!='' then core_crm_case_dim.reason else null end as reason
,  case when core_crm_case_dim.case_resolution__c!='' then core_crm_case_dim.case_resolution__c else null end as resolution
,  case when core_crm_case_dim.case_sub_resolution__c!='' then core_crm_case_dim.case_sub_resolution__c else null end as sub_resolution
--,  DATE(core_crm_livechat_transcript_dim.starttime) as chat_date
--,  hour(core_crm_livechat_transcript_dim.starttime) as chat_hour
, date(FROM_UTC_TIMESTAMP(cast(UNIX_TIMESTAMP(core_crm_livechat_transcript_dim.createddate, "yyyy-MM-dd hh:mm:ss") as timestamp), 'America/New_York'))  as chat_date
, hour(FROM_UTC_TIMESTAMP(cast(UNIX_TIMESTAMP(core_crm_livechat_transcript_dim.createddate, "yyyy-MM-dd hh:mm:ss") as timestamp), 'America/New_York')) as chat_hour
,  COUNT(distinct(case when (core_crm_case_dim.origin='' OR core_crm_case_dim.origin='Chat')and core_crm_livechat_transcript_dim.status='Completed' then core_crm_livechat_transcript_dim.id else null END)) as count_chats_completed
,  COUNT(distinct(case when (core_crm_case_dim.origin='' OR core_crm_case_dim.origin='Chat')and core_crm_livechat_transcript_dim.status='Missed' then core_crm_livechat_transcript_dim.id else null END)) as count_chats_missed
,  COUNT(distinct(case when (core_crm_case_dim.origin='' OR core_crm_case_dim.origin='Chat')and core_crm_livechat_transcript_dim.status='Missed' and coalesce(core_crm_livechat_transcript_dim.abandoned,0)>20 then core_crm_livechat_transcript_dim.id else null END)) as count_chats_missed_20sec
,  COUNT(distinct(case when (core_crm_case_dim.origin='' OR core_crm_case_dim.origin='Chat')and core_crm_livechat_transcript_dim.status='InProgress' then core_crm_livechat_transcript_dim.id else null END)) as count_chats_in_progress
,vts_performance_targets_lkp.target_pct_completed_chats as target_chats_handled_pct
,  sum(core_crm_livechat_transcript_dim.chatduration) as total_chat_duration
,  COUNT(distinct(case when upper(core_crm_livechat_transcript_dim.Booking_Flow__c) ='TRUE' then core_crm_livechat_transcript_dim.id else null END)) as count_booking_flow_chats
,  sum(core_crm_livechat_transcript_dim.waittime) as total_chat_wait_time
, core_crm_surveyresponses_dim.response__c as chat_survey_response
, count(distinct(src_surveyresponses_id)) as count_chat_survey_responses
, vts_performance_targets_lkp.target_pct_avg_loved_happy_response as target_loved_it_response_pct  
from
vv_db.hvtb_nbx_mart_crm_livechat_transcript_dim core_crm_livechat_transcript_dim
left join vv_db.hvtb_nbx_core_crm_case_dim core_crm_case_dim
on core_crm_case_dim.id = core_crm_livechat_transcript_dim.src_case_id
and core_crm_case_dim.rec_end_dttm='9999-12-31 00:00:00'
left join vv_db.hvtb_mart_vts_performance_targets_lkp vts_performance_targets_lkp
on vts_performance_targets_lkp.target_queue_name = core_crm_case_dim.Case_Owner_Queue__c and DATE(core_crm_livechat_transcript_dim.createddate) between vts_performance_targets_lkp.target_range_start_date and vts_performance_targets_lkp.target_range_end_date
left join vv_db.hvtb_nbx_mart_dim_crm_surveyresponses core_crm_surveyresponses_dim
on core_crm_surveyresponses_dim.case_number__c=core_crm_case_dim.id and trim(core_crm_surveyresponses_dim.case_number__c)<>''
group by
vts_performance_targets_lkp.target_team_name,
core_crm_case_dim.Case_Owner_Queue__c,
core_crm_case_dim.reason,
core_crm_case_dim.case_resolution__c,
core_crm_case_dim.case_sub_resolution__c,
--DATE(core_crm_livechat_transcript_dim.starttime),
--hour(core_crm_livechat_transcript_dim.starttime),
date(FROM_UTC_TIMESTAMP(cast(UNIX_TIMESTAMP(core_crm_livechat_transcript_dim.createddate, "yyyy-MM-dd hh:mm:ss") as timestamp), 'America/New_York')),
hour(FROM_UTC_TIMESTAMP(cast(UNIX_TIMESTAMP(core_crm_livechat_transcript_dim.createddate, "yyyy-MM-dd hh:mm:ss") as timestamp), 'America/New_York')),
vts_performance_targets_lkp.target_pct_completed_chats,
core_crm_surveyresponses_dim.response__c,
vts_performance_targets_lkp.target_pct_avg_loved_happy_response
""")


//chatSummaryDf.show(2,false)
//chatSummaryDf.printSchema()
//println("chat count= "+ chatSummaryDf.count)


val targetDf=chatSummaryDf.withColumnRenamed("team_name", "team").withColumnRenamed("queue_name", "queue")
//.withColumnRenamed("chat_date", "date").withColumnRenamed("chat_hour","hour")
.withColumnRenamed("count_chats_completed","completed_chats_cnt")
.withColumnRenamed("count_chats_missed", "missed_chats_cnt")
.withColumnRenamed("count_chats_missed_20sec", "missed_chats_greaterthan20sec_cnt")
.withColumnRenamed("count_chats_in_progress", "in_progress_chats_cnt")
.withColumnRenamed("total_chat_duration", "chat_duration")
.withColumnRenamed("count_booking_flow_chats", "booking_flow_chats_cnt")
.withColumnRenamed("chat_survey_response", "post_chat_survey_response")
.withColumnRenamed("count_chat_survey_responses", "survey_responses_cnt")
.withColumn("etl_ld_dt", current_timestamp())
.withColumn("etl_upd_dt", current_timestamp())                                  

val changetype = targetDf.withColumn("chat_duration", targetDf.col("chat_duration").cast(FloatType))
.withColumn("total_chat_wait_time", targetDf.col("total_chat_wait_time").cast(FloatType))
.withColumn("completed_chats_cnt",targetDf.col("completed_chats_cnt").cast(IntegerType))
.withColumn("missed_chats_cnt",targetDf.col("missed_chats_cnt").cast(IntegerType))
.withColumn("missed_chats_greaterthan20sec_cnt",targetDf.col("missed_chats_greaterthan20sec_cnt").cast(IntegerType))
.withColumn("in_progress_chats_cnt",targetDf.col("in_progress_chats_cnt").cast(IntegerType))
.withColumn("booking_flow_chats_cnt",targetDf.col("booking_flow_chats_cnt").cast(IntegerType))
.withColumn("survey_responses_cnt",targetDf.col("survey_responses_cnt").cast(IntegerType))
.select("team", "source", "origin", "queue", "reason", "resolution", "sub_resolution", "chat_date", "chat_hour", "completed_chats_cnt", "missed_chats_cnt", "missed_chats_greaterthan20sec_cnt", "in_progress_chats_cnt", "target_chats_handled_pct", "chat_duration", "booking_flow_chats_cnt", "total_chat_wait_time", "post_chat_survey_response", "survey_responses_cnt", "target_loved_it_response_pct", "etl_ld_dt", "etl_upd_dt")
//changetype.show(2,false)
//changetype.printSchema()
//println("chat count= "+ changetype.count)

log.info("We are entering the bigquery")
tempToRSTableLoad(spark, changetype)
log.info("BigQuery Connection Succesful")
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
log.info("******************in the catch of VtsChatSummaryFact Dim ******************");
e.printStackTrace();
throw new Exception("General Exception..please check the stacktrace")
}
}

spark.stop()
}

def tempToRSTableLoad(spark: SparkSession, targetDf: DataFrame) = {
//val redshift_table = spark.sparkContext.getConf.get("spark.pond.database").trim() + "." + spark.sparkContext.getConf.get("spark.pond.table").trim()
val EmptyDF = spark.emptyDataFrame
//EmptyDF.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())

targetDf.repartition(15).write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
//log.info("Alter RS TABLE to point to temp location" + redshift_table)
 log.info("First alter table")
 
/*Class.forName("com.amazon.redshift.jdbc.Driver");

val connectionURL = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.redshift.url").trim());
val userName = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.redshift.username").trim())
val passWord = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.redshift.password").trim())
val sqlConnection = DriverManager.getConnection(connectionURL.value, userName.value, passWord.value)
val redShiftTableName = spark.sparkContext.getConf.get("spark.redshift.schema").trim() + "." + spark.sparkContext.getConf.get("spark.redshift.tablename").trim()
*/
 
 
val tempLocation = spark.sparkContext.getConf.get("spark.target.temp").trim()
val BQTempExtnTbl = spark.sparkContext.getConf.get("spark.bq.dataset").trim() + "." + spark.sparkContext.getConf.get("spark.bq.temptblname").trim()
val BQViewName = spark.sparkContext.getConf.get("spark.bq.viewname").trim()

log.info(s"Alter View $BQViewName to point $BQTempExtnTbl ")
val temp_qry=s"CREATE OR REPLACE VIEW $BQViewName AS SELECT * FROM $BQTempExtnTbl"

try {
/*val AlterRedShiftToTemp = sqlConnection.prepareStatement(s"Alter Table $redShiftTableName SET LOCATION '$tempLocation' ")
AlterRedShiftToTemp.executeUpdate()
AlterRedShiftToTemp.close()
print("RedShift Connection Succesful")*/
val bigquery = BigQueryOptions.getDefaultInstance().getService()
						val config = QueryJobConfiguration.newBuilder(temp_qry).build()
						val job = bigquery.create(JobInfo.of(config))
						log.info("**Bigquery Case Summary Facts Load**" + "job val= " + job);
                                            if (job.getStatus().getError() != null)
                                         {  println("Job create view failed ..."+ job.getStatus().getError())
                              throw new RuntimeException(String.format("Job %s ended with error %s", job.getJobId(),
                   job.getStatus().getError().getMessage()))    }
                                      else println("view executed ")
} catch {
//Handle errors for JDBC
case e: BigQueryException =>
{
log.info("******************in the catch of BigQueryException ******************");
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
//log.info("Alter RS TABLE to point to target location" + redshift_table)

//log.info(s"Alter Table $redShiftTableName SET LOCATION '$finalLocation' ")
try {
/*val AlterRedShiftToFinal = sqlConnection.prepareStatement(s"Alter Table $redShiftTableName SET LOCATION '$finalLocation' ")
AlterRedShiftToFinal.executeUpdate()
AlterRedShiftToFinal.close()*/
 val bigquery = BigQueryOptions.getDefaultInstance().getService()
            val config = QueryJobConfiguration.newBuilder(perm_qry).build()
            val job = bigquery.create(JobInfo.of(config))
            
            log.info("**Bigquery Case Summary Facts Load**" + "job val= " + job);
                                            if (job.getStatus().getError() != null)
                                         {  println("Job create view failed ..."+ job.getStatus().getError())
                              throw new RuntimeException(String.format("Job %s ended with error %s", job.getJobId(),
                   job.getStatus().getError().getMessage()))    }
                                      else println("view executed ")
} catch {
//Handle errors for JDBC
case e: BigQueryException =>
{
log.info("******************in the catch of BigQuery Dimension ******************");
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
//sqlConnection.close()
}
}