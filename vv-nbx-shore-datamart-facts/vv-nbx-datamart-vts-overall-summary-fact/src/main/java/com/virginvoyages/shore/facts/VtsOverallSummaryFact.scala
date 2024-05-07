package com.virginvoyages.shore.facts

import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.virginvoyages.metadataframework.ManageMetadata
import java.sql.DriverManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryException
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.Job
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.QueryJobConfiguration
import org.apache.spark.sql.SparkSession

object VtsOverallSummaryFact {

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

								//        val overallSummaryDf = spark.sql("""select phone.team, phone.source, phone.origin, phone.queue, phone.campaign, phone.skill_name, phone.reason, phone.resolution, phone.sub_resolution, phone.phone_date, phone.phone_hour, phone.incoming_calls_cnt, phone.queued_calls_cnt, phone.total_call_queued_time, phone.total_call_hold_time, phone.held_calls_cnt, phone.transfers_cnt, phone.inbound_calls_cnt, phone.inbound_handled_calls_cnt, phone.outbound_handled_calls_cnt, phone.total_handled_calls, phone.target_handled_pct, phone.handle_time, phone.target_inbound_calls_handled_pct, phone.abandoned_calls_cnt, cast(null as int) as completed_chats_cnt, cast(null as int) as missed_chats_cnt, cast(null as int) as missed_chats_greaterthan20sec_cnt, cast(null as int) as in_progress_chats_cnt, cast(null as int) as target_chats_handled_pct, cast(null as float) as chat_duration, cast(null as int) as booking_flow_chats_cnt, cast(null as float) as total_chat_wait_time, cast(null as int) as total_cases_cnt, cast(null as int) as handled_cases_cnt, cast(null as int) as target_case_productivity_pct, cast(null as int) as target_cases_sla, cast(null as int) as cases_per_team_queue_cnt, cast(null as float) as total_value_duration, cast(null as int) as target_avg_value_duration_hrs, cast(null as int) as target_net_new_bookings_contribution_phone, cast(null as int) as target_net_new_voyage_protection_phone, cast(null as int) as target_casino_bookings, cast(null as string) as post_chat_survey_response, cast(null as int) as survey_responses_cnt, cast(null as int) as target_loved_it_response_pct, phone.target_avg_handle_time_hrs, phone.target_phone_service_level, phone.target_pct_call_abandon, phone.target_count_outbound_calls  
								//                                            from vv_db.hvtb_mart_vts_phone_summary_fact as phone 
								//                                            UNION 
								//                                            select chat.team, chat.source, chat.origin, chat.queue, cast(null as string) as campaign, cast(null as string) as skill_name, chat.reason, chat.resolution, chat.sub_resolution, chat.chat_date, chat.chat_hour, cast(null as int) as incoming_calls_cnt, cast(null as int) as queued_calls_cnt, cast(null as float) as total_call_queued_time, cast(null as float) as total_call_hold_time, cast(null as int) as held_calls_cnt, cast(null as int) as transfers_cnt, cast(null as int) as inbound_calls_cnt, cast(null as int) as inbound_handled_calls_cnt, cast(null as int) as outbound_handled_calls_cnt, cast(null as int) as total_handled_calls, cast(null as int) as target_handled_pct, cast(null as float) as handle_time, cast(null as float) as target_inbound_calls_handled_pct, cast(null as int) as abandoned_calls_cnt, chat.completed_chats_cnt, chat.missed_chats_cnt, chat.missed_chats_greaterthan20sec_cnt, chat.in_progress_chats_cnt, chat.target_chats_handled_pct, chat.chat_duration, chat.booking_flow_chats_cnt, chat.total_chat_wait_time, cast(null as int) as total_cases_cnt, cast(null as int) as handled_cases_cnt, cast(null as int) as target_case_productivity_pct, cast(null as int) as target_cases_sla, cast(null as int) as cases_per_team_queue_cnt, cast(null as float) as total_value_duration, cast(null as int) as target_avg_value_duration_hrs, cast(null as int) as target_net_new_bookings_contribution_phone, cast(null as int) as target_net_new_voyage_protection_phone, cast(null as int) as target_casino_bookings, chat.post_chat_survey_response, chat.survey_responses_cnt, chat.target_loved_it_response_pct, cast(null as int) as target_avg_handle_time_hrs, cast(null as int) as target_phone_service_level, cast(null as int) as target_pct_call_abandon, cast(null as int)  as target_count_outbound_calls  
								//                                            from vv_db.hvtb_mart_vts_chat_summary_fact as chat 
								//                                            UNION 
								//                                            select vts_case.team, vts_case.source, vts_case.origin, vts_case.queue, cast(null as string) as campaign, cast(null as string) as skill_name, vts_case.reason, vts_case.resolution, vts_case.sub_resolution, vts_case.case_date, vts_case.case_hour, cast(null as int) as incoming_calls_cnt, cast(null as int) as queued_calls_cnt, cast(null as float) as total_call_queued_time, cast(null as float) as total_call_hold_time, cast(null as int) as held_calls_cnt, cast(null as int) as transfers_cnt, cast(null as int) as inbound_calls_cnt, cast(null as int) as inbound_handled_calls_cnt, cast(null as int) as outbound_handled_calls_cnt, cast(null as int) as total_handled_calls, cast(null as int) as target_handled_pct, cast(null as float) as handle_time, cast(null as float) as target_inbound_calls_handled_pct, cast(null as int) as abandoned_calls_cnt, cast(null as int) as completed_chats_cnt, cast(null as int) as missed_chats_cnt, cast(null as int) as missed_chats_greaterthan20sec_cnt, cast(null as int) as in_progress_chats_cnt, cast(null as int) as target_chats_handled_pct, cast(null as float) as chat_duration, cast(null as int) as booking_flow_chats_cnt, cast(null as float) as total_chat_wait_time, vts_case.total_cases_cnt, vts_case.handled_cases_cnt, vts_case.target_case_productivity_pct, vts_case.target_cases_sla, vts_case.cases_per_team_queue_cnt, vts_case.total_value_duration, vts_case.target_avg_value_duration_hrs, cast(null as int) as target_net_new_bookings_contribution_phone, cast(null as int) as target_net_new_voyage_protection_phone, cast(null as int) as target_casino_bookings, cast(null as string) as post_chat_survey_response, cast(null as int) as survey_responses_cnt, cast(null as int) as target_loved_it_response_pct, cast(null as int) as target_avg_handle_time_hrs, cast(null as int) as target_phone_service_level, cast(null as int) as target_pct_call_abandon, cast(null as int)  as target_count_outbound_calls  
								//                                            from vv_db.hvtb_mart_vts_case_summary_fact as vts_case""").withColumn("etl_ld_dt", current_timestamp()).withColumn("etl_upd_dt", current_timestamp()).withColumnRenamed("phone_date", "vts_date").withColumnRenamed("phone_hour", "vts_hour")
								//        val targetDf = overallSummaryDf.select("team", "source", "origin", "queue", "campaign", "skill_name", "reason", "resolution", "sub_resolution", "vts_date", "vts_hour", "incoming_calls_cnt", "queued_calls_cnt", "total_call_queued_time", "total_call_hold_time", "held_calls_cnt", "transfers_cnt", "inbound_calls_cnt", "inbound_handled_calls_cnt", "outbound_handled_calls_cnt", "total_handled_calls", "target_handled_pct", "handle_time", "target_inbound_calls_handled_pct", "abandoned_calls_cnt", "completed_chats_cnt", "missed_chats_cnt", "missed_chats_greaterthan20sec_cnt", "in_progress_chats_cnt", "target_chats_handled_pct", "chat_duration", "booking_flow_chats_cnt", "total_chat_wait_time", "total_cases_cnt", "handled_cases_cnt", "target_case_productivity_pct", "target_cases_sla", "cases_per_team_queue_cnt", "total_value_duration", "target_avg_value_duration_hrs", "target_net_new_bookings_contribution_phone", "target_net_new_voyage_protection_phone", "target_casino_bookings", "post_chat_survey_response", "survey_responses_cnt", "target_loved_it_response_pct", "target_avg_handle_time_hrs", "target_phone_service_level", "target_pct_call_abandon", "target_count_outbound_calls", "etl_ld_dt", "etl_upd_dt")

								val overallSummaryDf = spark.sql("""select phone.team, phone.source, phone.origin, phone.queue, phone.campaign, phone.skill_name, phone.reason, phone.resolution, phone.sub_resolution, phone.phone_date, phone.phone_hour, phone.incoming_calls_cnt, phone.queued_calls_cnt, phone.total_call_queued_time, phone.total_call_hold_time, phone.held_calls_cnt, phone.transfers_cnt, phone.inbound_calls_cnt, phone.inbound_handled_calls_cnt, phone.outbound_handled_calls_cnt, phone.total_handled_calls, phone.target_handled_pct, phone.handle_time, phone.target_inbound_calls_handled_pct, phone.abandoned_calls_cnt,phone.count_total_tasks, cast(null as int) as completed_chats_cnt, cast(null as int) as missed_chats_cnt, cast(null as int) as missed_chats_greaterthan20sec_cnt, cast(null as int) as in_progress_chats_cnt, cast(null as int) as target_chats_handled_pct, cast(null as float) as chat_duration, cast(null as int) as booking_flow_chats_cnt, cast(null as float) as total_chat_wait_time, cast(null as int) as total_cases_cnt, cast(null as int) as handled_cases_cnt, cast(null as int) as target_case_productivity_pct, cast(null as int) as target_cases_sla, cast(null as int) as cases_per_team_queue_cnt, cast(null as float) as total_value_duration, cast(null as int) as target_avg_value_duration_hrs,cast(null as int) as total_case_resolution_time, cast(null as int) as target_net_new_bookings_contribution_phone, cast(null as int) as target_net_new_voyage_protection_phone, cast(null as int) as target_casino_bookings, cast(null as string) as post_chat_survey_response, cast(null as int) as survey_responses_cnt, cast(null as int) as target_loved_it_response_pct, phone.target_avg_handle_time_hrs, phone.target_phone_service_level, phone.target_pct_call_abandon, phone.target_count_outbound_calls from vv_db.hvtb_mart_vts_phone_summary_fact as phone    UNION 
										select chat.team, chat.source, chat.origin, chat.queue, cast(null as string) as campaign, cast(null as string) as skill_name, chat.reason, chat.resolution, chat.sub_resolution, chat.chat_date, chat.chat_hour, cast(null as int) as incoming_calls_cnt, cast(null as int) as queued_calls_cnt, cast(null as float) as total_call_queued_time, cast(null as float) as total_call_hold_time, cast(null as int) as held_calls_cnt, cast(null as int) as transfers_cnt, cast(null as int) as inbound_calls_cnt, cast(null as int) as inbound_handled_calls_cnt, cast(null as int) as outbound_handled_calls_cnt, cast(null as int) as total_handled_calls, cast(null as int) as target_handled_pct, cast(null as float) as handle_time, cast(null as float) as target_inbound_calls_handled_pct, cast(null as int) as abandoned_calls_cnt,cast(null as int) as count_total_tasks, chat.completed_chats_cnt, chat.missed_chats_cnt, chat.missed_chats_greaterthan20sec_cnt, chat.in_progress_chats_cnt, chat.target_chats_handled_pct, chat.chat_duration, chat.booking_flow_chats_cnt, chat.total_chat_wait_time, cast(null as int) as total_cases_cnt, cast(null as int) as handled_cases_cnt, cast(null as int) as target_case_productivity_pct, cast(null as int) as target_cases_sla, cast(null as int) as cases_per_team_queue_cnt, cast(null as float) as total_value_duration, cast(null as int) as target_avg_value_duration_hrs,cast(null as int) as total_case_resolution_time, cast(null as int) as target_net_new_bookings_contribution_phone, cast(null as int) as target_net_new_voyage_protection_phone, cast(null as int) as target_casino_bookings, chat.post_chat_survey_response, chat.survey_responses_cnt, chat.target_loved_it_response_pct, cast(null as int) as target_avg_handle_time_hrs, cast(null as int) as target_phone_service_level, cast(null as int) as target_pct_call_abandon, cast(null as int)  as target_count_outbound_calls from vv_db.hvtb_mart_vts_chat_summary_fact as chat
										UNION 
										select vts_case.team, vts_case.source, vts_case.origin, vts_case.queue, cast(null as string) as campaign, cast(null as string) as skill_name, vts_case.reason, vts_case.resolution, vts_case.sub_resolution, vts_case.case_date, vts_case.case_hour, cast(null as int) as incoming_calls_cnt, cast(null as int) as queued_calls_cnt, cast(null as float) as total_call_queued_time, cast(null as float) as total_call_hold_time, cast(null as int) as held_calls_cnt, cast(null as int) as transfers_cnt, cast(null as int) as inbound_calls_cnt, cast(null as int) as inbound_handled_calls_cnt, cast(null as int) as outbound_handled_calls_cnt, cast(null as int) as total_handled_calls, cast(null as int) as target_handled_pct, cast(null as float) as handle_time, cast(null as float) as target_inbound_calls_handled_pct, cast(null as int) as abandoned_calls_cnt,cast(null as int) as count_total_tasks, cast(null as int) as completed_chats_cnt, cast(null as int) as missed_chats_cnt, cast(null as int) as missed_chats_greaterthan20sec_cnt, cast(null as int) as in_progress_chats_cnt, cast(null as int) as target_chats_handled_pct, cast(null as float) as chat_duration, cast(null as int) as booking_flow_chats_cnt, cast(null as float) as total_chat_wait_time, vts_case.total_cases_cnt, vts_case.handled_cases_cnt, vts_case.target_case_productivity_pct, vts_case.target_cases_sla, vts_case.cases_per_team_queue_cnt, vts_case.total_value_duration, vts_case.target_avg_value_duration_hrs,vts_case.total_case_resolution_time, cast(null as int) as target_net_new_bookings_contribution_phone, cast(null as int) as target_net_new_voyage_protection_phone, cast(null as int) as target_casino_bookings, cast(null as string) as post_chat_survey_response, cast(null as int) as survey_responses_cnt, cast(null as int) as target_loved_it_response_pct, cast(null as int) as target_avg_handle_time_hrs, cast(null as int) as target_phone_service_level, cast(null as int) as target_pct_call_abandon, cast(null as int)  as target_count_outbound_calls from vv_db.hvtb_mart_vts_case_summary_fact as vts_case""").withColumn("etl_ld_dt", current_timestamp()).withColumn("etl_upd_dt", current_timestamp()).withColumnRenamed("phone_date", "vts_date").withColumnRenamed("phone_hour", "vts_hour")

								val targetDf = overallSummaryDf.select("team", "source", "origin", "queue", "campaign", "skill_name", "reason", "resolution", "sub_resolution", "vts_date", "vts_hour", "incoming_calls_cnt", "queued_calls_cnt", "total_call_queued_time", "total_call_hold_time", "held_calls_cnt", "transfers_cnt", "inbound_calls_cnt", "inbound_handled_calls_cnt", "outbound_handled_calls_cnt", "total_handled_calls", "target_handled_pct", "handle_time", "target_inbound_calls_handled_pct", "abandoned_calls_cnt","count_total_tasks", "completed_chats_cnt", "missed_chats_cnt", "missed_chats_greaterthan20sec_cnt", "in_progress_chats_cnt", "target_chats_handled_pct", "chat_duration", "booking_flow_chats_cnt", "total_chat_wait_time", "total_cases_cnt", "handled_cases_cnt", "target_case_productivity_pct", "target_cases_sla", "cases_per_team_queue_cnt", "total_value_duration", "target_avg_value_duration_hrs","total_case_resolution_time", "target_net_new_bookings_contribution_phone", "target_net_new_voyage_protection_phone", "target_casino_bookings", "post_chat_survey_response", "survey_responses_cnt", "target_loved_it_response_pct", "target_avg_handle_time_hrs", "target_phone_service_level", "target_pct_call_abandon", "target_count_outbound_calls", "etl_ld_dt", "etl_upd_dt")

                log.info("We are entering the bigquery")
								tempToRSTableLoad(spark, targetDf)
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
					log.info("******************in the catch of VtsOverallSummaryFact Dim ******************");
					e.printStackTrace();
					throw new Exception("General Exception..please check the stacktrace")
				}
				}

				spark.stop()
	}

	def tempToRSTableLoad(spark: SparkSession, targetDf: DataFrame) = {
			//val redshift_table = spark.sparkContext.getConf.get("spark.pond.database").trim() + "." + spark.sparkContext.getConf.get("spark.pond.table").trim()
					val EmptyDF = spark.emptyDataFrame
				//	EmptyDF.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())

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
						log.info("******************in the catch of BigQueryException Load ******************");
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
				//	log.info("Alter RS TABLE to point to target location" + redshift_table)
					val finalLocation = spark.sparkContext.getConf.get("spark.target.location").trim()
					val BQPermExtnTbl = spark.sparkContext.getConf.get("spark.bq.dataset").trim() + "." + spark.sparkContext.getConf.get("spark.bq.permtblname").trim()
          val BQErrorTableName = spark.sparkContext.getConf.get("spark.bq.dataset").trim() + "." + spark.sparkContext.getConf.get("spark.bq.permtblname").trim() + "_err"
          log.info(s"Alter View $BQViewName to point $BQPermExtnTbl ")
          val perm_qry=s"CREATE OR REPLACE VIEW $BQViewName AS SELECT * FROM $BQPermExtnTbl"

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
						log.info("******************in the catch of Bigquery Dimension  ******************");
						e.printStackTrace();
						throw new Exception("SQL Exception..please check the stacktrace", e);
					}
					print("Bigquery Query Failed")
					}
		//	sqlConnection.close()
	}

}