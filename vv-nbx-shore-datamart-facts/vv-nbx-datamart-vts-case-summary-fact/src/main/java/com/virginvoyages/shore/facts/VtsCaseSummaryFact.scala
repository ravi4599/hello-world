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

object VtsCaseSummaryFact {
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
								
								
								val caseSummaryDf = spark.sql(""" select 
										vts_performance_targets_lkp.target_team_name as team_name
										,'CRM' as source
										,core_crm_case_dim.origin as origin
										,core_crm_case_dim.Case_Owner_Queue__c as queue_name
										,case when core_crm_case_dim.reason!='' then core_crm_case_dim.reason else null end as case_reason
										,case when core_crm_case_dim.case_resolution__c!='' then core_crm_case_dim.case_resolution__c else null end as resolution
										,case when core_crm_case_dim.case_sub_resolution__c!='' then core_crm_case_dim.case_sub_resolution__c else null end as sub_resolution
										--,date(core_crm_case_dim.createddate) as case_date
										--,hour(core_crm_case_dim.createddate) as case_hour
										,date(FROM_UTC_TIMESTAMP(cast(UNIX_TIMESTAMP(case when core_crm_case_dim.Case_Owner_Queue__c = 'Sailor Services' then core_crm_case_dim.closeddate else core_crm_caseactivity.value_out_time__c end, "yyyy-MM-dd hh:mm:ss") as timestamp) , 'America/New_York')) as case_date
										,hour(FROM_UTC_TIMESTAMP(cast(UNIX_TIMESTAMP(case when core_crm_case_dim.Case_Owner_Queue__c = 'Sailor Services' then core_crm_case_dim.closeddate else core_crm_caseactivity.value_out_time__c end, "yyyy-MM-dd hh:mm:ss") as timestamp) , 'America/New_York')) as case_hour 
										,count(distinct(core_crm_case_dim.id)) as count_total_cases
										,COUNT(distinct(case when core_crm_case_dim.status = 'Closed' then core_crm_case_dim.id else null END)) as count_handled_cases
										,vts_performance_targets_lkp.target_pct_cases_handled as target_case_productivity_pct
										,vts_performance_targets_lkp.target_avg_total_case_resolution_hrs as cases_sla
										,count(distinct(core_crm_caseactivity.case__c)) as count_total_cases_per_team_queue 
										,sum(core_crm_caseactivity.value_duration__c) as total_value_duration
										,vts_performance_targets_lkp.target_avg_value_duration_hrs as target_avg_case_value_duration_hrs
										,sum(case when core_crm_caseactivity.seqnum=1 then core_crm_case_dim.total_case_resolution_time__c else 0 end) as total_case_resolution_time
										from
										vv_db.hvtb_nbx_core_crm_case_dim core_crm_case_dim
										left join vv_db.hvtb_mart_vts_performance_targets_lkp vts_performance_targets_lkp 
										on core_crm_case_dim.case_owner_queue__c=vts_performance_targets_lkp.target_queue_name
										and DATE(core_crm_case_dim.closeddate) between vts_performance_targets_lkp.target_range_start_date and vts_performance_targets_lkp.target_range_end_date
										left join (select ca.*, row_number() over (partition by ca.case__c order by ca.case__c) as seqnum from vv_db.hvtb_nbx_core_crm_caseactivity ca) core_crm_caseactivity on core_crm_caseactivity.case__c = core_crm_case_dim.id and core_crm_caseactivity.Value_Changed_To__c = core_crm_case_dim.Case_Owner_Queue__c and core_crm_caseactivity.change_type__c='Queue' 		
										where core_crm_case_dim.origin <> 'Chat'
										group by
										vts_performance_targets_lkp.target_team_name,
										core_crm_case_dim.origin,
										core_crm_case_dim.Case_Owner_Queue__c,
										core_crm_case_dim.reason,
										core_crm_case_dim.case_resolution__c,
										core_crm_case_dim.case_sub_resolution__c,
										date(FROM_UTC_TIMESTAMP(cast(UNIX_TIMESTAMP(case when core_crm_case_dim.Case_Owner_Queue__c = 'Sailor Services' then core_crm_case_dim.closeddate else core_crm_caseactivity.value_out_time__c end, "yyyy-MM-dd hh:mm:ss") as timestamp) , 'America/New_York')),			
										hour(FROM_UTC_TIMESTAMP(cast(UNIX_TIMESTAMP(case when core_crm_case_dim.Case_Owner_Queue__c = 'Sailor Services' then core_crm_case_dim.closeddate else core_crm_caseactivity.value_out_time__c end, "yyyy-MM-dd hh:mm:ss") as timestamp) , 'America/New_York')),                                         
										vts_performance_targets_lkp.target_pct_cases_handled,
										vts_performance_targets_lkp.target_avg_total_case_resolution_hrs,
										vts_performance_targets_lkp.target_avg_value_duration_hrs""")
										
										
						
								

//								val caseSummaryDf = spark.sql(""" select 
//										vts_performance_targets_lkp.target_team_name as team_name
//										,'CRM' as source
//										,core_crm_case_dim.origin as origin
//										,core_crm_case_dim.Case_Owner_Queue__c as queue_name
//										,case when core_crm_case_dim.reason!='' then core_crm_case_dim.reason else null end as case_reason
//										,case when core_crm_case_dim.case_resolution__c!='' then core_crm_case_dim.case_resolution__c else null end as resolution
//										,case when core_crm_case_dim.case_sub_resolution__c!='' then core_crm_case_dim.case_sub_resolution__c else null end as sub_resolution
//										--,date(core_crm_case_dim.createddate) as case_date
//										--,hour(core_crm_case_dim.createddate) as case_hour
//										,date(FROM_UTC_TIMESTAMP(cast(UNIX_TIMESTAMP(case when core_crm_case_dim.Case_Owner_Queue__c = 'Sailor Services' then core_crm_case_dim.closeddate else core_crm_caseactivity.value_out_time__c end, "yyyy-MM-dd hh:mm:ss")*1000 as timestamp) , 'America/New_York')) as case_date
//										,hour(FROM_UTC_TIMESTAMP(cast(UNIX_TIMESTAMP(case when core_crm_case_dim.Case_Owner_Queue__c = 'Sailor Services' then core_crm_case_dim.closeddate else core_crm_caseactivity.value_out_time__c end, "yyyy-MM-dd hh:mm:ss")*1000 as timestamp) , 'America/New_York')) as case_hour 
//										,count(distinct(core_crm_case_dim.id)) as count_total_cases
//										,COUNT(distinct(case when core_crm_case_dim.status = 'Closed' then core_crm_case_dim.id else null END)) as count_handled_cases
//										,vts_performance_targets_lkp.target_pct_cases_handled as target_case_productivity_pct
//										,vts_performance_targets_lkp.target_avg_total_case_resolution_hrs as cases_sla
//										,count(distinct(core_crm_caseactivity.case__c)) as count_total_cases_per_team_queue 
//										,sum(core_crm_caseactivity.value_duration__c) as total_value_duration
//										,vts_performance_targets_lkp.target_avg_value_duration_hrs as target_avg_case_value_duration_hrs
//										from
//										vv_db.hvtb_nbx_core_crm_case_dim core_crm_case_dim
//										left join vv_db.hvtb_mart_vts_performance_targets_lkp vts_performance_targets_lkp 
//										on core_crm_case_dim.case_owner_queue__c=vts_performance_targets_lkp.target_queue_name
//										and DATE(core_crm_case_dim.closeddate) between vts_performance_targets_lkp.target_range_start_date and vts_performance_targets_lkp.target_range_end_date
//										left join vv_db.hvtb_nbx_core_crm_caseactivity core_crm_caseactivity on core_crm_caseactivity.case__c = core_crm_case_dim.id and core_crm_caseactivity.Value_Changed_To__c = core_crm_case_dim.Case_Owner_Queue__c and core_crm_caseactivity.change_type__c='Queue' 		
//										where core_crm_case_dim.origin <> 'Chat'
//										group by
//										vts_performance_targets_lkp.target_team_name,
//										core_crm_case_dim.origin,
//										core_crm_case_dim.Case_Owner_Queue__c,
//										core_crm_case_dim.reason,
//										core_crm_case_dim.case_resolution__c,
//										core_crm_case_dim.case_sub_resolution__c,
//										date(FROM_UTC_TIMESTAMP(cast(UNIX_TIMESTAMP(case when core_crm_case_dim.Case_Owner_Queue__c = 'Sailor Services' then core_crm_case_dim.closeddate else core_crm_caseactivity.value_out_time__c end, "yyyy-MM-dd hh:mm:ss")*1000 as timestamp) , 'America/New_York')),			
//										hour(FROM_UTC_TIMESTAMP(cast(UNIX_TIMESTAMP(case when core_crm_case_dim.Case_Owner_Queue__c = 'Sailor Services' then core_crm_case_dim.closeddate else core_crm_caseactivity.value_out_time__c end, "yyyy-MM-dd hh:mm:ss")*1000 as timestamp) , 'America/New_York')),                                         
//										vts_performance_targets_lkp.target_pct_cases_handled,
//										vts_performance_targets_lkp.target_avg_total_case_resolution_hrs,
//										vts_performance_targets_lkp.target_avg_value_duration_hrs""")
										
										

								val targetDf=caseSummaryDf.withColumnRenamed("team_name", "team").withColumnRenamed("queue_name", "queue")
								//.withColumnRenamed("chat_date", "date").withColumnRenamed("chat_hour","hour")
								.withColumnRenamed("case_reason","reason")
								.withColumnRenamed("count_total_cases", "total_cases_cnt")
								.withColumnRenamed("count_handled_cases", "handled_cases_cnt")                                  
								.withColumnRenamed("cases_sla", "target_cases_sla")
								.withColumnRenamed("count_total_cases_per_team_queue", "cases_per_team_queue_cnt")
								.withColumnRenamed("target_avg_case_value_duration_hrs", "target_avg_value_duration_hrs")                                 
								.withColumn("etl_ld_dt", current_timestamp())
								.withColumn("etl_upd_dt", current_timestamp())
								//.withColumn("resolution", when(col("resolution") === "",lit(null).cast(StringType)).otherwise(col("resolution")))
								
								val coltypechanges=targetDf.withColumn("total_cases_cnt", col("total_cases_cnt").cast(IntegerType))
								.withColumn("handled_cases_cnt", col("handled_cases_cnt").cast(IntegerType))
								.withColumn("target_case_productivity_pct", col("target_case_productivity_pct").cast(IntegerType))
								.withColumn("cases_per_team_queue_cnt", col("cases_per_team_queue_cnt").cast(IntegerType))
								.withColumn("total_value_duration", col("total_value_duration").cast(FloatType))
								.withColumn("total_case_resolution_time",col("total_case_resolution_time").cast(IntegerType))
								.select("team", "source", "origin", "queue", "reason", "resolution", "sub_resolution", "case_date", "case_hour", "total_cases_cnt", "handled_cases_cnt", "target_case_productivity_pct", "target_cases_sla", "cases_per_team_queue_cnt", "total_value_duration", "target_avg_value_duration_hrs","total_case_resolution_time","etl_ld_dt", "etl_upd_dt")

								
								log.info("We are entering the bigquery")
								tempToRSTableLoad(spark, coltypechanges)
								
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
					log.info("******************in the catch of VtsCaseSummaryFact Dim ******************");
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
					
					val tempLocation = spark.sparkContext.getConf.get("spark.target.temp").trim()
          val BQTempExtnTbl = spark.sparkContext.getConf.get("spark.bq.dataset").trim() + "." + spark.sparkContext.getConf.get("spark.bq.temptblname").trim()
          val BQViewName = spark.sparkContext.getConf.get("spark.bq.viewname").trim()
          log.info(s"Alter View $BQViewName to point $BQTempExtnTbl ")
          val temp_qry=s"CREATE OR REPLACE VIEW $BQViewName AS SELECT * FROM $BQTempExtnTbl"	
					try {
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
						log.info("******************in the catch of Bigquery Dimension Load ******************");
						e.printStackTrace();
					}
					case e: Exception =>
					{
						log.info("******************in the catch of BigQuery Dimension Load ******************");
						e.printStackTrace();
						throw new Exception("SQL Exception..please check the stacktrace", e);
					}
					print("Bigquery Query Failed")
					}
			//sqlConnection.close()
	}
}