package com.virginvoyages

import org.apache.log4j.{Level, Logger,LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,lit}
import com.virginvoyages.metadataframework.ManageMetadata
import scala.util.control.Breaks._


object CheckExecStatus {
  
  val log = LogManager.getRootLogger
			log.setLevel(Level.INFO)

			def main(args: Array[String]): Unit = {

					    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
							val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)

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
							
							val jobType=spark.sparkContext.getConf.get("spark.jobType")
							val srcsparkBatchCols = spark.sparkContext.getConf.get("spark.source.sparkbatchidList")
							val srcSparkColsBatchList = srcsparkBatchCols.split(",").toSeq
							
							val srcTalendBatchCols = spark.sparkContext.getConf.get("spark.source.talendbatchidList")
							val srcTalendColsBatchList = srcTalendBatchCols.split(",").toSeq
							
							if(jobType.trim().toUpperCase().equals("SPARK") && !srcSparkColsBatchList.isEmpty)
							{
							  println("======================jobType============== "+jobType)
							  val LoopCount=spark.sparkContext.getConf.get("spark.source.NoOfChecks").trim().toInt;
					      val WaitTimeMin=spark.sparkContext.getConf.get("spark.source.WaitTimeMinutes").trim().toLong; // 600000 milli === 10 min
					    
					    var flag=true
							import spark.implicits._
							
							
							val MetadataDF = spark.read.format(spark.sparkContext.getConf.get("spark.target.ops.table.format").trim())
										.option("table", spark.sparkContext.getConf.get("spark.target.enablerops_table").trim())
										.option("zkUrl", spark.sparkContext.getConf.get("spark.target.zkurl").trim()).load()
										.where($"BATCH_ID".isin(srcSparkColsBatchList:_*))
										.selectExpr("BATCH_ID","BATCHSTARTTIME","BATCHENDTIME","BATCH_EXECUTION_STARTTIME","BATCH_EXECUTION_ENDTIME","status")
										
										var randomNo= scala.util.Random
										val temp_table1Rnd="meta_table"+ randomNo.nextInt(10000)
                    MetadataDF.createOrReplaceTempView(temp_table1Rnd)

                    
                    val temp_table1Sql="select BATCH_ID,BATCHSTARTTIME,BATCHENDTIME,BATCH_EXECUTION_STARTTIME,BATCH_EXECUTION_ENDTIME,status from (select BATCH_ID,BATCHSTARTTIME,BATCHENDTIME,BATCH_EXECUTION_STARTTIME,BATCH_EXECUTION_ENDTIME,lower(status) as status,row_number() over(partition by BATCH_ID order by BATCH_EXECUTION_ENDTIME desc) rn from "+temp_table1Rnd+") where rn=1"
										val LatestDF=spark.sql(temp_table1Sql)
										val CheckRunningDF=LatestDF.where($"status" === "running")
										
							if(!CheckRunningDF.head(1).isEmpty)
							{
							  breakable
							  {
							for( a <- 1 until LoopCount)
							{

								val CheckDF = spark.read.format(spark.sparkContext.getConf.get("spark.target.ops.table.format").trim())
										.option("table", spark.sparkContext.getConf.get("spark.target.enablerops_table").trim())
										.option("zkUrl", spark.sparkContext.getConf.get("spark.target.zkurl").trim()).load()
										.where($"BATCH_ID".isin(srcSparkColsBatchList:_*))
										.selectExpr("BATCH_ID","BATCHSTARTTIME","BATCHENDTIME","BATCH_EXECUTION_STARTTIME","BATCH_EXECUTION_ENDTIME","status")

										
										var randomNo= scala.util.Random
										val temp_table1Rnd="meta_table"+ randomNo.nextInt(10000)
                    CheckDF.createOrReplaceTempView(temp_table1Rnd)

										val temp_table1Sql="select BATCH_ID,BATCHSTARTTIME,BATCHENDTIME,BATCH_EXECUTION_STARTTIME,BATCH_EXECUTION_ENDTIME,status from (select BATCH_ID,BATCHSTARTTIME,BATCHENDTIME,BATCH_EXECUTION_STARTTIME,BATCH_EXECUTION_ENDTIME,lower(status) as status,row_number() over(partition by BATCH_ID order by BATCH_EXECUTION_ENDTIME desc) rn from "+ temp_table1Rnd+" ) where rn=1"
										val LatestRecordDF=spark.sql(temp_table1Sql)
										val RunningDF=LatestRecordDF.where($"status" === "running")
										
										import java.util.Calendar
										val cal = Calendar.getInstance();
								    val date_formater1 = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
										var DateTime=date_formater1.format(cal.getTime())
										
										println("==================for loop = "+a+" date time is "+DateTime)
										
										if(!RunningDF.head(1).isEmpty)
										{   Thread.sleep(WaitTimeMin*60*1000)   }
										else
											{  flag=false;
										  break 	}

							} // end of loop here
							    }  // end of break
							  
							  if(flag == true)
					{		log.info("=============ChkCurrExecStatus Job Failed================");
						println("==============ChkCurrExecStatus Job Failed================")
						ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
						sys.exit(1) 	}
				
							  
							} // end of if 
							
							
							} //end of if (jobtype)
					    
					    
							else  if(jobType.trim().toUpperCase().equals("TALEND") && !srcTalendColsBatchList.isEmpty)
							{
							    println("======================jobType============== "+jobType)
							  val LoopCount=spark.sparkContext.getConf.get("spark.source.NoOfChecks").trim().toInt;
					      val WaitTimeMin=spark.sparkContext.getConf.get("spark.source.WaitTimeMinutes").trim().toLong; // 600000 milli === 10 min
					    
					    var flag=true
							import spark.implicits._
							
							
							val MetadataDF = spark.read.format(sparkConfiguration.value.get("spark.src.con.format").get)
        .options(Map(
          "url" -> sparkConfiguration.value.get("spark.src.con.url").get,
          "user" -> sparkConfiguration.value.get("spark.src.user").get, "password" -> sparkConfiguration.value.get("spark.src.password").get,
          "dbtable" -> sparkConfiguration.value.get("spark.src.table").get, "driver" -> sparkConfiguration.value.get("spark.src.con.driver").get)).load()
          .where($"job_name".isin(srcTalendColsBatchList:_*))
							
          MetadataDF.show(20,false)
          MetadataDF.printSchema
          
          var randomNo= scala.util.Random
					val temp_table1Rnd="meta_table"+ randomNo.nextInt(10000)
          MetadataDF.createOrReplaceTempView(temp_table1Rnd)
						
          /** If count > 0 then the job waits in the loop otherwise the jobs continues */
          
          val temp_table1Sql="Select COALESCE(count(1),0)  as cnt_job from "+ temp_table1Rnd +" where (load_job_status = '' or load_job_status is null) and load_id in (Select  max(load_id) from  "+ temp_table1Rnd+" )"
					val LatestDF=spark.sql(temp_table1Sql)
          
          //val LatestDF=spark.sql("Select COALESCE(count(1),0)  as cnt_job from meta_table where (load_job_status = '' or load_job_status is null) and load_id in (Select  max(load_id) from meta_table)")
					val CheckRunningDF=LatestDF.where($"cnt_job" > lit(0))          
          
					//CheckRunningDF.show(false)
					
						if(!CheckRunningDF.head(1).isEmpty)
							{
							  breakable
							  {
							for( a <- 1 until LoopCount)
							{

								val CheckDF = spark.read.format(sparkConfiguration.value.get("spark.src.con.format").get)
                              .options(Map(
          "url" -> sparkConfiguration.value.get("spark.src.con.url").get,
          "user" -> sparkConfiguration.value.get("spark.src.user").get, "password" -> sparkConfiguration.value.get("spark.src.password").get,
          "dbtable" -> sparkConfiguration.value.get("spark.src.table").get, "driver" -> sparkConfiguration.value.get("spark.src.con.driver").get)).load()
          .where($"job_name".isin(srcTalendColsBatchList:_*))
									

										CheckDF.createOrReplaceTempView("meta_table")
										 val LatestRecordDF=spark.sql("Select COALESCE(count(1),0)  as cnt_job from meta_table where (load_job_status = '' or load_job_status is null) and load_id in (Select  max(load_id) from meta_table)")
					           val RunningDF=LatestDF.where($"cnt_job" > lit(0))  
					            
					           println("==========Running==========")
					            RunningDF.show(false)
					            RunningDF.printSchema
					            
					            
					            	import java.util.Calendar
										val cal = Calendar.getInstance();
								    val date_formater1 = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
										var DateTime=date_formater1.format(cal.getTime())
										
										println("==================for loop = "+a+" date time is "+DateTime)
										
										if(!RunningDF.head(1).isEmpty)
										{   Thread.sleep(WaitTimeMin*60*1000)   }
										else
											{  flag=false;
										  break 	}

							} // end of loop here
							    }  // end of break
							  
							  if(flag == true)
					{		log.info("=============ChkCurrExecStatus Job Failed================");
						println("==============ChkCurrExecStatus Job Failed================")
						ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
						sys.exit(1) 	}
				
							} //end of if
							  
							  
							} // end of else if (job type)
					    
							else
							{
							log.info("=============Invalid Job type Job ================");
						  println("==============Invalid Job type Job ================")
							}
  
            log.info("=============ChkCurrExecStatus Job SuccessFul================");
						println("==============ChkCurrExecStatus Job SuccessFul================")
						ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
						spark.stop()
	



	} //end of main
}