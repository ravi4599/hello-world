package com.virginvoyages.postvoyageSailorSurveyRpt

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{ col, to_date, monotonically_increasing_id }

import java.sql.Timestamp
import org.apache.spark.sql.types._
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions.{ col, lit, when }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import java.sql.DriverManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ to_date }
import java.time.{LocalDate, ZoneId}

import org.apache.spark.sql.Dataset
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryException
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.Job
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.QueryJobConfiguration

object postvoyageSailorSurveyRptLoad {
   
  def main(args: Array[String]) {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
   
    var batch_instance_id1: String = null
    var batch_id1: String = null
    var batchInstanceId: String = null
      
    try {
      sqlContext.setConf("hive.exec.dynamic.partition", "true")
      sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
        val voyage_id=spark.sparkContext.getConf.get("spark.voyage.id")

/*        val temp_qry1 = s"call shipdw.postvoyage_sailor_survey_rpt_sp()"
	
        try {
          val bigquery = BigQueryOptions.getDefaultInstance().getService()
          val config1 = QueryJobConfiguration.newBuilder(temp_qry1).build()
	         val job1 = bigquery.create(JobInfo.of(config1))
		         log.info("**Bigquery stored  Procedure **" + "job1 val= " + job1);
             println("**Bigquery stored  Procedure **" + "job1 val= " + job1);
		            if (job1.getStatus().getError() != null) {
            println("Job1 create query failed ..." + job1.getStatus().getError())
            throw new RuntimeException(String.format("Job1 %s ended with error %s", job1.getJobId(),
              job1.getStatus().getError().getMessage()))
                 }
		            else println("Query executed ")
		  
		   println("bigquery proc execution Succesful")
*/                    
//          val hiveTable = spark.sparkContext.getConf.get("spark.hivetable.sql").trim()
          
        val targetTable = spark.sparkContext.getConf.get("spark.target.table").trim()
		   val hiveTableQuery = spark.sparkContext.getConf.get("spark.hivetable.sql").trim()
          
		   val sourceLocation = spark.sparkContext.getConf.get("spark.source.location").trim()
          val postvoyageDf = spark.read.parquet(sourceLocation)
          val postvoyageDf1 = postvoyageDf.dropDuplicates
          println(" Total record count " +postvoyageDf1.count)
//          postvoyageDf1.show(2,false)
          postvoyageDf1.createOrReplaceTempView("postVoyageData")
     
//          spark.sql(s"""select * from postVoyageData """)

          val whereClause = s""" where part_dt=(select max(part_dt) from postVoyageData)"""
          println(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
          log.info(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
          
          println("Execution of Hive Query")
          val stageFinalDf = spark.sql(hiveTableQuery.replace("*whereclause*", whereClause))
          stageFinalDf.write.mode("append").insertInto(targetTable)
          println(" inserted records to hive land table")


/*    
        } catch {
          //Handle errors for BQ
          case e: BigQueryException =>
            { log.info("******************in the catch of  Bigquery stored  procedure   ******************"); e.printStackTrace(); }
          case e: Exception =>
            { log.info("******************in the catch of  Bigquery stored  procedure ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace", e); }
            print(" Bigquery stored  procedure  Failed")
        }
        
*/
        

    } catch {
      case e: SQLException => {
        e.printStackTrace();
        log.info("Exception while executing the SQL command");
      }
      case e: Exception => {
        log.info("******************in the catch of Load ******************");
        e.printStackTrace();
        throw new Exception("General Exception..please check the stacktrace")
      }
    }

    spark.stop()
  }
}
