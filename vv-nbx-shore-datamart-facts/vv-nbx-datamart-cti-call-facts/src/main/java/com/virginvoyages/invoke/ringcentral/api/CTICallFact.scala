package com.virginvoyages.invoke.ringcentral.api

import java.util.Date

import com.virginvoyages.scd.ChangeDataCapture.loadDimFact

import java.util.Properties
import java.sql.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ StructType, StructField, TimestampType, StringType, IntegerType, DateType, LongType };
import org.apache.spark.sql.expressions.Window
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import com.virginvoyages.metadataframework.ManageMetadata
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
import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.sql.SaveMode
import java.text.SimpleDateFormat

object CTICallFact {
  def main(args: Array[String]): Unit = {
    /*if (args.length <= 1) {
      println("This job required at least two parameters")
      System.exit(1)
    }

    val batchStartTime1 = args(0)
    val batchEndTime1 = args(1)
    
    val batchStartTime = batchStartTime1.replace("T", " ")
    val batchEndTime = batchEndTime1.replace("T", " ")*/
    
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    val max_var = spark.sql(s"SELECT max(batchtime) as batchtime  FROM vv_db.hvtb_nbx_landing_cti_skill").collect()(0).getTimestamp(0)
    
    
/**********************************MetaData framework**********************************************/
      val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
      metadata.productIterator.foreach(println)
      val batch_id1 = metadata._1
      val batch_instance_id1 = metadata._2
      var batchStartTime = metadata._3
      var batchEndTime = metadata._4
      val inputFormat = "yyyy-MM-dd HH:mm:ss"
      val outputFormat = "yyyy-MM-dd HH:mm:ss"
       batchStartTime = dateMinusSec(batchStartTime, 7200, inputFormat, outputFormat)
    batchEndTime = dateMinusSec(batchEndTime, 7200, inputFormat, outputFormat)
    println("After conversion starttime:" +batchStartTime + " endtime:"+batchEndTime)
    
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    import spark.implicits._

    try {

     val whereClause = s"""where batchtime >= from_unixtime(unix_timestamp('$batchStartTime', "yyyy-MM-dd' 'HH:mm:ss")) and batchtime <= from_unixtime(unix_timestamp('$batchEndTime', "yyyy-MM-dd' 'HH:mm:ss")) and part_date>=to_date('$batchStartTime') and part_date<=to_date('$batchEndTime') group by contactid,mastercontactid,pointofcontactname,agentid,teamid,skillid,campaignid,contactstart,prequeueseconds,inqueueseconds,postqueueseconds,totaldurationseconds,abandonseconds,callbacktime,agentseconds,isoutbound,transferindicatorid,abandoned,ACWSeconds,confSeconds,isLogged,isShortAbandon,isTakeover,releaseSeconds,routingTime,holdCount,holdSeconds"""     
     //val whereClause = s"""group by contactid,mastercontactid,pointofcontactname,agentid,teamid,skillid,campaignid,contactstart,prequeueseconds,inqueueseconds,postqueueseconds,totaldurationseconds,abandonseconds,callbacktime,agentseconds,isoutbound,transferindicatorid,abandoned,ACWSeconds,	confSeconds,isLogged,isShortAbandon,isTakeover,releaseSeconds,routingTime,holdCount,holdSeconds"""
      log.info("select contactid,mastercontactid,pointofcontactname,agentid,teamid,skillid,campaignid,contactstart,prequeueseconds,inqueueseconds,postqueueseconds,totaldurationseconds,abandonseconds,callbacktime,agentseconds,isoutbound,transferindicatorid,abandoned,ACWSeconds,	confSeconds,isLogged,isShortAbandon,isTakeover,releaseSeconds,routingTime,holdCount,holdSeconds from %s %s".format(sc.getConf.get("spark.target.cticall.table"), whereClause))
      var cticallfactdf = spark.sql("select distinct contactid,mastercontactid,pointofcontactname,agentid,teamid,skillid,campaignid,contactstart,prequeueseconds,inqueueseconds,postqueueseconds,totaldurationseconds,abandonseconds,callbacktime,agentseconds,isoutbound,transferindicatorid,abandoned,ACWSeconds,	confSeconds,isLogged,isShortAbandon,isTakeover,releaseSeconds,routingTime,holdCount,holdSeconds from %s %s".format(sc.getConf.get("spark.target.cticall.table"), whereClause))
    //  var cticallfactdf = spark.sql("select distinct contactid,mastercontactid,pointofcontactname,agentid,teamid,skillid,campaignid,contactstart,prequeueseconds,inqueueseconds,postqueueseconds,totaldurationseconds,abandonseconds,callbacktime,agentseconds,isoutbound,transferindicatorid,abandoned,ACWSeconds,	confSeconds,isLogged,isShortAbandon,isTakeover,releaseSeconds,routingTime,holdCount,holdSeconds from %s ".format(sc.getConf.get("spark.target.cticall.table")))    
      
      //val df=spark.read.parquet("gs://vv-prod-nbx-presail-backup/data/apps/nbx/hive/parsing/cti_call/hvtb_parse_cti_call/*/*")
      
      
     // df.createOrReplaceTempView("Tbl")
     // var cticallfactdf = spark.sql("select distinct contactid,mastercontactid,pointofcontactname,agentid,teamid,skillid,campaignid,contactstart,prequeueseconds,inqueueseconds,postqueueseconds,totaldurationseconds,abandonseconds,callbacktime,agentseconds,isoutbound,transferindicatorid,abandoned,ACWSeconds,	confSeconds,isLogged,isShortAbandon,isTakeover,releaseSeconds,routingTime,holdCount,holdSeconds from Tbl %s".format(whereClause))
      //val src=spark.sql("select * from  vv_db.hvtb_parse_cti_call_rec")

      
//      timedim.show(10,false)
//      timedim.printSchema() 
      
      import spark.implicits._
      import org.apache.spark.sql.functions.col
                             
       if (!cticallfactdf.head(1).isEmpty) {
         
               cticallfactdf.createOrReplaceTempView("src")
      val tgt=spark.sql("select src_contact_id from vv_db.hvtb_nbx_core_cti_call_fact")
      tgt.createOrReplaceTempView("tgt")  
    //  spark.sql("select src.* from src left join tgt on src.contactid=tgt.src_contact_id  where tgt.src_contact_id is null").show(false)
     cticallfactdf=  spark.sql("select src.* from src left join tgt on src.contactid=tgt.src_contact_id  where tgt.src_contact_id is null")
          
      val ctiagentdimdf = spark.sql(spark.sparkContext.getConf.get("spark.cti.call.agent.dim.sql").trim())
      val ctiteamdimdf = spark.sql(spark.sparkContext.getConf.get("spark.cti.call.team.dim.sql").trim())
      val ctiskilldimdf = spark.sql(spark.sparkContext.getConf.get("spark.cti.call.skill.dim.sql").trim())
      val cticompaigndimdf = spark.sql(spark.sparkContext.getConf.get("spark.cti.call.compaign.dim.sql").trim())
      val datedim = spark.sql(spark.sparkContext.getConf.get("spark.cti.call.date.dim.sql").trim())
      val timedim = spark.sql(spark.sparkContext.getConf.get("spark.cti.call.time.dim.sql").trim())
      
      
      cticallfactdf = cticallfactdf.join(ctiagentdimdf, cticallfactdf("agentId") === ctiagentdimdf("cti_agent_dim_agentId"), "left")
      cticallfactdf = cticallfactdf.join(ctiteamdimdf, cticallfactdf("teamId") === ctiteamdimdf("cti_team_dim_teamId"), "left")
      cticallfactdf = cticallfactdf.join(ctiskilldimdf, cticallfactdf("skillId") === ctiskilldimdf("cti_skill_dim_skillId"), "left")
      cticallfactdf = cticallfactdf.join(cticompaigndimdf, cticallfactdf("campaignId") === cticompaigndimdf("cti_compaign_dim_campaignId"), "left")
      cticallfactdf = cticallfactdf.join(datedim, to_date(cticallfactdf("contactStart")) === datedim("datedim_date"), "left")
      cticallfactdf = cticallfactdf.join(timedim, hour(cticallfactdf("contactStart")) * 60 * 60 + minute(cticallfactdf("contactStart")) * 60 + second(cticallfactdf("contactStart")) === timedim("time_dim_second_of_day"), "left")
      
//      cticallfactdf.show(10,false)
//      cticallfactdf.printSchema()
      
      //var cticallfacttabledf = cticallfactdf.select("contactId", "masterContactId", "pointOfContactName", "cti_agent_dim_cti_agent_skey", "cti_team_dim_cti_team_skey", "cti_skill_dim_cti_skill_skey", "cti_compaign_dim_cti_campaign_skey", "datedim_date_id", "time_dim_s_key", "preQueueSeconds", "inQueueSeconds", "postQueueSeconds", "totalDurationSeconds", "abandonSeconds", "callbackTime", "agentSeconds", "isOutbound", "transferIndicatorId", "abandoned")
      var cticallfacttabledf = cticallfactdf.select("contactId", "masterContactId", "pointOfContactName", "cti_agent_dim_cti_agent_skey", "cti_team_dim_cti_team_skey", "cti_skill_dim_cti_skill_skey", "cti_compaign_dim_cti_campaign_skey", "datedim_date_id", "time_dim_s_key", "preQueueSeconds", "inQueueSeconds", "postQueueSeconds", "totalDurationSeconds", "abandonSeconds", "callbackTime", "agentSeconds", "isOutbound", "transferIndicatorId", "abandoned","ACWSeconds","confSeconds","isLogged","isShortAbandon","isTakeover","releaseSeconds","routingTime","holdCount","holdSeconds")
      cticallfacttabledf = cticallfacttabledf.withColumnRenamed("cti_agent_dim_cti_agent_skey", "agentId")
        .withColumnRenamed("cti_team_dim_cti_team_skey", "teamId")
        .withColumnRenamed("cti_skill_dim_cti_skill_skey", "skillId")
        .withColumnRenamed("cti_compaign_dim_cti_campaign_skey", "campaignId")
        .withColumnRenamed("datedim_date_id", "dateskey")
        .withColumnRenamed("time_dim_s_key", "timeskey")
      
      cticallfacttabledf = cticallfacttabledf.withColumn("agentId", when(cticallfacttabledf.col("agentId").isNull, lit(-1)).otherwise(cticallfacttabledf.col("agentId")))
                                             .withColumn("teamId", when(cticallfacttabledf.col("teamId").isNull, lit(-1)).otherwise(cticallfacttabledf.col("teamId")))
                                             .withColumn("skillId", when(cticallfacttabledf.col("skillId").isNull, lit(-1)).otherwise(cticallfacttabledf.col("skillId")))
                                             .withColumn("campaignId", when(cticallfacttabledf.col("campaignId").isNull, lit(-1)).otherwise(cticallfacttabledf.col("campaignId")))
      
//      cticallfacttabledf.show(10,false)
//      cticallfacttabledf.printSchema() 
      
     
      val required_columns = spark.sparkContext.getConf.get("spark.source.columns").split(",")

      val missing_columns = ArrayBuffer[String]()
      val avaliable_columns = ArrayBuffer[String]()

      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

      for (col <- required_columns) {

        if (hasColumn(cticallfacttabledf, col)) {
          println(col, "column exists", avaliable_columns.toString)
          println("column exists", avaliable_columns.length)
          log.info(col, "column exists", avaliable_columns.toString, avaliable_columns.length)
          println(avaliable_columns.length, "length")
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
      val stage_final_df = missing_columns.foldLeft(cticallfacttabledf)((df, c) =>
        df.withColumn(s"$c", lit(null)))

      log.info("calling scd framework")

      //      var pond_columns:Array[String] = spark.sparkContext.getConf.get("spark.pond.allColumns").split(",")
      //      for ( x <- pond_columns ) {
      //         log.info("pond columns: "+ x)
      //      }
      
      //stage_final_df.createOrReplaceTempView("final")
     // spark.sql("select * from final where ")
     // println(stage_final_df.count)
     val  stage_final_dist_df=stage_final_df.dropDuplicates()
   // println(stage_final_dist_df.count)
    //stage_final_dist_df.show(false)
    stage_final_dist_df.createOrReplaceTempView("viewName")
    spark.sql("select count(1),dateskey from viewName group by dateskey  order by dateskey desc").show(100,false)
   
      loadDimFact(spark: SparkSession, stage_final_dist_df) 
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark);
      println("after metedata")
     }

    } catch {

      case e: Exception =>
        {
          log.info("in the catch of updateStatus ****************** ")
          ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
          e.printStackTrace()
          throw new Exception("General Exception..please check the stacktrace")
        }
  
    }

  }
        def dateMinusSec(date: String, seconds: Int, inputFormat: String, outputFormat: String): String = {
    import java.util.Calendar
    val dateAux = Calendar.getInstance()
    dateAux.setTime(new SimpleDateFormat(inputFormat).parse(date))
    dateAux.add(Calendar.SECOND, -seconds)
    return new SimpleDateFormat(outputFormat).format(dateAux.getTime())
  }
}