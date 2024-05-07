package com.virginvoyages.cti.parser

import org.apache.spark.SparkConf
import java.sql.SQLException

import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }

import com.virginvoyages.metadataframework.ManageMetadata
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.Row
import java.sql.Timestamp

import org.apache.spark.sql.functions.{ avg, explode, concat, lit, trim }
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.to_json
import java.sql.Struct
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.Column
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Column, DataFrame }
import scala.collection.mutable.ArrayBuffer

object CtiAgentParser {
  def main(args: Array[String]): Unit = {
   

      val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

       import spark.implicits._
       
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    
    /**********************************MetaData framework**********************************************/
      val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
      metadata.productIterator.foreach(println)
      val batch_id1 = metadata._1
      val batch_instance_id1 = metadata._2
      val batchStartTime = metadata._3
      val batchEndTime = metadata._4
try{     
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    

    
    var ctiskillsrcdf = spark.sql(spark.sparkContext.getConf.get("spark.source.ctiagent.table.query").trim())
    var ctiskilldatadf = ctiskillsrcdf.select("Message", "batchtime", "part_date").where($"batchtime" >= lit(batchStartTime).cast(TimestampType) && $"batchtime" < lit(batchEndTime).cast(TimestampType) and ($"part_date".between(lit(to_date(lit(batchStartTime).cast(TimestampType), "yyyy-MM-dd")), lit(to_date(lit(batchEndTime).cast(TimestampType), "yyyy-MM-dd")))))
    
    var ctiskilldf = ctiskilldatadf.select("Message").rdd.map{x => x.toString}
	if(ctiskilldf.count() !=0) 
    {
    var ctiskillParseDf = spark.read.json(ctiskilldf).withColumn("etl_load_dt", current_timestamp())
                                      .withColumn("batchtime", unix_timestamp(lit(batchStartTime), "yyyy-MM-dd' 'HH:mm:ss").cast(TimestampType))
                                      .withColumn("part_date", lit(to_date(col("batchtime"),"yyyy-MM-dd")))
                                      
    ctiskillParseDf = ctiskillParseDf.withColumn("agent_explode",explode_outer(ctiskillParseDf.col("agents")))
    
    
  ctiskillParseDf = ctiskillParseDf.withColumn("agentId", ctiskillParseDf.col("agent_explode.agentId"))
                                     .withColumn("teamId", ctiskillParseDf.col("agent_explode.teamId"))
                                     .withColumn("userName", ctiskillParseDf.col("agent_explode.userName"))
                                     .withColumn("firstName", ctiskillParseDf.col("agent_explode.firstName"))
                                     .withColumn("lastName", ctiskillParseDf.col("agent_explode.lastName"))
                                     .withColumn("emailAddress", ctiskillParseDf.col("agent_explode.emailAddress"))
                                     .withColumn("isActive", ctiskillParseDf.col("agent_explode.isActive"))
                                     .withColumn("isSupervisor", ctiskillParseDf.col("agent_explode.isSupervisor"))
                                     .withColumn("profileId", ctiskillParseDf.col("agent_explode.profileId"))
                                     .withColumn("profileName", ctiskillParseDf.col("agent_explode.profileName"))
                                     .withColumn("location", ctiskillParseDf.col("agent_explode.location"))
									 .withColumn("country", ctiskillParseDf.col("agent_explode.countryName"))
									 .withColumn("state", ctiskillParseDf.col("agent_explode.state"))
									 .withColumn("city", ctiskillParseDf.col("agent_explode.city"))
                                     
    //ctiskillParseDf.printSchema()
    //ctiskillParseDf.show(10,false)
    
    val ctiskillTableDf = ctiskillParseDf.select("agentId","teamId","userName","firstName","lastName","emailAddress","isActive","isSupervisor","profileId","profileName","location","country","state","city","etl_load_dt","batchtime","part_date")
    
    ctiskillTableDf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.ctiagent.table").trim())  
     
	}
	else
	{
	log.info("No data to be processed")
	 
	}
	ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark);
  }
catch{

case e: SQLException => {
ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
e.printStackTrace();
throw new Exception("SQL Exception..please check the stacktrace", e);

}
println("#----------------------------Process Has Failed---------------------------#")
System.exit(1)

}
  }
}