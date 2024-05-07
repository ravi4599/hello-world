package com.virginvoyages.invoke.ringcentral.api

import org.apache.spark.SparkConf
import java.sql.SQLException
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import com.virginvoyages.metadataframework.ManageMetadata
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
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Column, DataFrame }
import scala.collection.mutable.ArrayBuffer

object CTISkillParser {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

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
    import spark.implicits._
    
    var ctiskillsrcdf = spark.sql(spark.sparkContext.getConf.get("spark.source.ctiskill.table.query").trim())
    
    var ctiskilldatadf = ctiskillsrcdf.select("Message", "batchtime", "part_date").where($"batchtime" >= lit(batchStartTime).cast(TimestampType) && $"batchtime" < lit(batchEndTime).cast(TimestampType) and ($"part_date".between(lit(to_date(lit(batchStartTime).cast(TimestampType), "yyyy-MM-dd")), lit(to_date(lit(batchEndTime).cast(TimestampType), "yyyy-MM-dd")))))
    
    
    var ctiskilldf = ctiskilldatadf.select("Message").rdd.map{x => x.toString}
    
    if (ctiskilldf.count() != 0) 
    {
    
    var ctiskillParseDf = spark.read.json(ctiskilldf).withColumn("etl_load_dt", current_timestamp())
                                      .withColumn("batchtime", unix_timestamp(lit(batchStartTime), "yyyy-MM-dd' 'HH:mm:ss").cast(TimestampType))
                                      .withColumn("part_date", lit(to_date(col("batchtime"),"yyyy-MM-dd")))
                                      
                                    
                                      
    ctiskillParseDf = ctiskillParseDf.withColumn("skills_explode",explode_outer(ctiskillParseDf.col("skills")))
    
    
    ctiskillParseDf = ctiskillParseDf.withColumn("skillId", ctiskillParseDf.col("skills_explode.skillId"))
                                     .withColumn("skillName", ctiskillParseDf.col("skills_explode.skillName"))
                                     .withColumn("mediaTypeId", ctiskillParseDf.col("skills_explode.mediaTypeId"))
                                     .withColumn("mediaTypeName", ctiskillParseDf.col("skills_explode.mediaTypeName"))
                                     .withColumn("campaignId", ctiskillParseDf.col("skills_explode.campaignId"))
                                     .withColumn("isActive", ctiskillParseDf.col("skills_explode.isActive"))
                                     .withColumn("requireDisposition", ctiskillParseDf.col("skills_explode.requireDisposition"))
                                     .withColumn("isOutbound", ctiskillParseDf.col("skills_explode.isOutbound"))
                                     .withColumn("scriptId", ctiskillParseDf.col("skills_explode.scriptId"))
                                     .withColumn("scriptName", ctiskillParseDf.col("skills_explode.scriptName"))
                                     .withColumn("initialPriority", ctiskillParseDf.col("skills_explode.initialPriority"))
                                     .withColumn("maxPriority", ctiskillParseDf.col("skills_explode.maxPriority"))
                                     .withColumn("serviceLevelThreshold", ctiskillParseDf.col("skills_explode.serviceLevelThreshold"))
                                     .withColumn("serviceLevelGoal", ctiskillParseDf.col("skills_explode.serviceLevelGoal"))
    
    //ctiskillParseDf.printSchema()
    //ctiskillParseDf.show(10,false)
    
    val ctiskillTableDf = ctiskillParseDf.select("skillId","skillName","mediaTypeId","mediaTypeName","campaignId","isActive","requireDisposition","isOutbound","scriptId","scriptName","initialPriority","maxPriority","serviceLevelThreshold","serviceLevelGoal","etl_load_dt","batchtime","part_date")
    
    ctiskillTableDf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.ctiskill.table").trim())  
    
    }
    else
    {
      log.info("There is no data in the base table")
      
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