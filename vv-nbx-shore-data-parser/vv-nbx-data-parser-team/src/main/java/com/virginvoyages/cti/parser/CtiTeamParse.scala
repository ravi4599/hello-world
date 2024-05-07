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

object CtiTeamParser {
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
    
    //var ctiskillsrcdf = spark.sql(spark.sparkContext.getConf.get("spark.source.ctiteam.table.query").trim())
    //var ctiskilldatadf = ctiskillsrcdf.select("Message", "batchtime", "part_date").where($"batchtime" >= lit(batchStartTime).cast(TimestampType) && $"batchtime" < lit(batchEndTime).cast(TimestampType) and ($"part_date".between(lit(to_date(lit(batchStartTime).cast(TimestampType), "yyyy-MM-dd")), lit(to_date(lit(batchEndTime).cast(TimestampType), "yyyy-MM-dd")))))
    
    val whereClause = s"""where batchtime >= from_unixtime(unix_timestamp('$batchStartTime', "yyyy-MM-dd' 'HH:mm:ss")) and batchtime <= from_unixtime(unix_timestamp('$batchEndTime', "yyyy-MM-dd' 'HH:mm:ss")) and part_date>=to_date('$batchStartTime') and part_date<=to_date('$batchEndTime') and message is not null """	
    
    println(" %s %s".format(sc.getConf.get("spark.source.ctiteam.table.query"), whereClause))
    
    var ctiskilldatadf = spark.sql(" %s %s".format(sc.getConf.get("spark.source.ctiteam.table.query"), whereClause))
    
    var ctiskilldf = ctiskilldatadf.select("Message").rdd.map{x => x.toString}
    
    if (ctiskilldf.count() != 0) 
    {
    
    var ctiskillParseDf = spark.read.json(ctiskilldf).withColumn("etl_load_dt", current_timestamp())
                                      .withColumn("batchtime", unix_timestamp(lit(batchStartTime), "yyyy-MM-dd' 'HH:mm:ss").cast(TimestampType))
                                      .withColumn("part_date", lit(to_date(col("batchtime"),"yyyy-MM-dd")))
                                      
    ctiskillParseDf = ctiskillParseDf.withColumn("team_explode",explode_outer(ctiskillParseDf.col("teams")))
    
    
    ctiskillParseDf = ctiskillParseDf.withColumn("teamId", ctiskillParseDf.col("team_explode.teamId"))
                                     .withColumn("teamName", ctiskillParseDf.col("team_explode.teamName"))
                                     .withColumn("description", ctiskillParseDf.col("team_explode.description"))
                                     .withColumn("notes", ctiskillParseDf.col("team_explode.notes"))
                                     .withColumn("isActive", ctiskillParseDf.col("team_explode.isActive"))
                                     
    //ctiskillParseDf.printSchema()
    //ctiskillParseDf.show(10,false)
    
    val ctiskillTableDf = ctiskillParseDf.select("teamId","teamName","description","notes","isActive","etl_load_dt","batchtime","part_date")
    
    ctiskillTableDf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.ctiteam.table").trim())  
  
    }
    else {
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