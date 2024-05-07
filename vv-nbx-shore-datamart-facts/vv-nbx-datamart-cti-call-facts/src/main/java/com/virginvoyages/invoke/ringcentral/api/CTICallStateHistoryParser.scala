package com.virginvoyages.invoke.ringcentral.api

import org.apache.spark.SparkConf
import java.sql.SQLException
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.Row
import scala.util.Try
import com.virginvoyages.metadataframework.ManageMetadata
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
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Column
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Column, DataFrame }
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat

object CTICallStateHistoryParser {

  def main(args: Array[String]): Unit = {
   /*if (args.length <= 1) {
      println("This job required at least two parameters")
      System.exit(1)
    }*/
    
     def checkArray(df: DataFrame, colname: String): Boolean = {

        df.schema(colname).dataType match {
          case ArrayType(_, _) => return true
          case _               => return false
        }
      }
     def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
    
   /* val batchStartTime1 = args(0)
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
    
    /**********************************MetaData framework**********************************************/
      val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
      metadata.productIterator.foreach(println)
      val batch_id1 = metadata._1
      val batch_instance_id1 = metadata._2
      val batchStartTime = metadata._3
      val batchEndTime = metadata._4

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    import spark.implicits._

    val whereClause = s"""where batchtime >= from_unixtime(unix_timestamp('$batchStartTime', "yyyy-MM-dd' 'HH:mm:ss")) and batchtime <= from_unixtime(unix_timestamp('$batchEndTime', "yyyy-MM-dd' 'HH:mm:ss")) and part_date>=to_date('$batchStartTime') and part_date<=to_date('$batchEndTime')"""
    log.info("select * from %s %s".format(sc.getConf.get("spark.source.cticallstate.table"), whereClause))

    var cticallstatedatadf = spark.sql("select * from %s %s".format(sc.getConf.get("spark.source.cticallstate.table"), whereClause))

    if (!cticallstatedatadf.head(1).isEmpty) {
      cticallstatedatadf.collect().foreach(row => {

        try {
          var cticallstatedatadf1 = Seq(row.getString(1)).toDF("message")
          var cticallstatedf = cticallstatedatadf1.select("message").rdd.map { x => x.toString }
          var cticallstateParseDf = spark.read.json(cticallstatedf)
          // cticallstateParseDf.printSchema()
          if (hasColumn(cticallstateParseDf, "contactStateHistory")) {
          if (checkArray(cticallstateParseDf, "contactStateHistory")) {
      
          cticallstateParseDf = cticallstateParseDf.withColumn("contactStateHistory_explode", explode_outer(cticallstateParseDf.col("contactStateHistory")))

          //cticallstateParseDf.show(10, false)

          cticallstateParseDf = cticallstateParseDf.withColumn("contactId", cticallstateParseDf.col("contactStateHistory_explode.contactId"))
            .withColumn("stateIndex", cticallstateParseDf.col("contactStateHistory_explode.stateIndex"))
            .withColumn("contactStateName", cticallstateParseDf.col("contactStateHistory_explode.contactStateName"))
            .withColumn("startDate", cticallstateParseDf.col("contactStateHistory_explode.startDate"))
            .withColumn("agentId", cticallstateParseDf.col("contactStateHistory_explode.agentId"))
            .withColumn("skillId", cticallstateParseDf.col("contactStateHistory_explode.skillId"))
            .withColumn("duration", cticallstateParseDf.col("contactStateHistory_explode.duration"))
            .withColumn("etl_load_dt", current_timestamp())
            .withColumn("batchtime", unix_timestamp(lit(batchStartTime), "yyyy-MM-dd' 'HH:mm:ss").cast(TimestampType))
            .withColumn("part_date", lit(to_date(col("batchtime"), "yyyy-MM-dd")))


          cticallstateParseDf = cticallstateParseDf.withColumn("startDate_mod", unix_timestamp($"startDate", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast(TimestampType))

          val cticallstateTableDf = cticallstateParseDf.select("contactId", "stateIndex", "contactStateName", "startDate_mod", "agentId", "skillId", "duration", "etl_load_dt", "batchtime", "part_date")
          //cticallstateTableDf.show(10,false)
         // cticallstateTableDf.printSchema()

          cticallstateTableDf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.cticallstate.table"))
          ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark);
          }}
        } catch {

          case e: Exception =>
            {
              log.info("in the catch of updateStatus ****************** ")
              ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
              e.printStackTrace()
              throw new Exception("General Exception..please check the stacktrace")
            }
        }
      })
    }
  }
}

