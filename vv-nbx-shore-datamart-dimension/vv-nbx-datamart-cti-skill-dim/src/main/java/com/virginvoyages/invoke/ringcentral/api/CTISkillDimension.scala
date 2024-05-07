package com.virginvoyages.invoke.ringcentral.api

import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import org.apache.spark.sql.types.{ StructType, StructField, TimestampType, StringType, IntegerType, DateType, LongType };
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import com.virginvoyages.metadataframework.ManageMetadata
import java.sql.SQLException
import org.apache.spark.sql.functions._
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact

object CTISkillDimension {
  def main(args: Array[String]): Unit = {
  
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    
    import spark.implicits._   
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    
/**********************************MetaData framework**********************************************/
      val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
      metadata.productIterator.foreach(println)
      val batch_id1 = metadata._1
      val batch_instance_id1 = metadata._2
      val batchStartTime = metadata._3
      val batchEndTime = metadata._4
 
    try {
      val whereClause = s"""where batchtime >= from_unixtime(unix_timestamp('$batchStartTime', "yyyy-MM-dd' 'HH:mm:ss")) and batchtime <= from_unixtime(unix_timestamp('$batchEndTime', "yyyy-MM-dd' 'HH:mm:ss")) and part_date>=to_date('$batchStartTime') and part_date<=to_date('$batchEndTime')"""
      log.info("select * from %s %s".format(sc.getConf.get("spark.target.ctiskill.table"), whereClause))

      var cticallskilldatadf = spark.sql("select * from %s %s".format(sc.getConf.get("spark.target.ctiskill.table"), whereClause))

      val cticampaigndimdf = spark.sql(spark.sparkContext.getConf.get("spark.campaigndim.sql").trim())
         
      cticallskilldatadf = cticallskilldatadf.join(cticampaigndimdf, cticallskilldatadf("campaignId") === cticampaigndimdf("cti_campaign_dim_campaignId"), "left")
      
      var cticallskilltabledf = cticallskilldatadf.select("skillId","skillName","mediaTypeId","mediaTypeName", "cti_campaign_dim_cti_campaign_skey", "isActive","requireDisposition","isOutbound","scriptId","scriptName","initialPriority","maxPriority","serviceLevelThreshold","serviceLevelGoal")
      cticallskilltabledf = cticallskilltabledf.withColumnRenamed("cti_campaign_dim_cti_campaign_skey", "campaignId")
                                               .withColumn("mediaTypeId", cticallskilltabledf.col("mediaTypeId").cast(IntegerType))
      
 //     cticallskilltabledf.show(10,false)
 //     cticallskilltabledf.printSchema()
      
      cticallskilltabledf = cticallskilltabledf.withColumn("campaignId", when(cticallskilltabledf.col("campaignId").isNull, lit(-1)).otherwise(cticallskilltabledf.col("campaignId")))
      
      val required_columns = spark.sparkContext.getConf.get("spark.source.columns").split(",")

      val missing_columns = ArrayBuffer[String]()
      val avaliable_columns = ArrayBuffer[String]()
      
      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
      
      for (col <- required_columns) {

        if (hasColumn(cticallskilltabledf, col)) {
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
      val stage_final_df = missing_columns.foldLeft(cticallskilltabledf)((df, c) =>
      df.withColumn(s"$c", lit(null)))
 
      log.info("calling scd framework")
      
//      var pond_columns:Array[String] = spark.sparkContext.getConf.get("spark.pond.allColumns").split(",")
//      for ( x <- pond_columns ) {
//         log.info("pond columns: "+ x)
//      }
      
      loadDimFact(spark: SparkSession, stage_final_df)      
     ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark);   
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
}