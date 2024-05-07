package com.virginvoyages.dimension

import com.virginvoyages.metadataframework.ManageMetadata
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, TimestampType}


object FolioItemDimLoad {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)


    val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
    metadata.productIterator.foreach(println)
    val batch_id1 = metadata._1
    val batch_instance_id1 = metadata._2
    val batch_start_tme = metadata._3
    val batch_end_tme = metadata._4
    val part_read_start = metadata._5
    val part_read_end = metadata._6

    try {
      println("#---------------------------Starting the Execution------------------#")
      import spark.sqlContext.implicits._
      val folioItemDf = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").trim()).where($"batchtime" >= lit(batch_start_tme).cast(TimestampType) && $"batchtime" <= lit(batch_end_tme).cast(TimestampType) && $"part_date" >= lit(part_read_start).cast(DateType) && $"part_date" <= lit(part_read_end).cast(DateType))

      if (!folioItemDf.head(1).isEmpty) {
        loadDimFact(spark: SparkSession, folioItemDf)
      }
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
    } catch {
      case e: Exception => {
        ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
        e.printStackTrace();
        throw e;
      }
    }
  }
}