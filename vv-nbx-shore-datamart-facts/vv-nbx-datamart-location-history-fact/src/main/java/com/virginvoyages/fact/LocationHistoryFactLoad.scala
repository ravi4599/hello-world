package com.virginvoyages.fact

import com.virginvoyages.metadataframework.ManageMetadata
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

object LocationHistoryFactLoad {
  val log: Logger = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]) {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    val sparkConfiguration = sc.broadcast(sc.getConf.getAll.toMap)

    // Calling Metadata framework to get the batchtime and partdate which is
    // used to get incremental data from data source
    val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
    val batch_id1 = metadata._1
    val batch_instance_id1 = metadata._2
    val batchStartTme = metadata._3
    val batchEndTme = metadata._4
    val partReadStart = metadata._5
    val partReadEnd = metadata._6

    try {
      // val whereClause = s""" where 1=1 """
      val whereClause = s""" where batchtime>= '$batchStartTme' and batchtime<='$batchEndTme' and part_date>='$partReadStart' and part_date<='$partReadEnd'"""
      println(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")

      val query = spark.sparkContext.getConf.get("spark.source.sql").trim() + whereClause
      print(query)
      val sourceDF = spark.sql(query)
      //sourceDF.show(10, false)

      if (!sourceDF.head(1).isEmpty) {
        loadDimFact(spark, sourceDF)
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
