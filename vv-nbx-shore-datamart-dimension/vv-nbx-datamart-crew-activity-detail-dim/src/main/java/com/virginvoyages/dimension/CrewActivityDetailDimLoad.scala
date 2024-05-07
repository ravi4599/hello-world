package com.virginvoyages.dimension


import com.virginvoyages.metadataframework.ManageMetadata
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object CrewActivityDetailDimLoad {

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

	def getSparkSession() = {
		val spark = SparkSession
			.builder()
			.enableHiveSupport()
			.getOrCreate()

		spark
	}

  def main(args: Array[String]) {
    val spark = getSparkSession()

    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)

    /************************************* Metadata framework information *********************************************/
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

    try {
      val whereClause = s""" where batchtime>= '$batch_start_tme' and batchtime<='$batch_end_tme' and part_date>='$part_read_start' and part_date<='$part_read_end'"""
      println(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")

      println("Performing source SQL query")
      val sourceDF = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").trim() + whereClause)

      // Identify missing columns and fill with null values
      val required_columns = spark.sparkContext.getConf.get("spark.source.columns").split(",")
      val missing_columns = ArrayBuffer[String]()
      val avaliable_columns = ArrayBuffer[String]()
      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
      for (col <- required_columns) {
        if (hasColumn(sourceDF, col)) {
          println(col, "column exists", avaliable_columns.toString)
          println("column exists", avaliable_columns.length)
          log.info(col, "column exists", avaliable_columns.toString, avaliable_columns.length)
          println(avaliable_columns.length, "lenthg")
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
      val stage_final_df = missing_columns.foldLeft(sourceDF)((df, c) =>
        df.withColumn(s"$c", lit(null))
      )

      if (!stage_final_df.take(1).isEmpty) {
        loadDimFact(spark: SparkSession, stage_final_df)
      }
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
    } catch {
      case e: Exception => {
        ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
        e.printStackTrace()
        throw e
      }
    }
  }
}
