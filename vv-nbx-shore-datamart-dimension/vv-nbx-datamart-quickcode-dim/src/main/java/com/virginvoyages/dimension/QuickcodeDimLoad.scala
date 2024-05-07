package com.virginvoyages.dimension
import org.apache.spark.sql.Column
import org.apache.spark.SparkConf
import java.sql.SQLException
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{ avg, explode, concat, lit, trim }
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.udf
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
import java.text.SimpleDateFormat
import org.apache.spark.SparkConf
import java.io._
import java.nio.charset.StandardCharsets
import org.apache.spark.SparkFiles
import scala.util.Try
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact 
import com.virginvoyages.metadataframework.ManageMetadata

object QuickcodeDimLoad {

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
					sqlContext.setConf("hive.exec.dynamic.partition", "true")
					sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
					val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
					val batchId1 = metadata._1
					val batchInstanceId1 = metadata._2
					val batchStartTme = metadata._3
					val batchEndTme = metadata._4
					val partReadStart = metadata._5 
					val partReadEnd = metadata._6
					val startExecutionTime = metadata._7
					val part_write_date = metadata._8

					try {

						val whereClause = s""" where BatchTime>= '$batchStartTme' and BatchTime<='$batchEndTme' and part_date>='$partReadStart' and part_date<='$partReadEnd'"""
								import spark.implicits._
								// Reading person detail from hive table 
								var inpudf=spark.sql("select * from %s %s".format(sc.getConf.get("spark.quickcodeparsed.table"),whereClause))
								inpudf=inpudf.withColumn("voyage_id",lit(sc.getConf.get("spark.voyage.id")))
								//inpudf.show()

								if(!inpudf.take(1).isEmpty){
									loadDimFact(spark,inpudf)
								}

						ManageMetadata.updateStatus(batchInstanceId1, batchId1, "Successful", spark)
						spark.stop()

					} 
			catch { 

			case e: Exception =>
			{
				ManageMetadata.updateStatus(batchInstanceId1, batchId1, "Failed", spark); 
				log.info("in the catch of updateStatus ******************")
				e.printStackTrace()
				throw new Exception("General Exception..please check the stacktrace")
			}

			}

	}


}