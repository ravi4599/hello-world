package com.virginvoyages.parser

import org.apache.spark.SparkConf
import java.sql.SQLException
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.Row
import com.databricks.spark.xml.XmlReader
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
import org.apache.spark.SparkConf
import java.io._
import java.nio.charset.StandardCharsets
import org.apache.spark.SparkFiles
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window

import com.virginvoyages.metadataframework.ManageMetadata

object SeawareWearablesParser {

	val log = LogManager.getRootLogger
			log.setLevel(Level.INFO)

			def main(args: Array[String]): Unit = {
					val spark = SparkSession
							.builder()
							.enableHiveSupport()
							.getOrCreate()

							import spark.implicits._
							val sc = spark.sparkContext
							val sqlContext = new org.apache.spark.sql.SQLContext(sc)
							import spark.implicits._
							val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
							sqlContext.setConf("hive.exec.dynamic.partition", "true")
							sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
							var batch_instance_id1: String = null
							var batch_id1: String = null
							try {
								val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
										metadata.productIterator.foreach(println)

										val batch_start_time = metadata._3
										val batch_end_time = metadata._4
										val part_start_time = metadata._5.toString()
										val part_end_time = metadata._6.toString()
										val start_execution_time = metadata._7.toString()
										val part_write_date = metadata._8.toString()
										batch_instance_id1 = metadata._2
										batch_id1 = metadata._1

										import spark.sqlContext.implicits._


										val srcDf = spark.sql(spark.sparkContext.getConf.get("spark.source.table"))
																				.where($"BatchTime" >= lit(batch_start_time).cast(TimestampType) && $"BatchTime" <= lit(batch_end_time).cast(TimestampType) and ($"part_date".between(part_start_time, part_end_time)))


										if (!srcDf.head(1).isEmpty) {


											val itemsg = srcDf.select("Message").rdd.map(x => x.toString)
													val attJson = spark.read.json(itemsg)


													var maxID = 0
													maxID = spark.sql("select max(rowid) from  shipdw.hvtb_parse_seaware_wearables").na.fill(0).first().getInt(0)
													val maxID1 = maxID.toInt
													println("Max ID = "+maxID1)

													var tempDf = attJson.select("subscription.message", "subscription.names").withColumn("rowId", monotonically_increasing_id())


													import org.apache.spark.sql.types._

													val rdd1 = tempDf.select("message").map(r => r.getString(0)).rdd
													var xmlReaderDF = new XmlReader().xmlRdd(sqlContext, rdd1)

													xmlReaderDF.show(2,false)
													xmlReaderDF.printSchema
													xmlReaderDF = xmlReaderDF.withColumn("rowId", monotonically_increasing_id())
													var w = Window.orderBy("rowId")


													// Use row number with the window specification and Drop the created increasing data column
													tempDf = tempDf.withColumn("index", row_number().over(w) + maxID).drop("rowId")
													xmlReaderDF = xmlReaderDF.withColumn("index", row_number().over(w) + maxID).drop("rowId")


													println("after joining 2 df")
													val df = tempDf.as("df1").join(xmlReaderDF.as("df2"), tempDf("index") === xmlReaderDF("index"), "inner")
													.select("df1.index", "df1.names", "df2.Parameters")



													val xmldf = df.select("index", "names", "Parameters.Param")

													val explodedDf = xmldf.withColumn("Param", explode($"Param")).withColumn("names", explode($"names")) //.withColumn("BatchTime",lit(start_execution_time).cast(TimestampType))


													val expandedDf = explodedDf.select("index", "names", "Param.Name", "Param.Type", "Param.Value")


													val finalDF = expandedDf.withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
													.withColumn("BatchTime", lit(start_execution_time).cast(TimestampType))
													.withColumn("ShipCode", lit(spark.sparkContext.getConf.get("spark.ship.code")))
													.withColumn("Part_date", to_date(lit(part_write_date)))


													finalDF.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.table"))


													val transpose_df=finalDF.select("index", "names", "Name", "Type", "Value").groupBy(col("index"),col("names")).pivot("Name").agg(first(col("Value"),ignoreNulls = true)).select("index","names","CLIENT_ID","CLIENT_NAME","RES_ID")

													val transpose_df_final = transpose_df.withColumn("sw_generated_by_flg", lit(true)).withColumn("sw_wearable_type", lit(true))
													.withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
													.withColumn("BatchTime", lit(start_execution_time).cast(TimestampType))
													.withColumn("ShipCode", lit(spark.sparkContext.getConf.get("spark.ship.code")))
													.withColumn("Part_date", to_date(lit(part_write_date)))

										

													transpose_df_final.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.transpose.table"))


										}
																ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)

							} catch {

							case e: Exception =>
							{
																  ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark)
								log.info("in the catch of updateStatus ******************")
								e.printStackTrace()
								throw new Exception("General Exception..please check the stacktrace")
							}

							}
	}

}