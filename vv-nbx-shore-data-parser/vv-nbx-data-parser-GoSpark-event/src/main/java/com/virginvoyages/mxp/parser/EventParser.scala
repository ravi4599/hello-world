package com.virginvoyages.mxp.parser

import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Column, DataFrame }
import scala.collection.mutable.ArrayBuffer

import java.text.SimpleDateFormat

//XML validator imports
import org.apache.spark.SparkConf
import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import org.xml.sax.SAXException;
import java.io._
import java.nio.charset.StandardCharsets
import org.apache.spark.SparkFiles
import scala.util.Try
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.functions._

import com.virginvoyages.metadataframework.ManageMetadata
object EventParser {
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

					var batch_instance_id1: String = null
					var batch_id1: String = null
					try {

						def checkArray(df: DataFrame, colname: String): Boolean = {

								df.schema(colname).dataType match {
								case ArrayType(_, _) => return true
								case _               => return false
								}
						}

						def checkStructType(df: DataFrame, colname: String): Boolean = {
								df.schema(colname).dataType match {
								case StructType(_) => return true
								case _             => return false
								}
						}

						def checkStringType(df: DataFrame, colname: String): Boolean = {
								df.schema(colname).dataType match {
								case StringType => return true
								case _          => return false
								}
						}

						def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

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

								var eventdf = spark.sql(spark.sparkContext.getConf.get("spark.source.table"))

								var data = eventdf.select("Message", "BatchTime", "Part_Date").where($"BatchTime" >= lit(batch_start_time).cast(TimestampType) && $"BatchTime" <= lit(batch_end_time).cast(TimestampType) and ($"part_date".between(part_start_time, part_end_time)))
								if (!data.head(1).isEmpty) {
									var eventMsg = data.select("Message").rdd.map { x => x.toString }

									var eventData = spark.read.json(eventMsg).withColumn("BatchTime", lit(start_execution_time)).withColumn("VoyageId", lit(spark.sparkContext.getConf.get("spark.voyage.id"))).withColumn("partDate", lit(part_write_date))
											import spark.implicits._
											import org.apache.spark.sql.functions.col
											eventData.printSchema()
											eventData.show(1000, false)

											eventData = eventData
											.withColumn("CI", when(eventData.col("ci").isNotNull, eventData.col("ci")).otherwise(lit(null)))
											.withColumn("CN ", when(eventData.col("cn").isNotNull, eventData.col("cn")).otherwise(lit(null)))
											.withColumn("N", when(eventData.col("n").isNotNull, eventData.col("n")).otherwise(lit(null)))
											// .withColumn("PL", when(eventData.col("pl").isNotNull, eventData.col("pl")).otherwise(lit(null)))
											.withColumn("PLT", when(eventData.col("plt").isNotNull, eventData.col("plt")).otherwise(lit(null)))
											.withColumn("TG", when(eventData.col("tg").isNotNull, eventData.col("tg")).otherwise(lit(null)))
											.withColumn("TS", when(eventData.col("ts").isNotNull, eventData.col("ts")).otherwise(lit(null)))
											if (hasColumn(eventData, "pl")) {
												if (checkArray(eventData, "pl")) {
													eventData = eventData.withColumn("pl", explode_outer(eventData.col("pl")))
															print("pl")
															//  eventData.show
												}

												if (checkStructType(eventData, "pl")) {

													if (hasColumn(eventData, "pl.action")) {
														eventData = eventData.withColumn("PL_Action", when(eventData.col("pl.action").isNotNull, 
																eventData.col("pl.action")).otherwise(lit(null)))
													}
													else {
														eventData = eventData.withColumn("PL_Action",lit(null))
													}
													if (hasColumn(eventData, "pl.date")) {
														eventData = eventData.withColumn("PL_Date", when(eventData.col("pl.date").isNotNull, 
																eventData.col("pl.date")).otherwise(lit(null)))
													}
													else {
														eventData = eventData.withColumn("PL_Date",lit(null))
													}
													if (hasColumn(eventData, "pl.displaytype")) {
														eventData = eventData.withColumn("PL_Displaytype", when(eventData.col("pl.displaytype").isNotNull, 
																eventData.col("pl.displaytype")).otherwise(lit(null)))
													}
													else {
														eventData = eventData.withColumn("PL_Displaytype",lit(null))
													}
													if (hasColumn(eventData, "pl.duration")) {
														eventData = eventData.withColumn("PL_Duration", when(eventData.col("pl.duration").isNotNull, 
																eventData.col("pl.duration")).otherwise(lit(null)))
													}
													else {
														eventData = eventData.withColumn("PL_Duration",lit(null))
													}
													if (hasColumn(eventData, "pl.hidden")) {
														eventData = eventData.withColumn("PL_Hidden", when(eventData.col("pl.hidden").isNotNull, 
																eventData.col("pl.hidden")).otherwise(lit(null)))
													}
													else {
														eventData = eventData.withColumn("PL_Hidden",lit(null))
													}
													if (hasColumn(eventData, "pl.id")) {
														eventData = eventData.withColumn("PL_ID", when(eventData.col("pl.id").isNotNull, 
																eventData.col("pl.id")).otherwise(lit(null)))
													}
													else {
														eventData = eventData.withColumn("PL_ID",lit(null))
													}
													if (hasColumn(eventData, "pl.inventoried")) {
														eventData = eventData.withColumn("PL_InventoryID", when(eventData.col("pl.inventoried").isNotNull, 
																eventData.col("pl.inventoried")).otherwise(lit(null)))
													}
													else {
														eventData = eventData.withColumn("PL_InventoryID",lit(null))
													}
													if (hasColumn(eventData, "pl.inventory")) {
														eventData = eventData.withColumn("PL_Inventory", when(eventData.col("pl.inventory").isNotNull, 
																eventData.col("pl.inventory")).otherwise(lit(null)))
													}
													else {
														eventData = eventData.withColumn("PL_Inventory",lit(null))
													}
													if (hasColumn(eventData, "pl.location")) {
														eventData = eventData.withColumn("PL_Location", when(eventData.col("pl.location").isNotNull, 
																eventData.col("pl.location")).otherwise(lit(null)))
													}
													else {
														eventData = eventData.withColumn("PL_Location",lit(null))
													}
													if (hasColumn(eventData, "pl.name")) {
														eventData = eventData.withColumn("PL_Name", when(eventData.col("pl.name").isNotNull, 
																eventData.col("pl.name")).otherwise(lit(null)))
													}
													else {
														eventData = eventData.withColumn("PL_Name",lit(null))
													}
													if (hasColumn(eventData, "pl.sailing")) {
														eventData = eventData.withColumn("PL_Sailing", when(eventData.col("pl.sailing").isNotNull, 
																eventData.col("pl.sailing")).otherwise(lit(null)))
													}
													else {
														eventData = eventData.withColumn("PL_Sailing",lit(null))
													}
													if (hasColumn(eventData, "pl.template")) {
														eventData = eventData.withColumn("PL_Template", when(eventData.col("pl.template").isNotNull, 
																eventData.col("pl.template")).otherwise(lit(null)))
													}
													else {
														eventData = eventData.withColumn("PL_Template",lit(null))
													}
													if (hasColumn(eventData, "pl.venue")) {
														eventData = eventData.withColumn("PL_Venue", when(eventData.col("pl.venue").isNotNull, 
																eventData.col("pl.venue")).otherwise(lit(null)))
													}
													else {
														eventData = eventData.withColumn("PL_Venue",lit(null))
													}
													if (hasColumn(eventData, "pl.vip")) {
														eventData = eventData.withColumn("PL_Vip", when(eventData.col("pl.vip").isNotNull, 
																eventData.col("pl.vip")).otherwise(lit(null)))
													}
													else {
														eventData = eventData.withColumn("PL_Vip",lit(null))
													}

													if (hasColumn(eventData, "pl.startTime.hour")) {
														eventData = eventData.withColumn("PL_StartTime_Hour", when(eventData.col("pl.startTime.hour").isNotNull, 
																eventData.col("pl.startTime.hour")).otherwise(lit(null)))
													}
													else {
														eventData = eventData.withColumn("PL_StartTime_Hour",lit(null))
													}
													if (hasColumn(eventData, "pl.startTime.minute")) {
														eventData = eventData.withColumn("PL_StartTime_Minute", when(eventData.col("pl.startTime.minute").isNotNull, 
																eventData.col("pl.startTime.minute")).otherwise(lit(null)))
													}
													else {
														eventData = eventData.withColumn("PL_StartTime_Minute",lit(null))
													}



													eventData=eventData.withColumn("BatchTime", lit(start_execution_time).cast(TimestampType))
															.withColumn("Part_date", to_date(lit(part_write_date)))
															.withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id"))).withColumn("ShipCode", lit(spark.sparkContext.getConf.get("spark.ship.code")))

															//										      eventData = eventData
															//										        .withColumn("CI", when(eventData.col("ci").isNotNull, eventData.col("ci")).otherwise(lit(null)))
															//										        .withColumn("CN ", when(eventData.col("cn").isNotNull, eventData.col("cn")).otherwise(lit(null)))
															//										        .withColumn("N", when(eventData.col("n").isNotNull, eventData.col("n")).otherwise(lit(null)))
															//										        // .withColumn("PL", when(eventData.col("pl").isNotNull, eventData.col("pl")).otherwise(lit(null)))
															//										        .withColumn("PLT", when(eventData.col("plt").isNotNull, eventData.col("plt")).otherwise(lit(null)))
															//										        .withColumn("TG", when(eventData.col("tg").isNotNull, eventData.col("tg")).otherwise(lit(null)))
															//										        .withColumn("TS", when(eventData.col("ts").isNotNull, eventData.col("ts")).otherwise(lit(null)))
															//										        .withColumn("PL_Action", when(eventData.col("pl.action").isNotNull, eventData.col("pl.action")).otherwise(lit(null)))
															//										        .withColumn("PL_Date", when(eventData.col("pl.date").isNotNull, eventData.col("pl.date")).otherwise(lit(null)))
															//										        .withColumn("PL_Duration", when(eventData.col("pl.duration").isNotNull, eventData.col("pl.duration")).otherwise(lit(null)))
															//										        .withColumn("PL_Hidden", when(eventData.col("pl.hidden").isNotNull, eventData.col("pl.hidden")).otherwise(lit(null)))
															//										        .withColumn("PL_ID", when(eventData.col("pl.id").isNotNull, eventData.col("pl.id")).otherwise(lit(null)))
															//										        .withColumn("PL_InventoryID", when(eventData.col("pl.inventoried").isNotNull, eventData.col("pl.inventoried")).otherwise(lit(null)))
															//										        .withColumn("PL_Inventory", when(eventData.col("pl.inventory").isNotNull, eventData.col("pl.inventory")).otherwise(lit(null)))
															//										        .withColumn("PL_Sailing", when(eventData.col("pl.sailing").isNotNull, eventData.col("pl.sailing")).otherwise(lit(null)))
															//										        .withColumn("PL_Template", when(eventData.col("pl.template").isNotNull, eventData.col("pl.template")).otherwise(lit(null)))
															//										        .withColumn("PL_Venue", when(eventData.col("pl.venue").isNotNull, eventData.col("pl.venue")).otherwise(lit(null)))
															//										        .withColumn("PL_StartTime_Hour", when(eventData.col("pl.startTime.hour").isNotNull, eventData.col("pl.startTime.hour")).otherwise(lit(null)))
															//										        .withColumn("PL_StartTime_Minute", when(eventData.col("pl.startTime.minute").isNotNull, eventData.col("pl.startTime.minute")).otherwise(lit(null)))
															//										        .withColumn("BatchTime", lit(start_execution_time).cast(TimestampType))
															//										        .withColumn("Part_date", to_date(lit(part_write_date)))
															//										        .withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))


															eventData = eventData.select("CI", "CN", "N", "PLT", "TG", "TS", "PL_Action", "PL_Date", "PL_Displaytype", "PL_Duration", "PL_Hidden", "PL_ID", "PL_InventoryID", "PL_Inventory", "PL_Location", "PL_Name", "PL_Sailing", "PL_Template", "PL_Venue", "PL_Vip", "PL_StartTime_Hour", "PL_StartTime_Minute", "BatchTime", "VoyageID", "ShipCode", "Part_date")

															//										eventData = eventData.select("CI", "CN", "N", "PLT", "TG", "TS", "PL_Action", "PL_Date", "PL_Duration", "PL_Hidden", "PL_ID", "PL_InventoryID", "PL_Inventory", "PL_Sailing", "PL_Template", "PL_Venue", "PL_StartTime_Hour", "PL_StartTime_Minute", "BatchTime","VoyageID" ,"Part_date")
															eventData.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.table"))
															//eventData.show
												}
											}
									//eventData.show
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