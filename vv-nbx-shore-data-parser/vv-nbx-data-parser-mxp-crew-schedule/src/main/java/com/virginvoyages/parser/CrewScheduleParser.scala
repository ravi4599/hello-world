package com.virginvoyages.parser

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
import java.text.SimpleDateFormat
import org.apache.spark.SparkConf
import java.io._
import java.nio.charset.StandardCharsets
import org.apache.spark.SparkFiles
import scala.util.Try

import com.virginvoyages.metadataframework.ManageMetadata

object CrewScheduleParser {

	val log = LogManager.getRootLogger
			log.setLevel(Level.INFO)

			def main(args: Array[String]): Unit = {
					val spark = SparkSession
							.builder()
							.enableHiveSupport()
							.getOrCreate()

							import spark.implicits._

							val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
							val sc = spark.sparkContext
							val sqlContext = new org.apache.spark.sql.SQLContext(sc)

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

										val batchStartTme = metadata._3
										val batchEndTme = metadata._4
										val partReadStart = metadata._5 
										val partReadEnd = metadata._6
										val startExecutionTime = metadata._7
										val part_write_date = metadata._8

										batch_instance_id1 = metadata._2
										batch_id1 = metadata._1

										val whereClause = s""" where batchtime>= '$batchStartTme' and batchtime<='$batchEndTme' and part_date>='$partReadStart' and part_date<='$partReadEnd'"""
										println(s"""===========Starting the execution for $whereClause""")
										log.info(s"""===========Starting the execution for $whereClause""")

										import spark.implicits._


										var inputDf=spark.sql("select * from %s %s".format(sc.getConf.get("spark.source.table"),whereClause))
										val rdd=inputDf.select("message").rdd.map(x=>x.toString())
										var srcDf=spark.read.json(rdd).drop("BatchTime","Part_Date")

										srcDf.show(2,false)
										srcDf.printSchema()

										if (!srcDf.head(1).isEmpty) {
										  
										  if (hasColumn(srcDf, "ts")) {
												srcDf = srcDf.withColumn("ts", when(srcDf.col("ts").isNotNull, 
														srcDf.col("ts")).otherwise(lit(null)))
											}
											else {
												srcDf = srcDf.withColumn("ts",lit(null))
											}

											if (hasColumn(srcDf, "cn")) {
												srcDf = srcDf.withColumn("cn", when(srcDf.col("cn").isNotNull, 
														srcDf.col("cn")).otherwise(lit(null)))
											}
											else {
												srcDf = srcDf.withColumn("cn",lit(null))
											}

											if (hasColumn(srcDf, "n")) {
												srcDf = srcDf.withColumn("n", when(srcDf.col("n").isNotNull, 
														srcDf.col("n")).otherwise(lit(null)))
											}
											else {
												srcDf = srcDf.withColumn("n",lit(null))
											}

											if (hasColumn(srcDf, "ver")) {
												srcDf = srcDf.withColumn("ver", when(srcDf.col("ver").isNotNull, 
														srcDf.col("ver")).otherwise(lit(null)))
											}
											else {
												srcDf = srcDf.withColumn("ver",lit(null))
											}

											if (hasColumn(srcDf, "plt")) {
												srcDf = srcDf.withColumn("plt", when(srcDf.col("plt").isNotNull, 
														srcDf.col("plt")).otherwise(lit(null)))
											}
											else {
												srcDf = srcDf.withColumn("plt",lit(null))
											}

											if (hasColumn(srcDf, "mrc")) {
												srcDf = srcDf.withColumn("mrc", when(srcDf.col("mrc").isNotNull, 
														srcDf.col("mrc")).otherwise(lit(null)))
											}
											else {
												srcDf = srcDf.withColumn("mrc",lit(null))
											}

											if (hasColumn(srcDf, "rc")) {
												srcDf = srcDf.withColumn("rc", when(srcDf.col("rc").isNotNull, 
														srcDf.col("rc")).otherwise(lit(null)))
											}
											else {
												srcDf = srcDf.withColumn("rc",lit(null))
											}

											if (hasColumn(srcDf, "ri")) {
												srcDf = srcDf.withColumn("ri", when(srcDf.col("ri").isNotNull, 
														srcDf.col("ri")).otherwise(lit(null)))
											}
											else {
												srcDf = srcDf.withColumn("ri",lit(null))
											}


											if (hasColumn(srcDf, "pl")) {

												if (hasColumn(srcDf, "pl.activityType")) {
													srcDf = srcDf.withColumn("activityType", when(srcDf.col("pl.activityType").isNotNull, 
															srcDf.col("pl.activityType")).otherwise(lit(null)))
												}
												else {
													srcDf = srcDf.withColumn("activityType",lit(null))
												}



												if (hasColumn(srcDf, "pl.confirmed")) {
													srcDf = srcDf.withColumn("confirmed", when(srcDf.col("pl.confirmed").isNotNull, 
															srcDf.col("pl.confirmed")).otherwise(lit(null)))
												}
												else {
													srcDf = srcDf.withColumn("confirmed",lit(null))
												}


												if (hasColumn(srcDf, "pl.created")) {
													srcDf = srcDf.withColumn("created", when(srcDf.col("pl.created").isNotNull, 
															srcDf.col("pl.created").cast(TimestampType)).otherwise(lit(null)))
												}
												else {
													srcDf = srcDf.withColumn("created",lit(null))
												}


												if (hasColumn(srcDf, "pl.createdBy")) {
													srcDf = srcDf.withColumn("createdBy", when(srcDf.col("pl.createdBy").isNotNull, 
															srcDf.col("pl.createdBy")).otherwise(lit(null)))
												}
												else {
													srcDf = srcDf.withColumn("createdBy",lit(null))
												}


												if (hasColumn(srcDf, "pl.externalId")) {
													srcDf = srcDf.withColumn("externalId", when(srcDf.col("pl.externalId").isNotNull, 
															srcDf.col("pl.externalId")).otherwise(lit(null)))
												}
												else {
													srcDf = srcDf.withColumn("externalId",lit(null))
												}


												if (hasColumn(srcDf, "pl.id")) {
													srcDf = srcDf.withColumn("id", when(srcDf.col("pl.id").isNotNull, 
															srcDf.col("pl.id")).otherwise(lit(null)))
												}
												else {
													srcDf = srcDf.withColumn("id",lit(null))
												}

												if (hasColumn(srcDf, "pl.installation")) {
													srcDf = srcDf.withColumn("installation", when(srcDf.col("pl.installation").isNotNull, 
															srcDf.col("pl.installation")).otherwise(lit(null)))
												}
												else {
													srcDf = srcDf.withColumn("installation",lit(null))
												}

												if (hasColumn(srcDf, "pl.isApproved")) {
													srcDf = srcDf.withColumn("isApproved", when(srcDf.col("pl.isApproved").isNotNull, 
															srcDf.col("pl.isApproved")).otherwise(lit(null)))
												}
												else {
													srcDf = srcDf.withColumn("isApproved",lit(null))
												}

												if (hasColumn(srcDf, "pl.isDeleted")) {
													srcDf = srcDf.withColumn("isDeleted", when(srcDf.col("pl.isDeleted").isNotNull, 
															srcDf.col("pl.isDeleted")).otherwise(lit(null)))
												}
												else {
													srcDf = srcDf.withColumn("isDeleted",lit(null))
												}
												if (hasColumn(srcDf, "pl.manningAgent")) {
													srcDf = srcDf.withColumn("manningAgent", when(srcDf.col("pl.manningAgent").isNotNull, 
															srcDf.col("pl.manningAgent")).otherwise(lit(null)))
												}
												else {
													srcDf = srcDf.withColumn("manningAgent",lit(null))
												}

												if (hasColumn(srcDf, "pl.updated")) {
													srcDf = srcDf.withColumn("updated", when(srcDf.col("pl.updated").isNotNull, 
															srcDf.col("pl.updated").cast(TimestampType)).otherwise(lit(null)))
												}
												else {
													srcDf = srcDf.withColumn("updated",lit(null))
												}

												if (hasColumn(srcDf, "pl.updatedBy")) {
													srcDf = srcDf.withColumn("updatedBy", when(srcDf.col("pl.updatedBy").isNotNull, 
															srcDf.col("pl.updatedBy")).otherwise(lit(null)))
												}
												else {
													srcDf = srcDf.withColumn("updatedBy",lit(null))
												}


												if (hasColumn(srcDf, "pl.clockIn.clock")) {
													srcDf = srcDf.withColumn("clockIn_clock", when(srcDf.col("pl.clockIn.clock").isNotNull, 
															srcDf.col("pl.clockIn.clock")).otherwise(lit(null)))
												}
												else {
													srcDf = srcDf.withColumn("clockIn_clock",lit(null))
												}


												if (hasColumn(srcDf, "pl.clockIn.time")) {
													srcDf = srcDf.withColumn("clockIn_time", when(srcDf.col("pl.clockIn.time").isNotNull, 
															srcDf.col("pl.clockIn.time").cast(TimestampType)).otherwise(lit(null).cast(TimestampType)))
												}
												else {
													srcDf = srcDf.withColumn("clockIn_time",lit(null).cast(TimestampType))
												}

												if (hasColumn(srcDf, "pl.clockIn.manualEntry")) {
													srcDf = srcDf.withColumn("clockIn_manualEntry", when(srcDf.col("pl.clockIn.manualEntry").isNotNull, 
															srcDf.col("pl.clockIn.manualEntry")).otherwise(lit(null)))
												}
												else {
													srcDf = srcDf.withColumn("clockIn_manualEntry",lit(null))
												}

												if (hasColumn(srcDf, "pl.clockOut.clock")) {
													srcDf = srcDf.withColumn("clockOut_clock", when(srcDf.col("pl.clockOut.clock").isNotNull, 
															srcDf.col("pl.clockOut.clock")).otherwise(lit(null)))
												}
												else {
													srcDf = srcDf.withColumn("clockOut_clock",lit(null))
												}


												if (hasColumn(srcDf, "pl.clockOut.time")) {
													srcDf = srcDf.withColumn("clockOut_time", when(srcDf.col("pl.clockOut.time").isNotNull, 
															srcDf.col("pl.clockOut.time").cast(TimestampType)).otherwise(lit(null).cast(TimestampType)))
												}
												else {
													srcDf = srcDf.withColumn("clockOut_time",lit(null).cast(TimestampType))
												}

												if (hasColumn(srcDf, "pl.clockOut.manualEntry")) {
													srcDf = srcDf.withColumn("clockOut_manualEntry", when(srcDf.col("pl.clockOut.manualEntry").isNotNull, 
															srcDf.col("pl.clockOut.manualEntry")).otherwise(lit(null)))
												}
												else {
													srcDf = srcDf.withColumn("clockOut_manualEntry",lit(null))
												}


												if (hasColumn(srcDf, "pl.person.internalCode")) {
													srcDf = srcDf.withColumn("person_internalCode", when(srcDf.col("pl.person.internalCode").isNotNull, 
															srcDf.col("pl.person.internalCode")).otherwise(lit(null)))
												}
												else {
													srcDf = srcDf.withColumn("person_internalCode",lit(null))
												}

												if (hasColumn(srcDf, "pl.person.globalCode")) {
													srcDf = srcDf.withColumn("person_globalCode", when(srcDf.col("pl.person.globalCode").isNotNull, 
															srcDf.col("pl.person.globalCode")).otherwise(lit(null)))
												}
												else {
													srcDf = srcDf.withColumn("person_globalCode",lit(null))
												}


												if (hasColumn(srcDf, "pl.person.GUID")) {
													srcDf = srcDf.withColumn("person_GUID", when(srcDf.col("pl.person.GUID").isNotNull, 
															srcDf.col("pl.person.GUID")).otherwise(lit(null)))
												}
												else {
													srcDf = srcDf.withColumn("person_GUID",lit(null))
												}


												if (hasColumn(srcDf, "pl.person.firstName")) {
													srcDf = srcDf.withColumn("person_firstName", when(srcDf.col("pl.person.firstName").isNotNull, 
															srcDf.col("pl.person.firstName")).otherwise(lit(null)))
												}
												else {
													srcDf = srcDf.withColumn("person_firstName",lit(null))
												}


												if (hasColumn(srcDf, "pl.person.lastName")) {
													srcDf = srcDf.withColumn("person_lastName", when(srcDf.col("pl.person.lastName").isNotNull, 
															srcDf.col("pl.person.lastName")).otherwise(lit(null)))
												}
												else {
													srcDf = srcDf.withColumn("person_lastName",lit(null))
												}

												if (hasColumn(srcDf, "pl.person.company")) {
													srcDf = srcDf.withColumn("person_company", when(srcDf.col("pl.person.company").isNotNull, 
															srcDf.col("pl.person.company")).otherwise(lit(null)))
												}
												else {
													srcDf = srcDf.withColumn("person_company",lit(null))
												}


												srcDf.show(2,false)
												srcDf.printSchema()
												
												val FinalData = srcDf.withColumn("BatchTime",lit(startExecutionTime).cast(TimestampType))
												.withColumn("Part_Date", to_date(lit(part_write_date))).withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
												.withColumn("ShipCode", lit(spark.sparkContext.getConf.get("spark.ship.code")))
												.select("id","externalId","person_internalCode","person_globalCode","person_GUID","person_firstName","person_lastName","person_company","manningAgent","installation","activityType","clockIn_time","clockIn_clock","clockIn_manualEntry","clockOut_time","clockOut_clock","clockOut_manualEntry","confirmed","created","createdBy","updated","updatedBy","isDeleted","isApproved","ts","cn","n","ver","plt","mrc","rc","ri","VoyageId","BatchTime","ShipCode","Part_Date")
												

												
												FinalData.show(2,false)
												FinalData.printSchema()

												FinalData.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.table"))


											} //end of pl


										}



								ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
								spark.stop()

							} 
							catch {

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

