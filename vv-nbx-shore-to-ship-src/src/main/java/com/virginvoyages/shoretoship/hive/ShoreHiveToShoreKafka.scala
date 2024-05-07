package com.virginvoyages.shoretoship.hive

import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.kafka.clients.producer.ProducerRecord

import com.virginvoyages.metadataframework.ManageMetadata

object ShoreHiveToShoreKafka {

	// Creating logger
	val log = LogManager.getRootLogger
			log.setLevel(Level.INFO)

			def main(agrs: Array[String]) {
		// Creating spark Session
		val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
				val sc = spark.sparkContext
				val sparkConfig = sc.broadcast(sc.getConf.getAll.toMap)
				//Calling Metadataframework to get the batchtime and partdate which is used to  get incremental data from source database(Hive table)

				val metadata = ManageMetadata.fetchBatchTime(sparkConfig, spark)
				val batchId1 = metadata._1
				val batchInstanceId1 = metadata._2
				val batchStartTme = metadata._3
				val batchEndTme = metadata._4
				val partReadStart = metadata._5
				val partReadEnd = metadata._6
				val startExecutionTime = metadata._7
				val part_write_date = metadata._8
				
				println(s"batch start time $batchStartTme ")
				println(s"batch start time $batchEndTme ")
		

				try {
					import java.util.TimeZone

					import java.util.Properties

					// TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

					import spark.implicits._
					// Defining whereClause condition  which is used to pass inside spark sql query to get incremental data from Hive table
					//	val whereClause = s""" where BatchTime>= '$batchStartTme' and BatchTime <= '$batchEndTme' and part_date>='$partReadStart' and part_date <= '$partReadEnd'"""

					val whereClause = s""" where BatchTime>= '$batchStartTme' and BatchTime <= '$batchEndTme' """



					val query = "select * " + "from " + sc.getConf.get("spark.src.table") + " " + whereClause

					println(query)

					var inputDf = spark.sql(query)
					

					if (!inputDf.head(1).isEmpty) {

						val tzcols = inputDf.schema.toList.filter(x => x.dataType == TimestampType).map(c => col(c.name))
								// val tzCols = s"""orderdate,orderdeliverydate,modifieddate""".split(",")

								for (tzcol <- tzcols) {
									// println("tzcol:::::::::::::::;" + tzcol)
									var cols = tzcol.toString()
											inputDf = inputDf.withColumn(cols, inputDf.col(cols).cast(StringType))
								}

						// inputDf.printSchema
						// inputDf.show(false)

						spark.conf.set("spark.sql.session.timeZone", "UTC")
						TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
						for (tzcol <- tzcols) {
							var cols = tzcol.toString()
									println("tzcol:::::::::::::::;" + cols)
									inputDf = inputDf.withColumn(cols, inputDf.col(cols).cast(TimestampType))
						}
						
						val jsonData = inputDf.toJSON



						jsonData.coalesce(sc.getConf.get("spark.target.coalesce.value").toInt).rdd.foreachPartition(

								iteration => {
									val producer = ShipToShoreKafkaProducer.getKafkaProducer(sparkConfig)
											iteration.foreach(row => {
												log.info("*****row => " + row)
												val data = new ProducerRecord[String, String](sparkConfig.value.get("spark.target.kafka.topic").get.trim(), row)
												producer.send(data)
											})
											producer.close()
								})

					}
					
					
					ManageMetadata.updateStatus(batchInstanceId1, batchId1, "Successful", spark)
					spark.stop()
				} 
		catch {

		case e: Exception =>
		ManageMetadata.updateStatus(batchInstanceId1, batchId1, "Failed", spark);
		log.info("Exception................................................................" + e.getMessage)

		log.info("Exception stack trace  " + e.printStackTrace());
		spark.stop()

		}

	}
}
