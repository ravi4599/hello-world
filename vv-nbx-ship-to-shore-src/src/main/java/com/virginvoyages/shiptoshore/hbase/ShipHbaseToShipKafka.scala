package com.virginvoyages.shiptoshore.hbase
import com.virginvoyages.shiptoshore.hive.ShoreToShipKafkaProducer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.LogManager  
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.plans.logical.With
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.broadcast.Broadcast

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.Instant
import org.apache.spark.sql.types.StringType
import org.apache.kafka.clients.producer.ProducerRecord


object ShipHbaseToShipKafka {
	/**
	 * Initilize thte logger
	 */
	val log = LogManager.getRootLogger
			log.setLevel(Level.INFO)

			def main(args: Array[String]) {
		//Creating spark session and spark context 
		val spark = SparkSession.builder().getOrCreate()
				val sc=spark.sparkContext

				val sparkConfig = sc.broadcast(sc.getConf.getAll.toMap)
				val srcConfigId = sc.getConf.get("spark.src.srcConfig")
				val tgtConfigId = sc.getConf.get("spark.src.tgtConfig")
				//val voyageId = sc.getConf.get("spark.src.voyageid")
				var voyageId = ""//sparkSession.sqlContext.sparkContext.getConf.get("spark.src.voyageid")
				val configMode =  spark.sqlContext.sparkContext.getConf.get("spark.config.ismannual")
				if(configMode.toLowerCase().equals("y"))
				{
					voyageId = spark.sqlContext.sparkContext.getConf.get("spark.src.voyageid")
					println(voyageId)
            log.info(voyageId)
				}
				else
				{
					var  voyageDf=spark.sql("select voyageid,startdate,enddate from %s group by voyageid,startdate,enddate".format(spark.sparkContext.getConf.get("spark.voyagedetail.table").trim))
							voyageDf=voyageDf.withColumn("startdate",voyageDf.col("startdate").cast("timestamp"))
							.withColumn("enddate",voyageDf.col("enddate").cast("timestamp"))
							val timeUdf = udf{(time: java.sql.Timestamp) => new java.sql.Timestamp(time.getTime + 5*60*60*1000)}
					voyageDf=voyageDf.withColumn("derived_enddate",timeUdf(voyageDf("enddate"))).withColumn("current_time",current_timestamp())//lit("2021-05-10 00:00:00").cast("timestamp"))
//current_timestamp())
							voyageId=voyageDf.filter(voyageDf.col("derived_enddate") > voyageDf.col("current_time") && voyageDf.col("current_time") >= voyageDf.col("startdate")).sort(voyageDf.col("startdate").desc).select("voyageid").rdd
							.map(x=>x.getString(0)).collect()(0)
            println(voyageId)
            log.info(voyageId)
				}
		try {

			/**
			 * Pull out the sailor id's (client id) from the reservation table
			 */
			import spark.sqlContext.implicits._
			val sailorsFromReservation =spark.read.format(sc.getConf.get("spark.reservation.table.format"))
			.option("table", sc.getConf.get("spark.reservation.table"))
			.option("zkUrl", sc.getConf.get("spark.src.zkurl")).load.filter(upper($"SELECTEDSAILINGGROUP_VOYAGEID") === voyageId.toUpperCase())
			sailorsFromReservation.show()
			spark.stop()

			/**
			 * Source Table Type = HBase
			 */

			if (sc.getConf.get("spark.src.table.type").equalsIgnoreCase("hbase")) {
				/**
				 * Refresh Type - incremental
				 */
				if (sc.getConf.contains("spark.src.table.refreshType")
						&& sc.getConf.get("spark.src.table.refreshType").equalsIgnoreCase("incremental")) {
					import spark.sqlContext.implicits._
					val voyageOperationDF = spark.read.format(sc.getConf.get("spark.src.table.format"))
					.option("table", sc.getConf.get("spark.oper.metadata.table"))
					.option("zkUrl", sc.getConf.get("spark.src.zkurl")).load.filter(upper($"VOYAGEID") === voyageId.toUpperCase()
					&& upper($"SRCCONFIGID") === srcConfigId.toUpperCase()
					&& upper($"TGTCONFIGID") === tgtConfigId.toUpperCase() && upper($"STATUS") === "Succeeded".toUpperCase())
					voyageOperationDF.show()
					val maxLastModifiedDataRow = voyageOperationDF.agg(max(voyageOperationDF.col("LASTLOADDATETIME"))).head()
					val lastLoaddataValue = maxLastModifiedDataRow.getString(0)
					log.info("lastLoaddataValue  --> " + lastLoaddataValue)

					/**
					 * Load the Src table and Apply filters
					 */
					val sourceTableSeawareColumns = sc.getConf.get("spark.src.table.seawareIdColumn").trim
					val hbaseSrcTable = spark.read.format(sc.getConf.get("spark.src.table.format"))
					.option("table", sc.getConf.get("spark.src.table"))
					.option("zkUrl", sc.getConf.get("spark.src.zkurl"))
					.load.where(col(sourceTableSeawareColumns) isin (sailorsFromReservation.select("LOYALTYMEMBERSHIPID").map(r => r.getLong(0)).collect.toList: _*))
					hbaseSrcTable.show()
					/**
					 * Change Column is of type - epoch
					 */
					hbaseSrcTable.show()
					if (sc.getConf.contains("spark.src.table.epochType")
							&& sc.getConf.get("spark.src.table.epochType").equalsIgnoreCase("Y")) {

						/**
						 * Make an entry in the Operation Metadata with "Running" status
						 */
						import spark.sqlContext.implicits._
						var voyageOperDF = Seq(voyageId.concat("-").concat(srcConfigId).concat("-").concat(tgtConfigId).concat("-")
								.concat(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").format(LocalDateTime.now))).toDF("ROWKEY")
						upsertVoyageOperationMetadata(voyageOperDF, "Running", sparkConfig)
						/**
						 * Apply Filters on the SRC data and push to Kafka
						 */
						val srcChangeColumns = sc.getConf.get("spark.src.table.changeColumn")
						//val lastLoaddataValue = sc.getConf.get("spark.src.table.lastLoadValue").toLong
						var srcTableDF = hbaseSrcTable.withColumn(
								srcChangeColumns.concat("Tmp"),
								hbaseSrcTable.col(srcChangeColumns).cast(LongType)).drop(srcChangeColumns)
						.withColumnRenamed(srcChangeColumns.concat("Tmp"), srcChangeColumns).withColumn("load_timestamp", lit(current_timestamp()))
						srcTableDF = srcTableDF.filter(srcTableDF.col(srcChangeColumns) > lastLoaddataValue)
						if (sc.getConf.contains("spark.src.table.whereClause")
								&& sc.getConf.get("spark.src.table.whereClause").toString().length > 0) {
							srcTableDF = srcTableDF.where(sc.getConf.get("spark.src.table.whereClause"))
						}

						val srcTableDataJson = srcTableDF.na.fill("").toJSON
								/**
								 * Push the records to Src Kafka
								 */
								srcTableDataJson.coalesce(sc.getConf.get("spark.target.coalesce.value").toInt).foreachPartition(
										iteration => {
											val producer = ShoreToShipKafkaProducer.getKafkaProducer(sparkConfig)
													iteration.foreach(row => {
														log.info("*****row => " + row)
														val data = new ProducerRecord[String, String](sparkConfig.value.get("spark.target.kafka.topic").get.trim(), row)
														producer.send(data)
													})
													producer.close()
										})

								import spark.sqlContext.implicits._
								voyageOperDF = Seq(voyageId.concat("-").concat(srcConfigId).concat("-").concat(tgtConfigId).concat("-")
										.concat(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").format(LocalDateTime.now))).toDF("ROWKEY")
								upsertVoyageOperationMetadata(voyageOperDF, "Succeeded", sparkConfig)

					} else if (sc.getConf.contains("spark.src.table.timestampType")
							&& sc.getConf.get("spark.src.table.timestampType").equalsIgnoreCase("Y")) {

						import spark.sqlContext.implicits._
						var voyageOperDF = Seq(voyageId.concat("-").concat(srcConfigId).concat("-").concat(tgtConfigId).concat("-")
								.concat(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").format(LocalDateTime.now))).toDF("ROWKEY")
						upsertVoyageOperationMetadata(voyageOperDF, "Running", sparkConfig)

						val srcChangeColumns = sc.getConf.get("spark.src.table.changeColumn")
						log.info("srcChangeColumns --> " + srcChangeColumns)
						log.info("lastLoaddataValue --> " + lastLoaddataValue)

						var srcTableDF = hbaseSrcTable.filter(hbaseSrcTable.col(srcChangeColumns) > lastLoaddataValue).withColumn("load_timestamp", lit(current_timestamp()))

						if (sc.getConf.contains("spark.src.table.whereClause")
								&& sc.getConf.get("spark.src.table.whereClause").toString().length > 0) {
							srcTableDF = srcTableDF.where(sc.getConf.get("spark.src.table.whereClause"))
						}

						srcTableDF.printSchema();
						srcTableDF.show();

						val srcTableDataJson = srcTableDF.toJSON
								srcTableDataJson.coalesce(sc.getConf.get("spark.target.coalesce.value").toInt).foreachPartition(
										iteration => {
											val producer = ShoreToShipKafkaProducer.getKafkaProducer(sparkConfig)
													iteration.foreach(row => {
														log.info("*****row => " + row)
														val data = new ProducerRecord[String, String](sparkConfig.value.get("spark.target.kafka.topic").get.trim(), row)
														producer.send(data)
													})
													producer.close()
										})

								import spark.sqlContext.implicits._
								voyageOperDF = Seq(voyageId.concat("-").concat(srcConfigId).concat("-").concat(tgtConfigId).concat("-")
										.concat(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").format(LocalDateTime.now))).toDF("ROWKEY")
								upsertVoyageOperationMetadata(voyageOperDF, "Succeeded", sparkConfig)
					}
				} else if (sc.getConf.contains("spark.src.table.refreshType")
						&& sc.getConf.get("spark.src.table.refreshType").equalsIgnoreCase("full")) {

					/**
					 * Load the Src table - Full Refresh - NO filters to be applied
					 */
					val hbaseSrcTable = spark.read.format(sc.getConf.get("spark.src.table.format"))
							.option("table", sc.getConf.get("spark.src.table"))
							.option("zkUrl", sc.getConf.get("spark.src.zkurl")).load

							/**
							 * Make an entry in the Operation Metadata with "Running" status
							 */
							import spark.implicits._
							var voyageOperDF = Seq(voyageId.concat("-").concat(srcConfigId).concat("-").concat(tgtConfigId).concat("-")
									.concat(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").format(LocalDateTime.now))).toDF("ROWKEY")
							upsertVoyageOperationMetadata(voyageOperDF, "Running", sparkConfig)

							var srcTableDF = hbaseSrcTable.withColumn("load_timestamp", lit(current_timestamp()))
							val srcTableDataJson = srcTableDF.toJSON
							srcTableDataJson.coalesce(sc.getConf.get("spark.target.coalesce.value").toInt).foreachPartition(
									iteration => {
										val producer = ShoreToShipKafkaProducer.getKafkaProducer(sparkConfig)
												iteration.foreach(row => {
													log.info("*****row => " + row)
													val data = new ProducerRecord[String, String](sparkConfig.value.get("spark.target.kafka.topic").get.trim(), row)
													producer.send(data)
												})
												producer.close()
									})

							import spark.implicits._
							voyageOperDF = Seq(voyageId.concat("-").concat(srcConfigId).concat("-").concat(tgtConfigId).concat("-")
									.concat(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").format(LocalDateTime.now))).toDF("ROWKEY")
							upsertVoyageOperationMetadata(voyageOperDF, "Succeeded", sparkConfig)
				}
			}

		} catch {
		case e: SQLException => {
			e.printStackTrace();
			log.info("HBase connectioin issue..please check HBase service");
			/**
			 * Make an entry in the operational metadata with Failure
			 */
			val voyageOperRowKey = voyageId.concat("-").concat(srcConfigId).concat("-").concat(tgtConfigId).concat("-")
					.concat(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm").format(LocalDateTime.now))
					import spark.sqlContext.implicits._
					var voyageOperDF = Seq(voyageOperRowKey).toDF("ROWKEY")
					upsertVoyageOperationMetadata(voyageOperDF, "Failed", sparkConfig)
		}
		case e: Exception =>
		{
			log.info("******************in the catch of HBaseTableShoreToShipSync ******************");
			e.printStackTrace();
			/**
			 * Make an entry in the operational metadata with Failure
			 */
			val voyageOperRowKey = voyageId.concat("-").concat(srcConfigId).concat("-").concat(tgtConfigId).concat("-")
					.concat(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm").format(LocalDateTime.now))
					import spark.sqlContext.implicits._
					var voyageOperDF = Seq(voyageOperRowKey).toDF("ROWKEY")
					upsertVoyageOperationMetadata(voyageOperDF, "Failed", sparkConfig)
					throw new Exception("General Exception..please check the stacktrace")
		}
		}
		spark.stop()
	}

	@throws(classOf[Exception])
	def upsertVoyageOperationMetadata(voyageOperMetadaDF: DataFrame, status: String, configMap: Broadcast[Map[String, String]]) {
		try {
			var voyageOperDF: DataFrame = null
					if (configMap.value.contains("spark.src.table.epochType") && configMap.value.get("spark.src.table.epochType").get.equalsIgnoreCase("Y")) {
						if (status.equalsIgnoreCase("running") || status.equalsIgnoreCase("failed")) {
							voyageOperDF = voyageOperMetadaDF.withColumn("VOYAGEID", lit(configMap.value.get("spark.src.voyageid").get))
									.withColumn("SRCCONFIGID", lit(configMap.value.get("spark.src.srcConfig").get))
									.withColumn("TGTCONFIGID", lit(configMap.value.get("spark.src.tgtConfig").get)).withColumn("TYPE", lit("Src"))
									.withColumn("STARTTIME", lit(current_timestamp())).withColumn("ENDTIME", lit(null))
									.withColumn("STATUS", lit(status)).withColumn("LASTLOADDATETIME", lit(null))
						} else if (status.equalsIgnoreCase("succeeded")) {
							voyageOperDF = voyageOperMetadaDF.withColumn("VOYAGEID", lit(configMap.value.get("spark.src.voyageid").get))
									.withColumn("SRCCONFIGID", lit(configMap.value.get("spark.src.srcConfig").get))
									.withColumn("TGTCONFIGID", lit(configMap.value.get("spark.src.tgtConfig").get)).withColumn("TYPE", lit("Src"))
									.withColumn("STARTTIME", lit(null)).withColumn("ENDTIME", lit(current_timestamp()))
									.withColumn("STATUS", lit(status)).withColumn("LASTLOADDATETIME", lit(Instant.now.getEpochSecond.toString()))
						}
					} else if (configMap.value.contains("spark.src.table.timestampType") && configMap.value.get("spark.src.table.timestampType").get.equalsIgnoreCase("Y")) {
						if (status.equalsIgnoreCase("running") || status.equalsIgnoreCase("failed")) {
							voyageOperDF = voyageOperMetadaDF.withColumn("VOYAGEID", lit(configMap.value.get("spark.src.voyageid").get))
									.withColumn("SRCCONFIGID", lit(configMap.value.get("spark.src.srcConfig").get))
									.withColumn("TGTCONFIGID", lit(configMap.value.get("spark.src.tgtConfig").get)).withColumn("TYPE", lit("Src"))
									.withColumn("STARTTIME", lit(current_timestamp)).withColumn("ENDTIME", lit(null))
									.withColumn("STATUS", lit(status)).withColumn("LASTLOADDATETIME", lit(null))
						} else if (status.equalsIgnoreCase("succeeded")) {
							voyageOperDF = voyageOperMetadaDF.withColumn("VOYAGEID", lit(configMap.value.get("spark.src.voyageid").get))
									.withColumn("SRCCONFIGID", lit(configMap.value.get("spark.src.srcConfig").get))
									.withColumn("TGTCONFIGID", lit(configMap.value.get("spark.src.tgtConfig").get)).withColumn("TYPE", lit("Src"))
									.withColumn("STARTTIME", lit(null)).withColumn("ENDTIME", lit(current_timestamp()))
									.withColumn("STATUS", lit(status)).withColumn("LASTLOADDATETIME", lit(current_timestamp()).cast(StringType))
						}
					} else {
						if (status.equalsIgnoreCase("running") || status.equalsIgnoreCase("failed")) {
							voyageOperDF = voyageOperMetadaDF.withColumn("VOYAGEID", lit(configMap.value.get("spark.src.voyageid").get))
									.withColumn("SRCCONFIGID", lit(configMap.value.get("spark.src.srcConfig").get))
									.withColumn("TGTCONFIGID", lit(configMap.value.get("spark.src.tgtConfig").get)).withColumn("TYPE", lit("Src"))
									.withColumn("STARTTIME", lit(current_timestamp)).withColumn("ENDTIME", lit(null))
									.withColumn("STATUS", lit(status)).withColumn("LASTLOADDATETIME", lit(null))
						} else if (status.equalsIgnoreCase("succeeded")) {
							voyageOperDF = voyageOperMetadaDF.withColumn("VOYAGEID", lit(configMap.value.get("spark.src.voyageid").get))
									.withColumn("SRCCONFIGID", lit(configMap.value.get("spark.src.srcConfig").get))
									.withColumn("TGTCONFIGID", lit(configMap.value.get("spark.src.tgtConfig").get)).withColumn("TYPE", lit("Src"))
									.withColumn("STARTTIME", lit(null)).withColumn("ENDTIME", lit(current_timestamp()))
									.withColumn("STATUS", lit(status)).withColumn("LASTLOADDATETIME", lit(current_timestamp()).cast(StringType))
						}
					}
		voyageOperDF = voyageOperDF.withColumn("CURRENT_TIMESTAMP", lit(current_timestamp()))
				log.info("[START] ----- writing dataframe to phoenix table HBTB_NBX_SHORETOSHIP_SYNC_VOYAGEOPS ")
				voyageOperDF.printSchema()
				voyageOperDF.show(100, false)
				voyageOperDF.write
				.format(configMap.value.get("spark.src.table.format").get)
				.mode(configMap.value.get("spark.oper.metadata.table.mode").get)
				.option("table", configMap.value.get("spark.oper.metadata.table").get)
				.option("zkUrl", configMap.value.get("spark.src.zkurl").get)
				.save
				log.info("[END] ----- writing dataframe to phoenix table HBTB_NBX_SHORETOSHIP_SYNC_VOYAGEOPS ")
		} catch {
		case e: SQLException => {
			log.info("HBase connectioin issue..please check HBase service")
			e.printStackTrace()
			//throw new HBaseConException("Hbase Connection Exception")
		}
		case e: Exception => {
			log.info("in the catch of upsertVoyageOperationMetadata ******************")
			e.printStackTrace()
			throw new Exception("General Exception..please check the stacktrace")
		}
		}
	}
}
