package com.virginvoyages.shoretoship.hive

import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.kafka.clients.producer.ProducerRecord
import com.virginvoyages.metadataframework.ManageMetadata

object ShoreHivetoShoreKafkaPostvoyage {
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
      
          var whereClause = s""" """
          
          if(spark.sparkContext.getConf.get("spark.ship.code").trim.equals("SC")){
            println("Ship Code: SC - Writing for Scarlet")  
            whereClause = s""" where part_dt=(select max(part_dt) from vv_db.hvtb_nbx_core_postvoyage_sailor_survey_rpt) and ship_name = 'Scarlet Lady' """
          }else if(spark.sparkContext.getConf.get("spark.ship.code").trim.equals("VL")){
            println("Ship Code: VL - Writing for Valiant")
            whereClause = s""" where part_dt=(select max(part_dt) from vv_db.hvtb_nbx_core_postvoyage_sailor_survey_rpt) and ship_name = 'Valiant Lady' """
          }
					
					println(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
          log.info(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
          val query = spark.sparkContext.getConf.get("spark.source.sql").trim().replace("*whereclause*", whereClause)
					println(query)
					
  				var inputDf = spark.sql(query)
					inputDf.show(5,false)
					println("printing count of inputDf")
					println(inputDf.count())
					val sslenabled = spark.sparkContext.getConf.get("spark.ssl.enabled").trim()
					
					

					if (!inputDf.head(1).isEmpty) {

						val tzcols = inputDf.schema.toList.filter(x => x.dataType == TimestampType).map(c => col(c.name))
								// val tzCols = s"""orderdate,orderdeliverydate,modifieddate""".split(",")

								for (tzcol <- tzcols) {
									// println("tzcol:::::::::::::::;" + tzcol)
									var cols = tzcol.toString()
											inputDf = inputDf.withColumn(cols, inputDf.col(cols).cast(StringType))
								}

						// inputDf.printSchema
						 inputDf.show(2,false)

						spark.conf.set("spark.sql.session.timeZone", "UTC")
						TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
						for (tzcol <- tzcols) {
							var cols = tzcol.toString()
									println("tzcol:::::::::::::::;" + cols)
									inputDf = inputDf.withColumn(cols, inputDf.col(cols).cast(TimestampType))
						}
						
						val jsonData = inputDf.toJSON
						println("printing count of jsonData")
						println(jsonData.count())

                            if (sslenabled == "false") {

						jsonData.coalesce(sc.getConf.get("spark.target.coalesce.value").toInt).rdd.foreachPartition(

								iteration => {
									val producer = ShipToShoreKafkaProducer.getKafkaProducerNonSSL(sparkConfig)
									
									println("Kafka producer initialized  "+ producer)
											iteration.foreach(row => {
												log.info("*****row => " + row)
												val data = new ProducerRecord[String, String](sparkConfig.value.get("spark.target.kafka.topic").get.trim(), row)
												println("sending data to Kafka  "+ data)
												producer.send(data)
											})
											producer.close()
								})
								}
								
								if (sslenabled == "true") {

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