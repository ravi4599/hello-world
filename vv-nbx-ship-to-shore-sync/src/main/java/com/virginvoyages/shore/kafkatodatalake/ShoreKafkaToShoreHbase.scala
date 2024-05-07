package com.virginvoyages.shore.kafkatodatapond
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.sql.Column

import org.apache.spark.SparkConf
import java.sql.SQLException
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.CanCommitOffsets
import org.apache.spark.streaming.kafka010.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.Seconds
//import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

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
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.Column
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerRecord

//XML validator imports
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.kafka.common.config.SslConfigs
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
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.CommonClientConfigs
import com.virginvoyages.shore.util.KafkaUtil

import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkConnection
import org.apache.zookeeper.ZooKeeper
import org.apache.spark.streaming.kafka010.Assign
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.{OffsetRange, HasOffsetRanges, KafkaUtils}
import java.time.LocalDateTime
object ShoreKafkaToShoreHbase  {
     // spark session 
	val spark = SparkSession
			.builder()
			.getOrCreate()

			val streamingContext = new StreamingContext(spark.sparkContext,Seconds(spark.sparkContext.getConf.get("spark.src.seconds").toInt))
			val log = LogManager.getRootLogger
			log.setLevel(Level.INFO)

			def main(args: Array[String]): Unit = {
					val topics = Array(spark.sparkContext.getConf.get("spark.src.kafka.topic").trim)	
							val kafkaParams = Map[String, Object](
									ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> spark.sparkContext.getConf.get("spark.src.kafkabrokers").trim,
									ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
									ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
									ConsumerConfig.GROUP_ID_CONFIG -> spark.sparkContext.getConf.get("spark.src.consumer").trim(),
									//ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> spark.sparkContext.getConf.get("spark.src.personOffSet").trim(),
									ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean),
							   CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> "SSL",
									SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG -> spark.sparkContext.getConf.get("spark.passvalue").trim,
									/**
									 * Fetch JKS files from Current Spark Job Working Dir
									 */
									SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG -> spark.sparkContext.getConf.get("spark.truststore").trim,
									SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG -> spark.sparkContext.getConf.get("spark.keystore").trim,
									SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG -> spark.sparkContext.getConf.get("spark.passvalue").trim,
									SslConfigs.SSL_KEY_PASSWORD_CONFIG -> spark.sparkContext.getConf.get("spark.passvalue").trim)

							log.info("Reading From offset from Hbase****************")
							val fromOffsets=KafkaUtil.getLastCommittedOffset(spark.sparkContext.getConf.get("spark.src.kafka.topic").trim, spark.sqlContext, spark)
							log.info("Completed Reading offset from Hbase****************")
							val messages = KafkaUtils.createDirectStream[String, String](streamingContext, PreferConsistent,Assign[String,String](fromOffsets.keys,kafkaParams,fromOffsets))
							//val messages = KafkaUtils.createDirectStream[String, String](streamingContext,PreferConsistent, Subscribe[String, String](topics, kafkaParams))
							val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)

							messages.foreachRDD 
							{ (rdd, batchTime) =>
							val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
							offsetRanges.foreach(
									offset =>
									log.info(" offsets : " + offset.topic, offset.partition, offset.fromOffset, offset.untilOffset)	
									)

							var transCommit: Boolean = true; 
									log.info("###################     Processing kafka stream msgs started  ########################")
									val msgTuple = createMsg(rdd);

									try 
									{
										var msg_df=spark.read.json(msgTuple)
												msg_df.show(2)
												if(!msg_df.head(1).isEmpty){
												  
												//	var VOYAGEOPS_parameter=spark.sparkContext.getConf.get("spark.target.VOYAGEOPS").split(",")
															val VOYAGEID=spark.sparkContext.getConf.get("spark.src.voyageid")
															val SRCCONFIGID=spark.sparkContext.getConf.get("spark.src.srcConfig")
															val TGTCONFIGID=spark.sparkContext.getConf.get("spark.src.tgtConfig")
															val TYPE=spark.sparkContext.getConf.get("spark.job.type")
															val df= Seq(VOYAGEID)
															import java.time.format.DateTimeFormatter
															import java.time.LocalDateTime 
															val voyageOperRowKey = VOYAGEID.concat("-").concat(SRCCONFIGID).concat("-").concat(TGTCONFIGID)
															.concat(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm").format(LocalDateTime.now))
															import spark.implicits._
															var VOYAGEOPS_df=spark.sparkContext.parallelize(df).toDF("VOYAGEID")
															VOYAGEOPS_df=VOYAGEOPS_df.withColumn("ROWKEY",lit(voyageOperRowKey))
															.withColumn("SRCCONFIGID",lit(SRCCONFIGID).cast(StringType))
															.withColumn("TGTCONFIGID",lit(TGTCONFIGID).cast(StringType))
															.withColumn("TYPE",lit(TYPE).cast(StringType))
															.withColumn("STARTTIME",lit(current_timestamp()).cast(TimestampType))
															.withColumn("ENDTIME",lit(null).cast(TimestampType))
															.withColumn("STATUS",lit("RUNNING").cast(StringType))
															.withColumn("LASTLOADDATETIME",lit(null).cast(TimestampType))
															VOYAGEOPS_df=VOYAGEOPS_df.select("ROWKEY","VOYAGEID","SRCCONFIGID","TGTCONFIGID","TYPE","STARTTIME","ENDTIME","STATUS","LASTLOADDATETIME")
															upsertVoyageOperationMetadata(VOYAGEOPS_df,spark)
															val column_names1=spark.sparkContext.getConf.get("spark.target.table.schema").trim
															
															var json_df=msg_df
															var column_array1=column_names1.split(",")
															var column_seq1=column_array1.map(x=>col(x.split(" ")(0))).toSeq
															for(str<-column_array1){
																var colname=str.split(" ")(0)
																		var dtype=str.split(" ")(1)
																		if(!json_df.schema(colname).dataType.equals(inferType(str))){
																			json_df=json_df.withColumn(colname,col(colname).cast(inferType(str)))}
															}
													var final_df=json_df.select(column_seq1:_*)
															final_df.write
															.format(spark.sparkContext.getConf.get("spark.target.table.format").trim)
															.mode(spark.sparkContext.getConf.get("spark.target.table.mode").trim())
															.option("table",spark.sparkContext.getConf.get("spark.target.table").trim)
															.option("zkUrl", spark.sparkContext.getConf.get("spark.target.zkurl").trim)
															.save
															val voyageOperRowKey1 = VOYAGEID.concat("-").concat(SRCCONFIGID).concat("-").concat(TGTCONFIGID)
															.concat(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm").format(LocalDateTime.now))
															import spark.implicits._
															var VOYAGEOPS_df1=spark.sparkContext.parallelize(df).toDF("VOYAGEID")
															VOYAGEOPS_df1=VOYAGEOPS_df1.withColumn("ROWKEY",lit(voyageOperRowKey1))
															.withColumn("SRCCONFIGID",lit(SRCCONFIGID).cast(StringType))
															.withColumn("TGTCONFIGID",lit(TGTCONFIGID).cast(StringType))
															.withColumn("TYPE",lit(TYPE).cast(StringType))
															.withColumn("STARTTIME",lit(null).cast(TimestampType))
															.withColumn("ENDTIME",lit(current_timestamp()).cast(TimestampType))
															.withColumn("STATUS",lit("succeeded").cast(StringType))
															.withColumn("LASTLOADDATETIME",lit(null).cast(TimestampType))
															//VOYAGEOPS_df=VOYAGEOPS_df.withColumn("STATUS",lit("succeeded").cast(StringType)).withColumn("ENDTIME",lit())
															//VOYAGEOPS_df1.show(6)
															VOYAGEOPS_df1=VOYAGEOPS_df1.select("ROWKEY","VOYAGEID","SRCCONFIGID","TGTCONFIGID","TYPE","STARTTIME","ENDTIME","STATUS","LASTLOADDATETIME")

															//VOYAGEOPS_df1.show(7)
															upsertVoyageOperationMetadata(VOYAGEOPS_df1,spark)
															log.info("***********SAVING OFFSET IN HBASE**************")
															KafkaUtil.saveOffsets(sparkConfiguration.value.get("spark.src.kafka.topic").get.trim, offsetRanges, batchTime, sparkConfiguration, spark)
															log.info("***********SAVE OFFSET COMPLETED  IN HBASE**************")

												}


									} 
									catch 
									{

									case e: Exception =>
									log.info("Exception................................................................"+e.getMessage)
									transCommit = false;
									log.info("transCommit value in exception --> " + transCommit)
									log.info("Exception stack trace  "+e.printStackTrace());
									//var VOYAGEOPS_parameter=spark.sparkContext.getConf.get("spark.target.VOYAGEOPS").split(",")
											var VOYAGEID=spark.sparkContext.getConf.get("spark.src.voyageid")//VOYAGEOPS_parameter(0)//"VOYAGEID12"
											var SRCCONFIGID=spark.sparkContext.getConf.get("spark.src.srcConfig")//VOYAGEOPS_parameter(1)//"SRCCONFIGID11"
											var TGTCONFIGID=spark.sparkContext.getConf.get("spark.src.tgtConfig")//VOYAGEOPS_parameter(2)//"TGTCONFIGID11"
											var TYPE=spark.sparkContext.getConf.get("spark.job.type")//VOYAGEOPS_parameter(3)//"sync"
											val df= Seq(VOYAGEID)
											import java.time.format.DateTimeFormatter
											import java.time.LocalDateTime 
											val voyageOperRowKey = VOYAGEID.concat("-").concat(SRCCONFIGID).concat("-").concat(TGTCONFIGID)
											.concat(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm").format(LocalDateTime.now))

											import spark.implicits._
											var VOYAGEOPS_df=spark.sparkContext.parallelize(df).toDF("VOYAGEID")
											VOYAGEOPS_df=VOYAGEOPS_df.withColumn("ROWKEY",lit(voyageOperRowKey))
											.withColumn("SRCCONFIGID",lit(SRCCONFIGID).cast(StringType))
											.withColumn("TGTCONFIGID",lit(TGTCONFIGID).cast(StringType))
											.withColumn("TYPE",lit(TYPE).cast(StringType))
											.withColumn("STARTTIME",lit(current_timestamp()).cast(TimestampType))
											.withColumn("ENDTIME",lit(null).cast(TimestampType))
											.withColumn("STATUS",lit("failed").cast(StringType))
											.withColumn("LASTLOADDATETIME",lit(null).cast(TimestampType))
											VOYAGEOPS_df=VOYAGEOPS_df.select("ROWKEY","VOYAGEID","SRCCONFIGID","TGTCONFIGID","TYPE","STARTTIME","ENDTIME","STATUS","LASTLOADDATETIME")
											upsertVoyageOperationMetadata(VOYAGEOPS_df,spark)

											spark.stop()


									}

									log.info("transCommit value after foreach --> " + transCommit)
									if (transCommit) 
									{
										log.info("transCommit value in TRUE --> " + transCommit)
										log.info("Commit Offsets")
										messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
									}

							}


					streamingContext.start()
					streamingContext.awaitTermination()
	}
	def inferType(field: String) = field.split(" ")(1).toUpperCase() match 
			{
			case "BIGINT" => LongType 
			case "INTEGER" => IntegerType 
			case "DOUBLE" => DoubleType 
			case "FLOAT" => FloatType
			case "TIMESTAMP" => TimestampType
			case "BOOLEAN" => BooleanType 
			case "VARCHARARRAY" =>  ArrayType(StringType)
			case "DOUBLEARRAY" =>  ArrayType(DoubleType)
			case "INTEGERARRAY" =>  ArrayType(IntegerType)
			case "VARCHAR" => StringType
			case _ => StringType 
			}
	def upsertVoyageOperationMetadata(voyageOperMetadaDF: DataFrame,spark: SparkSession)
	{
		var voyageOperMetadaDF1=voyageOperMetadaDF
				voyageOperMetadaDF1=voyageOperMetadaDF1.withColumn("CURRENT_TIMESTAMP",lit(current_timestamp()))
				voyageOperMetadaDF1.write
				.format(spark.sparkContext.getConf.get("spark.target.table.format").trim)
				.mode(spark.sparkContext.getConf.get("spark.target.table.mode").trim())
				.option("table",spark.sparkContext.getConf.get("spark.target.VOYAGEOPS_table").trim())
				.option("zkUrl", spark.sparkContext.getConf.get("spark.target.zkurl").trim)
				.save


	}

	def createMsg(msgRdd: RDD[ConsumerRecord[String, String]]) = 
		{
				val tupleDF = msgRdd.map 
						{
					record =>
					val xmlMsg = record.value
					(xmlMsg)
						}
				tupleDF
		}
}

