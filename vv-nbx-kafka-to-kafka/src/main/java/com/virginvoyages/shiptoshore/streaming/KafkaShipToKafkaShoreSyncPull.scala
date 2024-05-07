package com.virginvoyages.shiptoshore.streaming

import java.sql.SQLException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.hadoop.fs.{ FileSystem, Path }
import scala.collection.immutable.Map
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer

import org.apache.log4j.Level
import org.apache.log4j.LogManager

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions.lit
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Assign
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import org.apache.kafka.clients.CommonClientConfigs
import com.virginvoyages.shiptoshore.streaming.KafkaOffsetStreamingDriver
import com.virginvoyages.metadataframework.ManageMetadata

object KafkaShipToKafkaShoreSyncPull extends Serializable {

   /** Initialize logger  */
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {

    def getSparkSession() =
      {
        val spark = SparkSession
          .builder()
          .getOrCreate()
        spark

      }

    
    val spark = getSparkSession()
     val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
      import spark.implicits._

    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(spark.sparkContext.getConf.get("spark.shore.seconds").toInt))
    val srcConfigId = spark.sqlContext.sparkContext.getConf.get("spark.src.config.id")
    val tgtConfigId = spark.sqlContext.sparkContext.getConf.get("spark.target.config.id")
    val voyageId = spark.sqlContext.sparkContext.getConf.get("spark.src.voyageid")
    /** Create Topic Array */
  
    
/*************************************calling metadata framework*********************************************/
    
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
    
    //ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark);

    var src_ssl_flag = "false";
    try {
      src_ssl_flag=spark.sparkContext.getConf.get("spark.source.sslflag").trim
    } catch {
      case e: NoSuchElementException => { src_ssl_flag = "false"; log.info("#--------------------No Source  SSL Flag -------------------#") }

    }
  
    var kafkaParams = Map[String, Object](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> spark.sparkContext.getConf.get("spark.source.kafkabrokers").trim,
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
				ConsumerConfig.GROUP_ID_CONFIG -> spark.sparkContext.getConf.get("spark.source.consumer").trim(),
				ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean))
    
    if(src_ssl_flag=="false"){
      kafkaParams = Map[String, Object](
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> spark.sparkContext.getConf.get("spark.source.kafkabrokers").trim,
					ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
					ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
					ConsumerConfig.GROUP_ID_CONFIG -> spark.sparkContext.getConf.get("spark.source.consumer").trim(),
					ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean))    
    }
    else{
      kafkaParams = Map[String, Object](
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> spark.sparkContext.getConf.get("spark.source.ssl.kafkabrokers").trim,
					ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
					ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
					ConsumerConfig.GROUP_ID_CONFIG -> spark.sparkContext.getConf.get("spark.source.consumer").trim(),
					ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean),
					CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> "SSL",
					SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG -> spark.sparkContext.getConf.get("spark.source.truststore").trim,
					SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG -> spark.sparkContext.getConf.get("spark.source.passvalue").trim,
					SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG -> spark.sparkContext.getConf.get("spark.source.keystore").trim,
					SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG -> spark.sparkContext.getConf.get("spark.source.passvalue").trim,
					SslConfigs.SSL_KEY_PASSWORD_CONFIG -> spark.sparkContext.getConf.get("spark.source.passvalue").trim)
    }
            
    log.info("Reading From offset from Postgres****************")
    val fromOffsets = KafkaOffsetStreamingDriver.getLastCommittedOffset(spark.sparkContext.getConf.get("spark.source.topicname").trim, spark.sqlContext, spark)
    
    /** Create a Direct Stream to read the Salesforce Messages */
    log.info("Completed Reading offset from Postgres - Started Mini Batch ****************")

    val messages = KafkaUtils.createDirectStream[String, String](streamingContext, PreferConsistent, Assign[String, String](fromOffsets.keys, kafkaParams, fromOffsets))
 
    log.info("in Read :::::: ");
    var transCommit: Boolean = true;
    
    log.info("********** Source fromOffsets.key :"+ fromOffsets.keys);
    log.info("********** Source kafkaParams :"+ kafkaParams);
    log.info("********** Source fromOffsets :"+ fromOffsets);
    log.info("********** Messages :"+ messages);
    log.info("********** Source Topic :" + spark.sparkContext.getConf.get("spark.source.topicname").trim);
    log.info("********** Target Topic :" + spark.sparkContext.getConf.get("spark.target.topicname").trim);
   
    
    messages.foreachRDD { (rdd, batchTime) =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
       offsetRanges.foreach(offset =>
        log.info(" offsets : " + offset.topic, offset.partition, offset.fromOffset, offset.untilOffset))

      val debugFlag = AssignConfigVal(spark.sqlContext.sparkContext.getConf.get("spark.debugFlag")) 
      
      if (debugFlag == "Y"){
        log.info("Rdd Obj -----------> " + rdd);
      }
      
      /** Iterate RDD and build a tuple with Json Msg */
      val msgTuple = readingJsonMessages(rdd);    
      log.info("----------RDD Count :----------" + rdd.count())
      log.info("----------msgTuple Count :----------" + msgTuple.count())
      
      try {
        log.info("*********************************")
        /** Parse JSON Message and parse them as String[s]  */

        import spark.sqlContext.implicits._
       
        if (debugFlag == "Y"){
          msgTuple.toDF().show(false)
        }
        
        if (!msgTuple.toDF().head(1).isEmpty) {
          if (spark.sqlContext.sparkContext.getConf.get("spark.src.table.type").equalsIgnoreCase("kafka")) {

            import spark.sqlContext.implicits._   
            
            val msgTuplewithShipCode = addShipCode(msgTuple)
            log.info("*********** Added Ship code if present ***********");
            
            val v_msgTuplewithShipCodeTemp = msgTuplewithShipCode.coalesce(spark.sqlContext.sparkContext.getConf.get("spark.target.coalesce.value").toInt)
            val no_p = v_msgTuplewithShipCodeTemp.getNumPartitions

           msgTuplewithShipCode.coalesce(spark.sqlContext.sparkContext.getConf.get("spark.target.coalesce.value").toInt).foreachPartition {
              iteration =>
                {
                  val producer = getKafkaProducer(sparkConfiguration)
                  
                 // log.info("*********** Before  iteration.foreach(row => ***********");
                  iteration.foreach(row => {                    
                  /*  if (debugFlag == "Y"){
                      log.info("***** row => " + row)
                    }*/
                    val data = new ProducerRecord[String, String](sparkConfiguration.value.get("spark.target.topicname").get.trim(), row)
                    producer.send(data)
                  })
                  producer.close();
                }
            }
            
        /*  msgTuplewithShipCode.coalesce(spark.sqlContext.sparkContext.getConf.get("spark.target.coalesce.value").toInt).foreachPartition {
              iteration =>
                {*/
                 
                 /* 
                  log.info("*********** Before  iteration.foreach(row => ***********");
                  msgTuplewithShipCode.coalesce(spark.sqlContext.sparkContext.getConf.get("spark.target.coalesce.value").toInt).foreach(row => {                    
                    val producer = getKafkaProducer(sparkConfiguration)
                    if (debugFlag == "Y"){
                      log.info("***** row => " + row)
                    }
                    val data = new ProducerRecord[String, String](sparkConfiguration.value.get("spark.target.topicname").get.trim(), row)
                    producer.send(data)
                    println("Sent")
                  producer.close();} )
                  */
                
            
          }
          
          
          log.info("***********SAVING OFFSET IN Postgres (spark.clientmerge.salesforce.kafkatopic)**************")
          KafkaOffsetStreamingDriver.saveOffsets(spark.sparkContext.getConf.get("spark.source.topicname"), offsetRanges, batchTime, sparkConfiguration, spark)
          
          log.info("***********SAVE OFFSET COMPLETED  IN POSTGRES**************")
          
          import spark.sqlContext.implicits._   
          ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark);

        }
        /* Start Code snippet to dynamically stop the job  */
        
        var TrgFile = "N"
        var ConfigFileName = batch_id1.replace('_', '-')
        var TrgFileLocn = s"""hdfs://data/streamingtriggers/""" + ConfigFileName + """.trg"""
        
        println(TrgFileLocn)
        
        if (spark.sparkContext.getConf.contains("spark.stop.trgFile")) {
          TrgFile = spark.sparkContext.getConf.get("spark.stop.trgFile")
          gracefulStop(streamingContext)
        }

        if (spark.sparkContext.getConf.contains("spark.trgFileName")) {
          TrgFileLocn = spark.sparkContext.getConf.get("spark.trgFileName")
          
        }
        
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        
        if (TrgFile == "Y") {
          val fileExists = fs.exists(new Path(TrgFileLocn))
          
          if (fileExists) {
            
            log.info("*************** Stopping the streaming Dynamically ******************")
            println("***************  Stopping the streaming Dynamically ******************")
            streamingContext.stop()

          }
        } 
        /* End Code snippet to dynamically stop the job  */
        
      } catch {
        case e: Exception =>
          { ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
            log.info("Exception Message!! " + e.getMessage)
            log.info("Exception stack trace  " + e.printStackTrace());
            println("Exception Message!! " + e.getMessage)
            println("Exception stack trace  " + e.printStackTrace());
          }
         
          import spark.sqlContext.implicits._
       
          gracefulStop(streamingContext);
      }

    }
    streamingContext.start()
    streamingContext.awaitTermination()

    def readingJsonMessages(msgRdd: RDD[ConsumerRecord[String, String]]) = {
      val tupleDF = msgRdd.map {
        record =>
          val xmlMsg = record.value
          (xmlMsg)
      }
      tupleDF
    }
    
    def AssignConfigVal(ConfigVal: => String):(String) = {
      var RetVal = "".toString
      try {RetVal = ConfigVal} catch {case e: NoSuchElementException => {RetVal = ""} }
      return(RetVal)
    }

    def addShipCode(msgRdd: RDD[String]): RDD[String] = {
      var msgTuplewithShipCode = msgRdd
      log.info("---------- Inside add ship code msgTuplewithShipCode Count :----------" + msgTuplewithShipCode.count())
      
      val shipFlag = AssignConfigVal(spark.sqlContext.sparkContext.getConf.get("spark.shipFlag"))
      if (shipFlag == "Y") {
        val columnName = AssignConfigVal(spark.sqlContext.sparkContext.getConf.get("spark.shipCodeColumn"))
        val shipCode = AssignConfigVal(spark.sqlContext.sparkContext.getConf.get("spark.shipCode"))
        msgTuplewithShipCode = spark.read.json(msgRdd).withColumn(columnName,lit(shipCode)).toJSON.rdd
      }
      msgTuplewithShipCode
    }    

    def pushDataRecord(recordValue: String, configMap: Broadcast[Map[String, String]]) = {
      import java.util.Properties
      val props = new Properties()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configMap.value.get("spark.ship.kafkabrokers").get)
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

      val producer = new KafkaProducer[String, String](props)
      log.info("*****producer configs added****** ")
      val t = System.currentTimeMillis()
      log.info("*****t valus system.currentTimeMillis---: " + t);
      val data = new ProducerRecord(configMap.value.get("spark.ship.target.topic.name").get, "", recordValue)
      log.info("*************Data sending to producer*****" + data);
      producer.send(data)
      log.info("**********Data sent to producer******************");
      producer.close();
    }

  
    def getKafkaProducer(sparkConf: Broadcast[Map[String,String]]) = {
      import java.util.Properties    
    
      val props = new Properties()
      var tgt_ssl_flag = "false";
     /* log.info("**********Inside getKafkaProducer******************");*/
      
      try {
        tgt_ssl_flag=sparkConf.value.get("spark.target.sslflag").get.trim()
        
      } catch {
        case e: NoSuchElementException => { tgt_ssl_flag = "false";/* log.info("#---------No target  SSL Flag -----------#") */}

      }
    
      if(tgt_ssl_flag == "true"){
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sparkConf.value.get("spark.target.ssl.kafkabrokers").get.trim())
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sparkConf.value.get("spark.target.truststore").get.trim())
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sparkConf.value.get("spark.target.passvalue").get.trim())
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sparkConf.value.get("spark.target.keystore").get.trim())
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sparkConf.value.get("spark.target.passvalue").get.trim())
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sparkConf.value.get("spark.target.passvalue").get.trim())
      } else{
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sparkConf.value.get("spark.target.kafkabrokers").get.trim())
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      }
      
      val producer = new KafkaProducer[String, String](props)
      val proddummystr = producer.toString()

      producer
    }

    /** Gracefully stop the job in case of Unrecoverable exception (Hbase/Zookeeper Failure)   */
    def gracefulStop(streamingContext: StreamingContext) = {
      log.info("****************************************** Closing the SparkStram ****************")
      streamingContext.stop(true);
    }    
  }
}