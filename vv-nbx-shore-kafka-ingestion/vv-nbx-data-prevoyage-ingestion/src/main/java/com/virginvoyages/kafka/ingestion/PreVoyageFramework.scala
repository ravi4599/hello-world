package com.virginvoyages.kafka.ingestion

 

import java.io.ByteArrayInputStream

import java.sql.SQLException

import java.io.File

import java.io.IOException

import java.nio.charset.StandardCharsets

 

import scala.util.Try

 

import org.apache.kafka.clients.CommonClientConfigs

import org.apache.kafka.clients.consumer.ConsumerConfig

import org.apache.kafka.clients.consumer.ConsumerRecord

import org.apache.kafka.common.config.SslConfigs

import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.log4j.Level

import org.apache.log4j.LogManager

import org.apache.spark.rdd.RDD

import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types.ArrayType

import org.apache.spark.sql.types.StructType

import org.apache.spark.streaming.Seconds

import org.apache.spark.streaming.StreamingContext

import org.apache.spark.streaming.kafka010.Assign

import org.apache.spark.streaming.kafka010._

import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.streaming.kafka010.ConsumerStrategies._

import org.apache.spark.streaming.kafka010.{ OffsetRange, HasOffsetRanges, KafkaUtils }

import java.time.LocalDateTime

//****************

import java.util.Date

import java.text.SimpleDateFormat

import org.apache.spark.sql.functions._

import org.apache.spark.broadcast.Broadcast

import org.apache.spark.sql.Column

//import java.util.uuid

import java.nio.ByteBuffer

import java.time.Instant

import org.apache.spark.rdd.RDD

import com.virginvoyages.kafka.ingestion.OffSetManager

import com.virginvoyages.metadataframework.ManageMetadata

import org.apache.spark.sql.types.StringType

import java.sql.Timestamp

import org.apache.spark.sql.Row

import org.apache.spark.sql.types.{ StructType, StructField, TimestampType }

import java.time.format.DateTimeFormatter

import java.time.LocalDateTime

import org.apache.spark.sql.SaveMode

 

object PreVoyageFramework {

 

 

      

  val spark = SparkSession

      .builder()

      .enableHiveSupport()

      .getOrCreate()

    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(20))

    val log = LogManager.getRootLogger

    log.setLevel(Level.INFO)

    import spark.implicits._

    val sc = spark.sparkContext

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

      /*

   * Initialize logger

   */

 

  log.setLevel(Level.INFO)

  var cnt = 1

 

  def main(args: Array[String]): Unit = {

 

    /**

     * Initialize spark context

     */

    val spark = SparkSession.builder() //.config("spark.sql.broadcastTimeout", "36000")

      .enableHiveSupport()

      .getOrCreate()

    val sc = spark.sparkContext

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

 

    //val streamingContext = new StreamingContext(spark.sparkContext, Seconds(20))

    val topics = Array(spark.sparkContext.getConf.get("spark.src.kafkatopic").trim)

    /**

     * Initialize kafka parameters

     */

 

    import spark.implicits._

 

    val kafkaParams = Map[String, Object](

      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> spark.sparkContext.getConf.get("spark.kafkabrokers").trim,

      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],

      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],

      ConsumerConfig.GROUP_ID_CONFIG -> spark.sparkContext.getConf.get("spark.mpx.consumer").trim(),

      //ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> spark.sparkContext.getConf.get("spark.src.personOffSet").trim(),

      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean))

 

    /*

       * Reading offset from Hbase

       */

 

    log.info("Reading From offset from Hbase****************")

    val fromOffsets = OffSetManager.getLastCommittedOffset(spark.sparkContext.getConf.get("spark.src.kafkatopic").trim, spark.sqlContext, spark)

    log.info("Completed Reading offset from Hbase****************")

 

    /*-

    * Create a Direct Stream to read the Messages from kafka topic

    */

 

    val messages = KafkaUtils.createDirectStream[String, String](streamingContext,PreferConsistent, Assign[String, String](fromOffsets.keys, kafkaParams, fromOffsets))

    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)

    log.info("Start Reading kafka")

    messages.foreachRDD { (rdd, batchTime) =>

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      offsetRanges.foreach(offset =>

        log.info(" offsets : " + offset.topic, offset.partition, offset.fromOffset, offset.untilOffset))

 

      var batchInstanceId: String = null

      var batch_id1: String = null

      var transCommit: Boolean = true;

      log.info("###################     Processing kafka stream msgs started  ########################")

      try {

        val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)

        batch_id1 = metadata._1

        batchInstanceId = metadata._2

       

        val msgTuple = createXMLMsgs(rdd);

       

          val batchtStartTime1 = args(0)
      val batchEndTime1 = args(1)

      val batchtStartTime = batchtStartTime1.replace("T", " ")
      val batchEndTime = batchEndTime1.replace("T", " ")

 

      var partReadStart: String = null

      val pattern = "\\d{4}-\\d{2}-\\d{2}".r

      partReadStart = (pattern findFirstIn batchtStartTime).map(_.toString).getOrElse("")

     

      var partReadEnd: String = null

      partReadEnd = (pattern findFirstIn batchEndTime).map(_.toString).getOrElse("")

     

      var partWriteDate:String = null

      partWriteDate=(pattern findFirstIn batchtStartTime).map(_.toString).getOrElse("")

     

      

      

      

      

 

        log.info(" Partition write date " + partWriteDate)

        var voyageId = spark.sparkContext.getConf.get("spark.voyage.id")

       

        var messageDf = rdd.map(row => row.value()).toDF("Message")

        messageDf.show

        if (!messageDf.head(1).isEmpty) {

          val finalDf = messageDf.withColumn("BatchTime", lit(batchtStartTime).cast(TimestampType)).withColumn("BatchInstanceID", lit(batchInstanceId)).withColumn("Part_Date", to_date(lit(partWriteDate))).withColumn("VoyageId", lit(voyageId).cast(StringType))

            .select("BatchInstanceID", "VoyageId", "BatchTime", "Message", "Part_Date")

 

          sqlContext.setConf("hive.exec.dynamic.partition", "true")

                  sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

 

         finalDf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.hive.table"))

        

          

          log.info("***********SAVING OFFSET IN HBASE**************")

          OffSetManager.saveOffsets(offsetRanges, batchInstanceId, sparkConfiguration, spark)

          log.info("***********SAVE OFFSET COMPLETED  IN HBASE**************")

         

          //streamingContext.stop()

          

        }

 

        ManageMetadata.updateStatus(batchInstanceId, batch_id1, "Successful", spark)

        streamingContext.stop()

        

 

      } catch {

 

        case e: Exception =>

          ManageMetadata.updateStatus(batchInstanceId, batch_id1, "Failed", spark)

          log.info("Exception................................................................" + e.getMessage)

          transCommit = false;

          log.info("transCommit value in exception ---> " + transCommit)

          log.info("Exception stack trace  " + e.printStackTrace()); throw new Exception("General Exception..please check the stacktrace")

 

        

          gracefulStop(streamingContext);

          System.exit(1)

      }

     

      

 

    }

 

  

   streamingContext.start()

    streamingContext.awaitTermination()

//streamingContext.stop()

    def createXMLMsgs(msgRdd: RDD[ConsumerRecord[String, String]]) = {

    val tupleDF = msgRdd.map {

      record =>

        val xmlMsg = record.value

        record.key()

        (xmlMsg)

    }

 

    tupleDF

 

  }

  }

  def gracefulStop(streamingContext: StreamingContext) = {

    log.info("****************************************** Closing the SparkStram ****************")

    streamingContext.stop(true);

  }

 
}
 