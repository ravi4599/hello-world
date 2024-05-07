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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.{ OffsetRange, HasOffsetRanges, KafkaUtils }
import java.time.LocalDateTime
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Column
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
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.SparkConf
import java.sql.SQLException
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark.streaming.kafka010.CanCommitOffsets
import org.apache.spark.streaming.kafka010.PreferConsistent
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import java.net.UnknownHostException
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ avg, explode, concat, lit, trim }
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.to_json
import java.sql.Struct
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.Column
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{ lit, max, row_number }
import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import org.xml.sax.SAXException;
import java.io._
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.from_json
import org.json4s.{ DefaultFormats, MappingException }
import org.json4s.jackson.JsonMethods._
import scala.util.Try
import org.apache.spark.sql.Row;
import scala.util.parsing.json._
import org.apache.log4j.Logger
import scala.util.matching.Regex
import scala.util.parsing.json._
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkConnection
import org.apache.zookeeper.ZooKeeper
import org.apache.spark.streaming.kafka010.Assign
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import java.util.Arrays
import javax.ws.rs.client.ClientBuilder
import org.glassfish.jersey.client.ClientConfig;


object SeawareClientMerge {
    /**
   * Initialize logger
   */
  val log = LogManager.getRootLogger
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
    var intv = 300
    if (spark.sparkContext.getConf.contains("spark.time.int")) {

      intv = spark.sparkContext.getConf.get("spark.time.int").toInt
    }

    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(intv))
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

    val messages = KafkaUtils.createDirectStream[String, String](streamingContext, PreferConsistent, Assign[String, String](fromOffsets.keys, kafkaParams, fromOffsets))
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
        /*-
   	   * Calling Metdataframework to get audit coulmn values
       */

        val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
        batch_id1 = metadata._1
        batchInstanceId = metadata._2
        val startExecutionTime = metadata._7
        val partWriteDate = metadata._8
       // val msgTuple = createXMLMsgs(rdd);

        log.info(" Partition write date " + partWriteDate)
        var voyageId = spark.sparkContext.getConf.get("spark.voyage.id")
        var shipCode:String = null       
        if (spark.sparkContext.getConf.contains("spark.ship.code")) {

          shipCode = spark.sparkContext.getConf.get("spark.ship.code").toString()
        }
     try {

          log.info("$$$$ RDD COUNT For Seaware Client Merge Kafka Topic $$$" + rdd.count())
   
          var msgTuple = createRDD(rdd)

          if (msgTuple.count() > 0) {
            log.info("$$$$ msgTuple For Seaware Client Merge Kafka Topic $$$" + msgTuple)
            var retiringIDs = retiringID(msgTuple, topics.lastOption.get, sparkConfiguration)
            log.info("$$$$ retiringIDs count For Salesforce Client Merge Kafka Topic  $$$" + retiringIDs.count())
            import spark.sqlContext.implicits._
            var SEAWARERAWDF = retiringIDs.toDF("COMBOID")
            //Splitted cobination ID of winner ID and Retiring ID
            SEAWARERAWDF = SEAWARERAWDF.withColumn("_tmp", split($"COMBOID", "\\|")).select(
              $"_tmp".getItem(0).as("WinnerID").cast(IntegerType),
              $"_tmp".getItem(1).as("LoserID").cast(IntegerType)).drop("_tmp")
            //Filter to remove null orblank wining id and loser ids
//            SEAWARERAWDF.filter((($"WinnerID" !== "") or $"WinnerID".isNotNull) || (($"LoserID" !== "") or $"LoserID".isNotNull)).foreach( row => log.info("Row value " + row.get(0) + "       second value " + row.get(1)))
//            SEAWARERAWDF = SEAWARERAWDF.filter((($"WinnerID" !== "") or $"WinnerID".isNotNull) || (($"LoserID" !== "") or $"LoserID".isNotNull or ($"WinnerID" !== null) or ($"WinnerID" !== "null")))
            
            SEAWARERAWDF.na.drop()
            SEAWARERAWDF.printSchema()
            SEAWARERAWDF.show(100, false)
            
            val finalDf = SEAWARERAWDF.withColumn("BatchTime", lit(startExecutionTime).cast(TimestampType)).withColumn("Part_Date", to_date(lit(partWriteDate)))
          sqlContext.setConf("hive.exec.dynamic.partition", "true")
          sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
//          finalDf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.hive.table"))
      
          val tgtLocation = spark.sparkContext.getConf.get("spark.target.location")
          val hiveTable = spark.sparkContext.getConf.get("spark.target.hive.table")
		  finalDf.write.mode(org.apache.spark.sql.SaveMode.Append).parquet(tgtLocation + "/part_date=" + partWriteDate)

		  val insertString = s"""alter table $hiveTable add IF NOT EXISTS  partition(part_date='$partWriteDate') location '$tgtLocation/part_date=$partWriteDate'""".stripMargin
		  
		  println(insertString)
		  spark.sql(insertString)
		  spark.sql("refresh table " + hiveTable)
		  spark.sql("Msck repair table " + hiveTable)
            
            log.info("***********SAVING OFFSET IN HBASE *************")
            OffSetManager.saveOffsets(offsetRanges, batchInstanceId, sparkConfiguration, spark)
            log.info("***********SAVE OFFSET COMPLETED  IN HBASE**************")
          }

        } catch {
          case e: Exception =>
            log.info(" -----Exception in Seaware Client Merge Kafka Topic Method---------")
            transCommit = false;
            log.info("........transCommit value in exception --> " + transCommit)
            e.printStackTrace();
            gracefulStop(streamingContext);
        }


        //Call Metadataframework to update the status
        println("batchInstanceId  "+ batchInstanceId)
        println("batch_id1  "+ batch_id1)
        ManageMetadata.updateStatus(batchInstanceId, batch_id1, "Successful", spark)

        var flag = "N"
        if (spark.sparkContext.getConf.contains("spark.stop.flag")) {

          flag = spark.sparkContext.getConf.get("spark.stop.flag")
        }
        if (flag == "Y") {

          log.info("*************** Stoping the streaming ******************")
          println("***************  Stoping the streaming ******************")
          streamingContext.stop()
        }
        
       var TrgFile = "N"
        var ConfigFileName = batch_id1.replace('_', '-')
        var TrgFileLocn = s"""hdfs://data/streamingtriggers/""" + ConfigFileName + """.trg"""
        println(TrgFileLocn)
        if (spark.sparkContext.getConf.contains("spark.stop.trgFile")) {
          TrgFile = spark.sparkContext.getConf.get("spark.stop.trgFile")
        }

        if (spark.sparkContext.getConf.contains("spark.trgFileName")) {
          TrgFileLocn = spark.sparkContext.getConf.get("spark.trgFileName")
        }
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        if (TrgFile == "Y") {
          val fileExists = fs.exists(new Path(TrgFileLocn))
          if (fileExists) {
            log.info("*************** Stoping the streaming Dynamically ******************")
            println("***************  Stoping the streaming Dynamically ******************")
            streamingContext.stop()

          }}

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
   
  }
    def retiringID(msgTuple: RDD[String], topics: String, configMap: Broadcast[Map[String, String]]) = {

    val tupleDF = msgTuple.map(
      messages => {
        //var retiringID = List[String]()
        var retiringID: String = null;

        var winningID: String = null;
        var comboID: String = null;

        log.info("check1")
        var jsondata = messages.replaceAll("\\s+", " ")

        implicit var formats = DefaultFormats

        //We are in seaware condition to parse winning and loser IDs
       
          log.info("check3 ==    In seaware client merge " + topics)
          val result = parse(jsondata)
          // Source Clients are retiring ids
          var SourceClientIDs = (result \ "ClientMergeNotification" \ "ClientMergeEvent" \ "SourceClientIDs").extract[String]
          retiringID = SourceClientIDs
          
          var TargetClientID = (result \ "ClientMergeNotification" \ "ClientMergeEvent" \ "TargetClientID").extract[String]
          winningID = TargetClientID //.toString()
        
        //Combination id consists of winning ID | REtiringid1;Teritingid2;Teritingid3
        comboID = winningID + "|" + retiringID

        comboID
      })

    tupleDF

  }
    
    def createRDD(msgRdd: RDD[ConsumerRecord[String, String]]) = {
    val tupleDF = msgRdd.map {
      record =>
        val Msg = record.value
        log.info("Msg " + Msg)
        (Msg)
    }

    tupleDF
  }

  def gracefulStop(streamingContext: StreamingContext) = {
    log.info("****************************************** Closing the SparkStram ****************")
    streamingContext.stop(true);
  }

}