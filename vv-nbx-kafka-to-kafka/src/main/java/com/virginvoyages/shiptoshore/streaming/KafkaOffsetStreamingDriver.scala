package com.virginvoyages.shiptoshore.streaming

import java.sql.SQLException

import org.apache.kafka.common.TopicPartition
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.OffsetRange

import kafka.utils.ZkUtils

import org.apache.log4j.Level
import org.apache.log4j.LogManager

object KafkaOffsetStreamingDriver {

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  case class OFFSET(TOPIC_NAME: String, PARTITION: Int, UPDATED_OFFSET: Long, TIMESTAMP: Long, ROWKEY: String)
  def saveOffsets(TOPIC_NAME: String, offsetRanges: Array[OffsetRange],
                  batchTime: org.apache.spark.streaming.Time, configMap: Broadcast[Map[String, String]], spark: SparkSession) = {
    val topic_name = TOPIC_NAME;
    val md_shipcode = configMap.value.get("spark.shipCode").get.trim();
    val md_topic_name = TOPIC_NAME + "-" + md_shipcode;

    import spark.implicits._
    val offsetdetails: OFFSET = null;
    var offsetlist = List[OFFSET]()
    for (offset <- offsetRanges) {
      /*val offsetdetails = OFFSET(topic_name, offset.partition, offset.untilOffset, batchTime.milliseconds, (topic_name + batchTime.milliseconds+"-"+offset.partition))*/
      val offsetdetails = OFFSET(md_topic_name, offset.partition, offset.untilOffset, batchTime.milliseconds, (md_topic_name + batchTime.milliseconds + "-" + offset.partition))
      offsetlist ::= offsetdetails
    }
    val finalOffsetDF = offsetlist.toSeq.toDF()
    println("----------Save offset Final DF ---------------")
    //finalOffsetDF.show()
    try {
      // finalOffsetDF.printSchema()g
  /*    finalOffsetDF.select("TOPIC_NAME", "PARTITION", "UPDATED_OFFSET", "TIMESTAMP", "ROWKEY").write
        .format(configMap.value.get("spark.personcreated.format").get) //personCreatedDF.sqlContext.getConf("spark.personcreated.format")
        .mode(configMap.value.get("spark.personcreated.mode").get) //personCreatedDF.sqlContext.getConf("spark.personcreated.mode")
        .option("table", configMap.value.get("spark.personcreated.offsettable").get) //personCreatedDF.sqlContext.getConf("spark.personcreated.table")
        .option("zkUrl", configMap.value.get("spark.personcreated.readhbasezkurl").get) //personCreatedDF.sqlContext.getConf("spark.personcreated.readhbasezkurl")
        .save*/
      val pguser = configMap.value.get("spark.metadata.user").get
      val pgpassword = configMap.value.get("spark.metadata.password").get
      val jdbcUrl = configMap.value.get("spark.metadata.con.url").get
      val tableName = configMap.value.get("spark.metadata.offset.table").get
      val jdbcDriver = configMap.value.get("spark.metadata.con.driver").get
      val con_format = configMap.value.get("spark.metadata.con.format").get
      val op_mode = configMap.value.get("spark.metadata.ops.table.mode").get
      finalOffsetDF.select("TOPIC_NAME", "PARTITION", "UPDATED_OFFSET", "TIMESTAMP", "ROWKEY").write
        .format(con_format)
        .mode(op_mode)
        .option("driver", jdbcDriver)
        .option("dbtable", tableName)
        .option("url", jdbcUrl)
        .option("user", pguser)
        .option("password", pgpassword)
        .save
    } catch {
      case e: SQLException => { log.info("Postgre db connectioin issue..please check Postgre db service");  }
      case e: Exception    => { log.info("in the catch of updatePostgre dbTable ******************");e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace") }
    }
  }

  def getLastCommittedOffset(TOPIC_NAME: String, sqlContext: SQLContext, spark: SparkSession): Map[TopicPartition, Long] =
    {
      import sqlContext.implicits._
      val pguser = sqlContext.sparkContext.getConf.get("spark.metadata.user").trim()
      val pgpassword = sqlContext.sparkContext.getConf.get("spark.metadata.password").trim()
      val jdbcUrl = sqlContext.sparkContext.getConf.get("spark.metadata.con.url").trim()
      val tableName = sqlContext.sparkContext.getConf.get("spark.metadata.offset.table").trim()
      val jdbcDriver = sqlContext.sparkContext.getConf.get("spark.metadata.con.driver").trim()
      val con_format = sqlContext.sparkContext.getConf.get("spark.metadata.con.format").trim()

      val fromOffsets = collection.mutable.Map[TopicPartition, Long]()
      val mapOfVals = collection.mutable.Map[Int, Long]()
      val topic_name = TOPIC_NAME;
      val md_shipcode = spark.sparkContext.getConf.get("spark.shipCode").trim;
      val md_topic_name = TOPIC_NAME + "-" + md_shipcode;

      log.info("*********** md_topic_name :" + md_topic_name);

      try {

        log.info(s"[START]----- reading postgre table jdbc record ${sqlContext.sparkContext.getConf.get("spark.metadata.offset.table")} as dataframe")
        val topicDF = spark.read.format(con_format)
          .option("driver", jdbcDriver)
          .option("dbtable", tableName)
          .option("Url", jdbcUrl)
          .option("user", pguser)
          .option("password", pgpassword)
          .load()
          topicDF.show

        /* val df = sqlContext.read.format(sqlContext.sparkContext.getConf.get("spark.personcreated.format").trim()).option("table", sqlContext.sparkContext.getConf.get("spark.personcreated.offsettable").trim()).
          option("zkUrl", sqlContext.sparkContext.getConf.get("spark.personcreated.readhbasezkurl").trim()).load().topicDF.filter($"topic_name" === md_topic_name).select("topic_name", "partition", "updated_offset").groupBy("topic_name", "partition").max("updated_offset").alias("updated_offset")
*/
        //val df=topicDF.filter($"topic" === topic_name).select("topic", "partition", "offset_end").groupBy("topic", "partition").max("offset_end").alias("offset_end")
        val df = topicDF.filter($"topic_name" === md_topic_name).select("topic_name", "partition", "updated_offset").groupBy("topic_name", "partition").max("updated_offset").alias("updated_offset")
        df.show()

        df.collect().foreach(
          row => {
            val partition = row.getInt(1)
            println("partiioin value "+partition);
            val offsetvalue = row.getLong(2)
            println("offsetvalue value "+offsetvalue);
            mapOfVals.put(partition, offsetvalue)
          })

        mapOfVals.foreach { keyVale => println("KEY Map of offset ----------: " + keyVale._1 + "| VALUE Map of offset -------: " + keyVale._2) }

        val zkUrl = sqlContext.sparkContext.getConf.get("spark.kafka.zookeeper").trim() + "/"
        val zkClientAndConnection = ZkUtils.createZkClientAndConnection(zkUrl, 10000, 10000)
        val zkUtils = new ZkUtils(zkClientAndConnection._1, zkClientAndConnection._2, false)
        val zKNumberOfPartitionsForTopic = zkUtils.getPartitionsForTopics(Seq(TOPIC_NAME)).get(TOPIC_NAME).toList.head.size

        print("***************************************************")
        println("Partition Number from Zookeeper**************" + zKNumberOfPartitionsForTopic)

        val result = df.select("partition").count()
        println("**************PARITTION COUNT FROM Hbase DF********** : " + result)
        var hbaseNumberOfPartitionsForTopic = 0 //Set the number of partitions discovered for a topic in HBase to 0
        if (result != null) {
          //If the result from hbase scanner is not null, set number of partitions from hbase to the number of cells
          hbaseNumberOfPartitionsForTopic = result.toInt
        }
        println("**************PARITTION COUNT FROM hbaseNumberOfPartitionsForTopic  ********** : " + hbaseNumberOfPartitionsForTopic)
        if (hbaseNumberOfPartitionsForTopic == 0) {
          // initialize fromOffsets to beginning
          for (partition <- 0 to zKNumberOfPartitionsForTopic - 1) {

            fromOffsets += (new TopicPartition(TOPIC_NAME, partition) -> 0)
          }
        } else if (zKNumberOfPartitionsForTopic > hbaseNumberOfPartitionsForTopic) {
          print("*************************** inside greater than **************")
          // handle scenario where new partitions have been added to existing kafka topic
          for (partition <- 0 to hbaseNumberOfPartitionsForTopic - 1) {

            if (partition == 1) {
              fromOffsets += (new TopicPartition(TOPIC_NAME, 1) -> 0)
            } else {
              fromOffsets += (new TopicPartition(TOPIC_NAME, partition) -> mapOfVals.get(partition).get)
            }
          }
          for (partition <- hbaseNumberOfPartitionsForTopic to zKNumberOfPartitionsForTopic - 1) {
            fromOffsets += (new TopicPartition(TOPIC_NAME, partition) -> 0)
          }
        } else {
          //initialize fromOffsets from last run
          for (partition <- 0 to hbaseNumberOfPartitionsForTopic - 1) {

            fromOffsets += (new TopicPartition(TOPIC_NAME, partition) -> mapOfVals.get(partition).get)
          }
        }
        fromOffsets.foreach { keyVal => println("KEY ----------: " + keyVal._1 + "| VALUE -------: " + keyVal._2) }
      } catch {
        case e: SQLException => { e.printStackTrace() }
        case e: Exception    => { e.printStackTrace() }

      }
       fromOffsets.foreach { keyVal => println("KEY Read----------: " + keyVal._1 + "| VALUE Read -------: " + keyVal._2) }
      fromOffsets.toMap
    }

}
