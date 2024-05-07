package com.virginvoyages.shore.util

import java.sql.SQLException

import org.apache.kafka.common.TopicPartition
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.OffsetRange
import kafka.utils.ZkUtils

object KafkaUtil {
  case class OFFSET(TOPIC_NAME: String, PARTITION: Int, UPDATED_OFFSET: Long, TIMESTAMP: Long, ROWKEY: String)
  def saveOffsets(TOPIC_NAME: String, offsetRanges: Array[OffsetRange],
                  batchTime: org.apache.spark.streaming.Time, configMap: Broadcast[Map[String, String]], spark: SparkSession) = {
    var topic_name = TOPIC_NAME;
    if (spark.sparkContext.getConf.contains("spark.reservation.activity")) {
      if (spark.sparkContext.getConf.get("spark.reservation.activity").toString().equalsIgnoreCase("true")) {
        topic_name = TOPIC_NAME + "-activity"
      }
    }

    import spark.implicits._
    val offsetdetails: OFFSET = null;
    var offsetlist = List[OFFSET]()
    for (offset <- offsetRanges) {
      val offsetdetails = OFFSET(topic_name, offset.partition, offset.untilOffset, batchTime.milliseconds, (topic_name + batchTime.milliseconds + "-" + offset.partition))
      offsetlist ::= offsetdetails
    }
    val finalOffsetDF = offsetlist.toSeq.toDF()
    println("----------Save offset Final DF ---------------")
    finalOffsetDF.show()
    try {
      finalOffsetDF.select("TOPIC_NAME", "PARTITION", "UPDATED_OFFSET", "TIMESTAMP", "ROWKEY").write
        .format(configMap.value.get("spark.personcreated.format").get)
        .mode(configMap.value.get("spark.personcreated.mode").get)
        .option("table", configMap.value.get("spark.personcreated.offsettable").get)
        .option("zkUrl", configMap.value.get("spark.personcreated.readhbasezkurl").get)
        .save
    } catch {
      case e: SQLException => { /*log.info("HBase connectioin issue..please check HBase service"); throw new HBaseConException("Hbase Connection Exception")*/ }
      case e: Exception    => { /*log.info("in the catch of updateHbaseTable ******************");e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace")*/ }
    }

  }

  def getLastCommittedOffset(TOPIC_NAME: String, sqlContext: SQLContext, spark: SparkSession): Map[TopicPartition, Long] =
    {
      import sqlContext.implicits._
      val fromOffsets = collection.mutable.Map[TopicPartition, Long]()
      val mapOfVals = collection.mutable.Map[Int, Long]()
      var topic_name = TOPIC_NAME;
      try {
        if (sqlContext.sparkContext.getConf.contains("spark.reservation.activity")) {
          if (sqlContext.sparkContext.getConf.get("spark.reservation.activity").equalsIgnoreCase("true")) {
            topic_name = TOPIC_NAME + "-activity"
          }
        }
        println(s"[START]----- reading phoenix table ${sqlContext.sparkContext.getConf.get("spark.personcreated.offsettable")} as dataframe")
        val df = sqlContext.read.format(sqlContext.sparkContext.getConf.get("spark.personcreated.format").trim()).option("table", sqlContext.sparkContext.getConf.get("spark.personcreated.offsettable").trim()).
          option("zkUrl", sqlContext.sparkContext.getConf.get("spark.personcreated.readhbasezkurl").trim()).load().filter($"topic_name" === topic_name).select("topic_name", "partition", "updated_offset").groupBy("topic_name", "partition").max("updated_offset").alias("updated_offset")

        df.show()

        df.collect().foreach(
          row => {
            val partition = row.getInt(1)
            val offsetvalue = row.getLong(2)
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
        /*Set the number of partitions discovered for a topic in HBase to 0*/
        var hbaseNumberOfPartitionsForTopic = 0
        if (result != null) {
          /*If the result from hbase scanner is not null, set number of partitions from hbase to the number of cells*/
          hbaseNumberOfPartitionsForTopic = result.toInt
        }
        println("**************PARITTION COUNT FROM hbaseNumberOfPartitionsForTopic  ********** : " + hbaseNumberOfPartitionsForTopic)
        if (hbaseNumberOfPartitionsForTopic == 0) {
          /* initialize fromOffsets to beginning */
          for (partition <- 0 to zKNumberOfPartitionsForTopic - 1) {

            fromOffsets += (new TopicPartition(TOPIC_NAME, partition) -> 0)
          }
        } else if (zKNumberOfPartitionsForTopic > hbaseNumberOfPartitionsForTopic) {
          print("*************************** inside greater than **************")
          /* handle scenario where new partitions have been added to existing kafka topic */
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
          /*initialize fromOffsets from last run */
          for (partition <- 0 to hbaseNumberOfPartitionsForTopic - 1) {

            fromOffsets += (new TopicPartition(TOPIC_NAME, partition) -> mapOfVals.get(partition).get)
          }
        }
        fromOffsets.foreach { keyVal => println("KEY ----------: " + keyVal._1 + "| VALUE -------: " + keyVal._2) }
      } catch {
        case e: SQLException => { e.printStackTrace() }
        case e: Exception    => { e.printStackTrace() }

      }
      fromOffsets.toMap
    }
}