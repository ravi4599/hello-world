package com.virginvoyages.kafka.ingestion


import org.apache.kafka.common.TopicPartition
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.OffsetRange
import kafka.utils.ZkUtils
import java.sql.SQLException
import com.virginvoyages.kafka.ingestion.HBaseConnetionException
import org.apache.log4j.Level
import org.apache.log4j.LogManager

object OffSetManager {
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  case class OFFSET(BATCH_INSTANCE_ID: String, TOPIC: String, PARTITION: Int, OFFSET_START: Long, OFFSET_END: Long)
  def saveOffsets(
    offsetRanges: Array[OffsetRange],
    batchID:      String, configMap: Broadcast[Map[String, String]], spark: SparkSession) = {
    import spark.implicits._
    val offsetdetails: OFFSET = null;
    var offsetlist = List[OFFSET]()
    for (offset <- offsetRanges) {
      val offsetdetails = OFFSET(batchID + "-" + offset.partition, offset.topic, offset.partition, offset.fromOffset, offset.untilOffset)
      offsetlist ::= offsetdetails
    }
    val finalOffsetDF = offsetlist.toSeq.toDF()
    println("----------Save offset Final DF ---------------")
    //finalOffsetDF.show()
    try {
      finalOffsetDF.select("BATCH_INSTANCE_ID", "TOPIC", "PARTITION", "OFFSET_START", "OFFSET_END").write
        .format(configMap.value.get("spark.target.table.format").get)
        .mode(configMap.value.get("spark.target.offset.mode").get)
        .option("table", configMap.value.get("spark.target.offset.table").get)
        .option("zkUrl", configMap.value.get("spark.target.zkurl").get)
        .save
    } catch {
      case e: SQLException => { log.info("HBase connectioin issue..please check HBase service"); throw new HBaseConnetionException("Hbase Connection Exception") }
      case e: Exception    => { log.info("in the catch of updateHbaseTable ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace") }

    }

  }

  def getLastCommittedOffset(TOPIC: String, sqlContext: SQLContext, spark: SparkSession): Map[TopicPartition, Long] =
    {
      import sqlContext.implicits._
      val fromOffsets = collection.mutable.Map[TopicPartition, Long]()
      val mapOfVals = collection.mutable.Map[Int, Long]()
      var topic_name = TOPIC;
      if (topic_name == null) {

        topic_name = spark.sparkContext.getConf.get("spark.src.kafkatopic").trim
      }
      try {
        println(s"[START]----- reading phoenix table ${sqlContext.sparkContext.getConf.get("spark.target.table.format")} as dataframe")
        val df = sqlContext.read.format(sqlContext.sparkContext.getConf.get("spark.target.table.format").trim()).option("table", sqlContext.sparkContext.getConf.get("spark.target.offset.table").trim()).
          option("zkUrl", sqlContext.sparkContext.getConf.get("spark.target.zkurl").trim()).load().filter($"topic" === topic_name).select("topic", "partition", "offset_end").groupBy("topic", "partition").max("offset_end").alias("offset_end")

        /*  val dff = sqlContext.read.format(sqlContext.sparkContext.getConf.get("spark.target.table.format").trim()).option("table", sqlContext.sparkContext.getConf.get("spark.target.offset.table").trim()).
          option("zkUrl", sqlContext.sparkContext.getConf.get("spark.target.zkurl").trim()).load().filter($"topic" === topic_name).select("topic", "partition", "offset_end")
          */
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
        val zKNumberOfPartitionsForTopic = zkUtils.getPartitionsForTopics(Seq(TOPIC)).get(TOPIC).toList.head.size

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

            fromOffsets += (new TopicPartition(TOPIC, partition) -> 0)
          }
        } else if (zKNumberOfPartitionsForTopic > hbaseNumberOfPartitionsForTopic) {
          print("*************************** inside greater than **************")
          /* handle scenario where new partitions have been added to existing kafka topic */
          for (partition <- 0 to hbaseNumberOfPartitionsForTopic - 1) {

            if (partition == 1) {
              fromOffsets += (new TopicPartition(TOPIC, 1) -> 0)
            } else {
              fromOffsets += (new TopicPartition(TOPIC, partition) -> mapOfVals.get(partition).get)
            }
          }
          for (partition <- hbaseNumberOfPartitionsForTopic to zKNumberOfPartitionsForTopic - 1) {
            fromOffsets += (new TopicPartition(TOPIC, partition) -> 0)
          }
        } else {
          /*initialize fromOffsets from last run */
          for (partition <- 0 to hbaseNumberOfPartitionsForTopic - 1) {

            fromOffsets += (new TopicPartition(TOPIC, partition) -> mapOfVals.get(partition).get)
          }
        }
        fromOffsets.foreach { keyVal => println("KEY ----------: " + keyVal._1 + "| VALUE -------: " + keyVal._2) }
      } catch {
        case e: SQLException => {
          e.printStackTrace()
          throw new HBaseConnetionException("Hbase Connection Exception")
        }
        case e: Exception => {
          e.printStackTrace()
          throw new Exception("General Exception..please check the stacktrace")
        }

      }
      fromOffsets.toMap
    }

}