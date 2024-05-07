package com.virginvoyages.shoretoship.hive

import org.apache.log4j.LogManager
import org.apache.log4j.Level
import java.util.Date
import java.util.Properties
import org.apache.spark.broadcast.Broadcast
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.StringSerializer
//import io.confluent.kafka.serializers.KafkaAvroSerializer
//import com.virginvoyages.data.event.DemographicsRefreshedEvent
//import com.virginvoyages.data.event.DemographicsRefreshedPayload
//import io.confluent.kafka.serializers.KafkaAvroSerializer


object ShipToShoreKafkaProducer {
  
/**
   * Initialize logger
   */
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
  
  /**
   * 
   * Main Method is for Testing Kafka push event 
   */
  def main(args: Array[String]): Unit = 
  {
    log.info("IN Main Method...")
   /* pushDataRecord("0010n000003egNb789","070570566456787916asdfasdfasdfsdafsdfasdfasdf01","10.9.100.157:9092,10.9.100.142:9092,10.9.100.37:9092",
        "dev-shore-bigdata.demographic-data-refresh-info","http://10.9.100.157:8081")*/
  }
  

  /**
   * Kafka Push Message Method - Takes SAILORID, C360ID as parameter 
   * 			builds AVRO object and sends to "dev-shore-bigdata.demographic-data-refresh"
   */
  def pushDataRecord(jsonMsg :String,sparkConf: Broadcast[Map[String,String]]) = {
    
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sparkConf.value.get("spark.target.sslkafkabrokers").get.trim())
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
   // props.put(ProducerConfig.BATCH_SIZE_CONFIG,sparkConf.value.get("spark.target.kafka.batch.size").get.trim())
    props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sparkConf.value.get("spark.target.truststore").get.trim())
    props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sparkConf.value.get("spark.target.passvalue").get.trim())
    props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sparkConf.value.get("spark.target.keystore").get.trim())
    props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sparkConf.value.get("spark.target.passvalue").get.trim())
    props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sparkConf.value.get("spark.target.passvalue").get.trim())
    
    val producer = new KafkaProducer[String, String](props)
   
    val data = new ProducerRecord[String, String](sparkConf.value.get("spark.target.kafka.topic").get.trim(), "", jsonMsg)

    producer.send(data)
    
    producer.close();
  }
  
  def getKafkaProducer(sparkConf: Broadcast[Map[String,String]]) = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,sparkConf.value.get("spark.target.sslkafkabrokers").get.trim())
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
    props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,sparkConf.value.get("spark.target.truststore").get.trim())
    props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,sparkConf.value.get("spark.target.passvalue").get.trim())
    props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,sparkConf.value.get("spark.target.keystore").get.trim())
    props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,sparkConf.value.get("spark.target.passvalue").get.trim())
    props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG,sparkConf.value.get("spark.target.passvalue").get.trim())
    
    val producer = new KafkaProducer[String, String](props)
    
    producer
  }
  
 def getKafkaProducerNonSSL(sparkConf: Broadcast[Map[String,String]]) = {
   
   println(" Inside Utils ")
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sparkConf.value.get("spark.kafkabrokers").get.trim())
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    val producer = new KafkaProducer[String, String](props)
    
    println("producer obj  "+ producer)
    producer
  }
  
}