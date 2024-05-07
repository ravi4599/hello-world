package com.virginvoyages.invoke.ringcentral.api

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Properties
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ StructType, StructField, TimestampType, StringType, IntegerType, DateType, LongType };
import org.apache.spark.sql.expressions.Window
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import com.virginvoyages.metadataframework.ManageMetadata
import java.sql.SQLException
import scala.collection.JavaConversions._
import org.apache.spark.sql.functions._
import scala.util.Try
import org.apache.spark.sql.DataFrame
import java.time.{ ZonedDateTime, ZoneId }
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{ Column, DataFrame }
import scala.collection.mutable.ArrayBuffer
import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.sql.SaveMode
import com.squareup.okhttp.OkHttpClient
import com.squareup.okhttp.MediaType
import com.squareup.okhttp.RequestBody
import com.squareup.okhttp.Request;
import scala.util.parsing.json.JSON
import java.time.LocalDate

//XML validator imports
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
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
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import org.apache.spark.broadcast.Broadcast

object InvokeCTISkillApi {
 def main(args: Array[String]): Unit = {
    

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)

    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

/**********************************MetaData framework**********************************************/
      val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
      metadata.productIterator.foreach(println)
      val batchStartTime = metadata._3
      val batchEndTime = metadata._4
      
    log.info("batchStartTime:" + batchStartTime)
    log.info("batchEndTime:" + batchEndTime)

    val access_token = getAccessToken(sparkConfiguration,spark)
    log.info("access_token: " + access_token)
    val jsonMessage = geJsonMessage(access_token)

    //log.info("json message:"+jsonMessage)

    try {
     
      var ctiagentdf = Seq(jsonMessage).toDF("Message")
      
      ctiagentdf = ctiagentdf.withColumn("batchtime", unix_timestamp(lit(batchStartTime), "yyyy-MM-dd' 'HH:mm:ss").cast(TimestampType))
        .withColumn("part_date", lit(to_date(col("batchtime"), "yyyy-MM-dd")))

      ctiagentdf = ctiagentdf.select("batchtime", "Message", "part_date")
      //ctiskilldf.show(10, false)
      //ctiskilldf.printSchema()

      ctiskilldf.write.mode("overwrite").insertInto(spark.sparkContext.getConf.get("spark.source.ctiskill.table").trim())

    } catch {

      case e: Exception =>
        {
          log.info("in the catch of updateStatus ****************** ")
          e.printStackTrace()
          throw new Exception("General Exception..please check the stacktrace")
        }
    }
  }

  def getAccessToken(configMap: Broadcast[Map[String, String]],spark:SparkSession): String = {
    try {
      val httpClient = new OkHttpClient();
      val log = LogManager.getRootLogger
      log.setLevel(Level.INFO)

      // json formatted data
      //var json = "{ \"username\":\"ashwin.john@capgemini.com\", \"password\":\"VV$Feb$022020\", \"grant_type\":\"password\"}"
      
      //var json="{\"accessKeyId\":\"65PY723U5MHD53CMHWSOM672QM352EBLQXWGNJI54727LK2ZLETA====\",\"accessKeySecret\":\"CI5SPEZFLU57T6QNVJAPAZQ2CWHJB43D5XGNTRY2T6AAXFQ2WQEQ====\"}"
      //var json="{\"accessKeyId\":\"XXJHETO535J2MRBFJZQPC62XI7A5F6FN6AKXMQ6BAU4W6ZLAT6PA====\",\"accessKeySecret\":\"EWSBWIWRIROAXV5WDNI5NOBQFO3TJKDAYSL4HL4YY77NWPS3Z2VA====\"}"
      var json="{\"accessKeyId\":\"XXJHETO535J2MRBFJZQPC62XI7A5F6FN6AKXMQ6BAU4W6ZLAT6PA====\",\"accessKeySecret\":\"EWSBWIWRIROAXV5WDNI5NOBQFO3TJKDAYSL4HL4YY77NWPS3Z2VA====\"}"
      
      
      /*val authKey = "basic VlZfRGF0YUxha2UxQENhcGdlbWluaTE6NDU5NzI3Mw=="
      val api_url = "https://api.incontact.com/InContactAuthorizationServer/Token"
      val mediaType = MediaType.parse("text/plain");*/
      
      
      val api_url = "https://na1.nice-incontact.com/authentication/v1/token/access-key"
      val mediaType = MediaType.parse("application/json");

      // json request body
      val body = RequestBody.create(mediaType, json);

      val request = new Request.Builder()
        .url(api_url)
        .method("POST", body)
        //.addHeader("Authorization", authKey)
        //.addHeader("Content-Type", "text/plain")
        .addHeader("Content-Type", "application/json")
        .addHeader("cache-control", "no-cache")
        .build();

      var accesToken: String = null;
      val response = httpClient.newCall(request).execute()
      val accesstk=response.body().string()
      if (response.isSuccessful()) {
        import spark.implicits._
        val outputdf = spark.sparkContext.parallelize(Seq(accesstk)).toDF("token")
        val token=  outputdf.select("token").rdd.map { x => x.toString }
        val df= spark.read.json(token)
        accesToken=df.select("access_token").collect()(0).getString(0)
        println("accesToken")
        println(accesToken)
        
      }
      return accesToken
    } catch {
      case e: Exception => {
        val log = LogManager.getRootLogger
        log.setLevel(Level.INFO)
        log.info("Exception stack trace  " + e.printStackTrace());
        throw new Exception("General Exception..please check stacktrace ")
      }
    }
  }

  def geJsonMessage(access_token: String): String = {
    
   // val url = "https://api-c29.incontact.com/inContactAPI/services/v17.0/skills"
	val url = "https://api-na1.niceincontact.com/inContactAPI/services/v18.0/skills"

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)

    val http = new OkHttpClient();

    val request = new Request.Builder()
      .url(url)
      .header("Authorization", "bearer " + access_token)
      .header("Accept", "application/json")
      .header("Content-Type", "application/json")
      .build();

    var message: String = null

    try {
      val response = http.newCall(request).execute()
      if (response.isSuccessful()) {
        message = response.body().string();
      }
      return message
    } catch {
      case e: Exception => { log.info("Exception stack trace  " + e.printStackTrace()); throw new Exception("General Exception..please check stacktrace ") }
    }
  }
}