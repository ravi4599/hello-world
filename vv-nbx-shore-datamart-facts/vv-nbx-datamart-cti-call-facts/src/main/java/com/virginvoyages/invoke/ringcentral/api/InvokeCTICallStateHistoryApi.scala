package com.virginvoyages.invoke.ringcentral.api

import java.util.Properties
import java.sql.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ StructType, StructField, TimestampType, StringType, IntegerType, DateType, LongType };
import org.apache.spark.sql.expressions.Window
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import scala.collection.JavaConversions._
import org.apache.spark.sql.functions._
import com.virginvoyages.metadataframework.ManageMetadata
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
import org.apache.spark.broadcast.Broadcast

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

object InvokeCTICallStateHistoryApi {

  def main(args: Array[String]): Unit = {
    /*if (args.length <= 1) {
      println("This job required at least two parameters")
      System.exit(1)
    }

    val batchStartTime1 = args(0)
    val batchEndTime1 = args(1)
    
    val batchStartTime = batchStartTime1.replace("T", " ")
    val batchEndTime = batchEndTime1.replace("T", " ")*/

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
      
        /**********************************MetaData framework**********************************************/
      

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)

    import spark.implicits._
    import org.apache.spark.sql.functions.col

    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    
    val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
      metadata.productIterator.foreach(println)
      val batchStartTime = metadata._3
      val batchEndTime = metadata._4

    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    val whereClause = s"""where batchtime >= from_unixtime(unix_timestamp('$batchStartTime', "yyyy-MM-dd' 'HH:mm:ss")) and batchtime <= from_unixtime(unix_timestamp('$batchEndTime', "yyyy-MM-dd' 'HH:mm:ss")) and part_date>=to_date('$batchStartTime') and part_date<=to_date('$batchEndTime') group by contactId"""
    log.info("select distinct contactId from %s %s".format(sc.getConf.get("spark.target.cticall.table"), whereClause))
    val cticallstatehisdf = spark.sql("select contactId from %s %s".format(sc.getConf.get("spark.target.cticall.table").trim(), whereClause))
    //cticallstatehisdf.printSchema()
    //cticallstatehisdf.show(10, false)
    
    cticallstatehisdf.collect().foreach(row => {

      try {
        val access_token = getAccessToken(sparkConfiguration)
        log.info("access_token: " + access_token)
        val jsonMessage = geJsonMessage(access_token, row.getLong(0))
        
        var cticallstatedf = Seq(jsonMessage).toDF("Message")

        cticallstatedf = cticallstatedf.withColumn("batchtime", unix_timestamp(lit(batchStartTime), "yyyy-MM-dd' 'HH:mm:ss").cast(TimestampType))
                                                    .withColumn("part_date", lit(to_date(col("batchtime"),"yyyy-MM-dd")))
                                                    
        cticallstatedf = cticallstatedf.select("batchtime", "Message", "part_date")
      //cticallstatedf.show(20, false)
      //cticallstatedf.printSchema()                                            
        cticallstatedf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.source.cticallstate.table").trim())

      } catch {

        case e: Exception =>
          {
            log.info("in the catch of updateStatus ****************** ")
            e.printStackTrace()
            throw new Exception("General Exception..please check the stacktrace")
          }
      }
    }) 
  }

  def getAccessToken(configMap: Broadcast[Map[String, String]]): String = {

    val httpClient = new OkHttpClient();
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)

    // json formatted data
    var json = "{ \"username\":\"ashwin.john@capgemini.com\", \"password\":\"VV$Feb$022020\", \"grant_type\":\"password\"}"
    val authKey = "basic VlZfRGF0YUxha2UxQENhcGdlbWluaTE6NDU5NzI3Mw=="
    val api_url = "https://api.incontact.com/InContactAuthorizationServer/Token"
    val mediaType = MediaType.parse("text/plain");

    // json request body
    val body = RequestBody.create(mediaType, json);

    val request = new Request.Builder()
      .url(api_url)
      .method("POST", body)
      .addHeader("Authorization", authKey)
      .addHeader("Content-Type", "text/plain")
      .build();

    try {
      var accesToken: String = null;
      val response = httpClient.newCall(request).execute()
      if (response.isSuccessful()) {
        val result = JSON.parseFull(response.body().string())

        result match {
          case Some(map: Map[String, String]) => map.get("access_token") match {
            case Some(ref) => accesToken = ref
          }
          case None => log.info("Invalid Access Token")
        }
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

  def geJsonMessage(access_token: String, contactID: Long): String = {
    val url = "https://api-c29.incontact.com/inContactAPI/services/v17.0/contacts/" + contactID + "/statehistory"

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val http = new OkHttpClient();
    //log.info("Access Key:" + access_token);
    log.info("URL to call:" + url)

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
