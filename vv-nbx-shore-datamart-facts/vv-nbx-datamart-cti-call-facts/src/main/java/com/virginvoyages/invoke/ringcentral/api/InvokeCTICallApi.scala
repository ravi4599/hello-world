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
import java.sql.SQLException
import com.virginvoyages.metadataframework.ManageMetadata
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
//import com.virginvoyages.invoke.ringcentral.api.MetaDataDate

object InvokeCTICallApi {
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
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)

    val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
    metadata.productIterator.foreach(println)
     val batch_id1 = metadata._1
      val batch_instance_id1 = metadata._2
    var batchStartTime = metadata._3
    var batchEndTime = metadata._4

      val inputFormat = "yyyy-MM-dd HH:mm:ss"
      val outputFormat = "yyyy-MM-dd HH:mm:ss"
    // val batchStartTime = dateAddSec(startBatchTime, 1, inputFormat, outputFormat)
      
    
    batchStartTime = dateMinusSec(batchStartTime, 7200, inputFormat, outputFormat)
    batchEndTime = dateMinusSec(batchEndTime, 7200, inputFormat, outputFormat)
    println("After conversion starttime:" +batchStartTime + " endtime:"+batchEndTime)
    
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    log.info("batchStartTime:" + batchStartTime)
    log.info("batchEndTime:" + batchEndTime)

    val access_token = getAccessToken(sparkConfiguration, spark)
    log.info("access_token: " + access_token)
    val jsonMessage = geJsonMessage(access_token, batchStartTime, batchEndTime)

    try {

      var cticalldf = Seq(jsonMessage).toDF("Message")

      cticalldf = cticalldf.withColumn("batchtime", unix_timestamp(lit(batchStartTime), "yyyy-MM-dd' 'HH:mm:ss").cast(TimestampType))
        .withColumn("part_date", lit(to_date(col("batchtime"), "yyyy-MM-dd")))

      cticalldf = cticalldf.select("batchtime", "Message", "part_date")
      //cticalldf.show(20, false)
      //cticalldf.printSchema()
        cticalldf.show
      cticalldf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.source.cticall.table"))
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark);
    } catch {

      case e: Exception =>
        {
          log.info("in the catch of updateStatus ****************** ")
          e.printStackTrace()
          throw new Exception("General Exception..please check the stacktrace")
        }
    }
  }

  def getAccessToken(configMap: Broadcast[Map[String, String]], spark: SparkSession): String = {
    try {
      val httpClient = new OkHttpClient();
      val log = LogManager.getRootLogger
      log.setLevel(Level.INFO)

      // json formatted data
      /*    var json = "{ \"username\":\"ashwin.john@capgemini.com\", \"password\":\"VV$Feb$022020\", \"grant_type\":\"password\"}"
      val authKey = "basic VlZfRGF0YUxha2UxQENhcGdlbWluaTE6NDU5NzI3Mw=="
      val api_url = "https://api.incontact.com/InContactAuthorizationServer/Token"
      val mediaType = MediaType.parse("text/plain");

*/
      //var json = "{\"accessKeyId\":\"65PY723U5MHD53CMHWSOM672QM352EBLQXWGNJI54727LK2ZLETA====\",\"accessKeySecret\":\"CI5SPEZFLU57T6QNVJAPAZQ2CWHJB43D5XGNTRY2T6AAXFQ2WQEQ====\"}"
  var json = "{\"accessKeyId\":\"XXJHETO535J2MRBFJZQPC62XI7A5F6FN6AKXMQ6BAU4W6ZLAT6PA====\",\"accessKeySecret\":\"EWSBWIWRIROAXV5WDNI5NOBQFO3TJKDAYSL4HL4YY77NWPS3Z2VA====\"}"
      val api_url = "https://na1.nice-incontact.com/authentication/v1/token/access-key"
      val mediaType = MediaType.parse("application/json");
      // json request body
      val body = RequestBody.create(mediaType, json);

      val request = new Request.Builder()
        .url(api_url)
        .method("POST", body)
        .addHeader("Content-Type", "application/json")
        .addHeader("cache-control", "no-cache")
        .build();

      var accesToken: String = null;
      val response = httpClient.newCall(request).execute()
      import spark.implicits._
      val accesstk = response.body().string()
      if (response.isSuccessful()) {
        val outputdf = spark.sparkContext.parallelize(Seq(accesstk)).toDF("token")
        val token = outputdf.select("token").rdd.map { x => x.toString }
        val df = spark.read.json(token)
        accesToken = df.select("access_token").collect()(0).getString(0)
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

  def geJsonMessage(access_token: String, batch_start_time: String, batch_end_time: String): String = {
    val batch_s_date = batch_start_time.substring(0, 10)
    val batch_s_time = batch_start_time.substring(11)
    val batch_e_date = batch_end_time.substring(0, 10)
    val batch_e_time = batch_end_time.substring(11)

    val batch_api_s_datetime = batch_s_date + "T" + batch_s_time + "Z"
    val batch_api_e_datetime = batch_e_date + "T" + batch_e_time + "Z"

  val url = "https://api-na1.niceincontact.com/inContactAPI/services/v18.0/contacts/completed?startDate="+batch_api_s_datetime+"&endDate="+batch_api_e_datetime+"&mediaTypeId=4"

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

  def dateMinusSec(date: String, seconds: Int, inputFormat: String, outputFormat: String): String = {
    import java.util.Calendar
    val dateAux = Calendar.getInstance()
    dateAux.setTime(new SimpleDateFormat(inputFormat).parse(date))
    dateAux.add(Calendar.SECOND, -seconds)
    return new SimpleDateFormat(outputFormat).format(dateAux.getTime())
  }
}