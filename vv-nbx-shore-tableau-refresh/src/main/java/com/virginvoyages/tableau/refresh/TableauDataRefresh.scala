package com.virginvoyages.tableau.refresh
import java.util.Date
import _root_.com.virginvoyages.tableau.refresh.XREFConnException
import _root_.com.virginvoyages.tableau.refresh.HBaseConException
import _root_.com.virginvoyages.tableau.refresh.StreamConstants
import java.net.UnknownHostException
import scalaj.http.Http

import org.apache.spark.broadcast.Broadcast
import java.text.SimpleDateFormat

import scalaj.http.HttpOptions
import scala.util.parsing.json._

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
import scala.util.Try
import org.apache.spark.sql.DataFrame
import java.time.{ ZonedDateTime, ZoneId }
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{ Column, DataFrame }
import scala.collection.mutable.ArrayBuffer
import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.sql.SaveMode

object TableauDataRefresh {
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
  def getSiteIdAuthCred(configMap: Broadcast[Map[String, String]]): String = {
    try {
      //var postData = "{ \"credentials\": \"" + nativeSourceIDValue + "\"" + "," + "\"referenceTypeID\": \"" + referenceTypeID + "\"," + "\"targetReferenceTypeID\": \"" + targetReferenceTypeID + "\" }"
      var postData = "{   \"credentials\": {     \"name\": \"" + configMap.value.get("spark.tableau.credentials").get + "\",     \"password\": \"" + configMap.value.get("spark.tableau.password").get + "\",     \"site\": {       \"contentUrl\": \"" + configMap.value.get("spark.tableau.contentUrl").get + "\"     }   } }"
      //println(postData)

      val response = Http(configMap.value.get("spark.tableau.authurl").get).postData(postData)

        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(100000)).asString
      log.info("TableauAPI :" + response.body)
      return response.body
    } catch {
      case e: UnknownHostException => {
        e.printStackTrace();
        log.info("XREF Connection Issue");
        throw new XREFConnException("XREF Connection Exception @ getSiteIdAuthCred")
      }
      case e: Exception => {
        log.info("in the catch of getSiteIdAuthCred ******************");
        e.printStackTrace();
        throw new Exception("General Exception..please check the stacktrace")
      }
    }
  }
  def refreshTableauDataSource(token: String, siteid: String, userid: String, refreshURL: String, datasourceid: String): String = {
    try {

      var postData = "<tsRequest></tsRequest>"
      println(postData)

      val url = refreshURL.replace("*siteid*", siteid).replace("*datasourceid*", datasourceid)
      log.info("#--------------------------Refresh URL -----------------#:" + url)
      val response = Http(url).postData(postData)

        .header("Content-Type", "application/xml")
        .header("Charset", "UTF-8")
        .header("X-Tableau-Auth", token)
        .option(HttpOptions.readTimeout(100000)).asString
      log.info("#-----------Data Source ID Refreshed-------# :" + response.body)
      println("#-----------Data Source ID Refreshed-------# :" + response.body)
      return response.body
    } catch {
      case e: UnknownHostException => {
        e.printStackTrace();
        log.info("XREF Connection Issue");
        throw new XREFConnException("XREF Connection Exception @ refreshTableauDataSource")
      }
      case e: Exception => {
        log.info("in the catch of refreshTableauDataSource ******************");
        e.printStackTrace();
        throw new Exception("General Exception..please check the stacktrace")
      }
    }
  }
  def parseAuthResponse(response: String) = {
    val tableauSiteIdAuthresponseXML = scala.xml.XML.loadString(response)
    val credentials = tableauSiteIdAuthresponseXML \\ "credentials"
    val tokenXml = credentials \\ "@token"
    val token = tokenXml.toString()
    val siteXML = tableauSiteIdAuthresponseXML \\ "site"
    val site_idXML = siteXML \\ "@id"
    val siteid = site_idXML.toString()
    val userXML = tableauSiteIdAuthresponseXML \\ "user"
    val user_idXML = userXML \\ "@id"
    val userid = user_idXML.toString()

    (token, siteid, userid)
  }
  def main(args: Array[String]) {
    def getSparkSession() =
      {
        val spark = SparkSession
          .builder()
          .enableHiveSupport()
          .getOrCreate()

        spark

      }
    val spark = getSparkSession()
    val sc = spark.sparkContext

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    val tableauSiteIdAuthresponse = getSiteIdAuthCred(sparkConfiguration)

    val (token, siteid, userid) = parseAuthResponse(tableauSiteIdAuthresponse)
   

    log.info("#--------------------Auth credentials Acquired---------------------#", token, siteid, userid)
    println("#--------------------Auth credentials Acquired---------------------#", token, siteid, userid)
    //val env = spark.sparkContext.getConf.get("spark.tableau.env").trim().toLowerCase()
    
    val refreshURL = spark.sparkContext.getConf.get("spark.tableau.refreshURL")
   // val dataSourceSeq = spark.sparkContext.getConf.get("spark.tableau.dataSourceSeq").split(",")
    //println(dataSourceSeq.toString())
    
    //datasourcesDf.show(false)
    //.collect.foreach{}
    // val processedDF=
    var datasourceName= args(0).toString().toLowerCase()
   
    var env=args(1).toString().toLowerCase()
    val datasourceLocation = spark.sparkContext.getConf.get("spark.tableau.datasources").trim().replace("*env*", env)
    println("gs location:"+datasourceLocation) 
   val datasourcesDf = spark.read.format("csv").option("header", "true").option("delimiter", "|").load(datasourceLocation) 
    
    println(s"""The Datasource Name """+datasourceName+ " environment " +env)
     
//    	if ( args(0).equals(datasourceName)) {
//    	}
    datasourcesDf.collect().foreach(x => {
      println(x.getString(2)+x.getString(3))
      //.toLowerCase()
      println(s"""The Datasource Name """+datasourceName+ " environment " +env)
      if (datasourceName.equals(x.getString(2)) == true && x.getString(3).toLowerCase() == env) {
        val finalResponse = refreshTableauDataSource(token, siteid, userid, refreshURL, x.getString(4))
        Thread.sleep(spark.sparkContext.getConf.get("spark.tableau.sleepTime").toLong)
        log.info("#----------------------------------#" + x.getString(1) + x.getString(3) + x.getString(4) + "------------------------------#")
      }
    })
    //val finalResponse=refreshTableauDataSource(token, siteid, userid, refreshURL, datasourceid)
    log.info("#--------------------Process Ended ---------------------#")
    println("#--------------------Final refresh Done---------------------#")
  }

}