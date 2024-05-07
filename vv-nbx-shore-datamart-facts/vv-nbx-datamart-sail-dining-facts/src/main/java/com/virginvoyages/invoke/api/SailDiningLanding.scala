package com.virginvoyages.invoke.api
import java.text.SimpleDateFormat
import org.apache.spark.sql.expressions.Window
import java.util.Date
import org.apache.spark.storage.StorageLevel
import scala.collection.immutable.Map
import scala.util.parsing.json._
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import scalaj.http.Http
import scalaj.http.HttpOptions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.xml.XML
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml.XmlReader
import scala.util.Try

object SailDiningLanding {
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    def findSailDetail(shipcode: String, date_from: Date, date_to: Date, configMap: Broadcast[Map[String, String]]): String = {

      log.info("Getting Sail Details")

      try {

        //log.info("shipcode " + shipcode)
       // log.info("date_from" + date_from)
       // log.info("date_to" + date_to)
       // println("shipcode " + shipcode)
        //println("date_from" + date_from)
        //println("date_to" + date_to)

        val api_url = configMap.value.get("spark.custapi.url").get
        var sailData  = "<GetAvailDinings_IN><MsgHeader><Version>1.0</Version><CallerInfo><UserInfo><Internal/></UserInfo></CallerInfo><Language>ENG</Language></MsgHeader><SearchParams><Ship>"+shipcode+"</Ship><DateRange><From>"+date_from+"</From><To>"+date_to+"</To></DateRange><TimeOfDay><From>00:00:01</From><To>23:59:59</To></TimeOfDay></SearchParams><SearchOptions><AvailabilityMode>ALL</AvailabilityMode></SearchOptions></GetAvailDinings_IN>"
        val response = Http(api_url).postData(sailData)
          .header("Content-Type", "application/x-versonix-api")
          .header("Charset", "UTF-8")
          .option(HttpOptions.readTimeout(100000)).asString
        log.info("ResponseGiven " + response.body)

        val responseReceived = response.body
        val xml = XML.loadString(responseReceived)

        val sailDetailXml = xml 
        var parsestring = sailDetailXml.toString()
        log.info("ParseString" + parsestring)
        return (parsestring)

      } catch {
        case e: Exception => { e.printStackTrace(); throw new Exception("General Exception..please check stacktrace ") }

      }

    }
    def checkArray(df: DataFrame, colname: String): Boolean = {

      df.schema(colname).dataType match {
        case ArrayType(_, _) => return true
        case _               => return false
      }
    }

    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    val sailLandingTable = spark.sparkContext.getConf.get("spark.landing.table").trim()
    log.info("#--------------------------Sail API-------------------#")
    import spark.implicits._

    println("#-------------------------------API is about to requested for response -----------------------#")
    val cutofmonths = spark.sparkContext.getConf.get("spark.maxcutoff.months").toInt
    val monthindex =  spark.sparkContext.getConf.get("spark.month.index").toInt
    var  shipcodesArray =   spark.sparkContext.getConf.get("spark.ship.codes").split(",")
       var shipDF= shipcodesArray.toSeq.toDF("shipcode")
       var finalshipDF = shipDF.withColumn("shipcode",lit(null))
                               .withColumn("date_from",lit(null))
                               .withColumn("date_to",lit(null))
      for(i<-0 to cutofmonths-1) {
       import spark.implicits._
     shipDF =   shipDF.withColumn("date_from",date_add(last_day(add_months(current_date(),i-monthindex-1)), 1))
                      .withColumn("date_to", last_day(col("date_from")))
        /*shipDF=   shipDF.withColumn("date_from",current_date())
                      .withColumn("date_to", current_date)*/
         finalshipDF =finalshipDF.union(shipDF)
     }
     finalshipDF =  finalshipDF.distinct().where(finalshipDF.col("shipcode").isNotNull)
     finalshipDF.show(100,false)
     val  responseDF =  finalshipDF.rdd.map{ t=>
      val ship = t.getString(0)
      val date_from = t.getDate(1)
      val date_to = t.getDate(2)
      val sailDetail = findSailDetail(ship, date_from, date_to, sparkConfiguration)
                       (ship,date_from, date_to, sailDetail)
       }.toDF("shipcode","date_from", "date_to", "response").withColumn("load_dt",current_date())
       responseDF.write.mode("Overwrite").insertInto(sailLandingTable)
   spark.stop()

  }

}