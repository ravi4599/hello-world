package com.virginvoyages.mxp.parser
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Column, DataFrame }
import scala.collection.mutable.ArrayBuffer

import java.text.SimpleDateFormat

//XML validator imports
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
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.broadcast.Broadcast
import java.net.UnknownHostException
import scala.util.parsing.json._
import scalaj.http.Http
import scalaj.http.HttpOptions
import org.apache.log4j.LogManager
import org.apache.log4j.Level

import java.text.SimpleDateFormat

import com.virginvoyages.metadataframework.ManageMetadata

object ReviewanswerParser extends Constants {

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
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

    val access_token = getOauthToken(sparkConfiguration)
    val alltpye = findAllTypes(access_token, sparkConfiguration)
    val findByTypeAndTargetType = sc.broadcast(sc.getConf.get("spark.findByTypeAndTargetType"))

    var batch_instance_id1: String = null
    var batch_id1: String = null

    def checkArray(df: DataFrame, colname: String): Boolean = {

      df.schema(colname).dataType match {
        case ArrayType(_, _) => return true
        case _               => return false
      }
    }

    def checkStructType(df: DataFrame, colname: String): Boolean = {
      df.schema(colname).dataType match {
        case StructType(_) => return true
        case _             => return false
      }
    }

    def checkStringType(df: DataFrame, colname: String): Boolean = {
      df.schema(colname).dataType match {
        case StringType => return true
        case _          => return false
      }
    }

    def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

    val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
    metadata.productIterator.foreach(println)

    val batch_start_time = metadata._3
    val batch_end_time = metadata._4
    val part_start_time = metadata._5.toString()
    val part_end_time = metadata._6.toString()
    val start_execution_time = metadata._7.toString()
    val part_write_date = metadata._8.toString()
    batch_instance_id1 = metadata._2
    batch_id1 = metadata._1

    try {

      //sailor-response scala project has been created for updated input file
      val reviewAnswerDF = spark.sql(spark.sparkContext.getConf.get("spark.source.table"))

      var Data = reviewAnswerDF.select("Message", "BatchTime", "Part_Date") //.where($"BatchTime" >= lit(batch_start_time).cast(TimestampType) && $"BatchTime" < lit(batch_end_time).cast(TimestampType) and ($"part_date".between(part_start_time, part_end_time)))

      var answerMsg = Data.select("Message").rdd.map { x => x.toString }

      var ansData = spark.read.json(answerMsg)
      ansData.show
      import spark.implicits._
      import org.apache.spark.sql.functions.col
      ansData.printSchema()
      ansData.show(1000, false)

      ansData =
        ansData.withColumn("ID", when(ansData.col("Id").isNotNull, ansData.col("Id")).otherwise(lit(null)))
          .withColumn("PersonId", when(ansData.col("PersonId").isNotNull, ansData.col("PersonId")).otherwise(lit(null)))
          .withColumn("ShipCode", when(ansData.col("ShipCode").isNotNull, ansData.col("ShipCode")).otherwise(lit(null)))
          .withColumn("LocationId", when(ansData.col("LocationId").isNotNull, ansData.col("LocationId")).otherwise(lit(null)))
          .withColumn("Visited_Timestamp", when(ansData.col("VisitTimestamp").isNotNull, ansData.col("VisitTimestamp")).otherwise(lit(null)))
          .withColumn("Created_Timestamp", when(ansData.col("CreatedTimestamp").isNotNull, ansData.col("CreatedTimestamp")).otherwise(lit(null)))
          .withColumn("Header", when(ansData.col("Header").isNotNull, ansData.col("Header")).otherwise(lit(null)))

          .withColumn("Questions", when(ansData.col("Questions").isNotNull, ansData.col("Questions")).otherwise(lit(null)))
          .withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
          .withColumn("Batchtime", lit(start_execution_time).cast(TimestampType))
          .withColumn("Part_date", to_date(lit(part_write_date)))

      var answerData = ansData.select("ID", "PersonId", "ShipCode", "LocationId", "Visited_Timestamp", "Created_Timestamp", "Header", "VoyageID", "Batchtime", "Part_date")
      answerData.show

      if (hasColumn(ansData, "Questions")) {
        if (checkArray(ansData, "Questions")) {
          ansData = ansData.withColumn("QuestionsData", explode_outer(ansData.col("Questions")))

          ansData = ansData.withColumn(
            "Question_Type", when(ansData.col("QuestionsData.Type").isNotNull, ansData.col("QuestionsData.Type")).otherwise(lit(null)))
            .withColumn("Question_Id", when(ansData.col("QuestionsData.Id").isNotNull, ansData.col("QuestionsData.Id")).otherwise(lit(null)))
            .withColumn("Question_Text", when(ansData.col("QuestionsData.Text").isNotNull, ansData.col("QuestionsData.Text")).otherwise(lit(null)))
            .withColumn("Question_Answer", when(ansData.col("QuestionsData.Answer").isNotNull, ansData.col("QuestionsData.Answer")).otherwise(lit(null)))
            .withColumn("Question_Category", when(ansData.col("QuestionsData.Category").isNotNull, ansData.col("QuestionsData.Category")).otherwise(lit(null)))
            .withColumn("MasterQuestion", when(ansData.col("QuestionsData.MasterQuestion").isNotNull, ansData.col("QuestionsData.MasterQuestion")).otherwise(lit(null)))
            .withColumn("ID", when(ansData.col("Id").isNotNull, ansData.col("Id")).otherwise(lit(null)))
            .withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
            .withColumn("Batchtime", lit(start_execution_time).cast(TimestampType))
            .withColumn("Part_date", to_date(lit(part_write_date)))

        }
      } else {
        ansData = ansData.withColumn("Question_Type", lit(null))
          .withColumn("Question_Id", lit(null))
          .withColumn("Question_Text", lit(null))
          .withColumn("Question_Answer", lit(null))
          .withColumn("Question_Category", lit(null))
          .withColumn("MasterQuestion", lit(null))
          .withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
          .withColumn("Batchtime", lit(start_execution_time).cast(TimestampType))
          .withColumn("Part_date", to_date(lit(part_write_date)))

      }

      var finalData = ansData.select("Question_Type", "Question_Id", "Question_Text", "Question_Answer", "Question_Category", "MasterQuestion", "ID", "VoyageID", "Batchtime", "Part_date")
      finalData.show

      // calling method to get access key from given xref url
      val access_token = getOauthToken(sparkConfiguration)
      // calling method to get type all types of source available
      val alltpye = findAllTypes(access_token, sparkConfiguration)
      // calling method to get idtype for source and target source
      val parsed_all_types = parseFindAllTypes(alltpye)
      var idtypeId = ""
      var target_idtypeid = ""
      val arrayOfId = parsed_all_types.split("@")
      for (id <- arrayOfId) {
        if (id.split(":")(0).equals("idtypeId")) {
          idtypeId = id.split(":")(1)
        } else if (id.split(":")(0).equals("targetidtypeid")) {
          target_idtypeid = id.split(":")(1)
        }

      }
      finalData = finalData.withColumn("access_token", lit(access_token))
        .withColumn("idtypeId", lit(idtypeId))
        .withColumn("targetidtypeid", lit(target_idtypeid))
      finalData.printSchema
      finalData.show
      // map tranformation for calling xref and getting seaware_id
      finalData = finalData.rdd.map { x =>
        val personGuid = x.getString(6).toLowerCase()
        print(personGuid)
        val accessToken = x.getString(10)
        val idtypeId1 = x.getString(11)
        val targetIdTypeid = x.getString(12)
        val url_for_clientid = findByTypeAndTargetType.value //"https://qa.virginvoyages.com/svc/xref-api/v1/references/search/findByTypeAndTargetType"
        val sewareid_string = findSewareDetail(url_for_clientid, accessToken, personGuid, idtypeId1, targetIdTypeid)
        val seawareid = parseSeawareId(sewareid_string)

        (x.getString(0), x.getString(1), x.getString(2), x.getString(3), x.getString(4), x.getString(5), x.getString(6), x.getString(7), x.getTimestamp(8), seawareid, x.getDate(9))
      }.toDF("Question_Type", "Question_Id", "Question_Text", "Question_Answer", "Question_Category", "MasterQuestion", "ID", "VoyageID", "Batchtime", "seaware_id", "Part_date")
      import java.sql._;
      import java.util._

      finalData.show
      println("final data")
      finalData.printSchema
      finalData.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.ans.ques.target.table"))

      answerData = answerData.withColumn("access_token", lit(access_token))
        .withColumn("idtypeId", lit(idtypeId))
        .withColumn("targetidtypeid", lit(target_idtypeid))
      answerData.printSchema
      answerData.show
      // map tranformation for calling xref and getting seaware_id
      answerData = answerData.rdd.map { x =>
        val personGuid = x.getString(1).toLowerCase()
        print(personGuid)
        val accessToken = x.getString(10)
        val idtypeId1 = x.getString(11)
        val targetIdTypeid = x.getString(12)
        val url_for_clientid = findByTypeAndTargetType.value //"https://qa.virginvoyages.com/svc/xref-api/v1/references/search/findByTypeAndTargetType"
        val sewareid_string = findSewareDetail(url_for_clientid, accessToken, personGuid, idtypeId1, targetIdTypeid)
        val seawareid = parseSeawareId(sewareid_string)

        (x.getString(0), x.getString(1), x.getString(2), x.getString(3), x.getString(4), x.getString(5), x.getString(6), x.getString(7), x.getTimestamp(8), seawareid, x.getDate(9))
      }.toDF("ID", "PersonId", "ShipCode", "LocationId", "Visited_Timestamp", "Created_Timestamp", "Header", "VoyageID", "Batchtime", "seaware_id", "Part_date")

      println("answer data")
      answerData.printSchema
      answerData.show

      answerData.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.ans.target.table"))

      println("end")

    } catch {

      case e: Exception =>
        {
          ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark)
          log.info("in the catch of updateStatus ******************")
          e.printStackTrace()
          throw new Exception("General Exception..please check the stacktrace")
        }

    }

  }

  //XREF calling part

  def findSewareDetail(nativeurl: String, access_token: String, nativeSourceIDValue: String, referenceTypeID: String, targetReferenceTypeID: String): String = {
    try {
      var postData = "{ \"nativeSourceIDValue\": \"" + nativeSourceIDValue + "\"" + "," + "\"referenceTypeID\": \"" + referenceTypeID + "\"," + "\"targetReferenceTypeID\": \"" + targetReferenceTypeID + "\" }"

      val response = Http(nativeurl).postData(postData)
        .header("Authorization", "Bearer " + access_token)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(100000)).asString
      log.info("findByNativesourceIdValueAndType :" + response.body)
      return response.body
    } catch {
      case e: UnknownHostException => {
        e.printStackTrace();
        log.info("XREF Connection Issue");
        throw new XREFConnException("XREF Connection Exception @ findByNativesourceIdValueAndType")
      }
      case e: Exception => {
        log.info("in the catch of findByNativesourceIdValueAndType ******************");
        e.printStackTrace();
        throw new Exception("General Exception..please check the stacktrace")
      }
    }
  }
  def findAllTypes(access_token: String, configMap: Broadcast[Map[String, String]]): String = {
    val url = configMap.value.get("spark.findalltypes").get
    val response = Http(url)
      .header("Authorization", "Bearer " + access_token)
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .option(HttpOptions.readTimeout(100000)).asString

    return response.body
  }

  def getOauthToken(configMap: Broadcast[Map[String, String]]): String = {
    log.info("IN getOauthToken Method")
    var accesToken: String = null;
    try {
      var oAuthUrl = configMap.value.get("spark.oauthurl").get
      val response = Http(oAuthUrl).postForm.param("grant_type", CLIENT_CREDENTIALS)
        .param("Username", configMap.value.get("spark.username").get)
        .param("password", configMap.value.get("spark.password").get)
        .header("Authorization", configMap.value.get("spark.authorization").get)
        .option(HttpOptions.readTimeout(100000)).asString

      accesToken = parseSecurityApiResponse(response.body, configMap: Broadcast[Map[String, String]])
    } catch {
      case hbaseex: HBaseConException => {
        log.info("Hbase connecion issue"); throw new HBaseConException
      }
      case xrefex: UnknownHostException => {
        log.info("Xref Token Error"); throw new XREFConnException
      }
      case e: Exception => { e.printStackTrace(); throw new Exception("General Exception..please check stacktrace ") }
    }
    accesToken
  }
  def parseSeawareId(jsonStr: String): String = {
    val result = JSON.parseFull(jsonStr)

    var result_str = "-1"

    result match {
      case Some(map: Map[String, Map[String, List[Map[String, String]]]]) =>
        if (map.contains("_embedded")) {
          val _embedded: Map[String, List[Map[String, String]]] = map.apply("_embedded")
          if (_embedded.contains("references")) {
            val references: List[Map[String, String]] = _embedded.apply("references")
            references.foreach { m: Map[String, String] =>
              if (m.contains("nativeSourceIDValue")) {
                result_str = m.apply("nativeSourceIDValue")
              }
            }
          }

        }
      case None  => log.info("parseFindAllTypes failed !!")
      case other => log.info("Unknown data structure: " + other); throw new XREFConnException("parseFindAllTypes parsing failed")
    }

    return result_str
  }
  def parseFindAllTypes(jsonStr: String): String = {
    val result = JSON.parseFull(jsonStr)
    var result_str = ""
    val refTypeNativeSouceIDList: Map[String, String] = Map()
    result match {
      case Some(list: List[Map[String, String]]) => list.foreach {
        mapList: Map[String, String] =>
          if (mapList.apply("referenceType") == "VXP - Guest") {
            result_str = result_str + "@" + "idtypeId" + ":" + mapList.apply("referenceTypeID")

          }
          if (mapList.apply("referenceType") == "Client") {
            result_str = result_str + "@" + "targetidtypeid" + ":" + mapList.apply("referenceTypeID")

          }

      }
      case None  => log.info("parseFindAllTypes failed !!")
      case other => log.info("Unknown data structure: " + other); throw new XREFConnException("parseFindAllTypes parsing failed")
    }

    return result_str
  }
  def parseSecurityApiResponse(jsonStr: String, configMap: Broadcast[Map[String, String]]): String = {
    log.info("IN parseSecurityApiResponse Method")
    var accesToken: String = null;
    try {
      val result = JSON.parseFull(jsonStr)
      result match {

        case Some(map: Map[String, String]) => map.get(ACCESS_TOKEN) match {
          case Some(ref) =>
            try {
              accesToken = ref
            } catch {
              case e: Exception => { e.printStackTrace(); throw new Exception }
            }
          case None => ""
        }
        case Some(map: Map[String, String]) => map.get(ACCESS_TOKEN_ERR) match {
          case Some(err) => {
            throw new XREFConnException("XREF Access Token Issue")
          }
        }
        case None  => log.info("parseSecurityApiResponse Parsing failed !!")
        case other => log.info("Unknown data structure: " + other)
      }
    } catch {
      case hbaseex: HBaseConException => {
        log.info("Hbase connecion issue"); throw new HBaseConException
      }
      case xrefex: XREFConnException => {
        log.info("XREF Access Token Exception ")
        throw new XREFConnException
      }
      case e: Exception => { e.printStackTrace(); throw new Exception("HBase Connection issue from findTypeByName -- Propagate the issue") }
    }
    accesToken
  }

}