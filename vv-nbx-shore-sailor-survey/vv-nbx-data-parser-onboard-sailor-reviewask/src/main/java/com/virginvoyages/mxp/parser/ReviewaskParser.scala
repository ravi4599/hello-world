package com.virginvoyages.mxp.parser


import org.apache.spark.SparkConf
import java.sql.SQLException
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.Row

import org.apache.spark.sql.functions.{ avg, explode, concat, lit, trim }
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.udf
import java.sql.Struct
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.Column
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Column, DataFrame }
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
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

import com.virginvoyages.metadataframework.ManageMetadata

object ReviewaskParser extends Constants {

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
    //xref calling
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

      val reviewAskDF = spark.sql(spark.sparkContext.getConf.get("spark.source.table"))

      var Data = reviewAskDF.select("Message", "BatchTime", "Part_Date")//.where($"BatchTime" >= lit(batch_start_time).cast(TimestampType) && $"BatchTime" < lit(batch_end_time).cast(TimestampType) and ($"part_date".between(part_start_time, part_end_time)))

      var askMsg = Data.select("Message").rdd.map { x => x.toString }

      var askData = spark.read.json(askMsg)
      import spark.implicits._
      import org.apache.spark.sql.functions.col
      askData.printSchema()
      askData.show(1000, false)

      askData =
        askData.withColumn("ID", when(askData.col("Id").isNotNull, askData.col("Id")).otherwise(lit(null)))
          .withColumn("Url", when(askData.col("Url").isNotNull, askData.col("Url")).otherwise(lit(null)))
          .withColumn("PersonId", when(askData.col("PersonId").isNotNull, askData.col("PersonId")).otherwise(lit(null)))
          .withColumn("ShipCode", when(askData.col("ShipCode").isNotNull, askData.col("ShipCode")).otherwise(lit(null)))
          .withColumn("LocationId", when(askData.col("LocationId").isNotNull, askData.col("LocationId")).otherwise(lit(null)))
          .withColumn("Visited_Timestamp", when(askData.col("VisitTimestamp").isNotNull, askData.col("VisitTimestamp")).otherwise(lit(null)))
          .withColumn("Created_Timestamp", when(askData.col("CreatedTimestamp").isNotNull, askData.col("CreatedTimestamp")).otherwise(lit(null)))
          .withColumn("Push_Timestamp", when(askData.col("PushTimestamp").isNotNull, askData.col("PushTimestamp")).otherwise(lit(null)))
          .withColumn("TargetGroup", when(askData.col("TargetGroup").isNotNull, askData.col("TargetGroup")).otherwise(lit(null)))
          .withColumn("Header", when(askData.col("Header").isNotNull, askData.col("Header")).otherwise(lit(null)))
          .withColumn("PrimaryQuestion_Type", when(askData.col("PrimaryQuestion.Type").isNotNull, askData.col("PrimaryQuestion.Type")).otherwise(lit(null)))
          .withColumn("PrimaryQuestion_ID", when(askData.col("PrimaryQuestion.Id").isNotNull, askData.col("PrimaryQuestion.Id")).otherwise(lit(null)))
          .withColumn("PrimaryQuestion_Text", when(askData.col("PrimaryQuestion.Text").isNotNull, askData.col("PrimaryQuestion.Text")).otherwise(lit(null)))
          .withColumn("PrimaryQuestion_Answer", when(askData.col("PrimaryQuestion.Answer").isNotNull, askData.col("PrimaryQuestion.Answer")).otherwise(lit(null)))
          .withColumn("PrimaryQuestion_Category", when(askData.col("PrimaryQuestion.Category").isNotNull, askData.col("PrimaryQuestion.Category")).otherwise(lit(null)))
          .withColumn("Primary_MasterQuestion", when(askData.col("PrimaryQuestion.MasterQuestion").isNotNull, askData.col("PrimaryQuestion.MasterQuestion")).otherwise(lit(null)))
          .withColumn("ClosingQuestion_Type", when(askData.col("ClosingQuestion.Type").isNotNull, askData.col("ClosingQuestion.Type")).otherwise(lit(null)))
          .withColumn("ClosingQuestion_ID", when(askData.col("ClosingQuestion.Id").isNotNull, askData.col("ClosingQuestion.Id")).otherwise(lit(null)))
          .withColumn("ClosingQuestion_Text", when(askData.col("ClosingQuestion.Text").isNotNull, askData.col("ClosingQuestion.Text")).otherwise(lit(null)))
          .withColumn("ClosingQuestion_Answer", when(askData.col("ClosingQuestion.Answer").isNotNull, askData.col("ClosingQuestion.Answer")).otherwise(lit(null)))
          .withColumn("ClosingQuestion_Category", when(askData.col("ClosingQuestion.Category").isNotNull, askData.col("ClosingQuestion.Category")).otherwise(lit(null)))
          .withColumn("Closing_MasterQuestion", when(askData.col("ClosingQuestion.MasterQuestion").isNotNull, askData.col("ClosingQuestion.MasterQuestion")).otherwise(lit(null)))
          .withColumn("Questions", when(askData.col("Questions").isNotNull, askData.col("Questions")).otherwise(lit(null)))
          .withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
          .withColumn("Batchtime", lit(start_execution_time).cast(TimestampType))
          .withColumn("Part_date", to_date(lit(part_write_date)))

      var questionData = askData.select("ID","Url","PersonId","ShipCode","LocationId", "Visited_Timestamp", "Created_Timestamp", "Push_Timestamp","TargetGroup", "Header", "PrimaryQuestion_Type", "PrimaryQuestion_ID", "PrimaryQuestion_Text","PrimaryQuestion_Answer","PrimaryQuestion_Category","Primary_MasterQuestion", "ClosingQuestion_Type", "ClosingQuestion_ID", "ClosingQuestion_Text","ClosingQuestion_Answer","ClosingQuestion_Category","Closing_MasterQuestion", "VoyageID", "Batchtime", "Part_date")
      questionData.show

      if (hasColumn(askData, "Questions")) {
        if (checkArray(askData, "Questions")) {
          askData = askData.withColumn("QuestionsData", explode_outer(askData.col("Questions")))

          askData = askData.withColumn(
            "Question_Type", when(askData.col("QuestionsData.Type").isNotNull, askData.col("QuestionsData.Type")).otherwise(lit(null)))
            .withColumn("ID", when(askData.col("Id").isNotNull, askData.col("Id")).otherwise(lit(null)))
            .withColumn("Question_Id", when(askData.col("QuestionsData.Id").isNotNull, askData.col("QuestionsData.Id")).otherwise(lit(null)))
            .withColumn("Question_Text", when(askData.col("QuestionsData.Text").isNotNull, askData.col("QuestionsData.Text")).otherwise(lit(null)))
            .withColumn("Question_Answer", when(askData.col("QuestionsData.Answer").isNotNull, askData.col("QuestionsData.Answer")).otherwise(lit(null)))
            .withColumn("Question_Category", when(askData.col("QuestionsData.Category").isNotNull, askData.col("QuestionsData.Category")).otherwise(lit(null)))
            .withColumn("MasterQuestion", when(askData.col("QuestionsData.MasterQuestion").isNotNull, askData.col("QuestionsData.MasterQuestion")).otherwise(lit(null)))
            .withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
            .withColumn("Batchtime", lit(start_execution_time).cast(TimestampType))
            .withColumn("Part_date", to_date(lit(part_write_date)))

        }
      } else {
        askData = askData.withColumn("Question_Type", lit(null))
          .withColumn("Question_Id", lit(null))
          .withColumn("Question_Text", lit(null))
          .withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
          .withColumn("Batchtime", lit(start_execution_time).cast(TimestampType))
          .withColumn("Part_date", to_date(lit(part_write_date)))

      }

      var finalData = askData.select("Question_Type", "Question_Id", "Question_Text","Question_Answer" ,"Question_Category" ,"MasterQuestion" ,"ID", "VoyageID", "Batchtime", "Part_date")
      finalData.show
      println("before xref")
      

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
        println("personguid")
        val accessToken = x.getString(10)
        val idtypeId1 = x.getString(11)
        val targetIdTypeid = x.getString(12)
        val url_for_clientid = findByTypeAndTargetType.value //"https://qa.virginvoyages.com/svc/xref-api/v1/references/search/findByTypeAndTargetType"
        val sewareid_string = findSewareDetail(url_for_clientid, accessToken, personGuid, idtypeId1, targetIdTypeid)
        val seawareid = parseSeawareId(sewareid_string)

        (x.getString(0), x.getString(1), x.getString(2), x.getString(3),x.getString(4),x.getString(5),x.getString(6), x.getString(7), x.getTimestamp(8),seawareid,x.getDate(9))
      }.toDF("Question_Type", "Question_Id", "Question_Text", "Question_Answer","Question_Category","MasterQuestion","ID", "VoyageID", "Batchtime","seaware_id", "Part_date")
      import java.sql._;
      import java.util._

      println("after xref")
      finalData.show()
      finalData.printSchema

      //finalData.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.ask.table"))
      
      println("question data")

      var questionData1 = questionData.withColumn("access_token", lit(access_token))
        .withColumn("idtypeId", lit(idtypeId))
        .withColumn("targetidtypeid", lit(target_idtypeid))
      questionData1.printSchema
      questionData1.show
      // map tranformation for calling xref and getting seaware_id
      questionData1 = questionData.rdd.map { x =>
        val personGuid = x.getString(4).toLowerCase()
        print(personGuid)
        val accessToken = x.getString(7)
        val idtypeId1 = x.getString(8)
        val targetIdTypeid = x.getString(9)
        val url_for_clientid = findByTypeAndTargetType.value //"https://qa.virginvoyages.com/svc/xref-api/v1/references/search/findByTypeAndTargetType"
        val sewareid_string = findSewareDetail(url_for_clientid, accessToken, personGuid, idtypeId1, targetIdTypeid)
        val seawareid = parseSeawareId(sewareid_string)

        (x.getString(0),x.getString(1),x.getString(2),seawareid)
      }.toDF("ID","Url","PersonId","seaware_id")
        
      /*  (x.getString(0), x.getString(1), x.getString(2), x.getString(3),x.getString(4), x.getString(5), x.getString(6), x.getString(7), x.getString(8), x.getString(9), x.getString(10), x.getString(11), x.getString(12), x.getString(13), x.getString(14),x.getString(15),x.getString(16),x.getString(17),x.getString(18),x.getString(19),x.getString(20),x.getString(21),x.getString(22),x.getString(23), x.getTimestamp(24), seawareid, x.getDate(26))
      }.toDF("ID","Url","PersonId","ShipCode","LocationId", "Visited_Timestamp", "Created_Timestamp", "Push_Timestamp","TargetGroup", "Header", "PrimaryQuestion_Type", "PrimaryQuestion_ID", "PrimaryQuestion_Text","PrimaryQuestion_Answer","PrimaryQuestion_Category","Primary_MasterQuestion", "ClosingQuestion_Type", "ClosingQuestion_ID", "ClosingQuestion_Text","ClosingQuestion_Answer","ClosingQuestion_Category","Closing_MasterQuestion", "VoyageID", "Batchtime", "seaware_id", "Part_date")*/

     println("question one data")
      questionData1.show
      println("join after this ")
      //val resGuest=guestDim.join(guestData,guestDim.col("client_id")=== stageItemDF.col("seaware_id"),"left")
      var joinQuestionData=questionData1.join(questionData,questionData1.col("Id")===questionData.col("Id"),"leftouter")
joinQuestionData.show



     questionData1.createOrReplaceTempView("questbl1")
     //val quesdf1=spark.sql("select personid, activitycode from shipdw.hvtb_parse_ars_booking group by personid, activitycode")
     
      questionData.createOrReplaceTempView("questbl")
     // val shorexDf=spark.sql("select c.EXTERNALID,c.NAME,a.personid from cmsTbl c join arsTbl a on c.EXTERNALID=a.activitycode")
 
      
      var df=spark.sql("select q2.seaware_id,q1.* from questbl q1 join questbl1 q2 on q1.ID  == q2.ID")
      println("data ")
      df.show

//var finalQuestiondata=joinQuestionData.select("ID","Url","PersonId","ShipCode","LocationId", "Visited_Timestamp", "Created_Timestamp", "Push_Timestamp","TargetGroup", "Header", "PrimaryQuestion_Type", "PrimaryQuestion_ID", "PrimaryQuestion_Text","PrimaryQuestion_Answer","PrimaryQuestion_Category","Primary_MasterQuestion", "ClosingQuestion_Type", "ClosingQuestion_ID", "ClosingQuestion_Text","ClosingQuestion_Answer","ClosingQuestion_Category","Closing_MasterQuestion", "VoyageID", "Batchtime","seaware_id", "Part_date")

//finalQuestiondata.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.question.table"))

println("final data loading")
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Succesful", spark)
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