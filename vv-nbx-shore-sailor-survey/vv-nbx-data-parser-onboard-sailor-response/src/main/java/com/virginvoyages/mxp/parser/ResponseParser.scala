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

object ResponseParser {
  
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

      // var reviewDF = spark.sql("select * from shipdw.hvtb_lnd_onboard_sailor_reviewask")
      val reviewDF = spark.sql(spark.sparkContext.getConf.get("spark.source.table"))

      var Data = reviewDF.select("Message", "BatchTime", "Part_Date") //.where($"BatchTime" >= lit(batch_start_time).cast(TimestampType) && $"BatchTime" < lit(batch_end_time).cast(TimestampType) and ($"part_date".between(part_start_time, part_end_time)))

      var reviewMsg = Data.select("Message").rdd.map { x => x.toString }

      var reviewData = spark.read.json(reviewMsg)
      import spark.implicits._
      import org.apache.spark.sql.functions.col
      reviewData.printSchema()
      reviewData.show(1000, false)

      reviewData =
        reviewData.withColumn("Function_Id", when(reviewData.col("Function").isNotNull, reviewData.col("Function")).otherwise(lit(null)))
          .withColumn("RequestId", when(reviewData.col("RequestId").isNotNull, reviewData.col("RequestId")).otherwise(lit(null)))
          /*start of sailor struct*/
          .withColumn("ShipID", when(reviewData.col("Sailor.ShipId").isNotNull, reviewData.col("Sailor.ShipId")).otherwise(lit(null)))
          .withColumn("Sailor_GuestId", when(reviewData.col("Sailor.GuestId").isNotNull, reviewData.col("Sailor.GuestId")).otherwise(lit(null)))
          .withColumn("Sailor_Id", when(reviewData.col("Sailor.SailorId").isNotNull, reviewData.col("Sailor.SailorId")).otherwise(lit(null)))
          .withColumn("Sailor_VoyageId", when(reviewData.col("Sailor.VoyageId").isNotNull, reviewData.col("Sailor.VoyageId")).otherwise(lit(null)))
          /*start of visit struct*/
          .withColumn("Visit_LocationId", when(reviewData.col("Visit.LocationId").isNotNull, reviewData.col("Visit.LocationId")).otherwise(lit(null)))
          .withColumn("Visit_Date", when(reviewData.col("Visit.VisitDate").isNotNull, reviewData.col("Visit.VisitDate")).otherwise(lit(null)))
          /*start of review ask*/
          .withColumn("ReviewAsk_Push", when(reviewData.col("ReviewAsk.Push").isNotNull, reviewData.col("ReviewAsk.Push")).otherwise(lit(null)))
          .withColumn("ReviewAsk_PushDate", when(reviewData.col("ReviewAsk.PushDate").isNotNull, reviewData.col("ReviewAsk.PushDate")).otherwise(lit(null)))
          /*start of review*/
          .withColumn("Review_Id", when(reviewData.col("ReviewAsk.Review.Id").isNotNull, reviewData.col("ReviewAsk.Review.Id")).otherwise(lit(null)))
          .withColumn("Review_TemplateId", when(reviewData.col("ReviewAsk.Review.TemplateId").isNotNull, reviewData.col("ReviewAsk.Review.TemplateId")).otherwise(lit(null)))
          .withColumn("Review_Completed", when(reviewData.col("ReviewAsk.Review.Completed").isNotNull, reviewData.col("ReviewAsk.Review.Completed")).otherwise(lit(null)))
          /*start of quest-primary quest*/
          .withColumn("PrimaryQuestion_Id", when(reviewData.col("ReviewAsk.Review.Questions.PrimaryQuestion.Id").isNotNull, reviewData.col("ReviewAsk.Review.Questions.PrimaryQuestion.Id")).otherwise(lit(null)))
          .withColumn("PrimaryQuestion_Text", when(reviewData.col("ReviewAsk.Review.Questions.PrimaryQuestion.Text").isNotNull, reviewData.col("ReviewAsk.Review.Questions.PrimaryQuestion.Text")).otherwise(lit(null)))
          .withColumn("PrimaryQuestion_Type", when(reviewData.col("ReviewAsk.Review.Questions.PrimaryQuestion.Type").isNotNull, reviewData.col("ReviewAsk.Review.Questions.PrimaryQuestion.Type")).otherwise(lit(null)))
          .withColumn("PrimaryQuestion_Answer", when(reviewData.col("ReviewAsk.Review.Questions.PrimaryQuestion.Answer").isNotNull, reviewData.col("ReviewAsk.Review.Questions.PrimaryQuestion.Answer")).otherwise(lit(null)))
          /*start of ques-slot 1*/
          .withColumn("QuestionSlot1_Id", when(reviewData.col("ReviewAsk.Review.Questions.QuestionSlot1.Id").isNotNull, reviewData.col("ReviewAsk.Review.Questions.QuestionSlot1.Id")).otherwise(lit(null)))
          .withColumn("QuestionSlot1_Icon", when(reviewData.col("ReviewAsk.Review.Questions.QuestionSlot1.Icon").isNotNull, reviewData.col("ReviewAsk.Review.Questions.QuestionSlot1.Icon")).otherwise(lit(null)))
          .withColumn("QuestionSlot1_Text", when(reviewData.col("ReviewAsk.Review.Questions.QuestionSlot1.Text").isNotNull, reviewData.col("ReviewAsk.Review.Questions.QuestionSlot1.Text")).otherwise(lit(null)))
          .withColumn("QuestionSlot1_Type", when(reviewData.col("ReviewAsk.Review.Questions.QuestionSlot1.Type").isNotNull, reviewData.col("ReviewAsk.Review.Questions.QuestionSlot1.Type")).otherwise(lit(null)))
          .withColumn("QuestionSlot1_Answer", when(reviewData.col("ReviewAsk.Review.Questions.QuestionSlot1.Answer").isNotNull, reviewData.col("ReviewAsk.Review.Questions.QuestionSlot1.Answer")).otherwise(lit(null)))
     /* start of ques-slot 2*/
         .withColumn("QuestionSlot2_Id", when(reviewData.col("ReviewAsk.Review.Questions.QuestionSlot2.Id").isNotNull, reviewData.col("ReviewAsk.Review.Questions.QuestionSlot2.Id")).otherwise(lit(null)))
         .withColumn("QuestionSlot2_Icon", when(reviewData.col("ReviewAsk.Review.Questions.QuestionSlot2.Icon").isNotNull, reviewData.col("ReviewAsk.Review.Questions.QuestionSlot2.Icon")).otherwise(lit(null)))
        .withColumn("QuestionSlot2_Text", when(reviewData.col("ReviewAsk.Review.Questions.QuestionSlot2.Text").isNotNull, reviewData.col("ReviewAsk.Review.Questions.QuestionSlot2.Text")).otherwise(lit(null)))
        .withColumn("QuestionSlot2_Type", when(reviewData.col("ReviewAsk.Review.Questions.QuestionSlot2.Type").isNotNull, reviewData.col("ReviewAsk.Review.Questions.QuestionSlot2.Type")).otherwise(lit(null)))
        .withColumn("QuestionSlot2_Answer", when(reviewData.col("ReviewAsk.Review.Questions.QuestionSlot2.Answer").isNotNull, reviewData.col("ReviewAsk.Review.Questions.QuestionSlot2.Answer")).otherwise(lit(null)))
      /*  start of quest-slot 3*/
         .withColumn("QuestionSlot3_Id", when(reviewData.col("ReviewAsk.Review.Questions.QuestionSlot3.Id").isNotNull, reviewData.col("ReviewAsk.Review.Questions.QuestionSlot3.Id")).otherwise(lit(null)))
         .withColumn("QuestionSlot3_Icon", when(reviewData.col("ReviewAsk.Review.Questions.QuestionSlot3.Icon").isNotNull, reviewData.col("ReviewAsk.Review.Questions.QuestionSlot3.Icon")).otherwise(lit(null)))
        .withColumn("QuestionSlot3_Text", when(reviewData.col("ReviewAsk.Review.Questions.QuestionSlot3.Text").isNotNull, reviewData.col("ReviewAsk.Review.Questions.QuestionSlot3.Text")).otherwise(lit(null)))
        .withColumn("QuestionSlot3_Type", when(reviewData.col("ReviewAsk.Review.Questions.QuestionSlot3.Type").isNotNull, reviewData.col("ReviewAsk.Review.Questions.QuestionSlot3.Type")).otherwise(lit(null)))
        .withColumn("QuestionSlot3_Answer", when(reviewData.col("ReviewAsk.Review.Questions.QuestionSlot3.Answer").isNotNull, reviewData.col("ReviewAsk.Review.Questions.QuestionSlot3.Answer")).otherwise(lit(null)))
         /*start of closing quest*/
        .withColumn("ClosingQuestion_ID", when(reviewData.col("ReviewAsk.Review.Questions.ClosingQuestion.Id").isNotNull, reviewData.col("ReviewAsk.Review.Questions.ClosingQuestion.Id")).otherwise(lit(null)))
        .withColumn("ClosingQuestion_Text", when(reviewData.col("ReviewAsk.Review.Questions.ClosingQuestion.Text").isNotNull, reviewData.col("ReviewAsk.Review.Questions.ClosingQuestion.Text")).otherwise(lit(null)))
        .withColumn("ClosingQuestion_Type", when(reviewData.col("ReviewAsk.Review.Questions.ClosingQuestion.Type").isNotNull, reviewData.col("ReviewAsk.Review.Questions.ClosingQuestion.Type")).otherwise(lit(null)))
        .withColumn("ClosingQuestion_Answer", when(reviewData.col("ReviewAsk.Review.Questions.ClosingQuestion.Answer").isNotNull, reviewData.col("ReviewAsk.Review.Questions.ClosingQuestion.Answer")).otherwise(lit(null)))
        .withColumn("BatchTime", lit(start_execution_time).cast(TimestampType))
        .withColumn("Part_date", to_date(lit(part_write_date)))
        .withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
      
      reviewData.printSchema()
      
   var review=reviewData.select("Function_Id","RequestId","ShipID","Sailor_GuestId","Sailor_Id","Sailor_VoyageId","Visit_LocationId","Visit_Date","ReviewAsk_Push","ReviewAsk_PushDate","Review_Id","Review_TemplateId","Review_Completed","BatchTime","VoyageID","Part_date")
      review.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.ans.target.table"))
      review.show()
      var reviewQuesData=reviewData.select("QuestionSlot1_Id","QuestionSlot1_Icon","QuestionSlot1_Text","QuestionSlot1_Type","QuestionSlot1_Answer","QuestionSlot2_Id","QuestionSlot2_Icon","QuestionSlot2_Text","QuestionSlot2_Type","QuestionSlot2_Answer","QuestionSlot3_Id","QuestionSlot3_Icon","QuestionSlot3_Text","QuestionSlot3_Type","QuestionSlot3_Answer","PrimaryQuestion_Id","PrimaryQuestion_Text","PrimaryQuestion_Type","PrimaryQuestion_Answer","ClosingQuestion_ID","ClosingQuestion_Text","ClosingQuestion_Type","ClosingQuestion_Answer","BatchTime","VoyageID","Part_date")
      reviewQuesData.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.ans.ques.target.table"))
      reviewQuesData.show
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
  
}