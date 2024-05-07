package com.virginvoyages.mxp.parser

import org.apache.spark.SparkConf
import java.sql.SQLException

import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }

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



object ReviewaskParser {

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
      log.info("After metadata call "+  batch_instance_id1 + "," + batch_id1)
      val reviewAskDF = spark.sql(spark.sparkContext.getConf.get("spark.source.table"))
      log.info("reviewAskDF.printSchema()"+reviewAskDF.printSchema())
      var Data = reviewAskDF.select("message", "batchtime", "part_date").where($"BatchTime" >= lit(batch_start_time).cast(TimestampType) && $"BatchTime" < lit(batch_end_time).cast(TimestampType) and ($"part_date".between(part_start_time, part_end_time)))
      
      var askMsg = Data.select("message").rdd.map { x => x.toString }

      var askData = spark.read.json(askMsg)
      import spark.implicits._
      import org.apache.spark.sql.functions.col
      // askData.printSchema()
	  if (!askData.head(1).isEmpty) {
      
	  
	  if (hasColumn(askData, "ID")) {
                        askData = askData.withColumn("ID", when(askData.col("Id").isNotNull, askData.col("Id")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("Id",lit(null))
                      }
					  
					  if (hasColumn(askData, "Url")) {
                        askData = askData.withColumn("Url", when(askData.col("Url").isNotNull, askData.col("Url")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("Url",lit(null))
                      }
					   if (hasColumn(askData, "PersonId")) {
                        askData = askData.withColumn("PersonId", when(askData.col("PersonId").isNotNull, askData.col("PersonId")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("PersonId",lit(null))
                      }
					  if (hasColumn(askData, "ShipCode")) {
                        askData = askData.withColumn("ShipCode", when(askData.col("ShipCode").isNotNull, askData.col("ShipCode")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("ShipCode",lit(null))
                      }
					  if (hasColumn(askData, "LocationId")) {
                        askData = askData.withColumn("LocationId", when(askData.col("LocationId").isNotNull, askData.col("LocationId")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("LocationId",lit(null))
                      }
					  if (hasColumn(askData, "VisitTimestamp")) {
                        askData = askData.withColumn("Visited_Timestamp", when(askData.col("VisitTimestamp").isNotNull, askData.col("VisitTimestamp")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("Visited_Timestamp",lit(null))
                      }
					  if (hasColumn(askData, "CreatedTimestamp")) {
                        askData = askData.withColumn("Created_Timestamp", when(askData.col("CreatedTimestamp").isNotNull, askData.col("CreatedTimestamp")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("Created_Timestamp",lit(null))
                      }
					   if (hasColumn(askData, "PushTimestamp")) {
                        askData = askData.withColumn("Push_Timestamp", when(askData.col("PushTimestamp").isNotNull, askData.col("PushTimestamp")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("Push_Timestamp",lit(null))
                      }
					   if (hasColumn(askData, "TargetGroup")) {
                        askData = askData.withColumn("TargetGroup", when(askData.col("TargetGroup").isNotNull, askData.col("TargetGroup")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("TargetGroup",lit(null))
                      }
					   if (hasColumn(askData, "Header")) {
                        askData = askData.withColumn("Header", when(askData.col("Header").isNotNull, askData.col("Header")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("Header",lit(null))
                      }
					   if (hasColumn(askData, "PrimaryQuestion.Type")) {
                        askData = askData.withColumn("PrimaryQuestion_Type", when(askData.col("PrimaryQuestion.Type").isNotNull, askData.col("PrimaryQuestion.Type")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("PrimaryQuestion_Type",lit(null))
                      }
					    if (hasColumn(askData, "PrimaryQuestion.Id")) {
                        askData = askData.withColumn("PrimaryQuestion_ID", when(askData.col("PrimaryQuestion.Id").isNotNull, askData.col("PrimaryQuestion.Id")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("PrimaryQuestion_ID",lit(null))
                      }
					   if (hasColumn(askData, "PrimaryQuestion.Text")) {
                        askData = askData.withColumn("PrimaryQuestion_Text", when(askData.col("PrimaryQuestion.Text").isNotNull, askData.col("PrimaryQuestion.Text")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("PrimaryQuestion_Text",lit(null))
                      }
					  if (hasColumn(askData, "PrimaryQuestion.Answer")) {
                        askData = askData.withColumn("PrimaryQuestion_Answer", when(askData.col("PrimaryQuestion.Answer").isNotNull, askData.col("PrimaryQuestion.Answer")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("PrimaryQuestion_Answer",lit(null))
                      }
					   if (hasColumn(askData, "PrimaryQuestion.Category")) {
                        askData = askData.withColumn("PrimaryQuestion_Category", when(askData.col("PrimaryQuestion.Category").isNotNull, askData.col("PrimaryQuestion.Category")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("PrimaryQuestion_Category",lit(null))
                      }
					   if (hasColumn(askData, "PrimaryQuestion.MasterQuestion")) {
                        askData = askData.withColumn("Primary_MasterQuestion", when(askData.col("PrimaryQuestion.MasterQuestion").isNotNull, askData.col("PrimaryQuestion.MasterQuestion")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("ClosingQuestion.Type",lit(null))
                      }
					  if (hasColumn(askData, "ClosingQuestion.Type")) {
                        askData = askData.withColumn("ClosingQuestion_Type", when(askData.col("ClosingQuestion.Type").isNotNull, askData.col("ClosingQuestion.Type")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("ClosingQuestion_Type",lit(null))
                      }
					   if (hasColumn(askData, "ClosingQuestion.Id")) {
                        askData = askData.withColumn("ClosingQuestion_ID", when(askData.col("ClosingQuestion.Id").isNotNull, askData.col("ClosingQuestion.Id")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("ClosingQuestion_ID",lit(null))
                      }
					  if (hasColumn(askData, "ClosingQuestion.Text")) {
                        askData = askData.withColumn("ClosingQuestion_Text", when(askData.col("ClosingQuestion.Text").isNotNull, askData.col("ClosingQuestion.Text")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("ClosingQuestion_Text",lit(null))
                      }
					  if (hasColumn(askData, "ClosingQuestion.Answer")) {
                        askData = askData.withColumn("ClosingQuestion_Answer", when(askData.col("ClosingQuestion.Answer").isNotNull, askData.col("ClosingQuestion.Answer")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("ClosingQuestion_Answer",lit(null))
                      }
					  if (hasColumn(askData, "ClosingQuestion.Category")) {
                        askData = askData.withColumn("ClosingQuestion_Category", when(askData.col("ClosingQuestion.Category").isNotNull, askData.col("ClosingQuestion.Category")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("ClosingQuestion_Category",lit(null))
                      }
					   if (hasColumn(askData, "ClosingQuestion.MasterQuestion")) {
                        askData = askData.withColumn("Closing_MasterQuestion", when(askData.col("ClosingQuestion.MasterQuestion").isNotNull, askData.col("ClosingQuestion.MasterQuestion")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("Closing_MasterQuestion",lit(null))
                      }
					  if (hasColumn(askData, "Questions")) {
                        askData = askData.withColumn("Questions", when(askData.col("Questions").isNotNull, askData.col("Questions")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("Questions",lit(null))
                      }
					  
					  
      askData =
        askData.
		//withColumn("ID", when(askData.col("Id").isNotNull, askData.col("Id")).otherwise(lit(null)))
         // .withColumn("Url", when(askData.col("Url").isNotNull, askData.col("Url")).otherwise(lit(null)))
          //.withColumn("PersonId", when(askData.col("PersonId").isNotNull, askData.col("PersonId")).otherwise(lit(null)))
          //.withColumn("ShipCode", when(askData.col("ShipCode").isNotNull, askData.col("ShipCode")).otherwise(lit(null)))
         /* .withColumn("LocationId", when(askData.col("LocationId").isNotNull, askData.col("LocationId")).otherwise(lit(null)))
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
          .withColumn("Questions", when(askData.col("Questions").isNotNull, askData.col("Questions")).otherwise(lit(null)))*/
          withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
          .withColumn("Batchtime", lit(start_execution_time).cast(TimestampType))
          .withColumn("Part_date", to_date(lit(part_write_date)))

      var questionData = askData.select("ID", "Url", "PersonId", "ShipCode", "LocationId", "Visited_Timestamp",
          "Created_Timestamp", "Push_Timestamp", "TargetGroup", "Header", "PrimaryQuestion_Type", "PrimaryQuestion_ID",
          "PrimaryQuestion_Text", "PrimaryQuestion_Answer", "PrimaryQuestion_Category", "Primary_MasterQuestion",
          "ClosingQuestion_Type", "ClosingQuestion_ID", "ClosingQuestion_Text", "ClosingQuestion_Answer", 
          "ClosingQuestion_Category", "Closing_MasterQuestion", "VoyageID", "Batchtime", "Part_date")
     // questionData.show

      if (hasColumn(askData, "Questions")) {
        if (checkArray(askData, "Questions")) {
          
          askData = askData.withColumn("QuestionsData", explode_outer(askData.col("Questions")))

  if (hasColumn(askData, "QuestionsData.Type")) {
                        askData = askData.withColumn(
            "Question_Type", when(askData.col("QuestionsData.Type").isNotNull, askData.col("QuestionsData.Type")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("Question_Type",lit(null))
                      }
					   if (hasColumn(askData, "QuestionsData.Id")) {
                        askData = askData.withColumn("Question_Id", when(askData.col("QuestionsData.Id").isNotNull, askData.col("QuestionsData.Id")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("Question_Id",lit(null))
                      }
					  if (hasColumn(askData, "QuestionsData.Text")) {
                        askData = askData.withColumn("Question_Text", when(askData.col("QuestionsData.Text").isNotNull, askData.col("QuestionsData.Text")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("Question_Text",lit(null))
                      }
					  if (hasColumn(askData, "QuestionsData.Answer")) {
                        askData = askData.withColumn("Question_Answer", when(askData.col("QuestionsData.Answer").isNotNull, askData.col("QuestionsData.Answer")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("Question_Answer",lit(null))
                      }
					  if (hasColumn(askData, "QuestionsData.Category")) {
                        askData = askData.withColumn("Question_Category", when(askData.col("QuestionsData.Category").isNotNull, askData.col("QuestionsData.Category")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("Question_Category",lit(null))
                      }
if (hasColumn(askData, "QuestionsData.MasterQuestion")) {
                        askData = askData.withColumn("MasterQuestion", when(askData.col("QuestionsData.MasterQuestion").isNotNull, askData.col("QuestionsData.MasterQuestion")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("MasterQuestion",lit(null))
                      }
					  if (hasColumn(askData, "QuestionsData.Category")) {
                        askData = askData.withColumn("Question_Category", when(askData.col("QuestionsData.Category").isNotNull, askData.col("QuestionsData.Category")).otherwise(lit(null)))
                      }
                      else {
                        askData = askData.withColumn("Question_Category",lit(null))
                      }
					  
          askData = askData /*withColumn(
            "Question_Type", when(askData.col("QuestionsData.Type").isNotNull, askData.col("QuestionsData.Type")).otherwise(lit(null)))
            .withColumn("ID", when(askData.col("Id").isNotNull, askData.col("Id")).otherwise(lit(null)))
            .withColumn("Question_Id", when(askData.col("QuestionsData.Id").isNotNull, askData.col("QuestionsData.Id")).otherwise(lit(null)))
            .withColumn("Question_Text", when(askData.col("QuestionsData.Text").isNotNull, askData.col("QuestionsData.Text")).otherwise(lit(null)))
            .withColumn("Question_Answer", when(askData.col("QuestionsData.Answer").isNotNull, askData.col("QuestionsData.Answer")).otherwise(lit(null)))
            .withColumn("Question_Category", when(askData.col("QuestionsData.Category").isNotNull, askData.col("QuestionsData.Category")).otherwise(lit(null)))
            .withColumn("MasterQuestion", when(askData.col("QuestionsData.MasterQuestion").isNotNull, askData.col("QuestionsData.MasterQuestion")).otherwise(lit(null)))*/
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
      
      var finalData = askData.select("Question_Type", "Question_Id", "Question_Text", "Question_Answer", "Question_Category", "MasterQuestion", "ID","PersonId", "VoyageID", "Batchtime", "Part_date")

      val personDimDf = spark.sql("select distinct person_guid from shipdw.hvtb_mart_dim_person")

      val askDataSeawareIddf = finalData.join(personDimDf,finalData.col("PersonId") === personDimDf.col("person_guid"),"left").select(finalData("*"))
      val finalaskDataSeawareIddf=askDataSeawareIddf.select("Question_Type", "Question_Id", "Question_Text", "Question_Answer", "Question_Category", "MasterQuestion", "ID","PersonId", "VoyageID", "Batchtime", "Part_date")
      
      finalaskDataSeawareIddf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.ask.table"))
      
      val questionDataSeawareIddf = questionData.join(personDimDf,questionData.col("PersonId") === personDimDf.col("person_guid"),"left").select(questionData("*"))
      
      var finalquestionDataSeawareIddf = questionDataSeawareIddf.select("ID","Url","PersonId","ShipCode","LocationId","Visited_Timestamp","Created_Timestamp","Push_Timestamp","TargetGroup","Header","PrimaryQuestion_Type","PrimaryQuestion_ID","PrimaryQuestion_Text","PrimaryQuestion_Answer","PrimaryQuestion_Category","Primary_MasterQuestion","ClosingQuestion_Type","ClosingQuestion_ID","ClosingQuestion_Text","ClosingQuestion_Answer","ClosingQuestion_Category","Closing_MasterQuestion","VoyageID","Batchtime","Part_date")
      
      finalquestionDataSeawareIddf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.question.table"))
                 
      println("final data loading")
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
    } 
	}
	catch {

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