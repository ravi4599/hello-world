package com.virginvoyages.mxp.parser
import scala.util.Try

import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode_outer
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType

import com.virginvoyages.metadataframework.ManageMetadata

object ReviewanswerParser {

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
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

      //sailor-response scala project has been created for updated input file
      val whereClause = s""" where BatchTime >= '$batch_start_time' and BatchTime <='$batch_end_time' and part_date>='$part_start_time' and part_date<='$part_end_time'"""
      val reviewAnswerDF = spark.sql(spark.sparkContext.getConf.get("spark.source.table").trim()+ whereClause)

      
      
      //var Data = reviewAnswerDF.select("message", "batchtime", "part_date").+ whereClause//.
      //.where($"BatchTime" >= lit(batch_start_time).cast(TimestampType) && $"BatchTime" < lit(batch_end_time).cast(TimestampType) and ($"part_date".between(part_start_time, part_end_time)))
      

      var answerMsg = reviewAnswerDF.select("Message").rdd.map { x => x.toString }

      var ansData = spark.read.json(answerMsg)
	  
	  if (!ansData.head(1).isEmpty) {
      // ansData.show
     // ansData.printSchema()
     // ansData.show(1000, false)
      ansData.printSchema() 
if (hasColumn(ansData, "ID")) {
                        ansData = ansData.withColumn("ID", when(ansData.col("Id").isNotNull, ansData.col("Id")).otherwise(lit(null)))
                      }
                      else {
                        ansData = ansData.withColumn("Id",lit(null))
                      }
if (hasColumn(ansData, "PersonId")) {
                        ansData = ansData.withColumn("PersonId", when(ansData.col("PersonId").isNotNull, ansData.col("PersonId")).otherwise(lit(null)))
                      }
                      else {
                        ansData = ansData.withColumn("PersonId",lit(null))
                      }
if (hasColumn(ansData, "ShipCode")) {
                        ansData = ansData.withColumn("ShipCode", when(ansData.col("ShipCode").isNotNull, ansData.col("ShipCode")).otherwise(lit(null)))
                      }
                      else {
                        ansData = ansData.withColumn("ShipCode",lit(null))
                      }
if (hasColumn(ansData, "LocationId")) {
                        ansData = ansData.withColumn("LocationId", when(ansData.col("LocationId").isNotNull, ansData.col("LocationId")).otherwise(lit(null)))
                      }
                      else {
                        ansData = ansData.withColumn("LocationId",lit(null))
                      }
if (hasColumn(ansData, "VisitTimestamp")) {
                        ansData = ansData.withColumn("Visited_Timestamp", when(ansData.col("VisitTimestamp").isNotNull, ansData.col("VisitTimestamp")).otherwise(lit(null)))
                      }
                      else {
                        ansData = ansData.withColumn("Visited_Timestamp",lit(null))
                      }
if (hasColumn(ansData, "CreatedTimestamp")) {
                        ansData = ansData.withColumn("Created_Timestamp", when(ansData.col("CreatedTimestamp").isNotNull, ansData.col("CreatedTimestamp")).otherwise(lit(null)))
                      }
                      else {
                        ansData = ansData.withColumn("Created_Timestamp",lit(null))
                      }
if (hasColumn(ansData, "Header")) {
                        ansData = ansData.withColumn("Header", when(ansData.col("Header").isNotNull, ansData.col("Header")).otherwise(lit(null)))
                      }
                      else {
                        ansData = ansData.withColumn("Header",lit(null))
                      }
if (hasColumn(ansData, "Questions")) {
                        ansData = ansData.withColumn("Questions", when(ansData.col("Questions").isNotNull, ansData.col("Questions")).otherwise(lit(null)))
                      }
                      else {
                        ansData = ansData.withColumn("Questions",lit(null))
                      }
      ansData =
        ansData.//withColumn("ID", when(ansData.col("Id").isNotNull, ansData.col("Id")).otherwise(lit(null)))
         // withColumn("PersonId", when(ansData.col("PersonId").isNotNull, ansData.col("PersonId")).otherwise(lit(null)))
        //  .withColumn("ShipCode", when(ansData.col("ShipCode").isNotNull, ansData.col("ShipCode")).otherwise(lit(null)))
        //  .withColumn("LocationId", when(ansData.col("LocationId").isNotNull, ansData.col("LocationId")).otherwise(lit(null)))
        //  .withColumn("Visited_Timestamp", when(ansData.col("VisitTimestamp").isNotNull, ansData.col("VisitTimestamp")).otherwise(lit(null)))
        //  .withColumn("Created_Timestamp", when(ansData.col("CreatedTimestamp").isNotNull, ansData.col("CreatedTimestamp")).otherwise(lit(null)))
         // .withColumn("Header", when(ansData.col("Header").isNotNull, ansData.col("Header")).otherwise(lit(null)))

         // .withColumn("Questions", when(ansData.col("Questions").isNotNull, ansData.col("Questions")).otherwise(lit(null)))
          withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
          .withColumn("Batchtime", lit(start_execution_time).cast(TimestampType))
          .withColumn("Part_date", to_date(lit(part_write_date)))

      var answerData = ansData.select("ID", "PersonId", "ShipCode", "LocationId", "Visited_Timestamp", "Created_Timestamp", "Header", "VoyageID", "Batchtime", "Part_date")

      if (hasColumn(ansData, "Questions")) {
        if (checkArray(ansData, "Questions")) {
          ansData = ansData.withColumn("QuestionsData", explode_outer(ansData.col("Questions")))

if (hasColumn(ansData, "QuestionsData.Type")) {
                        ansData = ansData.withColumn(
            "Question_Type", when(ansData.col("QuestionsData.Type").isNotNull, ansData.col("QuestionsData.Type")).otherwise(lit(null)))
                      }
                      else {
                        ansData = ansData.withColumn("Question_Type",lit(null))
                      }
					  
					  if (hasColumn(ansData, "QuestionsData.Id")) {
                        ansData = ansData.withColumn("Question_Id", when(ansData.col("QuestionsData.Id").isNotNull, ansData.col("QuestionsData.Id")).otherwise(lit(null)))
                      }
                      else {
                        ansData = ansData.withColumn("QuestionsData.Text",lit(null))
                      }
					   if (hasColumn(ansData, "QuestionsData.Text")) {
                        ansData = ansData.withColumn("Question_Text", when(ansData.col("QuestionsData.Text").isNotNull, ansData.col("QuestionsData.Text")).otherwise(lit(null)))
                      }
                      else {
                        ansData = ansData.withColumn("Question_Text",lit(null))
                      }
					   if (hasColumn(ansData, "QuestionsData.Answer")) {
                        ansData = ansData.withColumn("Question_Answer", when(ansData.col("QuestionsData.Answer").isNotNull, ansData.col("QuestionsData.Answer")).otherwise(lit(null)))
                      }
                      else {
                        ansData = ansData.withColumn("Question_Answer",lit(null))
                      }
					  if (hasColumn(ansData, "QuestionsData.Category")) {
                        ansData = ansData .withColumn("Question_Category", when(ansData.col("QuestionsData.Category").isNotNull, ansData.col("QuestionsData.Category")).otherwise(lit(null)))
                      }
                      else {
                        ansData = ansData.withColumn("Question_Category",lit(null))
                      }
					  if (hasColumn(ansData, "QuestionsData.MasterQuestion")) {
                        ansData = ansData.withColumn("MasterQuestion", when(ansData.col("QuestionsData.MasterQuestion").isNotNull, ansData.col("QuestionsData.MasterQuestion")).otherwise(lit(null)))
                      }
                      else {
                        ansData = ansData.withColumn("MasterQuestion",lit(null))
                      }
					 /* if (hasColumn(ansData, "Id")) {
                        ansData = ansData.withColumn("ID", when(ansData.col("Id").isNotNull, ansData.col("Id")).otherwise(lit(null)))
                      }
                      else {
                        ansData = ansData.withColumn("ID",lit(null))
                      }*/
          ansData = ansData
		   /*.withColumn(
            "Question_Type", when(ansData.col("QuestionsData.Type").isNotNull, ansData.col("QuestionsData.Type")).otherwise(lit(null)))
            .withColumn("Question_Id", when(ansData.col("QuestionsData.Id").isNotNull, ansData.col("QuestionsData.Id")).otherwise(lit(null)))
            .withColumn("Question_Text", when(ansData.col("QuestionsData.Text").isNotNull, ansData.col("QuestionsData.Text")).otherwise(lit(null)))
            .withColumn("Question_Answer", when(ansData.col("QuestionsData.Answer").isNotNull, ansData.col("QuestionsData.Answer")).otherwise(lit(null)))
            .withColumn("Question_Category", when(ansData.col("QuestionsData.Category").isNotNull, ansData.col("QuestionsData.Category")).otherwise(lit(null)))
            .withColumn("MasterQuestion", when(ansData.col("QuestionsData.MasterQuestion").isNotNull, ansData.col("QuestionsData.MasterQuestion")).otherwise(lit(null)))*/
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
ansData.printSchema() 
      var ansDataDf = ansData.select("Question_Type", "Question_Id", "Question_Text", "Question_Answer", "Question_Category", "MasterQuestion", "ID", "PersonId", "VoyageID", "Batchtime", "Part_date")
      val personDimDf = spark.sql("select distinct person_guid from shipdw.hvtb_mart_dim_person")

      val ansQDataSeawareIddf = ansDataDf.join(personDimDf, ansDataDf.col("PersonId") === personDimDf.col("person_guid"), "left").select(ansDataDf("*"))
      val finalansDataSeawareIddf = ansQDataSeawareIddf.select("Question_Type", "Question_Id", "Question_Text", "Question_Answer", "Question_Category", "MasterQuestion", "ID", "PersonId", "VoyageID", "Batchtime","Part_date")

      println("final data")

      finalansDataSeawareIddf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.ans.ques.target.table"))

      var ansDataSeawareIddf = answerData.join(personDimDf, answerData.col("PersonId") === personDimDf.col("person_guid"), "left").select(answerData("*"))
      ansDataSeawareIddf = ansDataSeawareIddf.select("ID", "PersonId", "ShipCode", "LocationId", "Visited_Timestamp", "Created_Timestamp", "Header", "VoyageID", "Batchtime", "Part_date")

      println("answer data")
     // answerData.printSchema

      ansDataSeawareIddf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.ans.target.table"))
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
