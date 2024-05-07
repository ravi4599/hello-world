package com.virginvoyages.dimension

import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import org.apache.spark.sql.types.{ StructType, StructField, TimestampType, StringType, IntegerType, DateType, LongType };
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.functions._
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact
import com.virginvoyages.metadataframework.ManageMetadata

object SurveyQuestionsDimLoad {
  def main(args: Array[String]): Unit = {
    /*if (args.length <= 1) {
      println("This job required at least two parameters")
      System.exit(1)
    }*/
    
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)

    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
    metadata.productIterator.foreach(println)
    val batch_id1 = metadata._1
    val batch_instance_id1 = metadata._2
    val batch_start_tme = metadata._3
    val batch_end_tme = metadata._4
    val part_read_start = metadata._5
    val part_read_end = metadata._6
    val start_execution_time = metadata._7
    val part_write_date = metadata._8
    
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    import spark.implicits._

    try {      
      val whereClause = s"""where hvtb_parse_sailor_review_question.batchtime>= '$batch_start_tme' and hvtb_parse_sailor_review_question.batchtime<='$batch_end_tme' and hvtb_parse_sailor_review_question.part_date>='$part_read_start' and hvtb_parse_sailor_review_question.part_date<='$part_read_end'"""
      
      println(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
      log.info(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
      var reviewAskQuestionDf = spark.sql(s"""select hvtb_parse_sailor_review_ask.id,hvtb_parse_sailor_review_ask.question_id,hvtb_parse_sailor_review_ask.question_text,hvtb_parse_sailor_review_ask.masterquestion,hvtb_parse_sailor_review_ask.question_category,hvtb_parse_sailor_review_ask.question_type,hvtb_parse_sailor_review_ask.batchtime,hvtb_parse_sailor_review_ask.part_date,hvtb_parse_sailor_review_question.locationid from shipdw.hvtb_parse_sailor_review_ask left join shipdw.hvtb_parse_sailor_review_question on hvtb_parse_sailor_review_ask.id=hvtb_parse_sailor_review_question.id and hvtb_parse_sailor_review_ask.personid=hvtb_parse_sailor_review_question.personid $whereClause""").withColumn("Is_PrimaryQuestion", lit("N")).withColumn("Is_ClosingQuestion", lit("N"))

      var primaryQDf = spark.sql(s"""select id,primaryquestion_id,primaryquestion_text,primary_masterquestion,primaryquestion_category,primaryquestion_type,batchtime,part_date,locationid from  shipdw.hvtb_parse_sailor_review_question $whereClause""").withColumn("Is_PrimaryQuestion", lit("Y")).withColumn("Is_ClosingQuestion", lit("N"))

      var closingQDf = spark.sql(s"""select id,closingquestion_id,closingquestion_text,closing_masterquestion,closingquestion_category,closingquestion_type,batchtime,part_date,locationid from  shipdw.hvtb_parse_sailor_review_question $whereClause""").withColumn("Is_PrimaryQuestion", lit("N")).withColumn("Is_ClosingQuestion", lit("Y"))

      var df_question_dim = reviewAskQuestionDf.union(primaryQDf).union(closingQDf)

      df_question_dim = df_question_dim.select("question_id", "question_text", "masterquestion", "question_category", "question_type", "locationid", "Is_PrimaryQuestion", "Is_ClosingQuestion", "batchtime", "part_date")

      df_question_dim = df_question_dim.withColumnRenamed("question_id", "src_question_id").withColumnRenamed("masterquestion", "master_question").withColumnRenamed("question_type", "Type").withColumnRenamed("question_category", "category").withColumnRenamed("locationid", "master_location")
      
      val required_columns = spark.sparkContext.getConf.get("spark.source.columns").split(",")

      val missing_columns = ArrayBuffer[String]()
      val avaliable_columns = ArrayBuffer[String]()

      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

      for (col <- required_columns) {

        if (hasColumn(df_question_dim, col)) {
          println(col, "column exists", avaliable_columns.toString)
          println("column exists", avaliable_columns.length)
          log.info(col, "column exists", avaliable_columns.toString, avaliable_columns.length)
          println(avaliable_columns.length, "length")
          avaliable_columns.append(col)
        } else {
          println(col, "column missing", missing_columns.length)
          println(missing_columns.length, "length")
          log.info(col, "column exists", missing_columns.toString, missing_columns.length)
          println("column missing", missing_columns)
          missing_columns.append(col)
        }

      }

      print(missing_columns, "Here are the missing columns")
      val stage_final_df = missing_columns.foldLeft(df_question_dim)((df, c) =>
        df.withColumn(s"$c", lit(null)))

      log.info("calling scd framework")
      if (!stage_final_df.take(1).isEmpty) {
        loadDimFact(spark: SparkSession, stage_final_df)
      }
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)

    } catch {

      case e: Exception =>
        {
          log.info("in the catch of updateStatus ****************** ")
          e.printStackTrace()
          throw new Exception("General Exception..please check the stacktrace")
        }
    }
  }
}