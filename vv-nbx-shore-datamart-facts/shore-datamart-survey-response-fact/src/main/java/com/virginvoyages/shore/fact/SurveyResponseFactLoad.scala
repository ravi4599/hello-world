package com.virginvoyages.shore.fact

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
object SurveyResponseFactLoad {
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
    val voyageId = spark.sparkContext.getConf.get("spark.voyage.id")
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    //sqlContext.setConf("set hive.strict.checks.cartesian.product", "false")
    //sqlContext.setConf("hive.strict.checks.cartesian.product", "nonstrict")

    val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
    //batch_instance_id1, batch_id1
    val batch_id1 = metadata._1
    val batch_instance_id1 = metadata._2
    val batchStartTme = metadata._3
    val batchEndTme = metadata._4
    val partReadStart = metadata._5

    val partReadEnd = metadata._6
    val startExecutionTime = metadata._7
    val part_write_date = metadata._8

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    import spark.implicits._

    try {
      val whereClause = s""" batchtime>= '$batchStartTme' and batchtime<='$batchEndTme' and part_date>='$partReadStart' and part_date<='$partReadEnd'"""

      val askdf = spark.sql("""select id,personid,question_id,question_text,masterquestion,question_category,question_type,batchtime,part_date,row_number() over(partition by id,question_id order by batchtime desc) as rownum from shipdw.hvtb_parse_sailor_review_ask""").select("*").where(col("rownum") === 1).drop("rownum")

      val reviewQuesDf = spark.sql("select id,personid,locationid,shipcode,push_timestamp,cast(visited_timestamp as timestamp) as visited_timestamp,row_number() over(partition by id order by created_timestamp desc) as rownum from shipdw.hvtb_parse_sailor_review_question").select("*").where(col("rownum") === 1).drop("rownum")

      val askReviewQuesJoinDf = askdf.join(reviewQuesDf, (askdf("id") === reviewQuesDf("id")) && (askdf("personid") === reviewQuesDf("personid")), "left").select(askdf("*"), reviewQuesDf("locationid"), reviewQuesDf("shipcode"), reviewQuesDf("push_timestamp"), reviewQuesDf("visited_timestamp"))

      val primaryQDf = spark.sql(s"""select id,personid,primaryquestion_id,primaryquestion_text,primary_masterquestion,primaryquestion_category,primaryquestion_type,batchtime,part_date,locationid,shipcode,push_timestamp,cast(visited_timestamp as timestamp) as visited_timestamp,row_number() over(partition by id,primaryquestion_id order by created_timestamp desc) as rownum from shipdw.hvtb_parse_sailor_review_question""").select("*").where(col("rownum") === 1).drop("rownum")

      val closingQDf = spark.sql(s"""select id,personid,closingquestion_id,closingquestion_text,closing_masterquestion,closingquestion_category,closingquestion_type,batchtime,part_date,locationid,shipcode,push_timestamp,cast(visited_timestamp as timestamp) as visited_timestamp,row_number() over(partition by id,closingquestion_id order by created_timestamp desc) as rownum from shipdw.hvtb_parse_sailor_review_question""").select("*").where(col("rownum") === 1).drop("rownum")

      val questionDf = askReviewQuesJoinDf.union(primaryQDf).union(closingQDf).select("id", "personid", "shipcode", "push_timestamp", "visited_timestamp", "locationid", "batchtime", "part_date", "question_id")

      val questionDimDf = spark.sql("select src_question_id,master_location,question_skey from shipdw.hvtb_mart_shipboard_survey_questions_dim")

      val qdimJoinDf = questionDf.join(questionDimDf, (questionDf("question_id") === questionDimDf("src_question_id")) && (questionDf("locationid") === questionDimDf("master_location")), "inner").select(questionDf("*"), questionDimDf("src_question_id"), questionDimDf("question_skey"), questionDimDf("master_location"))

      val reviewAnsQDf = spark.sql("""select id,question_id,question_answer,question_type,question_text,personid,row_number() over(partition by id,question_id order by batchtime desc) as rownum from shipdw.hvtb_parse_sailor_review_ans_quest""").select("*").where(col("rownum") === 1).drop("rownum")

      val reviewAnsdf = spark.sql("select id,personid,created_timestamp,locationid as finalloc,row_number() over(partition by id order by created_timestamp desc) as rownum from shipdw.hvtb_parse_sailor_review_answer").select("*").where(col("rownum") === 1).drop("rownum")

      val twoanswerTblJoinDf = reviewAnsQDf.join(reviewAnsdf, (reviewAnsQDf("id") === reviewAnsdf("id")) and (reviewAnsQDf("personid") === reviewAnsdf("personid")), "left").select(reviewAnsQDf("id"), reviewAnsQDf("personid"), reviewAnsQDf("question_id"), reviewAnsQDf("question_type"), reviewAnsQDf("question_answer"), reviewAnsQDf("question_type"), reviewAnsQDf("question_text"), reviewAnsdf("created_timestamp"), reviewAnsdf("finalloc"))

      var reviewAnsQusJoinDf = qdimJoinDf.join(twoanswerTblJoinDf, (qdimJoinDf("id") === twoanswerTblJoinDf("id")) && (qdimJoinDf("personid") === twoanswerTblJoinDf("personid")) && (qdimJoinDf("question_id") === twoanswerTblJoinDf("question_id")), "left").drop(qdimJoinDf("question_id")).drop(twoanswerTblJoinDf("id")).drop(twoanswerTblJoinDf("personid"))
      reviewAnsQusJoinDf=reviewAnsQusJoinDf.withColumn("location_val", when(reviewAnsQusJoinDf("finalloc").isNotNull,reviewAnsQusJoinDf("finalloc")).otherwise(reviewAnsQusJoinDf("locationid")).cast(StringType))
      
      val personDf = spark.sql("select person_skey,reservationguest_guid,voyage_skey,row_number() over(partition by reservationguest_guid order by upd_dt desc,md5_hash desc) as rownum  from shipdw.hvtb_mart_dim_person where reservationguest_guid is not null and booking_arrival_date is not null and booking_departure_date is not null").select("*").where(col("rownum") === 1).drop("rownum")
      
      val personDimJoinDf = reviewAnsQusJoinDf.join(personDf, reviewAnsQusJoinDf("personid") === personDf("reservationguest_guid"), "left").select(reviewAnsQusJoinDf("*"), personDf("person_skey"), personDf("voyage_skey"))            
      
      val orgDf = spark.sql("select org_unit_abbreviation,org_unit_skey from shipdw.hvtb_mart_dim_org_unit")

      val orgUnitDimJoinDf = personDimJoinDf.join(orgDf, personDimJoinDf("shipcode") === orgDf("org_unit_abbreviation"), "left").select(personDimJoinDf("*"), orgDf("org_unit_skey")).withColumnRenamed("push_timestamp", "request_time").withColumnRenamed("created_timestamp", "response_time").withColumnRenamed("visited_timestamp", "visit_time").withColumnRenamed("location_val", "location").withColumn("answer_rating", when(col("question_type").contains("Rating"), col("question_answer")).otherwise(lit(null))).withColumn("answer_text", when(col("question_type").contains("Text"), col("question_answer")).otherwise(lit(null))).withColumnRenamed("id", "request_id").withColumn("voyage_id", lit(voyageId).cast(StringType))
      
      //val finalDf = orgUnitDimJoinDf.withColumn("person_skey", when(orgUnitDimJoinDf("person_skey").isNotNull,orgUnitDimJoinDf("person_skey")).otherwise(lit(-1).cast(LongType))).withColumn("voyage_skey", when(orgUnitDimJoinDf("voyage_skey").isNotNull,orgUnitDimJoinDf("voyage_skey")).otherwise(lit(-1).cast(IntegerType))).select("voyage_id", "request_id", "person_skey", "org_unit_skey", "voyage_skey", "request_time", "response_time", "visit_time", "location", "question_skey", "answer_rating", "answer_text").distinct
      val finalDff = orgUnitDimJoinDf.withColumn("person_skey", when(orgUnitDimJoinDf("person_skey").isNotNull,orgUnitDimJoinDf("person_skey")).otherwise(lit(-1).cast(LongType))).withColumn("voyage_skey", when(orgUnitDimJoinDf("voyage_skey").isNotNull,orgUnitDimJoinDf("voyage_skey")).otherwise(lit(-1).cast(IntegerType))).select("voyage_id", "request_id", "person_skey", "org_unit_skey", "voyage_skey", "request_time", "response_time", "visit_time", "location", "question_skey", "answer_rating", "answer_text").distinct
	  
	  finalDff.createOrReplaceTempView("finaltempview")

      val finalDf = spark.sql("""select voyage_id,request_id,person_skey,org_unit_skey,voyage_skey,to_utc_timestamp(request_time,'America/New_York') as request_time,to_utc_timestamp(response_time,'America/New_York') as response_time,visit_time,location,question_skey,answer_rating,answer_text from finaltempview""")
      
      val required_columns = spark.sparkContext.getConf.get("spark.source.columns").split(",")

      val missing_columns = ArrayBuffer[String]()
      val avaliable_columns = ArrayBuffer[String]()

      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

      for (col <- required_columns) {

        if (hasColumn(finalDf, col)) {
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
      val stage_final_df = missing_columns.foldLeft(finalDf)((df, c) =>
        df.withColumn(s"$c", lit(null)))

      log.info("calling scd framework")

      if (!stage_final_df.head(1).isEmpty) {
        loadDimFact(spark: SparkSession, stage_final_df)
      }
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
    } catch {

      case e: Exception =>
        {
          log.info("in the catch of updateStatus ******************SurveyResponseFactLoad ")
          e.printStackTrace()
          throw new Exception("General Exception..please check the stacktrace")
        }
    }
  }
}