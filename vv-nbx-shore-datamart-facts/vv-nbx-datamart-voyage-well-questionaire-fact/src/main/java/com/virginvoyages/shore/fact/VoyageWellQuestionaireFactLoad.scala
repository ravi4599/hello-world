package com.virginvoyages.shore.fact
 
import java.util.Date
import com.virginvoyages.metadataframework.ManageMetadata
 
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact
 
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
import org.apache.spark.sql.expressions.Window
import scala.collection.mutable.ArrayBuffer
import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.sql.SaveMode
 
object VoyageWellQuestionaireFactLoad {
val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
 
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
 
    /**calling metadata framework**/
    val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
 
    val batch_id1 = metadata._1
    val batch_instance_id1 = metadata._2
    val batch_start_tme = metadata._3
    val batch_end_tme = metadata._4
    val part_read_start = metadata._5
    val part_read_end = metadata._6
    val start_execution_time = metadata._7
    val part_write_date = metadata._8
    try {
 
      val whereClause = s""" where batchtime>='$batch_start_tme' and batchtime<='$batch_end_tme' and part_date>='$part_read_start' and part_date<='$part_read_end'"""
 
      log.info(s"""#-------Starting the Execution--for $whereClause ----------------#""")
      println(s" batch start time is $batch_start_tme")
      log.info(s" batch start time is $batch_start_tme")
      println(s" batch end time is $batch_end_tme")
      log.info(s" batch end time is $batch_end_tme")
 
      import spark.sqlContext.implicits._
 
      val reservationguestdf = spark.sql("select reservationguestid,lastmodifieddate,reservationid as reservationid_resvguest from shipdw.hvtb_parse_vxp_reservationguest").withColumn("rank", row_number().over(Window.partitionBy($"reservationguestid").orderBy($"lastmodifieddate".desc)))
      //reservationguestdf.show(5,false)
      val finalreservationguestdf = reservationguestdf.filter(reservationguestdf("rank") === 1)
      //finalreservationguestdf.show(5,false)
      val query = spark.sparkContext.getConf.get("spark.source.sql").replace("*whereClause*", whereClause)
      val healthdf = spark.sql(query)
      val health_reservationguestiddf = healthdf.join(finalreservationguestdf, healthdf("health_reservationguestid") === finalreservationguestdf("reservationguestid"), "left").select(healthdf("*"), finalreservationguestdf("reservationguestid"), finalreservationguestdf("reservationid_resvguest"))
      val reservationdf = spark.sql("select voyagenumber,reservationid,lastmodifieddate from shipdw.hvtb_parse_vxp_reservation").withColumn("rank", row_number().over(Window.partitionBy($"reservationid").orderBy($"lastmodifieddate".desc)))
      val finalreservationdf = reservationdf.filter(reservationdf("rank") === 1)
      val health_resvguest_resvedf = health_reservationguestiddf.join(finalreservationdf, health_reservationguestiddf("reservationid_resvguest") === finalreservationdf("reservationid"), "left").select(health_reservationguestiddf("*"), finalreservationdf("*"))
      val voyagedf = spark.sql("select ship_code,voyage_skey,voyage_number from shipdw.hvtb_mart_dim_voyage")
      val health_resvguest_resve_voygdf = health_resvguest_resvedf.join(voyagedf, health_resvguest_resvedf("voyagenumber") === voyagedf("voyage_number"), "left")
      val persondf = spark.sql("select person_skey,reservationguest_guid from shipdw.hvtb_mart_dim_person")
      val health_resvguest_resve_voyg_persdf = health_resvguest_resve_voygdf.join(persondf, health_resvguest_resve_voygdf("health_reservationguestid") === persondf("reservationguest_guid"), "left")
      val voyage_ques_dimdf = spark.sql("select voyage_well_question_skey,question_code from shipdw.hvtb_mart_dim_voyage_well_questionaire")
      val finaldf = health_resvguest_resve_voyg_persdf.join(voyage_ques_dimdf, health_resvguest_resve_voyg_persdf("healthquestioncode") === voyage_ques_dimdf("question_code"), "left")
      //finaldf.show(5,false)
     /* val stageVoyageWellQuestionaireFactDF = finaldf.select("voyage_id", "ship_code", "voyage_skey", "person_skey", "voyage_well_question_skey", "voyage_well_answer").withColumn("voyage_skey", when(finaldf("voyage_skey").isNull, lit(-1)).otherwise(finaldf("voyage_skey"))).withColumn("person_skey", when(finaldf("person_skey").isNull, lit(-1)).otherwise(finaldf("person_skey"))).withColumn("voyage_well_question_skey", when(finaldf("voyage_well_question_skey").isNull, lit(-1)).otherwise(finaldf("voyage_well_question_skey")))*/
      finaldf.createOrReplaceTempView("finaldftable")
      val stageVoyageWellQuestionaireFactDF= spark.sql("select distinct voyage_id,ship_code,voyage_skey,person_skey,voyage_well_question_skey,voyage_well_answer from finaldftable")
      //stageVoyageWellQuestionaireFactDF.printSchema()
      //stageVoyageWellQuestionaireFactDF.show(5,false)
 
      val required_columns = spark.sparkContext.getConf.get("spark.source.columns").split(",")
 
      val missing_columns = ArrayBuffer[String]()
      val avaliable_columns = ArrayBuffer[String]()
      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
 
      for (col <- required_columns) {
 
        if (hasColumn(stageVoyageWellQuestionaireFactDF, col)) {
          log.info(col, "column exists", avaliable_columns.toString, avaliable_columns.length)
          avaliable_columns.append(col)
        } else {
          log.info(col, "column exists", missing_columns.toString, missing_columns.length)
          missing_columns.append(col)
        }
 
      }
 
      val stage_final_df = missing_columns.foldLeft(stageVoyageWellQuestionaireFactDF)((df, c) =>
        df.withColumn(s"$c", lit("N/A")))
        //stage_final_df.show(5,false)
 
      loadDimFact(spark: SparkSession, stage_final_df)
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
 
    } catch {
      case e: SQLException => {
        ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
        log.info("******** in the catch of VoyageWellQuestionaireFactLoad *********");
        e.printStackTrace();
        throw new Exception("SQL Exception..please check the stacktrace", e);
      }
 
      case e: Exception =>
        {
          ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
          log.info("********in the catch of VoyageWellQuestionaireFactLoad *********");
          e.printStackTrace();
          throw new Exception("General Exception..please check the stacktrace", e);
        }
        log.info("#-------Process Has Failed-------#")
        System.exit(1)
 
    }
 
  }
 
}