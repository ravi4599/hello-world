package com.virginvoyages.dimension

import com.virginvoyages.metadataframework.ManageMetadata
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

import com.virginvoyages.scd.ChangeDataCapture.loadDimFact

object PersonLoyaltyDimLoad {

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
/*************************************calling metadata framework*********************************************/
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

    try {

      val whereClause = s""" batchtime>= '$batch_start_tme' and batchtime<='$batch_end_tme' and part_date>='$part_read_start' and part_date<='$part_read_end'"""

      println(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
      log.info(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
      import spark.sqlContext.implicits._
      val dailyptsumDf=spark.sql("select voyage_id,cast(PTNID as string) as person_loyalty_id,CARDTYPEDESC as person_card_type,CARDSTATUSDESC as person_card_status,cast(CARDID as string) as CARDID,batchtime,row_number() over(partition by ptnid order by lastmoddate desc) as rownum from shipdw.hvtb_parse_konami_vw_odbc_dailyptsum").select("*").where(col("rownum") === 1).drop("rownum").where(whereClause)
      val persondimDf=spark.sql("select distinct person_id,seaware_id from shipdw.hvtb_mart_dim_person where seaware_id is not null")
      val dailyptsumPersondimDf=dailyptsumDf.join(persondimDf,dailyptsumDf("person_loyalty_id") === persondimDf("seaware_id"),"left").select(dailyptsumDf("*"),persondimDf("person_id"))      
      val ptncardDf=spark.sql("select CARDLVL as person_card_level,CARDCOLOR as person_card_color,CARDCOLORVALUE as person_card_color_value,CARDID,row_number() over(partition by PTNID,CARDID order by LASTMODDATE desc) as rownum from shipdw.hvtb_parse_konami_vw_odbc_ptncard").select("*").where(col("rownum") === 1).drop("rownum")
      val ptncardJoinDf=dailyptsumPersondimDf.join(ptncardDf ,dailyptsumPersondimDf("CARDID") === ptncardDf("CARDID"),"left").select(dailyptsumPersondimDf("*"),ptncardDf("person_card_level"),ptncardDf("person_card_color"),ptncardDf("person_card_color_value")).drop("CARDID").select("voyage_id","person_loyalty_id","person_id","person_card_type","person_card_status","person_card_level","person_card_color","person_card_color_value","batchtime")
      ptncardJoinDf.printSchema()
      log.info("-----ptncardJoinDf-----")
      
      val required_columns = spark.sparkContext.getConf.get("spark.source.columns").split(",")
      val missing_columns = ArrayBuffer[String]()

      val avaliable_columns = ArrayBuffer[String]()

      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

      for (col <- required_columns) {

        if (hasColumn(ptncardJoinDf, col)) {

          println(col, "column exists", avaliable_columns.toString)

          println("column exists", avaliable_columns.length)

        } else {

          println(col, "column missing", missing_columns.length)

          println(missing_columns.length, "length")

        }

      }

      print(missing_columns, "Here are the missing columns")

      val stage_final_df = missing_columns.foldLeft(ptncardJoinDf)((df, c) =>

        df.withColumn(s"$c", lit(null)))

      if (!stage_final_df.take(1).isEmpty) {
        loadDimFact(spark: SparkSession, stage_final_df)
      }
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)

    } catch {

      case e: SQLException => { ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("******************in the catch of item Dimension Load ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace", e); }

      case e: Exception =>
        { ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); 
        log.info("******************in the catch of item Dimension Load ******************");
        e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace", e); }
        println("#----------------------------Process Has Failed---------------------------#")
        System.exit(1)
      //exit(1);
    }

    //stage_final_df.show

  }

}
