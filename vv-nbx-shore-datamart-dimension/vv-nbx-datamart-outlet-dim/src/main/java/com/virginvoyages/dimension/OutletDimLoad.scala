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

object OutletDimLoad {

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

    //calling metadata framework to read the incremental data
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

      val whereClause = s""" where batchtime>= '$batch_start_tme' and batchtime<='$batch_end_tme' and part_date>='$part_read_start' and part_date<='$part_read_end'"""
      log.info(s"""whereclause:: $whereClause """)

      import spark.sqlContext.implicits._
      log.info(s"""Starting the Execution""")
      val query = spark.sparkContext.getConf.get("spark.source.sql").replace("*whereclause*", whereClause)
      val outlet_Dim = spark.sql(query)
      log.info(s"""query : $query""")

      //Perform Column Validation
      val required_columns = spark.sparkContext.getConf.get("spark.source.columns").split(",")
      val missing_columns = ArrayBuffer[String]()
      val avaliable_columns = ArrayBuffer[String]()

      //Function returns true if the dataframe contains the column passed to it
      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

      //Foreach column in the required_columns list check if the column exists in the dataframe
      for (col <- required_columns) {
        if (hasColumn(outlet_Dim, col)) {
          avaliable_columns.append(col)
          log.info(col, "column exists", avaliable_columns.toString, avaliable_columns.length)
        } else {
          missing_columns.append(col)
          log.info(col, "column does not exists", missing_columns.toString, missing_columns.length)
        }
      }

      //Stitch the missing columns to the outletDim dataframe with the null values
      val stage_final_df = missing_columns.foldLeft(outlet_Dim)((df, c) =>
        df.withColumn(s"$c", lit(null)))

      /*Call framework that perform SCD type 1 processing
         * alone with inserting data into hdfs/pond table and postgres table
         */
      if (!stage_final_df.head(1).isEmpty) {
        loadDimFact(spark: SparkSession, stage_final_df)
      }
      //Update the Hbase metadata table with the Successful Status
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
    } catch {

      case e: SQLException => {
        //Update the Hbase metadata table with the Failed Status
        ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
        log.info("SQL Excpetion caught and thrown in Dimension Load");
        e.printStackTrace();
        throw new Exception("SQL Exception..please check the stacktrace", e);
      }

      case e: Exception =>
        {
          //Update the Hbase metadata table with the Failed Status
          ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
          log.info("Excpetion caught and thrown in Dimension Load");
          e.printStackTrace();
          throw new Exception("General Exception..please check the stacktrace", e);
        }
        System.exit(1)
    }

  }

}
