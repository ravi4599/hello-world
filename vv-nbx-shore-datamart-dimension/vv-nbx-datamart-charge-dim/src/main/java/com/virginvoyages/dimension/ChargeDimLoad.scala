package com.virginvoyages.dimension

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


import com.virginvoyages.metadataframework.ManageMetadata
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact

object ChargeDimLoad {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    
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
      var personAccDf = spark.sql("select person_account_id,rec_deleted,person_id,voyageid,batchtime,part_date,org_unit_id,external_account_id,row_number() over(partition by person_account_id,external_account_id order by last_changed desc) as rownum from shipdw.hvtb_parse_mxp_person_account  where person_account_id is not null ").select("*").where(col("rownum") === 1).drop("rownum").withColumnRenamed("voyageid", "voyage_id")
	  
	  
      personAccDf = personAccDf.where(whereClause)
      val peraccidDf = personAccDf.withColumnRenamed("external_account_id", "charge_id")
      //val personDimDf = spark.sql("select cast(person_id as int) as person_id,person_skey,booking_id,row_number() over(partition by person_id order by booking_departure_date desc,md5_hash desc) as rownum from shipdw.hvtb_mart_dim_person").select("*").where(col("rownum") === 1).drop("rownum")      
      //val chargeDimDf = peraccidDf.join(personDimDf, peraccidDf("person_id") === personDimDf("person_id"), "left").select(peraccidDf("*"), personDimDf("person_skey"), personDimDf("booking_id"),personDimDf("person_id").alias("personperson_id"))
       val personDimDf1 = spark.sql("select cast(person_id as int) as person_id,person_skey,booking_id,person_booking_charge_id,persons_op,personbooking_op, row_number() over(partition by person_id,person_booking_charge_id order by ts_ms desc) as rownum from shipdw.hvtb_mart_dim_person").select("*").where(col("rownum") === 1).drop("rownum")
       
	    personDimDf1.createOrReplaceTempView("personDimDf1")
	    val personDimDf=spark.sql("""select * from personDimDf1 where persons_op<> 'd' and personbooking_op <> 'd'""")
	  
      val chargeDimDf = peraccidDf.join(personDimDf, peraccidDf("charge_id") === personDimDf("person_booking_charge_id"), "left").select(peraccidDf("*"), personDimDf("person_skey"), personDimDf("booking_id"),personDimDf("person_id").alias("personperson_id"))

      val persoaccidDf = chargeDimDf.withColumn("person_account_id",when((chargeDimDf("person_id") === chargeDimDf("personperson_id")) && (chargeDimDf("rec_deleted") === false),chargeDimDf("person_account_id")).otherwise(null).cast(StringType)).drop("personperson_id").withColumnRenamed("rec_deleted", "src_deleted_flag")      
      val orgUnitDf= spark.sql("select * from (select org_unit_id,org_unit_abbreviation, row_number () over (partition by org_unit_id order by ts_ms desc ) as rn from shipdw.hvtb_parse_mxp_org_units)units where units.rn = 1")
     // val shipCodeDf=persoaccidDf.join(orgUnitDf,persoaccidDf("org_unit_id") === orgUnitDf("org_unit_id"),"left").select(persoaccidDf("*"),orgUnitDf("org_unit_abbreviation").alias("ship_code")).select("voyage_id", "charge_id","person_account_id", "ship_code","person_id", "person_skey", "booking_id", "src_deleted_flag","batchtime")
     val shipCodeDff=persoaccidDf.join(orgUnitDf,persoaccidDf("org_unit_id") === orgUnitDf("org_unit_id"),"left").select(persoaccidDf("*"),orgUnitDf("org_unit_abbreviation").alias("ship_code")).select("voyage_id", "charge_id","person_account_id", "ship_code","person_id", "person_skey", "booking_id", "src_deleted_flag","batchtime")
      shipCodeDff.createOrReplaceTempView("shipcodedff")
      val shipCodeDf=spark.sql("""select * from shipcodedff where person_account_id is not null""")
      
      log.info("-----shipCodeDf-----")
      
      val required_columns = spark.sparkContext.getConf.get("spark.source.columns").split(",")
      val missing_columns = ArrayBuffer[String]()

      val avaliable_columns = ArrayBuffer[String]()

      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

      for (col <- required_columns) {

        if (hasColumn(shipCodeDf, col)) {

          println(col, "column exists", avaliable_columns.toString)

          println("column exists", avaliable_columns.length)

        } else {

          println(col, "column missing", missing_columns.length)

          println(missing_columns.length, "length")

        }

      }

      print(missing_columns, "Here are the missing columns")

      val stage_final_df = missing_columns.foldLeft(shipCodeDf)((df, c) =>

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

