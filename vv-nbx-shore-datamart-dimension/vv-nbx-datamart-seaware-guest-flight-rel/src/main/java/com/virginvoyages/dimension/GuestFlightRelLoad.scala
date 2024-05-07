package com.virginvoyages.dimension

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
import scala.collection.mutable.ArrayBuffer
import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.sql.SaveMode
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact

object GuestFlightRelLoad {
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
    val sparkConfiguration_stage = spark.sparkContext.getConf.getAll.toMap
    sparkConfiguration_stage.keys.foreach{ i =>
      print("key = " +i)
      println("value = " + sparkConfiguration_stage(i))
    }
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

      import spark.sqlContext.implicits._
   /* val alogDf = spark.sql(""" select res_id, farebaseamount as amadeus_fare_base, totaltax as amadeus_total_tax, cast(airsegmentreference as int) as airsegreference,responded_at from ( select res_id, responded_at, xpath_double(response, 'Booking_OUT/BookingAssociationList/BookingAssociation/PNRList/PNR/BookingCostList/BookingCost/MultiTicketList/Cost/AirCost/FareBaseAmount') as farebaseamount, xpath_double(response, 'Booking_OUT/BookingAssociationList/BookingAssociation/PNRList/PNR/BookingCostList/BookingCost/MultiTicketList/Cost/AirCost/TotalTax') as totaltax, xpath(response, 'Booking_OUT/BookingAssociationList/BookingAssociation/PNRList/PNR/BookingCostList/BookingCost/MultiTicketList/FareSegmentNumberList/AirSegmentReferenceIndicator/AirSegmentReference/text()') as airsegmentreferencetemp from vv_db.hvtb_parse_sw_rpl_air_amadeus_log where action = 'Booking' and success = 'Y') lateral view explode(airsegmentreferencetemp) airsegmentreferencetable as airsegmentreference""")
      alogDf.createOrReplaceTempView("alog")
     var alog_stagedf = spark.sql(""" select * from (select res_id,amadeus_fare_base,amadeus_total_tax,airsegreference, responded_at, row_number() over(partition by res_id,airsegreference order by responded_at desc) rn from alog) where rn=1""")
     alog_stagedf.createOrReplaceTempView("aaaaTbl")
     log.info("log for alog")
    spark.sql("""select * from aaaaTbl where res_id = 31623 """).show(false)*/


      val whereClause = s""" where batchtime>= '$batch_start_tme' and batchtime<='$batch_end_tme' and part_date>='$part_read_start' and part_date<='$part_read_end'"""
      println(s"""#---------------------------Starting the Execution--for whereClause ----------------#""")
      log.info(s"""#---------------------------Starting the Execution--for whereClause ----------------#""")
    /* val whereClause_temp = spark.read.load(whereClause)
     whereClause_temp.show(10,false)*/ 
      var sourceDf = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").trim().replace("*whereclause*", whereClause)) 
      //sourceDf.show(10,false)
			var alogDf = spark.sql(spark.sparkContext.getConf.get("spark.alog.sql").trim())
//var alogDf = spark.sql(spark.sparkContext.getConf.get("spark.alog.sql"))
      //val alogDf = spark.sql("""select res_id, farebaseamount as amadeus_fare_base, totaltax as amadeus_total_tax, cast(airsegmentreference as int) as airsegreference,responded_at from ( select res_id, responded_at, xpath_double(response,'Booking_OUT/BookingAssociationList/BookingAssociation/PNRList/PNR/BookingCostList/BookingCost/MultiTicketList/Cost/AirCost/FareBaseAmount') as farebaseamount, xpath_double(response,'Booking_OUT/BookingAssociationList/BookingAssociation/PNRList/PNR/BookingCostList/BookingCost/MultiTicketList/Cost/AirCost/TotalTax') as totaltax, xpath(response,'Booking_OUT/BookingAssociationList/BookingAssociation/PNRList/PNR/BookingCostList/BookingCost/MultiTicketList/FareSegmentNumberList/AirSegmentReferenceIndicator/AirSegmentReference/text()') as airsegmentreferencetemp  from vv_db.hvtb_parse_sw_rpl_air_amadeus_log where action = 'Booking' and success = 'Y') lateral view explode(airsegmentreferencetemp) as airsegmentreference""")
      alogDf.createOrReplaceTempView("alog")
     // alogDf.show(false)
	var alog_stagedf =   spark.sql(""" select * from (select res_id,amadeus_fare_base,amadeus_total_tax,airsegreference, responded_at, row_number() over(partition by res_id,airsegreference order by responded_at desc) rn from alog) where rn=1""")
//	val newAlogDf = alog_stagedf.where(col("res_id") === 31623)
	//	val newAlogDf = alog_stagedf
	//newAlogDf.show()
	val colsToDrop = Seq("airsegreference","responded_at","rn")
	var final_stageDf = sourceDf.join(alog_stagedf, sourceDf.col("src_res_id") === alog_stagedf.col("res_id") and sourceDf.col("segment_seqn") === alog_stagedf.col("airsegreference"),"left").drop(colsToDrop :_*)
	//var final_stageDf = sourceDf.join(alog_stagedf, sourceDf.col("src_res_id") === alog_stagedf.col("res_id"))
	//final_stageDf.show(5,false)
	      val required_columns = spark.sparkContext.getConf.get("spark.source.columns").split(",")

      val missing_columns = ArrayBuffer[String]()
      val avaliable_columns = ArrayBuffer[String]()
      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

      for (col <- required_columns) {

        if (hasColumn(final_stageDf, col)) {
          println(col, "column exists", avaliable_columns.toString)
          println("column exists", avaliable_columns.length)
          log.info(col, "column exists", avaliable_columns.toString, avaliable_columns.length)
          println(avaliable_columns.length, "lenthg")
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
      val finalDf = missing_columns.foldLeft(final_stageDf)((df, c) =>
        df.withColumn(s"$c", lit(0)))
	   val  stageFinalDf = finalDf
	   stageFinalDf.show()
		if (!finalDf.head(1).isEmpty) {
      loadDimFact(spark: SparkSession, finalDf)
      }
      println("after SCD")
      log.info("after SCD")
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
    }
    catch {
      case e: SQLException => { ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("******************in the catch of Res Addon Rel Load ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace", e); }

      case e: Exception =>
       { ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("******************in the catch of Res Addon Rel Load ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace", e); }
        //case e: SQLException => println("#----------------------------Process Has Failed---------------------------#")
        System.exit(1)
      //exit(1); */
       // case e: Exception => {System.exit(1)}
    }
  }
}