package com.virginvoyages.vxp.parser

import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.DataFrame
import java.io._
import scala.util.Try
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ BooleanType, StringType }
import com.virginvoyages.metadataframework.ManageMetadata

object RRDFileFromSftp {
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

    var batchInstanceId: String = null
    var batchId: String = null
    try {

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
      val batchStartTime = metadata._3
      batchId = metadata._1
      batchInstanceId = metadata._2
      val batchEndTime = metadata._4
      val partStartTime = metadata._5.toString()
      val partEndTime = metadata._6.toString()
      val startExecutionTime = metadata._7.toString()
      val partDate = metadata._8.toString()

      val whereClause = s""" batchtime>= '$batchStartTime' and batchtime<='$batchEndTime' and part_date>='$partStartTime' and part_date<='$partEndTime'"""

      println(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
      log.info(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
	  
	  //val rrdDataDf = spark.sql("select message,batchtime,part_date,shipcode,keyattr from shipdw.hvtb_lnd_rddfile_fromsftp_check").where(whereClause)
       
      val rrdDataDf = spark.sql("select message,batchtime,part_date,shipcode,keyattr from %s".format(sc.getConf.get("spark.source.table"))).where(whereClause)
            
   
      log.info("rrdDataDf.head(1).isEmpty" + rrdDataDf.head(1).isEmpty)
      
      if (!rrdDataDf.head(1).isEmpty) {        
        val filename = rrdDataDf.select("keyattr").head(1)(0).mkString
        val splitRec = rrdDataDf.map(_.mkString(",")).flatMap(rec => rec.split("\\n"))
        var splitRow = splitRec.map(rowVal => rowVal.split(",")).withColumn("shipname", ($"value".getItem(0)).cast(StringType)).withColumn("saildate", ($"value".getItem(1)).cast(StringType)).withColumn("reservationId", ($"value".getItem(2)).cast(StringType)).withColumn("wearableColor", ($"value".getItem(3)).cast(StringType)).withColumn("sku", ($"value".getItem(4)).cast(StringType)).withColumn("status", ($"value".getItem(5)).cast(StringType)).drop("value")
        splitRow = splitRow.withColumn("status", regexp_replace(col("status"),"[\n\r]","")).select("shipname", "saildate", "reservationId", "wearableColor", "sku", "status")
        splitRow = splitRow.withColumn("shipname", regexp_replace(col("shipname"),"\"","")).withColumn("wearableColor", regexp_replace(col("wearableColor"),"\"","")).withColumn("sku", regexp_replace(col("sku"),"\"","")).withColumn("status", regexp_replace(col("status"),"\"",""))
        val filenameDf = splitRow.withColumn("filename", lit(filename).cast(StringType)).withColumn("voyageid", lit(spark.sparkContext.getConf.get("spark.voyage.id"))).withColumn("batchtime", lit(startExecutionTime)).withColumn("part_date", lit(partDate)).select("shipname", "saildate", "reservationId", "wearableColor", "sku", "status", "filename", "voyageid", "batchtime","part_date")        
        filenameDf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.table"))
        log.info("filenameDf" + filenameDf.count)        
      }
      ManageMetadata.updateStatus(batchInstanceId, batchId, "Successful", spark)
    } catch {

      case e: Exception =>
        {
          ManageMetadata.updateStatus(batchInstanceId, batchId, "Failed", spark)
          log.info("RRDFileFromSftp ::: in the catch of updateStatus")
          e.printStackTrace()
          throw new Exception("General Exception..please check the stacktrace")
        }

    }

  }

}