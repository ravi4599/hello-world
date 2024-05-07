package com.virginvoyages.jdbc.ingestion
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import scala.util.Try
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import scala.collection.mutable.ArrayBuffer
import com.virginvoyages.metadataframework.ManageMetadata
import org.apache.spark.sql.types.{ StringType, TimestampType,DateType,LongType }
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import java.sql.SQLException
import org.apache.spark._
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import java.sql.SQLException
import java.time.LocalDateTime
import java.util.Date
import java.sql.Timestamp

object FullLoad {
  
    def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    var batchInstanceId: String = null
    var batchId: String = null
    try {
      sqlContext.setConf("hive.exec.dynamic.partition", "true")
      sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
      val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
/*************************************calling metadata framework*********************************************/
/*      val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
      metadata.productIterator.foreach(println)
      batchId = metadata._1
      batchInstanceId = metadata._2
      val batchStartTime = metadata._3
      val batchEndTime = metadata._4
      val startExecutionTiime = metadata._7
      val partWriteDate = metadata._8
*/
      val currentDate = java.time.LocalDateTime.now
      val batchStartTime = currentDate.toString.replace("T"," ")
      val partWriteDate = java.time.LocalDate.now
      
      val voyageId = spark.sparkContext.getConf.get("spark.voyage.id")
	   var shipCode:String = null
        if (spark.sparkContext.getConf.contains("spark.ship.code")) {

          shipCode = spark.sparkContext.getConf.get("spark.ship.code").toString()
        }
		
      val selectQuery = spark.sparkContext.getConf.get("spark.src.table.query").trim()
      val tableNames = spark.sparkContext.getConf.get("spark.parse.table").trim()

      //Connecting to source database
      val data = spark.read.format(sparkConfiguration.value.get("spark.src.con.format").get).options(Map("url" -> sparkConfiguration.value.get("spark.src.con.url").get, "user" -> sparkConfiguration.value.get("spark.src.user").get, "password" -> sparkConfiguration.value.get("spark.src.password").get,"dbtable" -> selectQuery, "driver" -> sparkConfiguration.value.get("spark.src.con.driver").get,"fetchSize" -> sparkConfiguration.value.get("spark.src.con.fetchSize").get )).load()

      val filteredDataDF = data.select("*")//.where(col(conditionColumn) >= lit(batchStartTime).cast(TimestampType) && col(conditionColumn) <= lit(batchEndTime).cast(TimestampType)).select("*")
      //var Part_date = s"1900-01-01 00:00:00"
      var Part_date = current_timestamp()
      if (!filteredDataDF.head(1).isEmpty) {
        spark.sqlContext.setConf("spark.sql.parquet.writeLegacyFormat", "true")
        var finalDF = filteredDataDF.withColumn("VoyageId", lit(voyageId).cast(StringType)).withColumn("BatchTime", lit(batchStartTime).cast(TimestampType)).withColumn("ShipCode", lit("Shore").cast(StringType)).withColumn("Part_Date", lit(current_date())).withColumn("op",lit("c")).withColumn("ts_ms", lit(Part_date)).withColumn("lsn", lit("00000000000").cast(LongType))
        //println(filteredDataDF.count)
        
        def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
        
        val colList = spark.catalog.listColumns(tableNames).select("name").collect().map(_.getAs[String]("name")).mkString(",").trim
        
        val required_columns = colList.split(",")
        val missing_columns = ArrayBuffer[String]()
        val avaliable_columns = ArrayBuffer[String]()
        for (col <- required_columns) {

        if (hasColumn(finalDF, col)) {
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
      var targetDF = missing_columns.foldLeft(finalDF)((df, c) =>
        df.withColumn(s"$c", lit(null)))
        
        
        
        
        var count = 1
        
        var Query  = ""
        val tableStruct = spark.catalog.listColumns(tableNames).select("name","dataType")
        val length = tableStruct.count()
        for (row <- tableStruct.rdd.collect())
        {
         if (count != length) 
           {
             Query = Query+"cast("+row.mkString(",").split(",",2)(0)+" as " +row.mkString(",").split(",",2)(1)+" ) as "+row.mkString(",").split(",",2)(0) +","
            }
         else 
           {
           Query = Query+"cast("+row.mkString(",").split(",",2)(0)+" as " +row.mkString(",").split(",",2)(1)+" ) as "+row.mkString(",").split(",",2)(0)
           }
         count = count +1
        }
        
        
        targetDF.createOrReplaceTempView("temp_tab")
        val finaleQuery = s"""select """ + Query + s""" from temp_tab"""
        println("----------------------------")
        println(finaleQuery)
        
        val FinalData = spark.sql(finaleQuery)
        
        val tgtLocation = spark.sparkContext.getConf.get("spark.target.location")
        //val hiveTable = spark.sparkContext.getConf.get("spark.target.hive.table")
        FinalData.show(10, false)
        FinalData.printSchema()
        //print(finalDF)
          FinalData.repartition(15).write.mode("Overwrite").partitionBy("part_date").parquet(spark.sparkContext.getConf.get("spark.target.location"))

        //finalDF.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.table"))
        
//        val insertString = s"""alter table $hiveTable add IF NOT EXISTS  partition(Part_Date='$partWriteDate') location '$tgtLocation/$partWriteDate'""".stripMargin
//        // val insertString1 = "alter table" + hiveTable = "add if not exists partition(partitionKey='" + partWriteDate + "') location '" + tgtLocation + "/" + partWriteDate + "'"
//        println(insertString)
//        spark.sql(insertString)
//        spark.sql("refresh table " + hiveTable)
//        spark.sql("Msck repair table " + hiveTable)

      }
      //ManageMetadata.updateStatus(batchInstanceId, batchId, "Successful", spark)
    } catch {

      case e: SQLException => {
       // ManageMetadata.updateStatus(batchInstanceId, batchId, "Failed", spark)
        log.info("***************in the catch of Jdbc Ingestion ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace")
      }
      case e: Exception =>
        {
        //  ManageMetadata.updateStatus(batchInstanceId, batchId, "Failed", spark)
          log.info("***************in the catch of Jdbc Ingestion ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace")
        }
        System.exit(1)

    }
  }
}
  
