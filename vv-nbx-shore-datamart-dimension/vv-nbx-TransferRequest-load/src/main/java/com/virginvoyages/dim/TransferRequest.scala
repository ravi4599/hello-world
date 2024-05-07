package com.virginvoyages.ingestion
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import org.apache.spark.SparkConf
import java.sql.SQLException
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.Row
import com.virginvoyages.metadataframework.ManageMetadata
import org.apache.log4j.{ Level, LogManager }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI
import org.apache.spark.sql.functions.input_file_name

object TransferRequestLoad {

  def readExcel(spark: SparkSession, filePath: String, tab: String): DataFrame = {
    spark.read.format("com.crealytics.spark.excel")
      .option("dataAddress", s"'$tab'!A1") // Optional, default: "A1"/
      .option("header", "true") 
      //.option("useHeader", "true")// Required
      .option("treatEmptyValuesAsNulls", "true") // Optional, default: true
      .option("inferSchema", "true") // Optional, default: false
      .option("addColorColumns", "false") // Optional, default: false
      .load(filePath)
      
       //.option("dataAddress", s"'$tab'!A1") // Optional, default: "A1"
    
   /*     .format("com.crealytics.spark.excel")
    .option("location", filePath)
     .option("sheetName", tab)
    .option("useHeader", "true")
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "true")
    .option("addColorColumns", "False")
    .load()
*/

  }

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
    //val sparkConfiguration = sc.broadcast(sc.getConf.getAll.toMap)
    var batchInstanceId: String = null
    var batchId: String = null

    try {
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

      println("Starting")
      sqlContext.setConf("hive.exec.dynamic.partition", "true")
      sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
      spark.sqlContext.setConf("spark.sql.parquet.writeLegacyFormat", "true")
      val inputfilepath = spark.sparkContext.getConf.get("spark.input.filepath").trim()
     // val processfilepath = spark.sparkContext.getConf.get("spark.input.processedfilepath").trim()

      if (FileSystem.get(new URI(inputfilepath), sc.hadoopConfiguration).exists(new Path(inputfilepath))) {
        println("File Exist")
      }
      val files = FileSystem.get(new URI(inputfilepath), sc.hadoopConfiguration).listStatus(new Path(inputfilepath))

      val fileNames = files.map(_.getPath.getName).toList

      val fileName = fileNames.find(_.contains("xlsx"))
      val filen = fileName.get
      val finalpath = inputfilepath + filen
      println(finalpath)
      import sys.process._

      val excell = readExcel(spark, finalpath, "POI")

      var renamedexcel1 = excell.withColumnRenamed("Point of Interest", "point_of_interest").withColumnRenamed("Address", "address")
        .withColumnRenamed("City", "city").withColumnRenamed("Zip", "zip")

      renamedexcel1 = renamedexcel1.withColumn("filename", lit(filen))
      renamedexcel1 = renamedexcel1.withColumn("batchtime", lit(startExecutionTime)).withColumn("voyageid", lit(spark.sparkContext.getConf.get("spark.voyage.id"))).withColumn("part_date", lit(partDate))
      renamedexcel1 = renamedexcel1.select("point_of_interest", "address", "city", "zip", "filename", "batchtime", "part_date")
      println("FINAL POI")
      renamedexcel1.show(false)
      renamedexcel1.printSchema
      
    renamedexcel1.write.mode("Overwrite").insertInto(spark.sparkContext.getConf.get("spark.poi.table"))
		//val excel2 = readExcel(spark, "s3://vv-dev-emr-cluster/data/xlsxfile/Virgin_Voyages_transfer.xlsx", "Transfer Deets")
    val excel2 = readExcel(spark, finalpath, "TransferDeets")  
		excel2.show(false)
      excel2.printSchema
      var renamedexcel2 = excel2.withColumnRenamed("Transfer By", "transfer_by").withColumnRenamed("Sail Date From", "sail_date_from")
        .withColumnRenamed("Sail Date To", "sail_date_to").withColumnRenamed("Pick-up Date", "pick_up_date")
        .withColumnRenamed("Virgin Voyages Res Number", "virgin_voyages_res_number").withColumnRenamed("Sailor Name", "sailor_name")
        .withColumnRenamed("Contact Number", "contact_number").withColumnRenamed("Number of Passengers", "number_of_passengers")
        .withColumnRenamed("Qty of Luggage", "qty_of_luggage").withColumnRenamed("Flight Number", "flight_number")
        .withColumnRenamed("Origin City", "origin_city").withColumnRenamed("Pick-up Time", "pick_up_time1")
        .withColumnRenamed("Pick-up Location", "pick_up_location").withColumnRenamed("Pickup Address", "pickup_address")
        .withColumnRenamed("Pickup City", "pick_up_city").withColumnRenamed("Pickup Zip", "pick_up_zip")
        .withColumnRenamed("Drop Off Location", "drop_off_location").withColumnRenamed("Destination Address", "destination_address")
        .withColumnRenamed("City", "drop_off_city").withColumnRenamed("Zip", "drop_off_zip")
        renamedexcel2.show(false)
      renamedexcel2=renamedexcel2.withColumn("picktime2", split(col("pick_up_time1")," "))
      .select($"transfer_by",$"sail_date_from",$"sail_date_to",$"pick_up_date",$"virgin_voyages_res_number",$"sailor_name",$"contact_number",$"number_of_passengers",$"qty_of_luggage",$"flight_number",$"origin_city",$"picktime2".getItem(1).as("pick_up_time"),$"pick_up_location",$"pickup_address",$"pick_up_city",$"pick_up_zip",$"drop_off_location",$"destination_address",$"drop_off_city",$"drop_off_zip")
      renamedexcel2.show(false)
      renamedexcel2.printSchema
      renamedexcel2 = renamedexcel2.withColumn("filename", lit(filen))
      renamedexcel2 = renamedexcel2.withColumn("batchtime", lit(startExecutionTime)).withColumn("voyageid", lit(spark.sparkContext.getConf.get("spark.voyage.id"))).withColumn("part_date", lit(partDate))
    
      renamedexcel2 = renamedexcel2.select("transfer_by", "sail_date_from", "sail_date_to", "pick_up_date", "virgin_voyages_res_number", "sailor_name", "contact_number", "number_of_passengers", "qty_of_luggage", "flight_number", "origin_city", "pick_up_time", "pick_up_location", "pickup_address", "pick_up_city", "pick_up_zip", "drop_off_location", "destination_address", "drop_off_city", "drop_off_zip","filename", "batchtime", "part_date")

      println("Final df")
//renamedexcel2.show(false)
 //     renamedexcel2.printSchema

      renamedexcel2.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.transfer.table"))

    } catch {

      case e: Exception =>
        {

          ManageMetadata.updateStatus(batchInstanceId, batchId, "Failed", spark)
          log.info("in the catch of updateStatus ******************")
          e.printStackTrace()
          throw new Exception("General Exception..please check the stacktrace")
        }

    }
  }
}