package com.virginvoyages.jdbc.ingestion

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import com.virginvoyages.metadataframework.ManageMetadata
import org.apache.spark.sql.types.{ StringType, TimestampType }
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import java.sql.SQLException
import org.apache.spark._
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import java.sql.SQLException
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
object IncrementalTimestamp {

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    var batchInstanceId: String = null
    var batchId: String = null
    try {
      sqlContext.setConf("hive.exec.dynamic.partition", "true")
      sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
      val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)

/**********************************MetaData framework**********************************************/
      val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
      metadata.productIterator.foreach(println)
      batchId = metadata._1
      batchInstanceId = metadata._2
      val batchStartTime = metadata._3
      val batchEndTime = metadata._4
      val startExecutionTiime = metadata._7
      val partWriteDate = metadata._8

      log.info("After Metadata Framework")

      val voyageId = spark.sparkContext.getConf.get("spark.voyage.id")
      val selectQuery = spark.sparkContext.getConf.get("spark.src.table.query")
      val conditionColumn = spark.sparkContext.getConf.get("spark.condition.column").trim()
      var tabl1 = selectQuery.replace("\"", "");
      if (selectQuery.contains("(") || selectQuery.contains(")")) { tabl1 = selectQuery.substring(selectQuery.indexOf("(") + 1, selectQuery.lastIndexOf(")")) }
      
      if (tabl1.toLowerCase.contains("from")) {if (!selectQuery.toLowerCase.contains("where")){tabl1 = selectQuery.substring(selectQuery.indexOf(" from ") + 5, selectQuery.lastIndexOf(")")).trim }else if (selectQuery.toLowerCase.contains("where")){tabl1 = selectQuery.substring(selectQuery.indexOf(" from ") + 5, selectQuery.lastIndexOf(" where ")).trim } }
      
      val temptab1 = tabl1.replace("select * from ", "")
      val temptabarr: Seq[String] = temptab1.split("\\.")
      val count = temptabarr.length
      var temptab = temptab1
      if (count == 1) { temptab = "public." + temptab1 }
      
      val TbldetailsPattern = "(\\w+)\\.(\\w+)".r
      val TbldetailsPattern(srcSchema, srcTbl) = temptab
      val QueryColumnList = """(SELECT data_type FROM INFORMATION_SCHEMA.columns WHERE lower(table_schema) = '""" + srcSchema + """' AND lower(table_name) = '""" + srcTbl + """' AND lower(column_name) = '""" + conditionColumn + """') as ColList"""
      val ColDataType = spark.read.format(sparkConfiguration.value.get("spark.src.con.format").get).options(Map("url" -> sparkConfiguration.value.get("spark.src.con.url").get, "user" -> sparkConfiguration.value.get("spark.src.user").get, "password" -> sparkConfiguration.value.get("spark.src.password").get,
        "dbtable" -> QueryColumnList, "driver" -> sparkConfiguration.value.get("spark.src.con.driver").get)).load().collect().mkString(",").replace("[", "").replace("]", "")

      /*val whereQueryClause = """ where """ + conditionColumn + """ >= '""" + batchStartTime + """' and """ + conditionColumn + """ <= '""" + batchEndTime + """'"""
      val SelectQuerywithWhere = """(""" + selectQueryTmp + whereQueryClause + """) as  temptab"""
      println(selectQuery)*/
        
      println("QueryColumnList = "+QueryColumnList)
  
      println("ColDataType = "+ColDataType)


      var conditionalColumnZone = ""
      val BatchStartTimeZone = """TIMESTAMP with TIME ZONE '""" + batchStartTime + """'"""
      val BatchEndTimeZone = """TIMESTAMP with TIME ZONE '""" + batchEndTime + """'"""
      val timeZone = "UTC"
      if (ColDataType == "timestamp with time zone") {
        conditionalColumnZone = """ date_trunc('seconds',""" + conditionColumn + """ AT TIME ZONE '""" + timeZone + """')"""
      } else conditionalColumnZone = conditionColumn
      
//      val selectQueryTmp = "select * from " + temptab
      var selectQuery1 = selectQuery.replace("\"", "").trim
      if (selectQuery.contains("(") || selectQuery.contains(")")) {selectQuery1 = selectQuery.substring(selectQuery.indexOf("(") + 1, selectQuery.lastIndexOf(")")).trim}
      var selectQueryTmp = selectQuery1.trim
      if(selectQuery1.trim.toLowerCase.contains("select") && !selectQuery1.toLowerCase.contains("from")){ selectQueryTmp = selectQuery1.trim + " from " + temptab }else if(!selectQuery1.trim.toLowerCase.contains("select") && !selectQuery1.toLowerCase.contains("from")){ if (selectQuery1 == srcTbl || selectQuery1 == temptab){ selectQueryTmp = "select * from " + temptab }else if(selectQuery1.split(",").length > 1){ selectQueryTmp = "select " + selectQuery1.trim + " from " + temptab } }else if(!selectQuery1.trim.toLowerCase.contains("select") && selectQuery1.toLowerCase.contains("from")){ selectQueryTmp = "select " + selectQuery1.trim }else if(selectQuery1.trim.toLowerCase.contains("select") && selectQuery1.toLowerCase.contains("from")){ if (!selectQuery1.toLowerCase.contains("where")){ selectQueryTmp = selectQuery1.trim }else if(selectQuery1.toLowerCase.contains("where")){ selectQueryTmp = selectQuery1.substring(selectQuery1.indexOf("select "), selectQuery1.lastIndexOf(" where ")).trim } }

      
      val whereQueryClause = """ where """ + conditionalColumnZone + """ >= """ + BatchStartTimeZone + """ and """ + conditionalColumnZone + """ <= """ + BatchEndTimeZone
      val SelectQuerywithWhere = """(""" + selectQueryTmp + whereQueryClause + """) as  temptab"""
      println(SelectQuerywithWhere)
	  log.info("Final_Query : " + SelectQuerywithWhere )
      //Connecting to source database
      val data = spark.read.format(sparkConfiguration.value.get("spark.src.con.format").get).options(Map("url" -> sparkConfiguration.value.get("spark.src.con.url").get, "user" -> sparkConfiguration.value.get("spark.src.user").get, "password" -> sparkConfiguration.value.get("spark.src.password").get,
        "dbtable" -> SelectQuerywithWhere, "driver" -> sparkConfiguration.value.get("spark.src.con.driver").get)).load() //.option("timestampFormat", "MM-dd-yyyy hh mm ss")
      data.show(false)
      // data.createOrReplaceTempView("temp_table")
      // val tempDateDf = spark.sql(s"""select *,from_unixtime(unix_timestamp($conditionColumn, 'yyyy-MM-dd HH:mm:ss.SSS'),'yyyy-MM-dd HH:mm:ss') as incrementalCol from temp_table""")
      
      var shipCode: String = null
      if (spark.sparkContext.getConf.contains("spark.ship.code")) {

        shipCode = spark.sparkContext.getConf.get("spark.ship.code").toString()
      }
/*      val data = spark.read.format(sparkConfiguration.value.get("spark.src.con.format").get)
        .options(Map(
          "url" -> sparkConfiguration.value.get("spark.src.con.url").get,
          "user" -> sparkConfiguration.value.get("spark.src.user").get, "password" -> sparkConfiguration.value.get("spark.src.password").get,
          "dbtable" -> selectQuery, "driver" -> sparkConfiguration.value.get("spark.src.con.driver").get)).load()
*/
      log.info("After dbeaver connection")

      //data.show(false)
      data.printSchema()


      log.info("Filtered Data")
      var filteredDataDF = data
      val source_column_list = filteredDataDF.columns.toSeq
      val source_column_count = source_column_list.length

      if (!filteredDataDF.take(1).isEmpty) {
        println("***********************************************Data in new batch************************************************")
        val tgtLocation = spark.sparkContext.getConf.get("spark.target.location")
        val hiveTable = spark.sparkContext.getConf.get("spark.target.hive.table")
        val required_columns: Seq[String] = spark.sparkContext.getConf.get("spark.source.columns").split(",")
        val required_columns_count = required_columns.length
        val shipcode_flag = source_column_list.map(_.toLowerCase()).contains("shipcode")

        if (source_column_count != required_columns_count) {
          var flag = "Y"
          if (source_column_count > required_columns_count) {

            log.info("*************** Extra column added in Source ******************")
            println("*************** Extra column added in Source ******************")

          } else if (source_column_count < required_columns_count) {
            log.info("*************** Column removed in Source ******************")
            println("*************** Column removed in Source ******************")

            if (spark.sparkContext.getConf.contains("spark.stop.flag")) {

              flag = spark.sparkContext.getConf.get("spark.stop.flag")
            }
            if (flag == "Y") {

              log.info("*************** Please fix the column mismatch and rerun ******************")
              println("*************** Please fix the column mismatch and rerun ******************")
              System.exit(1)
            }

          }
          println("Source column count :" + source_column_count + " Target column count :" + required_columns_count)
          log.info("Source column count :" + source_column_count + " Target column count :" + required_columns_count)

        }
        val missing_columns = ArrayBuffer[String]()
        val avaliable_columns = ArrayBuffer[String]()
        def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

        for (trimcol <- required_columns) {
          val col = trimcol.trim()

          if (hasColumn(filteredDataDF, col)) {
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

        println(missing_columns, "Here are the missing columns")
        filteredDataDF = missing_columns.foldLeft(filteredDataDF)((df, c) =>
          df.withColumn(s"$c", lit(null)))
/********code chnage to convert timestamp columns to utc Start:****/

        val currTZ = spark.conf.getOption("spark.sql.session.timeZone").toString
        val beginIndex = currTZ.indexOf("(") + 1
        val endIndex = currTZ.indexOf(')')
        val tzSession = currTZ.substring(beginIndex, endIndex)
        if (sparkConfiguration.value.contains("spark.tz.columns")) {
          val tzCols = spark.sparkContext.getConf.get("spark.tz.columns").split(",")

          for (tzcol <- tzCols) {
            println("tzcol:::::::::::::::;" + tzcol)
            filteredDataDF = filteredDataDF.withColumn(tzcol, to_utc_timestamp(filteredDataDF.col(tzcol), tzSession))
          }
        }
/******************************End***********************/

        var finalDF = filteredDataDF.withColumn("VoyageId", lit(voyageId).cast(StringType)).withColumn("BatchTime", lit(batchStartTime).cast(TimestampType)).withColumn("Part_Date", to_date(lit(partWriteDate)))
        //.withColumn("ShipCode", lit(shipCode).cast(StringType))
         if (shipcode_flag == false){
           finalDF = finalDF.withColumn("ShipCode", lit(shipCode).cast(StringType))
        }

        // var finalDF = filteredDataDF.withColumn("VoyageId", lit(voyageId).cast(batchStartTime)).withColumn("BatchTime", lit(batchStartWriteTime).cast(TimestampType)).withColumn("Part_Date", to_date(lit(partWriteDate)))
        //.withColumn("ShipCode", lit(shipCode).cast(StringType))
       
        val temp_tab = spark.sparkContext.getConf.get("spark.target.table").replace('.', '_')
        finalDF.createOrReplaceTempView(temp_tab)
         var clause = s""",VoyageId,BatchTime,Part_Date from $temp_tab"""
        if (shipcode_flag == false) {
          clause = s""",VoyageId,BatchTime,ShipCode,Part_Date from $temp_tab"""
        }
        var finaleQuery: String = null
        if (sparkConfiguration.value.contains("spark.source.query")) {

          finaleQuery = "select " + spark.sparkContext.getConf.get("spark.source.query") + clause
        } else {
          finaleQuery = "select " + spark.sparkContext.getConf.get("spark.source.columns") + clause

        }

        println(finaleQuery)
        // val finaleQuery = "select " + spark.sparkContext.getConf.get("spark.source.columns") + s""",VoyageId,BatchTime,Part_Date from $temp_tab"""
        //printlnnaleQuery)//
        val stageFinalDf = spark.sql(finaleQuery)
        stageFinalDf.printSchema
        stageFinalDf.show(false)

        stageFinalDf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.table"))

      }

      ManageMetadata.updateStatus(batchInstanceId, batchId, "Successful", spark)
    } catch {

      case e: SQLException => {
        ManageMetadata.updateStatus(batchInstanceId, batchId, "Failed", spark)
        log.info("***************SQLexception in the catch of Konami Ingestion ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace")
      }
      case e: Exception =>
        {
          ManageMetadata.updateStatus(batchInstanceId, batchId, "Failed", spark)
          log.info("*************** Exception catch in the catch of Konami Ingestion ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace")
        }
        System.exit(1)

    }

  }
}