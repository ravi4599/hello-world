package com.virginvoyages.metadataframework
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import java.time.format.DateTimeFormatter
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import com.virginvoyages.metadataframework.HBaseConnectionException
import org.apache.spark.sql.types.{ StringType, TimestampType }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import scala.collection.Seq
import org.apache.spark.sql.functions.{ lower, upper }
import org.apache.spark.broadcast.Broadcast
import java.util.Date
import java.sql.Timestamp
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql._
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.util.Properties

object ManageMetadata {

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def fetchBatchTime(configMap: Broadcast[Map[String, String]], spark: SparkSession): (String, String, String, String, String, String, String, String) = {
    import spark.implicits._
    val srcConfigId = configMap.value.get("spark.src.config.id").get
    val tgtConfigId = configMap.value.get("spark.target.config.id").get
    val voyageId = configMap.value.get("spark.voyage.id").get
    val batch1 = srcConfigId.concat("-").concat(tgtConfigId) 
    val batchInstance = batch1.concat("-").concat(DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now))

    var currenttime1 = current_timestamp().cast(TimestampType)

    val ts = currenttime1.expr.eval().toString.toLong
    val currenttime = new java.sql.Timestamp(ts / 1000)
    var endBatchTime: String = currenttime.toString()
    var startBatchTime: String = currenttime.toString()
    var startExecutionTime: String = currenttime.toString()
    var partReadStart: String = null
    var partReadEnd: String = null
    var partWriteEnd: String = null
    import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType, DateType, LongType };
    val s = col(startBatchTime).cast(TimestampType)
    try {

      var env = "DEV"
      var ingType = "Ingestion"
      val configValue = configMap.value
      if (configValue.contains("spark.tgt.env")) {
        env = configMap.value.get("spark.tgt.env").get
      }
      if (configValue.contains("spark.ing.type")) {
        ingType = configMap.value.get("spark.ing.type").get
      }
      log.info(" Inside fetch batch time proc") 

/**************************Reading from Metadata table ***************************/
      
       val pguser=spark.sparkContext.getConf.get("spark.metadata.user").trim()
       val pgpassword=spark.sparkContext.getConf.get("spark.metadata.password").trim()
       val jdbcUrl = spark.sparkContext.getConf.get("spark.metadata.con.url").trim() 
       val tableName =  spark.sparkContext.getConf.get("spark.metadata.enablerops.table").trim()
       val jdbcDriver = spark.sparkContext.getConf.get("spark.metadata.con.driver").trim()
       val con_format = spark.sparkContext.getConf.get("spark.metadata.con.format").trim()
       val op_mode = spark.sparkContext.getConf.get("spark.metadata.ops.table.mode").trim()
       
        val metatadataTableDF = spark.read.format(con_format)
        .option("mode",op_mode)
        .option("driver",jdbcDriver)
        .option("dbtable",tableName)
        .option("url",jdbcUrl)
        .option("user",pguser)
        .option("password",pgpassword)
        .load()
       log.info("Below records from Postgre Cloud SQL source Metadata table")
       metatadataTableDF.show(5)  
     
      
      var parentBatchId: String = null
      var parentBatchIds = List[String]()
      var flag = 0
      var metatadataTableFilteredDF = spark.emptyDataFrame

      if (configValue.contains("spark.same.parent")) {
        parentBatchId = configMap.value.get("spark.parent.id").get
        parentBatchIds = (configMap.value.get("spark.parent.id").get).split(",").map(_.trim).toList
        metatadataTableFilteredDF = metatadataTableDF.filter($"BATCH_ID" === batch1 || $"BATCH_ID".isin(parentBatchIds: _*)).select("BATCHSTARTTIME", "BATCHENDTIME", "STATUS", "BATCH_ID", "PARENTBATCH", "BATCH_INSTANCE_ID")

        //  metatadataTableFilteredDF.show(false)
        val currentBatchDf = metatadataTableDF.filter($"BATCH_ID" === batch1 && lower($"STATUS") === "successful").select("BATCHSTARTTIME", "BATCHENDTIME", "STATUS", "BATCH_ID")
        //currentBatchDf.show(false)

        val maxEndCurrent: String = currentBatchDf.agg(max($"BATCHENDTIME")).first.get(0).toString
        println("MAX starttime for current batch string" + maxEndCurrent)

        val succssParentDf = metatadataTableFilteredDF.filter(col("BATCH_ID").isin(parentBatchIds: _*) && lower($"STATUS") === "successful").select("*")
        val newBatchDf = succssParentDf.filter(col("BATCHSTARTTIME") > maxEndCurrent).select("*")
        newBatchDf.show(false)

        if (!newBatchDf.head(1).isEmpty) {
          startBatchTime = newBatchDf.agg(min("BATCHSTARTTIME")).first.get(0).toString
          endBatchTime = newBatchDf.agg(max("BATCHENDTIME")).first.get(0).toString
        } else {
          startBatchTime = maxEndCurrent
          endBatchTime = maxEndCurrent

        }

      } else {
        if (configValue.contains("spark.parent.id")) {
          //Reading from current batchid and parent batch

          parentBatchId = configMap.value.get("spark.parent.id").get
          parentBatchIds = (configMap.value.get("spark.parent.id").get).split(",").map(_.trim).toList
          flag = 1
          metatadataTableFilteredDF = metatadataTableDF.filter($"BATCH_ID" === batch1 || $"BATCH_ID".isin(parentBatchIds: _*)).select("BATCHSTARTTIME", "BATCHENDTIME", "STATUS", "BATCH_ID", "PARENTBATCH", "BATCH_INSTANCE_ID")
        } else {
          //Reading only for current batch
          metatadataTableFilteredDF = metatadataTableDF.filter($"BATCH_ID" === batch1).select("BATCHSTARTTIME", "BATCHENDTIME", "STATUS", "BATCH_ID")
        }
        println("metatadataTableFilteredDF records  ")
        metatadataTableFilteredDF.show(5)
        //metatadataTableFilteredDF.show(false)

        //If parent is present
        if (flag == 1) {
          log.info("Parent Batch ID Presnt")
          log.info("Teting the script")
          val succssParentDf = metatadataTableFilteredDF.filter(col("BATCH_ID").isin(parentBatchIds: _*) && lower($"STATUS") === "successful").select("*")
          parentBatchIds.foreach(println)
           println("succssParentDf  records  ")
          succssParentDf.show(5)
          if (!succssParentDf.head(1).isEmpty) {

               endBatchTime = succssParentDf.groupBy($"BATCH_ID").agg(max($"BATCHSTARTTIME") as "BATCHSTARTTIME").agg(min("BATCHSTARTTIME")).first.get(0).toString
              log.info("Endtime " + endBatchTime)

            
          }
        }

        val successDf = metatadataTableFilteredDF.filter($"BATCH_ID" === batch1 && lower($"STATUS") === "successful")
        if (!successDf.head(1).isEmpty) {

          startBatchTime = successDf.agg(max("BATCHENDTIME")).first.get(0).toString
        } else {
          import java.sql.Timestamp
          import spark.implicits._

          // startBatchTime = Timestamp.valueOf("1900-01-01 00:00:00").toString()
          val temp = new SimpleDateFormat("yyyy-MM-dd 00:00:00.000").format(new Date)
          startBatchTime = temp.toString()

        }

        log.info("Batch Start time before--> " + startBatchTime)
        log.info("Batch End  time before--> " + endBatchTime)

        val inputFormat = "yyyy-MM-dd HH:mm:ss.SSS"
        val outputFormat = "yyyy-MM-dd HH:mm:ss"
        startBatchTime = dateAddSec(startBatchTime, 1, inputFormat, outputFormat)
        endBatchTime = dateAddSec(endBatchTime, 0, inputFormat, outputFormat)

        log.info("Batch Start time after--> " + startBatchTime)
        log.info("Batch End before time --> " + endBatchTime)
      }

      val enablerFinalDF = spark.sparkContext.parallelize(Seq(batchInstance)).toDF("BATCH_INSTANCE_ID").withColumn("VOYAGEID", lit(voyageId).cast(StringType))
        .withColumn("BATCHSTARTTIME", lit(startBatchTime).cast(TimestampType))
        .withColumn("BATCHENDTIME", lit(endBatchTime).cast(TimestampType))
        .withColumn("BATCH_ID", lit(batch1).cast(StringType))
        .withColumn("PARENTBATCH", lit(parentBatchId).cast(StringType))
        .withColumn("SRCCONFIGID", lit(srcConfigId).cast(StringType))
        .withColumn("TGTCONFIGID", lit(tgtConfigId).cast(StringType))
        .withColumn("BATCH_EXECUTION_STARTTIME", lit(startExecutionTime).cast(TimestampType))
        .withColumn("BATCH_EXECUTION_ENDTIME", lit(current_timestamp()).cast(TimestampType))
        .withColumn("STATUS", lit("Running").cast(StringType))
        .withColumn("ENVIRONMENT", lit(env).cast(StringType)).withColumn("TYPE", lit(ingType).cast(StringType))
        .select("BATCH_INSTANCE_ID", "BATCH_ID", "PARENTBATCH", "SRCCONFIGID", "TGTCONFIGID", "BATCHSTARTTIME", "BATCHENDTIME", "BATCH_EXECUTION_STARTTIME",
          "BATCH_EXECUTION_ENDTIME",
          "STATUS", "ENVIRONMENT", "VOYAGEID", "TYPE")
    
         println("fetch batch time proc -- writing to Postgre Cloud SQL database ")
         enablerFinalDF.write
        .format(configMap.value.get("spark.metadata.con.format").get)
        .mode(configMap.value.get("spark.metadata.ops.table.mode").get)
        .option("driver",configMap.value.get("spark.metadata.con.driver").get)
        .option("dbtable", configMap.value.get("spark.metadata.enablerops.table").get)
        .option("url", configMap.value.get("spark.metadata.con.url").get)
        .option("user", configMap.value.get("spark.metadata.user").get)
        .option("password", configMap.value.get("spark.metadata.password").get)
        .save
          

      val pattern = raw"(\d{4})-(\d{2})-(\d{2})".r
      partReadStart = (pattern findFirstIn startBatchTime).map(_.toString).getOrElse("")
      partReadEnd = (pattern findFirstIn endBatchTime).map(_.toString).getOrElse("")
      partWriteEnd = (pattern findFirstIn startExecutionTime).map(_.toString).getOrElse("")

    } catch {
      case e: SQLException => {
        log.info("cloudsql connectioin issue..please check cloudsql service")
        e.printStackTrace()
        throw new HBaseConnectionException("Connection Exception")
      }

      case e: Exception => {
        log.info("in the catch of upsertVoyageOperationMetadata ******************")
        e.printStackTrace()
        throw new Exception("General Exception..please check the stacktrace")
      }
    }

    log.info("Batch: " + batch1)
    log.info("batchInstance: " + batchInstance)
    log.info("startBatchTime: " + startBatchTime)
    log.info("endBatchTime: " + endBatchTime)
    log.info("partReadStart: " + partReadStart)
    log.info("partReadEnd: " + partReadEnd)
    log.info("startBatchTime: " + startBatchTime)
    log.info("partWriteEnd: " + partWriteEnd)
    return (batch1, batchInstance, startBatchTime, endBatchTime, partReadStart, partReadEnd, startBatchTime, partReadStart)
  }

   def dateAddSec(date: String, seconds: Int, inputFormat: String, outputFormat: String): String = {
    import java.util.Calendar
    val dateAux = Calendar.getInstance()
    dateAux.setTime(new SimpleDateFormat(inputFormat).parse(date))
    dateAux.add(Calendar.SECOND, +seconds)
    return new SimpleDateFormat(outputFormat).format(dateAux.getTime())
  }

  //Update status fail or successful
  def updateStatus(batchInstance: String, batch_id: String, status: String, spark: SparkSession) {
      try {
      import spark.implicits._
       log.info("in   updateStatus ******************")
       println( "start reading from jdbc ")
       val pguser=spark.sparkContext.getConf.get("spark.metadata.user").trim()
       val pgpassword=spark.sparkContext.getConf.get("spark.metadata.password").trim()
       val jdbcUrl = spark.sparkContext.getConf.get("spark.metadata.con.url").trim() 
       val jdbcDriver = spark.sparkContext.getConf.get("spark.metadata.con.driver").trim()
   
    println( "start reading from  2 ")
            
    var currenttime1 = current_timestamp().cast(TimestampType)
    val ts = currenttime1.expr.eval().toString.toLong
    val currenttime = new java.sql.Timestamp(ts / 1000)
    var endBatchTime = currenttime 
    
       
 try{
      println( "start reading from in try ")
      Class.forName(jdbcDriver);
      val connObj = DriverManager.getConnection(jdbcUrl, pguser, pgpassword);
      val upd_qry="UPDATE shipdw.HBTB_INGESTION_METADATA  SET  STATUS=?, BATCH_EXECUTION_ENDTIME=?  WHERE  BATCH_INSTANCE_ID = ? and BATCH_ID = ? "
      println("update query  "+ upd_qry)
      val statement = connObj.prepareStatement(upd_qry)
      try{
          statement.setString(1,status);
          statement.setTimestamp(2,endBatchTime);
          statement.setString(3,batchInstance);
          statement.setString(4,batch_id);     
          val number_of_rows_updated = statement.executeUpdate();
          println("number_of_rows_updated  " +number_of_rows_updated)
      }
      finally{
          statement.close();
          }
      connObj.close();
  }
  catch {
      case e:SQLException => e.printStackTrace();
  }
  
    } catch {
      case e: SQLException => {
        e.printStackTrace()
        throw new HBaseConnectionException("CloudSQL Connection Exception")
      }

     case e: Exception => {
        log.info("in the catch of updateStatus ******************")
        e.printStackTrace()
        throw new Exception("General Exception..please check the stacktrace")
      }
    }

  }

}
