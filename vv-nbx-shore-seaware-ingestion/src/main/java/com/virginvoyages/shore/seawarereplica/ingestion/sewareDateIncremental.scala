package com.virginvoyages.shore.seawarereplica.ingestion
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

object sewareDateIncremental {
  
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
      val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
      metadata.productIterator.foreach(println)
      batchId = metadata._1
      batchInstanceId = metadata._2
      val batchStartTime = metadata._3
      val batchEndTime = metadata._4
      val startExecutionTiime = metadata._7
      val partWriteDate = metadata._8
      val voyageId = spark.sparkContext.getConf.get("spark.voyage.id")
      val selectQuery = spark.sparkContext.getConf.get("spark.src.table.query")
      val conditionColumn = spark.sparkContext.getConf.get("spark.condition.column")
//rep_user/reportdev@321 
      //Connecting to source database
      val data = spark.read.format(sparkConfiguration.value.get("spark.src.con.format").get).options(Map("url" -> sparkConfiguration.value.get("spark.src.con.url").get, "user" -> sparkConfiguration.value.get("spark.src.user").get, "password" -> sparkConfiguration.value.get("spark.src.password").get,
        "dbtable" -> selectQuery, "driver" -> sparkConfiguration.value.get("spark.src.con.driver").get)).load()
  /* val data = spark.read.format("jdbc").options(Map("url" ->"jdbc:postgresql://d-aws-seaware-db-dmsreplica.cluster-ro-cbduemg5i5w8.us-east-1.rds.amazonaws.com:5432/SEAWARE", "user" -> "rep_user", "password" ->"reportdev@321",
        "dbtable" -> "\"SEAWARE\".\"REVENUE_TARGET_VALUE\"", "driver" -> "org.postgresql.Driver")).load()*/
  
  

     // val filteredDataDF = data.where(col(conditionColumn) >= lit(batchStartTime).cast(TimestampType) && col(conditionColumn) <= lit(batchEndTime).cast(TimestampType)).select("*")
      val finalDF = data.withColumn("VoyageId", lit(voyageId).cast(StringType)).withColumn("BatchTime", lit(batchStartTime).cast(TimestampType)).withColumn("Part_Date", to_date(lit(partWriteDate)))
      // df1.write.mode("append").format("parquet").insertInto(sparkConfiguration.value.get("spark.target.hive.table").get)
      val tgtLocation = spark.sparkContext.getConf.get("spark.target.location")
      val hiveTable = spark.sparkContext.getConf.get("spark.target.hive.table")
      

      finalDF.show
       finalDF.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.table"))
    /* finalDF.write.mode(org.apache.spark.sql.SaveMode.Append).parquet(tgtLocation + "/" + partWriteDate)

      val insertString = s"""alter table $hiveTable add IF NOT EXISTS  partition(Part_Date='$partWriteDate') location '$tgtLocation/$partWriteDate'""".stripMargin
      // val insertString1 = "alter table" + hiveTable = "add if not exists partition(partitionKey='" + partWriteDate + "') location '" + tgtLocation + "/" + partWriteDate + "'"
      println(insertString)
      spark.sql(insertString)
      spark.sql("refresh table " + hiveTable)
      spark.sql("Msck repair table " + hiveTable)*/
      ManageMetadata.updateStatus(batchInstanceId, batchId, "Successful", spark)
    } catch {

      case e: SQLException => {
        ManageMetadata.updateStatus(batchInstanceId, batchId, "Failed", spark)
        log.info("***************in the catch of Konami Ingestion ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace")
      }
      case e: Exception =>
        {
          ManageMetadata.updateStatus(batchInstanceId, batchId, "Failed", spark)
          log.info("***************in the catch of Konami Ingestion ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace")
        }
        System.exit(1)

    }
  }
  
  
  
}