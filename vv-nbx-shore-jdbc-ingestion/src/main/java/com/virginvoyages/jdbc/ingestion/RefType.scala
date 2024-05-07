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

object RefType {
  def main(args: Array[String]): Unit = {}
  val spark = SparkSession.builder()
    .enableHiveSupport()
    .getOrCreate()

  val sc = spark.sparkContext
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
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

    var shipCode: String = null
    if (spark.sparkContext.getConf.contains("spark.ship.code")) {

      shipCode = spark.sparkContext.getConf.get("spark.ship.code").toString()
    }
    val voyageId = spark.sparkContext.getConf.get("spark.voyage.id")
    val selectQuery = spark.sparkContext.getConf.get("spark.src.table.query")

    //val data = spark.read.format("jdbc").options(Map("url" ->":5432/N_AWS_API_DB", "user" -> "vvadmin", "password" ->"VvAdmin9$","dbtable" -> "\"xref_dev\".\"reference_type\"", "driver" -> "org.postgresql.Driver")).load()
    var data = spark.read.format(sparkConfiguration.value.get("spark.src.con.format").get).options(Map("url" -> sparkConfiguration.value.get("spark.src.con.url").get, "user" -> sparkConfiguration.value.get("spark.src.user").get, "password" -> sparkConfiguration.value.get("spark.src.password").get,
      "dbtable" -> selectQuery, "driver" -> sparkConfiguration.value.get("spark.src.con.driver").get)).load()
    data.show
    val landingTable = spark.sparkContext.getConf.get("spark.target.table")
    val tempLocation = spark.sparkContext.getConf.get("spark.target.temp").trim()
    val tableLocation = spark.sparkContext.getConf.get("spark.target.table.loc").trim()

    val datadf = data.withColumn("batchtime", lit(batchStartTime).cast(TimestampType))
      .withColumn("part_date", to_date(lit(partWriteDate)))
      .withColumn("voyageid", lit(voyageId).cast(StringType))
      .withColumn("shipcode", lit(shipCode).cast(StringType))
      .withColumn("op", lit("NA").cast(StringType))
      .withColumn("ts_ms", lit(null).cast(TimestampType))


    datadf.createOrReplaceTempView("temp_ref")
    val finaleQuery = "select reference_type_id,reference_source_id,reference_type,op, ts_ms,voyageid, batchtime, part_date from temp_ref"
    val targetDF = spark.sql(finaleQuery)

    targetDF.write.mode(org.apache.spark.sql.SaveMode.Overwrite).parquet(tempLocation)
    spark.sql(s"Alter Table $landingTable SET LOCATION '$tempLocation' ")
    println(targetDF.count)
    //targetDF.write.mode(org.apache.spark.sql.SaveMode.Overwrite).parquet(tableLocation)
    //spark.target.table
    val final_df = spark.read.parquet(tempLocation)
    final_df.write.mode("Overwrite").parquet(tableLocation)
    //val finalLocation = spark.sparkContext.getConf.get("spark.target.table").trim()
    println(tableLocation)
    println(final_df.count)

    /*spark.sql("""alter Table shipdw.hvtb_nbx_landing_xref_reference_type SET LOCATION
's3://vv-dev-emr-cluster/data/landing/xref/hvtb_nbx_landing_xref_reference_type'""")*/
    spark.sql(s"""Alter Table $landingTable SET LOCATION '$tableLocation' """)
    //

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