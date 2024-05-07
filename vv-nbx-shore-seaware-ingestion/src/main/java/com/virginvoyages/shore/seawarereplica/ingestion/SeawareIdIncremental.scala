package com.virginvoyages.shore.seawarereplica.ingestion
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.LogManager
//import com.virginvoyages.metadataframework.ManageMetadata
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

object SeawareIdIncremental {
  
    def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    val selectQuery = spark.sparkContext.getConf.get("spark.src.table.query")
    val conditionCol=spark.sql(spark.sparkContext.getConf.get("spark.hive.table.max.id")).first().getInt(0)
   val newselectQuery:String = selectQuery +conditionCol
    
    
   val data = spark.read.format(sparkConfiguration.value.get("spark.src.con.format").get).options(Map("url" -> sparkConfiguration.value.get("spark.src.con.url").get, "user" -> sparkConfiguration.value.get("spark.src.user").get, "password" -> sparkConfiguration.value.get("spark.src.password").get,
        "dbtable" -> newselectQuery, "driver" -> sparkConfiguration.value.get("spark.src.con.driver").get)).load()
        
        
    }
  
}