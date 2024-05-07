package com.virginvoyages.dimension
import com.virginvoyages.metadataframework.ManageMetadata
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact
import org.apache.spark.sql.SparkSession
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import scala.util.Try
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import java.sql.SQLException

object PreferenceDimLoad {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    
    import spark.implicits._
    
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    
    
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")

    
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

      val whereClause = s""" where batchtime>= '$batch_start_tme' and batchtime<='$batch_end_tme' and part_date>='$part_read_start' and part_date<='$part_read_end'"""
      
      log.info(s"""whereclause:: $whereClause """)

      import spark.sqlContext.implicits._
      
      
      
      val personPrefDf = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").replace("*whereclause*", whereClause))
      
      val allergyDf = spark.sql(spark.sparkContext.getConf.get("spark.allergy.sql"))
      
      val prefCatgDf = spark.sql(spark.sparkContext.getConf.get("spark.prefCatgDf.sql"))
      
      val preferenceDf = personPrefDf.join(prefCatgDf,personPrefDf.col("person_preference_type_id") === prefCatgDf.col("lookup_item_id") , "left")
                                     .join(allergyDf,personPrefDf.col("person_preference_type_id") === allergyDf.col("lookup_item_id") , "left")
                                     .select("preference_id", "person_id","preference_category_code","category","description","comment","allergy","allergy_code","preference_upd_date","src_deleted_flag","voyage_id","batchtime","part_date")
                                     
                                     
       preferenceDf.show                              

      if (!preferenceDf.head(1).isEmpty) {

        loadDimFact(spark: SparkSession, preferenceDf)
      }
      
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)

    } 
    
    
  catch{  
     
     
  
    case e: SQLException => { ManageMetadata.updateStatus(batch_instance_id1,batch_id1,"Failed",spark);
    log.info("******************in the catch of Dimension Load ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace",e) ;}  

    case e: Exception => { ManageMetadata.updateStatus(batch_instance_id1,batch_id1,"Failed",spark);
    log.info("******************in the catch of  Dimension Load ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace",e) ;}
    println("#----------------------------Process Has Failed---------------------------#")
    System.exit(1)
    //exit(1);
    }


  }

}
