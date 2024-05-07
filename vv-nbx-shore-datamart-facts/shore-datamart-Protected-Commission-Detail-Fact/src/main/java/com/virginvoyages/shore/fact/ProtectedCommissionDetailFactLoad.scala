package com.virginvoyages.shore.fact
import com.virginvoyages.metadataframework.ManageMetadata
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact
import org.apache.spark.broadcast.Broadcast
import java.net.UnknownHostException
import scala.util.parsing.json._
//import scalaj.http.Http
//import scalaj.http.HttpOptions
import org.apache.log4j.LogManager
import org.apache.log4j.Level


object ProtectedCommissionDetailFactLoad {
  // Creating logger
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
  // Defining the main method
  def main(args: Array[String]): Unit = {

    //creating spark session
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    // creating spark context
    val sc = spark.sparkContext
   
    val sparkConfiguration = sc.broadcast(sc.getConf.getAll.toMap)
    //Calling Metadataframework to get the batchtime and partdate which is used to  get incremental data from source database(Hive table)

    
    val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
      //batch_instance_id1, batch_id1
      val batch_id1 = metadata._1
      val batch_instance_id1 = metadata._2
      val batchStartTme = metadata._3
      val batchEndTme = metadata._4
      val partReadStart = metadata._5

      val partReadEnd = metadata._6
      val startExecutionTime = metadata._7
      val part_write_date = metadata._8
      val env = spark.sparkContext.getConf.get("spark.api.env")
    try {
      
     
      //val whereClause = s""" where 1=1 """      
      val whereClause = s""" where Schedulefact.batchtime>= '$batchStartTme' and Schedulefact.batchtime<='$batchEndTme' and Schedulefact.part_date>='$partReadStart' and Schedulefact.part_date<='$partReadEnd'"""
      println(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
      log.info(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")

      import spark.sqlContext.implicits._
      println("#---------------------------Starting the Execution------------------#")
      val query = spark.sparkContext.getConf.get("spark.source.sql").trim()+whereClause
      
      
      val stageRevenueDF = spark.sql(query)    
      println(query)
      //stageRevenueDF.show(30,false)
      
      log.info(query)
      
      if (!stageRevenueDF.head(1).isEmpty) {
        loadDimFact(spark, stageRevenueDF)
        // Calling Metadataframe to save the status of the job

      }
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)

      spark.stop()
    } catch {
      //case e: SQLException => { ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("******************in the catch ofvv-nbx-datamart-order-factsn Load ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace", e); }

      case e: Exception =>
        { ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("******************in thevv-nbx-datamart-order-factsLoad ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace", e); }
        //case e: SQLException => println("#----------------------------Process Has Failed---------------------------#")
        System.exit(1)
        spark.stop()

    }

  }

 
}
