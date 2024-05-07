package com.virginvoyages

import org.apache.log4j.{Level, Logger,LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,lit}
import com.virginvoyages.metadataframework.ManageMetadata
import scala.util.control.Breaks._
import java.util.Properties
import org.apache.spark.sql.SaveMode

object HbaseMetadataUpdate {

  val log = LogManager.getRootLogger
			log.setLevel(Level.INFO)

			def main(args: Array[String]): Unit = {
			
			val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
							val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)

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
			
			import spark.implicits._
			
		val MetadataDF = spark.read.format(sparkConfiguration.value.get("spark.src.con.format").get)
        .options(Map(
          "url" -> sparkConfiguration.value.get("spark.src.con.url").get,
          "user" -> sparkConfiguration.value.get("spark.src.user").get, "password" -> sparkConfiguration.value.get("spark.src.password").get,
          "dbtable" -> sparkConfiguration.value.get("spark.src.table").get, "driver" -> sparkConfiguration.value.get("spark.src.con.driver").get)).load()
          
		  println("inside the metadata")
		  if (!MetadataDF.head(1).isEmpty)
		  {
		      //val MetadataDF = MetadataDF.withColumn("loadtime", current_date())
				 MetadataDF.show(false)
		     
				 MetadataDF.createOrReplaceTempView("tempview")
				 
				 val temp_table1Sql = "select load_id,load_frequency as Load_frequency,load_start_dt as Talend_Start,load_end_dt as Talend_End,current_timestamp as loadtime FROM tempview where load_frequency = 'SW_rpl_daily' order by load_id desc limit 1"
				 print(temp_table1Sql)
				 val LatestDF=spark.sql(temp_table1Sql)
				 
				 val finalDF = LatestDF.select("Load_frequency","Talend_Start","Talend_End","loadtime")
				 
				 finalDF.createOrReplaceTempView("temp")

finalDF.write
  .format(spark.sqlContext.sparkContext.getConf.get("spark.metadata.format")) 
  .mode(spark.sqlContext.sparkContext.getConf.get("spark.metadata.mode"))
  .option("table", spark.sqlContext.sparkContext.getConf.get("spark.metadata.table")) 
  .option("zkUrl", spark.sqlContext.sparkContext.getConf.get("spark.zkurl"))
  .save()
  
					
		  }
		  else
		  {
		  log.info("=============Connection is not established================");
						  println("==============Connection is not established================")
		  }
		  
			
			}

}