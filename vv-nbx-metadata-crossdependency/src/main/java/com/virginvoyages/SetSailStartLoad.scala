package com.virginvoyages

import org.apache.log4j.{Level, Logger,LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,lit}
import com.virginvoyages.metadataframework.ManageMetadata
import scala.util.control.Breaks._
import java.util.Properties
import org.apache.spark.sql.SaveMode

object SetSailStartLoad {
  
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
				 
				 val temp_table1Sql = "select min(dwhDt.load_start_dt) as load_start_dt, max(dwhDt.load_end_dt) as load_end_dt, dwhDt.load_frequency  from tempview dwhDt where dwhDt.load_frequency like 'SW_DWH_daily' and dwhDt.load_start_dt > ( select COALESCE (max(load_start_dt), '01-01-1900 00:00:00') from tempview dwhRpl  where dwhRpl.load_frequency like 'SW_rpl_daily' and dwhRpl.load_status  = 'Success') group by dwhDt.load_frequency"
				 print(temp_table1Sql)
				 val LatestDF=spark.sql(temp_table1Sql)
				 
				 LatestDF.createOrReplaceTempView("temp")
				

               val url: String = spark.sparkContext.getConf.get("spark.src.con.url")
                 //spark.sparkContext.getConf.get("spark.src.con.url")
                 
               val tableName: String = "vv_metadata.talend_spark_dependency"
               val user: String = spark.sparkContext.getConf.get("spark.src.user")
               val password: String = spark.sparkContext.getConf.get("spark.src.password")
               val properties = new Properties()
               properties.setProperty("user", user)
               properties.setProperty("password", password)
               properties.put("driver", "com.mysql.jdbc.Driver")
                  val finalDF = spark.sql("""select 'SW_rpl_daily' as Load_frequency,load_start_dt as Talend_Start,load_end_dt as Talend_End,current_timestamp as loadtime from temp""")
                  finalDF.write.mode(SaveMode.Overwrite).jdbc(url, tableName, properties)
					
		  }
		  else
		  {
		  log.info("=============Connection is not established================");
						  println("==============Connection is not established================")
		  }
		  
			
			}

}