package com.virginvoyages

import org.apache.log4j.{Level, Logger,LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,lit}
import com.virginvoyages.metadataframework.ManageMetadata
import scala.util.control.Breaks._
import java.util.Properties
import org.apache.spark.sql.SaveMode

object SetSailMetadataUpd {
  
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
          "dbtable" -> sparkConfiguration.value.get("spark.src.talendmetadata.update.table").get, "driver" -> sparkConfiguration.value.get("spark.src.con.driver").get)).load()
          
		  println("inside the metadata")
		  if (!MetadataDF.head(1).isEmpty)
		  {
		      //val MetadataDF = MetadataDF.withColumn("loadtime", current_date())
				 MetadataDF.show(false)
		     
				 MetadataDF.createOrReplaceTempView("tempview")
				 
				 val temp_table1Sql = "select Load_frequency,Talend_Start,Talend_End FROM tempview where Load_frequency = 'SW_rpl_daily'"
				 print(temp_table1Sql)
				 val LatestDF=spark.sql(temp_table1Sql)
				 
				 LatestDF.createOrReplaceTempView("temp")
				

              val url: String = spark.sparkContext.getConf.get("spark.src.con.url")
                 //spark.sparkContext.getConf.get("spark.src.con.url")
                 
               val tableName: String = "vv_metadata.load_dt"
               val user: String = spark.sparkContext.getConf.get("spark.src.user")
               val password: String = spark.sparkContext.getConf.get("spark.src.password")
               val properties = new Properties()
               properties.setProperty("user", user)
               properties.setProperty("password", password)
               properties.put("driver", "com.mysql.jdbc.Driver")
                  val finalDF = spark.sql("""select Talend_Start as load_start_dt,Talend_End as load_end_dt,current_timestamp as load_exec_start_dt,'In Progress' as load_status,'P' as load_ind, 'Incremental' as load_type, 'VV_NBX_ETL' as project_name, Load_frequency as load_frequency from temp""")
                  finalDF.write.mode(SaveMode.Append).jdbc(url, tableName, properties)
					
		  }
		  else
		  {
		  log.info("=============Connection is not established================");
						  println("==============Connection is not established================")
		  }
		  
			
			}

}