package com.virginvoyages.dimension

import java.util.Date
import com.virginvoyages.metadataframework.ManageMetadata
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact

import java.util.Properties
import java.sql.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StructType, StructField, TimestampType,StringType, IntegerType,DateType,LongType};
import org.apache.spark.sql.expressions.Window
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import scala.collection.JavaConversions._
import org.apache.spark.sql.functions._
import scala.util.Try
import org.apache.spark.sql.DataFrame
import java.time.{ZonedDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{Column, DataFrame}
import scala.collection.mutable.ArrayBuffer
import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.sql.SaveMode

object DiningReservationSpecialNeedDimLoad {
  val log = LogManager.getRootLogger
			log.setLevel(Level.INFO)


			def main(args: Array[String]) {
		def getSparkSession() =
			{
					val spark = SparkSession
							.builder()
							.enableHiveSupport()
							.getOrCreate()

							spark

			}
		val spark = getSparkSession()
				val sc = spark.sparkContext
				val sqlContext = new org.apache.spark.sql.SQLContext(sc)
				//
				//spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap) 
				val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
				/*************************************calling metadata framework*********************************************/  		


				val metadata=ManageMetadata.fetchBatchTime( sparkConfiguration,spark)        
				metadata.productIterator.foreach(println) 
				val batch_id1=metadata._1
				val batch_instance_id1=metadata._2
				val batch_start_tme=metadata._3
				val batch_end_tme=metadata._4
				val part_read_start=metadata._5
				val part_read_end=metadata._6
				val start_execution_time=metadata._7
				val part_write_date=metadata._8 

				try{

					import spark.sqlContext.implicits._ 

					val whereClause=s""" where batchtime>= '$batch_start_tme' and batchtime<='$batch_end_tme' and part_date>='$part_read_start' and part_date<='$part_read_end'"""
					println(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
					log.info(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")


					println("=======================org unit query==============================")
					val chatDF = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").trim()+ whereClause)

					
								
					

					val required_columns = spark.sparkContext.getConf.get("spark.source.columns").split(",")

					val missing_columns = ArrayBuffer[String]()
					val avaliable_columns = ArrayBuffer[String]()
					def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

					for ( col <-required_columns ) 
					{ 


						if(hasColumn(chatDF, col) ){
							println(col,"column exists",avaliable_columns.toString)
							println("column exists",avaliable_columns.length)
							log.info(col,"column exists",avaliable_columns.toString,avaliable_columns.length)
							println(avaliable_columns.length,"lenthg")
							avaliable_columns.append(col)
						}
						else{
							println(col,"column missing",missing_columns.length)
							println(missing_columns.length,"length")
							log.info(col,"column exists",missing_columns.toString,missing_columns.length)
							println("column missing",missing_columns)
							missing_columns.append(col)
						}

					} 


					print(missing_columns,"Here are the missing columns")
					val stage_final_df=missing_columns.foldLeft(chatDF)((df, c) =>
					df.withColumn(s"$c",  lit(null))
							)
					
					


					println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxx-------------------------------Source-Stage position------------------------------------xxxxxxxxxxxxxxxxxxxxxx")



					if(!stage_final_df.take(1).isEmpty){
						loadDimFact(spark: SparkSession, stage_final_df)}
					ManageMetadata.updateStatus(batch_instance_id1,batch_id1,"Successful",spark)

				}

		catch{  



		case e: SQLException => { ManageMetadata.updateStatus(batch_instance_id1,batch_id1,"Failed",spark);log.info("******************in the catch of chatqueue_detail Dimension Load ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace",e) ;}  

		case e: Exception => { ManageMetadata.updateStatus(batch_instance_id1,batch_id1,"Failed",spark);log.info("******************in the catch of chatqueue_detail  Dimension Load ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace",e) ;}
		println("#----------------------------Process Has Failed---------------------------#")
		System.exit(1)
		//exit(1);
		}







	}

}