package com.virginvoyages.mxp.parser



import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import org.apache.spark.SparkConf
import java.sql.SQLException
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.Row

import org.apache.spark.sql.functions.{ avg, explode, concat, lit, trim }
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.to_json
import java.sql.Struct
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.Column
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Column, DataFrame }
import scala.collection.mutable.ArrayBuffer

import java.text.SimpleDateFormat
import org.apache.spark.SparkConf
import java.io._
import java.nio.charset.StandardCharsets
import org.apache.spark.SparkFiles
import scala.util.Try
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

import com.virginvoyages.metadataframework.ManageMetadata

object MealCountParser {
  
  
   def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    import spark.implicits._
    
    var batch_instance_id1: String = null
    var batch_id1: String = null
    try{
      
      sqlContext.setConf("hive.exec.dynamic.partition", "true")
      sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
      val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
      metadata.productIterator.foreach(println)

      val batch_start_time = metadata._3
      val batch_end_time = metadata._4
      val part_start_time = metadata._5.toString()
      val part_end_time = metadata._6.toString()
      val start_execution_time = metadata._7.toString()
      val part_write_date = metadata._8.toString()

      batch_instance_id1 = metadata._2
      batch_id1 = metadata._1
      
        val sourceDF = spark.sql(spark.sparkContext.getConf.get("spark.source.table").trim()) 
      
        var mealCountDF = sourceDF.select("message", "batchtime", "part_date","ShipCode").where($"BatchTime" >= lit(batch_start_time).cast(TimestampType) && $"BatchTime" <= lit(batch_end_time).cast(TimestampType) and ($"part_date".between(part_start_time, part_end_time)))
			  					
		    //if(! mealCountDF.head(1).isEmpty)
        if( mealCountDF.count() > 0)
        {
			var shipcode=mealCountDF.select("ShipCode").toString()
		    val Kafkamessage = mealCountDF.select("message").collect().map(x=>x.getString(0)).apply(0)		
		    
        val mealCountItemsMsg = spark.read.json(Seq(Kafkamessage).toDS)
        .withColumn("BatchTime",lit(start_execution_time))
        .withColumn("VoyageId", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
        .withColumn("partDate",lit(part_write_date))
        .withColumn("ShipCode", lit(shipcode).cast(StringType))            
                                      
          val FullmealCountDF = mealCountItemsMsg
                              .select(col("voyageid"),col("batchtime"),col("ID").as ("mealid"),
                               col("date").as ("mealdate"),col("restaurant"),col("restaurantCode"),
                               col("meal"),col("mealCode"),col("ShipCode"),col("partdate"))
          FullmealCountDF.show(false)
                               
          FullmealCountDF.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.mealcount.table"))
        
          log.info("data inserted in meal count table")
         
		 		  if (hasColumn(mealCountItemsMsg , "dishes")) 
		 		  {
            if (checkArray (mealCountItemsMsg , "dishes"))
            {
              val mealCountDishesMsg = mealCountItemsMsg.withColumn("virtual_dishes" ,explode_outer($"dishes"))
             

              val mealCountDishes = mealCountDishesMsg.select(col("VoyageId").as ("voyageid"),
                                                            col("BatchTime").as ("batchtime"),
                                                            col("ID").as ("mealid"),
                                                            col("virtual_dishes.course").as ("course"),
                                                            col("virtual_dishes.courseCode").as ("courseCode"),
                                                            col("virtual_dishes.dishId").as ("dishId"),
                                                            col("virtual_dishes.dishName") as ("dishName"),
                                                            col("virtual_dishes.dishCode").as ("dishCode"),                                                          
                                                            col("virtual_dishes.order").as ("dishorder"),
                                                            col("ShipCode"),
                                                            col("partdate"))
              mealCountDishes.show(false)                                                
                                                  
              mealCountDishes.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.dishes.table"))
               
	           }
		         ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark) 
         }
   	     else 
	        {
	  
	           log.info("Dishes message blank")
  	      }  
        
        }else{
        log.info("""#---------------------------DataFrame is null ----------------#""")

      } 

          def checkArray(df: DataFrame, colname: String): Boolean = 
          {
            df.schema(colname).dataType match 
            {
              case ArrayType(_, _) => return true
              case _               => return false
            }
          }
      
          def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
        }
        catch 
        {
      
          case e: Exception =>
          {
         
            ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark)
            log.info("in the catch of updateStatus ******************")
            e.printStackTrace()
            throw new Exception("General Exception..please check the stacktrace")
          }
        }
      }
  }