package com.virginvoyages.dimension

import java.util.Date

import com.virginvoyages.scd.ChangeDataCapture.loadDimFact

import java.util.Properties
import java.sql.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ StructType, StructField, TimestampType, StringType, IntegerType, DateType, LongType };
import org.apache.spark.sql.expressions.Window
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import scala.collection.JavaConversions._
import org.apache.spark.sql.functions._
import scala.util.Try
import java.io.IOException
import org.apache.spark.sql.DataFrame
import java.time.{ ZonedDateTime, ZoneId }
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{ Column, DataFrame }
import scala.collection.mutable.ArrayBuffer
import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.sql.SaveMode
import java.net.URI
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object QuestionAnswerDimLoad {
   
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

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    var file_check_flag = 0
    try {
      file_check_flag = fs.listStatus(new Path(spark.sparkContext.getConf.get("spark.source.location"))).filter(_.isDir).map(_.getPath).length
      println("----------------------The Path is " + spark.sparkContext.getConf.get("spark.source.location") + "--------------------------------")
      file_check_flag = 1
    } catch {
      case e: IOException => { file_check_flag = 0; log.info("******************Folder not found ******************"); println("******************Folder not found ******************"); }

    }
    print("The file status is ", file_check_flag, "1 ==> files are present ")
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)

    if (file_check_flag != 0) {

      import spark.sqlContext.implicits._

      println(s"""#---------------------------Starting the Execution--for $file_check_flag ----------------#""")
      log.info(s"""#---------------------------Starting the Execution--for $file_check_flag ----------------#""")

      val srcQuestionDF = spark.read.option("sep", "\\t").option("header", "true").csv(spark.sparkContext.getConf.get("spark.source.location") + "/*")
                
                
                
                
      //srcQuestionDF.show()
      //srcQuestionDF.write.parquet("spark.landing.location") //land for backup
                
      srcQuestionDF.createOrReplaceTempView("src_questionanswer_dim")
               
      val src_question_landing_df = spark.sql("select now() as batchtime,`Question ID` as src_question_id ,`Variable ID` as variable_id ,`Question Label` as question_text,Type as Type,`Category` as category,cast(`Answer Code` as integer) as answer_code,`Answer Label` as answer_code_text from src_questionanswer_dim ").filter($"src_question_id" rlike "^Q[0-9].*").as("bp")
                
                
                //src_question_landing_df.printSchema 
                                                          
                                                          
                src_question_landing_df.createOrReplaceTempView("srcdata")
                val srcdatawithoutQdata = spark.sql("""select * from srcdata where src_question_id  <> 'Q27' and variable_id <> 'Q27'""")

     // val srcdatawithQdata = spark.sql("""select src.batchtime,src.src_question_id,src.variable_id,src.question_text,src.Type,src.category,event.answer_code,src.answer_code_text from srcdata src left join shipdw.hvtb_nbx_lkp_entertainment event on src.src_question_id = event.Question_code where src.src_question_id = 'Q27' and src.variable_id = 'Q27'""")
              
               val srcdatawithQdata = spark.sql("""select src.batchtime,src.src_question_id,src.variable_id,src.question_text,src.Type,src.category,event.EventCode as answer_code,event.event as answer_code_text from srcdata src left join shipdw.hvtb_nbx_lkp_entertainment event on src.src_question_id = event.Question_code where src.src_question_id = 'Q27' and src.variable_id = 'Q27'""")
                
                val dfUnion = srcdatawithoutQdata.union(srcdatawithQdata)
                val dfDup = dfUnion.dropDuplicates()
                
                
                 dfDup.show(10,false)
                
                
      //src_question_landing_df.show()
                
      //df.write.mode("append").parquet(spark.sparkContext.getConf.get("spark.landing.location"))
                
      val src_question_df = dfDup.filter($"src_question_id" rlike "^Q[0-9].*").as("bp")
                
      //src_question_df.filter($"src_question_id"==="Q2").show()
      //src_question_df.printSchema()
                
                
                
                
                
                             
      val required_columns = spark.sparkContext.getConf.get("spark.source.columns").split(",")

      val missing_columns = ArrayBuffer[String]()
      val avaliable_columns = ArrayBuffer[String]()
      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

      for (col <- required_columns) {

        if (hasColumn(src_question_df, col)) {
          println(col, "column exists", avaliable_columns.toString)
          println("column exists", avaliable_columns.length)
          log.info(col, "column exists", avaliable_columns.toString, avaliable_columns.length)
          println(avaliable_columns.length, "length")
          avaliable_columns.append(col)
        } else {
          println(col, "column missing", missing_columns.length)
          println(missing_columns.length, "length")
          log.info(col, "column exists", missing_columns.toString, missing_columns.length)
          println("column missing", missing_columns)
          missing_columns.append(col)
        }

      }

      print(missing_columns, "Here are the missing columns")
      val stage_final_df = missing_columns.foldLeft(src_question_df)((df, c) =>
        df.withColumn(s"$c", lit(null)))
      // stage_final_df.show()
      println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxx-------------------------------Calling SCD Source-Stage QuestionDim------------------------------------xxxxxxxxxxxxxxxxxxxxxx")
      log.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxx-------------------------------Calling SCD Source-Stage QuestionDim------------------------------------xxxxxxxxxxxxxxxxxxxxxx")

      if (!stage_final_df.head(1).isEmpty) {
        loadDimFact(spark: SparkSession, stage_final_df)
      }
    } else {
      println("#---------------------------------------File does not exists-----------------------------------#")
      //System.exit(0)

    }

  }
  
}
