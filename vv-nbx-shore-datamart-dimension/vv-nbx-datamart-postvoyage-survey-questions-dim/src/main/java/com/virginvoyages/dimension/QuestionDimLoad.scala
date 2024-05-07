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

object QuestionDimLoad {
    
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

      val srcQuestionlandingDF = spark.read.option("sep", "\\t").option("header", "true").csv(spark.sparkContext.getConf.get("spark.source.location") + "/*")
      
    val srcWithoutQ27QuestionlandingDF = srcQuestionlandingDF.filter("`question id` != 'Q27' and `variable id` != 'Q27'")
    srcWithoutQ27QuestionlandingDF.createOrReplaceTempView("withoutq27df")
	
	val srcWithoutQ27FinalDF = spark.sql("""select `Question ID`,`Variable ID`,Type,Start,Finish,`Answer Code`,`Category`,`Question Label`,`Answer Label` from withoutq27df""")
    
    
    val Q27lkp = spark.sql("select start_date,end_date,Ship,Event,EventCode, Question_code from shipdw.hvtb_nbx_lkp_entertainment")
    
    Q27lkp.createOrReplaceTempView("q27lkp")
    
    var srcWithQ27QuestionlandingDF = srcQuestionlandingDF.filter("`question id` = 'Q27' and `variable id` = 'Q27'")
    
    srcWithQ27QuestionlandingDF.createOrReplaceTempView("withq27")
    
    val srcWithQ27QuestionlandingDF1 = spark.sql("""select `Question ID`,`Variable ID`,Type,Start,Finish,EventCode as `Answer Code` , Category,`Question Label`,event as `Answer Label` from (select distinct `Question ID`,`Variable ID`,Type,Start,Finish,Category,`Question Label` from withq27) a left join q27lkp b on a.`question id`=b.Question_code""")
    
         val srcQuestionDF = srcWithoutQ27FinalDF.union(srcWithQ27QuestionlandingDF1)
         
      //srcQuestionDF.show()
      //srcQuestionDF.write.parquet("spark.landing.location") //land for backup
      srcQuestionDF.createOrReplaceTempView("src_questions_dim")
      val src_question_landing_df = spark.sql("select now() as batchtime,`Question ID` as src_question_id ,`Variable ID` as variable_id ,Type as Type,Start,Finish,cast(`Answer Code` as integer) as answer_code,`Category` as category,`Question Label` as question_text,`Answer Label` as answer_label from src_questions_dim ").filter($"src_question_id" rlike "^Q[0-9].*").as("bp")
      //src_question_landing_df.show()
      //src_question_landing_df.write.mode("append").parquet("gs://vv-dev-nbx-cluster/Testing/Sarang")
      val src_question_df = src_question_landing_df.filter($"src_question_id" rlike "^Q[0-9].*").as("bp")
      //src_question_df.filter($"src_question_id"==="Q2").show()
      //src_question_df.printSchema()
      val stage_groupedQuestionDimDF = src_question_df.groupBy($"src_question_id", $"variable_id").agg(min($"answer_code").as("answer_code_min"), max($"answer_code").as("answer_code_max")).as("gp")
      //stage_groupedQuestionDimDF.filter($"src_question_id"==="Q2").show()
      //stage_groupedQuestionDimDF.printSchema()
      val stage_QuestionDimDFMax = src_question_df.join(stage_groupedQuestionDimDF, col("bp.src_question_id") === col("gp.src_question_id") && col("bp.variable_id") === col("gp.variable_id") && col("bp.answer_code") === col("gp.answer_code_max"), "inner").selectExpr("bp.src_question_id", "answer_code_max", "answer_code_min", "bp.variable_id", "answer_label as answer_code_max_text").as("max_src")
      //stage_QuestionDimDFMax.filter($"src_question_id"==="Q2").show()
      val stage_QuestionDimDFMin = src_question_df.join(stage_groupedQuestionDimDF, col("bp.src_question_id") === col("gp.src_question_id") && col("bp.variable_id") === col("gp.variable_id") && col("bp.answer_code") === col("gp.answer_code_min"), "inner").selectExpr("bp.src_question_id", "answer_code_max", "answer_code_min", "bp.variable_id", "answer_label as answer_code_min_text").as("min_src")
      //stage_QuestionDimDFMin.filter($"src_question_id"==="Q2").show()
      val stage_lkpMinMaxlabelDF = stage_QuestionDimDFMax.join(stage_QuestionDimDFMin, col("min_src.src_question_id") === col("max_src.src_question_id") && col("min_src.variable_id") === col("max_src.variable_id"), "inner").selectExpr("max_src.src_question_id", "max_src.answer_code_max", "max_src.answer_code_min", "max_src.variable_id", "min_src.answer_code_min_text", "max_src.answer_code_max_text").as("lkp")
      //stage_lkpMinMaxlabelDF.filter($"src_question_id"==="Q2").show()
      val stage_QuestionDimDF = src_question_df.join(stage_lkpMinMaxlabelDF, col("bp.src_question_id") === col("lkp.src_question_id") && col("bp.variable_id") === col("lkp.variable_id"), "left").selectExpr("bp.src_question_id", "answer_code_max", "answer_code_min", "bp.variable_id", "Type", "Start", "Finish", "question_text","category","answer_code_min_text", "answer_code_max_text").as("final")
         
         
         stage_QuestionDimDF.createOrReplaceTempView("finaldf")
         
          var stage_QuestionDimFinalDF = spark.sql("select src_question_id,answer_code_max,answer_code_min,variable_id,Type,Start,Finish,question_text,category,case when regexp_replace(src_question_id,'[^0-9]+', '') < 500 then 'Y' else 'N' end as is_postcruise,case when (regexp_replace(src_question_id,'[^0-9]+', '') >= 500 and regexp_replace(src_question_id,'[^0-9]+', '') <= 1000 ) then 'Y' else 'N' end as is_reminiscence,answer_code_min_text,answer_code_max_text from finaldf")
         
         stage_QuestionDimFinalDF = stage_QuestionDimFinalDF.dropDuplicates()
         
         
      //stage_QuestionDimDF.filter($"src_question_id"==="Q2").show()
      val required_columns = spark.sparkContext.getConf.get("spark.source.columns").split(",")

      val missing_columns = ArrayBuffer[String]()
      val avaliable_columns = ArrayBuffer[String]()
      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

      for (col <- required_columns) {

        if (hasColumn(stage_QuestionDimFinalDF, col)) {
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
      val stage_final_df = missing_columns.foldLeft(stage_QuestionDimFinalDF)((df, c) =>
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
