package com.virginvoyages.dimension
import com.virginvoyages.metadataframework.ManageMetadata
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import org.apache.spark.sql.types.{ StructType, StructField, TimestampType, StringType, IntegerType, DateType, LongType };
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact
object DiningTableDim {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    
    import spark.implicits._    
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    
/*************************************calling metadata framework*********************************************/
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
      val whereClause = s""" A.BatchTime>= '$batch_start_tme' and A.BatchTime<='$batch_end_tme' and A.Part_Date>='$part_read_start' and A.Part_Date<='$part_read_end'"""
                  
      log.info(s"""#---------------------------where clause Query execution $whereClause ----------------#""")

      import spark.sqlContext.implicits._
      var DiningTableDim = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").trim()+ whereClause)
      
      //+ whereClause
      log.info(s"""#---------------------------Query execution $DiningTableDim ----------------#""")

      
      val voyage_id = spark.sparkContext.getConf.get("spark.voyage.id")
           
       
      val required_columns = spark.sparkContext.getConf.get("spark.source.columns").split(",")
      
      val missing_columns = ArrayBuffer[String]()
      val avaliable_columns = ArrayBuffer[String]()
      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
      for (col <- required_columns) {

        if (hasColumn(DiningTableDim, col)) {

          println(col, "column exists", avaliable_columns.toString)

          println("column exists", avaliable_columns.length)

        } else {

          println(col, "column missing", missing_columns.length)

          println(missing_columns.length, "length")

        }

      }

      print(missing_columns, "Here are the missing columns")

      val stage_final_df = missing_columns.foldLeft(DiningTableDim)((df, c) =>

        df.withColumn(s"$c", lit(null)))

       if(!DiningTableDim.take(1).isEmpty){
      loadDimFact(spark: SparkSession, stage_final_df)
      } 
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)

    } catch {

      case e: SQLException => { ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("******************in the catch of  Dimension Load ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace", e); }

      case e: Exception =>
        { ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("******************in the catch of Dimension Load ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace", e); }
        println("#----------------------------Process Has Failed---------------------------#")
        System.exit(1)
      //exit(1);
    }

    //spark.sparkContext.stop()

    //stage_final_df.show

  }

}
