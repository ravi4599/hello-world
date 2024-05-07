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

import com.virginvoyages.scd.ChangeDataCapture.loadDimFact

object RestrictionDimLoad {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    //calling metadata framework to read the incremental data
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

      val whereClause = s""" batchtime>= '$batch_start_tme' and batchtime<='$batch_end_tme' and part_date>='$part_read_start' and part_date<='$part_read_end' """
      log.info(s"""whereclause:: $whereClause """)

      import spark.sqlContext.implicits._
      log.info(s"""Starting the Execution""")
      val lookupitemDf = spark.sql("select lookup_sub_category_id,active,lookup_item_id,last_changed,display_code,lookup_item_name,row_number() over(partition by lookup_item_id order by last_changed desc) as rownum from shipdw.hvtb_parse_mxp_lookup_items where active=true").select("lookup_sub_category_id", "rownum", "lookup_item_id", "display_code", "lookup_item_name").where(col("rownum") === 1).drop("rownum")

      val lookupcatDf = spark.sql("select lookup_sub_category_name,lookup_sub_category_id,row_number() over(partition by lookup_sub_category_id order by last_changed desc) as rownum from shipdw.hvtb_parse_mxp_lookup_sub_categories where lower(lookup_sub_category_name)='off site restrictions'").select("*").where(col("rownum") === 1).drop("rownum")

      val lkpupitemscatgeoryDf = lookupitemDf.join(lookupcatDf, (lookupitemDf("lookup_sub_category_id") === lookupcatDf("LOOKUP_SUB_CATEGORY_ID")), "inner").select(lookupitemDf("*"))

      import org.apache.spark.sql.types.{ StructType, StructField, TimestampType, StringType, IntegerType, DateType, LongType };
      var persondisembarkexpDf = spark.sql("select *,row_number() over(partition by person_disembark_exception_id order by last_changed desc) as rownum from shipdw.hvtb_parse_mxp_person_disembark_exceptions").select("*").where(col("rownum") === 1).drop("rownum").withColumnRenamed("voyageid", "voyage_id").withColumnRenamed("shipcode", "ship_code").withColumn("restriction_id", col("person_disembark_exception_id").cast(IntegerType)).withColumnRenamed("guid", "restriction_guid").withColumnRenamed("disembark_exception_type", "restriction_type").withColumnRenamed("disembark_exception_category_id", "restriction_categoryId").withColumnRenamed("disembark_exception_comment", "restriction_comment").withColumnRenamed("disembark_exception_from", "restriction_from_date").withColumnRenamed("disembark_exception_to", "restriction_to_date").withColumnRenamed("rec_deleted", "src_deleted_flag").withColumnRenamed("created", "src_created_date")
      persondisembarkexpDf=persondisembarkexpDf.where(whereClause)

      val itempersondisembarkexpDf = persondisembarkexpDf.join(lkpupitemscatgeoryDf, persondisembarkexpDf("restriction_categoryId") === lkpupitemscatgeoryDf("lookup_item_id"), "left").select(persondisembarkexpDf("*"), lkpupitemscatgeoryDf("display_code"), lkpupitemscatgeoryDf("lookup_item_name")).withColumnRenamed("display_code", "restriction_category_code").withColumnRenamed("lookup_item_name", "restriction_category").select("voyage_id", "restriction_id", "person_id", "restriction_guid", "restriction_type", "restriction_categoryId", "restriction_category_code", "restriction_category", "restriction_comment", "restriction_from_date", "restriction_to_date", "src_deleted_flag", "src_created_date", "batchtime","part_date")

      //Perform Column Validation
      val required_columns = spark.sparkContext.getConf.get("spark.source.columns").split(",")
      val missing_columns = ArrayBuffer[String]()

      val avaliable_columns = ArrayBuffer[String]()

      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

      for (col <- required_columns) {

        if (hasColumn(itempersondisembarkexpDf, col)) {

          println(col, "column exists", avaliable_columns.toString)

          println("column exists", avaliable_columns.length)

        } else {

          println(col, "column missing", missing_columns.length)

          println(missing_columns.length, "length")

        }

      }

      print(missing_columns, "Here are the missing columns")

      val stage_final_df = missing_columns.foldLeft(itempersondisembarkexpDf)((df, c) =>

        df.withColumn(s"$c", lit(null)))

      if (!stage_final_df.take(1).isEmpty) {
        print("***********loadDimFact************")
        loadDimFact(spark: SparkSession, stage_final_df)
      }
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)

    
    }  catch {

      case e: SQLException => {
        //Update the Hbase metadata table with the Failed Status
        ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
        log.info("SQL Excpetion caught and thrown in Dimension Load");
        e.printStackTrace();
        throw new Exception("SQL Exception..please check the stacktrace", e);
      }

      case e: Exception =>
        {
          //Update the Hbase metadata table with the Failed Status
          ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
          log.info("Excpetion caught and thrown in Dimension Load");
          e.printStackTrace();
          throw new Exception("General Exception..please check the stacktrace", e);
        }
        System.exit(1)
    }

  }

}