package com.virginvoyages.parser

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
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import java.sql.DriverManager
import java.sql.Connection
import java.sql.SQLException

import com.virginvoyages.metadataframework.ManageMetadata

object EntityHashParser {

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

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

      //val whereClause = s""" and entity_hash.batchtime>= '$batch_start_tme' and entity_hash.batchtime<='$batch_end_tme' and entity_hash.part_date>='$part_read_start' and entity_hash.part_date<='$part_read_end'"""
      val whereClause = s""" batchtime>= '$batch_start_tme' and batchtime<='$batch_end_tme' and part_date>='$part_read_start' and part_date<='$part_read_end'"""

      println(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
      log.info(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")

      import spark.sqlContext.implicits._

      val df = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").trim()) //+ whereClause)
      //										val df = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").trim()  )
      df.createOrReplaceTempView("table2")
      //df.show(false)

      if (!df.head(1).isEmpty) {

        /*       val sourcetabledf = spark.sql("""select entitycode, entityhashid, uniquesourceid, failedhashcode, additionalinfo,seaware_to_vxp_success_flg, seaware_to_vxp_status, vxp_to_rrd_success_flg, vxp_to_rrd_status, vxp_wearable_color, vxp_rrd_file_received_flg, rrd_to_vxp_ingestion_success_flg, rrd_to_vxp_ingestion_status,addeddate, change_dt from (
select entitycode, entityhashid, uniquesourceid, failedhashcode, additionalinfo, addeddate,lastmodifieddate as change_dt
,coalesce((max(case when failedhashcode='RECEIVED_FROM_SEAWARE' then true else false end) over (partition by '1')),false) as seaware_to_vxp_success_flg
,max(case when failedhashcode='RECEIVED_FROM_SEAWARE' then failedhashcode else null end) over (partition by '1') as seaware_to_vxp_status
,coalesce((max(case when failedhashcode='SUCCESS_SENDING_TO_RRD' then true else false end) over (partition by '1')),false) as vxp_to_rrd_success_flg
,max(case when failedhashcode in ('SUCCESS_SENDING_TO_RRD','FAILED_SENDING_TO_RRD_AT_FTP','FAILED_SENDING_TO_RRD_AT_VXP','FAILED_SENDING_TO_RRD_AT_SAILOR_API')  then failedhashcode else null end) over (partition by '1') as vxp_to_rrd_status
,max(case when failedhashcode='SUCCESS_SENDING_TO_RRD' then split(additionalinfo,',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)')[53] else null end) over (partition by '1') as vxp_wearable_color
,coalesce((max(case when failedhashcode='RECEIVED_FROM_RRD' then true else false end) over (partition by '1')),false) as vxp_rrd_file_received_flg
,coalesce((max(case when failedhashcode='SUCCESS_TO_ASSIGN' then true else false end) over (partition by '1')),false) as rrd_to_vxp_ingestion_success_flg
,max(case when failedhashcode in ('SUCCESS_TO_ASSIGN','PARTIAL_SUCCESS_TO_ASSIGN','FAILED_TO_ASSIGN') then failedhashcode else null end) over (partition by '1')  as rrd_to_vxp_ingestion_status
,row_number() over(partition by uniquesourceid order by lastmodifieddate desc,addeddate desc) rn
from table2 where failedhashcode in ('RECEIVED_FROM_SEAWARE','SUCCESS_SENDING_TO_RRD','FAILED_SENDING_TO_RRD_AT_FTP','FAILED_SENDING_TO_RRD_AT_VXP','FAILED_SENDING_TO_RRD_AT_SAILOR_API','RECEIVED_FROM_RRD','SUCCESS_TO_ASSIGN','PARTIAL_SUCCESS_TO_ASSIGN','FAILED_TO_ASSIGN')
) outerQry where rn = 1 and uniquesourceid=131350""")
*/
        val sourcetabledf = spark.sql("""select entitycode, entityhashid, uniquesourceid, failedhashcode, additionalinfo,seaware_to_vxp_success_flg, seaware_to_vxp_status, vxp_to_rrd_success_flg, vxp_to_rrd_status, vxp_wearable_color, vxp_rrd_file_received_flg, rrd_to_vxp_ingestion_success_flg, rrd_to_vxp_ingestion_status,addeddate, change_dt from (
select entitycode, entityhashid, uniquesourceid, failedhashcode, additionalinfo, addeddate,lastmodifieddate as change_dt            
,coalesce((max(case when failedhashcode='RECEIVED_FROM_SEAWARE' then true else false end) over (partition by uniquesourceid)),false) as seaware_to_vxp_success_flg
,max(case when failedhashcode='RECEIVED_FROM_SEAWARE' then failedhashcode else null end) over (partition by uniquesourceid) as seaware_to_vxp_status
,coalesce((max(case when failedhashcode='SUCCESS_SENDING_TO_RRD' then true else false end) over (partition by uniquesourceid)),false) as vxp_to_rrd_success_flg
,max(case when failedhashcode in ('SUCCESS_SENDING_TO_RRD','FAILED_SENDING_TO_RRD_AT_FTP','FAILED_SENDING_TO_RRD_AT_VXP','FAILED_SENDING_TO_RRD_AT_SAILOR_API')  then failedhashcode else null end) over (partition by uniquesourceid) as vxp_to_rrd_status
,max(case when failedhashcode='SUCCESS_SENDING_TO_RRD' then split(additionalinfo,',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)')[53] else null end) over (partition by uniquesourceid) as vxp_wearable_color
,coalesce((max(case when failedhashcode='RECEIVED_FROM_RRD' then true else false end) over (partition by uniquesourceid)),false) as vxp_rrd_file_received_flg
,coalesce((max(case when failedhashcode='SUCCESS_TO_ASSIGN' then true else false end) over (partition by uniquesourceid)),false) as rrd_to_vxp_ingestion_success_flg
,max(case when failedhashcode in ('SUCCESS_TO_ASSIGN','PARTIAL_SUCCESS_TO_ASSIGN','FAILED_TO_ASSIGN') then failedhashcode else null end) over (partition by uniquesourceid)  as rrd_to_vxp_ingestion_status
,row_number() over(partition by uniquesourceid order by lastmodifieddate desc,addeddate desc) rn 
from table2 where failedhashcode in ('RECEIVED_FROM_SEAWARE','SUCCESS_SENDING_TO_RRD','FAILED_SENDING_TO_RRD_AT_FTP','FAILED_SENDING_TO_RRD_AT_VXP','FAILED_SENDING_TO_RRD_AT_SAILOR_API','RECEIVED_FROM_RRD','SUCCESS_TO_ASSIGN','PARTIAL_SUCCESS_TO_ASSIGN','FAILED_TO_ASSIGN') 
) outerQry where rn = 1 """)
        sourcetabledf.createOrReplaceTempView("sourcetable")
        println("source data")
     //   sourcetabledf.show(false)

        val targettbldf = spark.sql("select entitycode as tgt_entitycode,entityhashid as tgt_entityhashid,uniquesourceid as tgt_uniquesourceid,failedhashcode as tgt_failedhashcode,additionalinfo as tgt_additionalinfo,seaware_to_vxp_success_flg as tgt_seaware_to_vxp_success_flg,seaware_to_vxp_status as tgt_seaware_to_vxp_status,vxp_to_rrd_success_flg as tgt_vxp_to_rrd_success_flg,vxp_to_rrd_status as tgt_vxp_to_rrd_status,vxp_wearable_color as tgt_vxp_wearable_color,vxp_rrd_file_received_flg as tgt_vxp_rrd_file_received_flg,rrd_to_vxp_ingestion_success_flg as tgt_rrd_to_vxp_ingestion_success_flg,rrd_to_vxp_ingestion_status as tgt_rrd_to_vxp_ingestion_status,addeddate as tgt_addeddate, change_dt as tgt_change_dt from  shipdw.hvtb_parse_entity_hash_wearable ")
        targettbldf.createOrReplaceTempView("targettbl")
        println("targettbldf data")
       // targettbldf.show(false)
        val entityDf = spark.sql(""" select
entitycode, entityhashid, uniquesourceid, failedhashcode, additionalinfo, addeddate,change_dt,
greatest(seaware_to_vxp_success_flg,tgt_seaware_to_vxp_success_flg) as seaware_to_vxp_success_flg,
greatest(seaware_to_vxp_status,tgt_seaware_to_vxp_status) as seaware_to_vxp_status, 
greatest(vxp_to_rrd_success_flg,tgt_vxp_to_rrd_success_flg) as vxp_to_rrd_success_flg,
greatest(vxp_to_rrd_status,tgt_vxp_to_rrd_status) as vxp_to_rrd_status,
greatest(vxp_wearable_color,tgt_vxp_wearable_color) as vxp_wearable_color,
greatest(vxp_rrd_file_received_flg,tgt_vxp_rrd_file_received_flg) as vxp_rrd_file_received_flg,
greatest(rrd_to_vxp_ingestion_success_flg,tgt_rrd_to_vxp_ingestion_success_flg) as rrd_to_vxp_ingestion_success_flg,
greatest(rrd_to_vxp_ingestion_status,tgt_rrd_to_vxp_ingestion_status) as rrd_to_vxp_ingestion_status from
sourcetable src left join targettbl tgt on  src.uniquesourceid=tgt.tgt_uniquesourceid
""")

      //  entityDf.show(false)

        val pond_table = spark.sparkContext.getConf.get("spark.pond.table").trim()
        val pondSelectList = spark.sparkContext.getConf.get("spark.pond.allColumns").trim() //spark.pond.Columns
        val pondSelectSQL = "select " + pondSelectList + " from " + pond_table
        val pondDataDF = spark.sql(pondSelectSQL)

        val pondPKCols = spark.sparkContext.getConf.get("spark.source.primaryKeyColumnsPond")
        val pondColsPKList = pondPKCols.split(",")
        val pondPKColSEQ = pondColsPKList.map(x => col(x)).toSeq

        val pondAllColList = spark.sparkContext.getConf.get("spark.source.allColumnsPond")
        val pondAllColArray = pondAllColList.split(",")
        val pondAllColSEQ = pondAllColArray.map(x => col(x)).toSeq

        val PondHashedSrcDF = pondDataDF.withColumn("pond_md5_hash", md5(concat_ws(",", pondDataDF.select(pondAllColSEQ: _*).columns.map(c => col(c)): _*))).as("srh")

        val PondHashedSrcPrimaryDF = PondHashedSrcDF.withColumn("pond_primaryhash", md5(concat_ws(",", pondDataDF.select(pondPKColSEQ: _*).columns.map(c => col(c)): _*)))

        //										=================

        val srcPKCols = spark.sparkContext.getConf.get("spark.source.primaryKeyColumns")
        val srcColsPKList = srcPKCols.split(",")
        val srcPKColSEQ = srcColsPKList.map(x => col(x)).toSeq

        val srcAllColList = spark.sparkContext.getConf.get("spark.source.allColumns")
        val srcAllColArray = srcAllColList.split(",")
        val srcAllColSEQ = srcAllColArray.map(x => col(x)).toSeq

        val HashedSrcDF = entityDf.withColumn("md5_hash", md5(concat_ws(",", entityDf.select(srcAllColSEQ: _*).columns.map(c => col(c)): _*))).as("srh")
        val HashedSrcPrimaryDF = HashedSrcDF.withColumn("primaryhash", md5(concat_ws(",", entityDf.select(srcPKColSEQ: _*).columns.map(c => col(c)): _*)))

        val HashedJoinedDF = HashedSrcPrimaryDF.join(PondHashedSrcPrimaryDF, col("primaryhash") === col("pond_primaryhash"), "full")

        val insertUpdateFlaggedDF = HashedJoinedDF.withColumn("insert_update_flag", when($"md5_hash" =!= $"pond_md5_hash", "U")

          .when($"pond_md5_hash".isNull, "I").otherwise("UC"))

        val insert_flagged_df = insertUpdateFlaggedDF.filter(col("insert_update_flag") === "I")
        val update_flagged_df = insertUpdateFlaggedDF.filter(col("insert_update_flag") === "U")
        val dfs = Seq(insert_flagged_df, update_flagged_df)
        val insert_update_df = dfs.reduce(_ union _)

        val finalDF = insert_update_df.select("entitycode", "entityhashid", "uniquesourceid", "failedhashcode", "additionalinfo", "seaware_to_vxp_success_flg", "seaware_to_vxp_status", "vxp_to_rrd_success_flg", "vxp_to_rrd_status", "vxp_wearable_color", "vxp_rrd_file_received_flg", "rrd_to_vxp_ingestion_success_flg", "rrd_to_vxp_ingestion_status", "addeddate", "change_dt")

        if (!finalDF.head(1).isEmpty) {
          //println("count:"+ finalDF.count)
         // finalDF.show(false)
           finalDF.repartition(10).write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.table"))
        }

      }
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)

    } catch {

      case e: SQLException => { ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("******************in the catch of Event Dimension Load ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace", e); }

      case e: Exception =>
        { ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("******************in the catch of Event Dimension Load ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace", e); }
        println("#----------------------------Process Has Failed---------------------------#")
        System.exit(1)

    }

    //stage_final_df.show

  }

}