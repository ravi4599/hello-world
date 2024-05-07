package com.virginvoyages.scd
import org.apache.spark.storage.StorageLevel
import java.util.Date

import java.net.UnknownHostException

import org.apache.spark.broadcast.Broadcast

import java.text.SimpleDateFormat

import scala.util.parsing.json._

import java.util.Properties
import java.sql.Timestamp
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ StructType, StructField, TimestampType, StringType, IntegerType, DateType, LongType };

import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import scala.collection.JavaConversions._

import org.apache.spark.sql.functions._
import scala.util.Try
import org.apache.spark.sql.DataFrame
import java.time.{ ZonedDateTime, ZoneId }
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{ Column, DataFrame }
import scala.collection.mutable.ArrayBuffer
import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.sql.SaveMode
import scala.util.Random
object GetXrefData {

  val log = LogManager.getRootLogger

  log.setLevel(Level.INFO)
  // val src_data=spark.sql("")
  def srcReferenceTypeTotgtReferenceType(spark: SparkSession, src_data: DataFrame): DataFrame = {
    import spark.sqlContext.implicits._
    val frameworkEnv = spark.sparkContext.getConf.get("spark.frameworkEnv").trim()
    val today = ZonedDateTime.now(ZoneId.of("UTC"))
    var finalDf = spark.emptyDataFrame
    val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
    //val dateformatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    val sessiondate = formatter format today
    val rand_name = s"rnd_company_${Random.alphanumeric take 10 mkString}"
    val src_view_name = rand_name + "_" + sessiondate

    var debug_flag = "False";
    try {
      debug_flag = spark.sparkContext.getConf.get("spark.debug.flag").trim()
    } catch {
      case e: NoSuchElementException => { debug_flag = "False"; log.info("#--------------------No Debug Flag No debug  -------------------#") }

    }
    if (debug_flag.trim().toUpperCase().equals("TRUE")) {
      println("#---------------------------------------src_data in XREF-----------------#" + spark.sparkContext.getConf.get("spark.person.sql"))
      src_data.show(false)
    }
    println("#-------------------------------------------view name ----------------------------#", src_view_name)
    src_data.printSchema()
    //srcidtypeId is the input's type that is given.
    //tgttypeId is the output's type that is to be given.
    //srcId is incoming source id for example seaware_id or vxp_guest
    if (frameworkEnv.trim().toUpperCase().equals("SHORE")) {
      println("#---------------------------Xref Environment----------------#", frameworkEnv.trim().toUpperCase())
      val srcDF = src_data.select("sourceID", "srcReferenceType", "tgtReferenceType")
      srcDF.createOrReplaceTempView(src_view_name)

      val referenceType = spark.sql(s"""select reference_type,reference_type_id from 
    (select reference_type,reference_type_id, row_number()over(partition by reference_type_id  order by batchtime desc  ) as rn
    from vv_db.hvtb_nbx_parse_xref_reference_type)r_type 
    where r_type.rn=1""")
      referenceType.createOrReplaceTempView("referenceTypeTbl")

      spark.conf.set("spark.sql.crossJoin.enabled", "true")

    /*  val xrefDf = spark.sql(s""" select reference_type_id ,reference_type ,native_source_id_value ,master_id , sourceID , tgtReferenceType
	from(
	select type.reference_type_id ,type.reference_type ,
	audit.native_source_id_value ,audit.master_id,
	src.sourceID , src.tgtReferenceType ,
	row_number() over (partition by audit.native_source_id_value order by audit.event_timestamp desc)  as LatestRec 
	from $src_view_name src
	inner join vv_db.hvtb_nbx_parse_xref_reference_audit audit on lower(audit.native_source_id_value) = lower(src.sourceID)
	inner  join referenceTypeTbl type on lower(audit.reference_type_id) = lower(type.reference_type_id) 
	and  lower( regexp_replace(type.reference_type,' ','')) = lower(regexp_replace(src.srcReferenceType,' ',''))
	where lower(audit.event_type) != 'deleted') srcQ
	where LatestRec = 1""")*/
    
      val xrefDf = spark.sql(s""" select reference_type_id ,reference_type ,native_source_id_value ,master_id , sourceID , tgtReferenceType
	from(
	select type.reference_type_id ,type.reference_type ,
	audit.native_source_id_value ,audit.master_id,
	src.sourceID , src.tgtReferenceType ,
	row_number() over (partition by audit.native_source_id_value order by audit.event_timestamp desc)  as LatestRec 
	from $src_view_name src
	inner join (select * from vv_db.hvtb_nbx_parse_xref_reference_audit where lower(event_type) != 'deleted') audit on lower(audit.native_source_id_value) = lower(src.sourceID)
	inner  join referenceTypeTbl type on lower(audit.reference_type_id) = lower(type.reference_type_id) 
	and  lower( regexp_replace(type.reference_type,' ','')) = lower(regexp_replace(src.srcReferenceType,' ',''))) srcQ
	where LatestRec = 1""")
    
      xrefDf.createOrReplaceTempView("xref_vw")

      if (debug_flag.trim().toUpperCase().equals("TRUE")) {
        println("#---------------------------------------XREF-----------------#" + spark.sparkContext.getConf.get("spark.person.sql"))
        xrefDf.show(false)
      }
      xrefDf.createOrReplaceTempView("xref_vw")
     /* val masterDf = spark.sql(s""" select sourceID_master , targetID   from
   (select xref_vw.sourceID as sourceID_master,xref_vw.reference_type as srcReferenceType,audit.native_source_id_value as targetID,xref_vw.tgtReferenceType ,
   row_number() over (partition by audit.master_id,type.reference_type order by audit.event_timestamp desc)  as LatestRec
   from xref_vw
   inner join vv_db.hvtb_nbx_parse_xref_reference_audit audit on xref_vw.master_id = audit.master_id
   inner join referenceTypeTbl type on lower(audit.reference_type_id) = lower(type.reference_type_id) and lower(regexp_replace(type.reference_type,' ','')) = lower(regexp_replace(xref_vw.tgtReferenceType,' ',''))
   where lower(audit.event_type) != 'deleted') tgtQ
   where LatestRec = 1 """).as("master")*/
   val masterDf = spark.sql(s""" select sourceID_master , targetID,LatestRec   from
(select xref_vw.sourceID as sourceID_master,xref_vw.reference_type as srcReferenceType,audit.native_source_id_value as targetID,xref_vw.tgtReferenceType ,LatestRec from xref_vw
inner join
(select *, row_number() over (partition by master_id,reference_type_id order by event_timestamp desc) as LatestRec from vv_db.hvtb_nbx_parse_xref_reference_audit  where lower(event_type) != 'deleted')audit on xref_vw.master_id = audit.master_id
inner join referenceTypeTbl type on lower(audit.reference_type_id) = lower(type.reference_type_id) and lower(regexp_replace(type.reference_type,' ','')) = lower(regexp_replace(xref_vw.tgtReferenceType,' ',''))) tgtQ where LatestRec = 1
order by sourceID_master""").as("master")
   
      if (debug_flag.trim().toUpperCase().equals("TRUE")) {
        println("#---------------------------------------XREF Master DF-----------------#" + spark.sparkContext.getConf.get("spark.person.sql"))
        masterDf.show(false)
      }
      //masterDf.persist(StorageLevel.MEMORY_ONLY)masterDf.persist(StorageLevel.MEMORY_AND_DISK)
      masterDf.persist(StorageLevel.MEMORY_AND_DISK)
      finalDf = src_data.join(masterDf, col("sourceID") === col("master.sourceID_master"), "left").drop(col("sourceID_master"))
      if (debug_flag.trim().toUpperCase().equals("TRUE")) {
        println("#---------------------------------------XREF Final DF-----------------#" + spark.sparkContext.getConf.get("spark.person.sql"))
        finalDf.show(false)
      }
    } else if (frameworkEnv.trim().toUpperCase().equals("SHIP")) {
      println("#---------------------------Xref Environment----------------#", frameworkEnv.trim().toUpperCase())
      //src_data.createOrReplaceTempView(src_view_name)
      val srcDF = src_data.select("sourceID", "srcIdType", "srcSrcType", "tgtIdType", "tgtSrcType")
      srcDF.createOrReplaceTempView(src_view_name)
      if (debug_flag.trim().toUpperCase().equals("TRUE")) {
        println("#---------------------------------------XREF SOURCE DF-----------------#" + spark.sparkContext.getConf.get("spark.person.sql"))
        srcDF.show(false)
      }
      val TypeDF = spark.sql(s"""select idtypeid,idname from 
    (select idtypeid,idname, row_number()over(partition by idtypeid,idname  order by lastmodifieddate desc  ) as rn
    from shipdw.hvtb_parse_vxp_dxp_idtype)r_type 
    where r_type.rn=1""")
      TypeDF.createOrReplaceTempView("TypeDF")

      val SrcTypeDF = spark.sql(s"""select idsourceid,idname from 
    (select idsourceid,idname, row_number()over(partition by idsourceid,idname  order by lastmodifieddate desc  ) as rn
    from shipdw.hvtb_parse_vxp_dxp_idsource)r_type 
    where r_type.rn=1""")
      SrcTypeDF.createOrReplaceTempView("SrcTypeDF")

      val xrefDf = spark.sql(s""" select *
	from(
	select srcIdType,sourceID ,linkid,tgtIdType,tgtSrcType,
	row_number() over (partition by audit.idvalue order by audit.lastmodifieddate desc)  as LatestRec 
	from $src_view_name src
	inner join shipdw.hvtb_parse_vxp_dxp_idcrossreference audit on lower(audit.idvalue) = lower(src.sourceID)
	inner  join TypeDF type on lower(audit.idtypeid) = lower(type.idtypeid) 
	and  lower( regexp_replace(type.idname,' ','')) = lower(regexp_replace(src.srcIdType,' ',''))
	inner join SrcTypeDF src_type on lower(audit.idsourceid) = lower(src_type.idsourceid) and  lower( regexp_replace(src_type.idname,' ','')) = lower(regexp_replace(src.srcSrcType,' ',''))
	where audit.isdeleted = false) srcQ
	where LatestRec = 1""")
      if (debug_flag.trim().toUpperCase().equals("TRUE")) {
        println("#---------------------------------------XREF XREF DF-----------------#" + spark.sparkContext.getConf.get("spark.person.sql"))
        xrefDf.show(false)
      }
      xrefDf.createOrReplaceTempView("xref_vw")
      val masterDf = spark.sql(s""" select sourceID as sourceID_master,targetID  from
  (select audit.idvalue as targetID,srcIdType,sourceID ,xref_vw.linkid as xref_linkid,tgtIdType,tgtSrcType,
  row_number() over (partition by audit.linkid,type.idtypeid order by audit.lastmodifieddate desc) as LatestRec
  from xref_vw
  inner join shipdw.hvtb_parse_vxp_dxp_idcrossreference audit on lower(xref_vw.linkid) = lower(audit.linkid)
  inner join TypeDF type on lower(audit.idtypeid) = lower(type.idtypeid) 
and lower( regexp_replace(type.idname,' ','')) = lower(regexp_replace(xref_vw.tgtIdType,' ',''))
inner join SrcTypeDF src_type on lower(audit.idsourceid) = lower(src_type.idsourceid) and  lower( regexp_replace(src_type.idname,' ','')) = lower(regexp_replace(xref_vw.tgtSrcType,' ',''))
  where audit.isdeleted = false) tgtQ
  where LatestRec = 1 """).as("master")

      finalDf = src_data.join(masterDf, col("sourceID") === col("master.sourceID_master"), "left").drop(col("sourceID_master"))

      if (debug_flag.trim().toUpperCase().equals("TRUE")) {
        println("#---------------------------------------XREF Master DF-----------------#" + spark.sparkContext.getConf.get("spark.person.sql"))
        masterDf.show(false)
      }
      masterDf.persist(StorageLevel.MEMORY_AND_DISK)

      if (debug_flag.trim().toUpperCase().equals("TRUE")) {
        println("#---------------------------------------XREF Final DF-----------------#" + spark.sparkContext.getConf.get("spark.person.sql"))
        finalDf.show(false)
      }

    } else {
      println("---------------------------------xxx----------Invalid env-------------xxx---------------")
      sys.exit(1)
    }
    return finalDf
  }
}
