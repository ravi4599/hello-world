package com.virginvoyages.scd
import java.util.Date
import java.sql._;
import java.sql.SQLException;
import org.apache.spark.sql.functions.hash
import java.util.Properties
import org.apache.spark.sql.SaveMode
import java.sql.Timestamp
import com.virginvoyages.scd.ChangeDataCaptureLAD.factLoadLAD
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType, DateType, LongType ,TimestampType};
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
import java.time.{ ZonedDateTime, ZoneId }
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{ Column, DataFrame,Row }
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryException
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.Job
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.QueryJobConfiguration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import java.util.Calendar

object ChangeDataCapture {
  
  def ConvertSchema(spark: SparkSession, dff: DataFrame, tgtTbl: String): DataFrame = {

    val SelectStr = "select * from  " + tgtTbl + " where 1 = 0"
    println(SelectStr)
    val tbltp = spark.sql(SelectStr)

    var changedValue: scala.collection.mutable.ArrayBuffer[(String, org.apache.spark.sql.types.DataType)] = scala.collection.mutable.ArrayBuffer.empty[(String, org.apache.spark.sql.types.DataType)]

    val srcFieldsAll = dff.schema.fields.sortBy { case (x: StructField) => x.name }
    val tgtFieldsAll = tbltp.schema.fields.sortBy { case (x: StructField) => x.name }

    /* get only the common attributes across source and target */
    for (i <- 0 to srcFieldsAll.length - 1) {
      val NameSrc = srcFieldsAll(i).name
      val TypeSrc = srcFieldsAll(i).dataType
      for (j <- 0 to tgtFieldsAll.length - 1) {
        val NameTgt = tgtFieldsAll(j).name
        val TypeTgt = tgtFieldsAll(j).dataType
        //println("NameSrc ==>" + NameSrc + " NameTgt ==> "+ NameTgt)
        if (NameSrc == NameTgt && TypeSrc != TypeTgt) {
          //(srcFieldsAll(i).name, tgtFieldsAll(j).dataType)
          //println("NameSrc ==>" + NameSrc +" "+TypeSrc  +" NameTgt ==> "+ NameTgt+ " " +TypeTgt)
          //val newValue : Array[(String, org.apache.spark.sql.types.DataType)] = Array((NameSrc,TypeTgt))
          //val (colName,colType) = (NameSrc,TypeTgt)
          val addValue: scala.Array[(String, org.apache.spark.sql.types.DataType)] = scala.Array((NameSrc, TypeTgt))
        // scala.Array
          changedValue.append(addValue(0))
        }
      }
    }
    val commonFields = changedValue.toArray
    var targetDF = spark.emptyDataFrame

    /* convert the common attributes to  target type*/
    if (commonFields.nonEmpty) {
      targetDF = commonFields.foldLeft(dff)((df, value) =>
        df.withColumn(value._1, df.col(value._1).cast(value._2)))
      targetDF.dtypes.foreach(f => println(f._1 + "," + f._2))
    } else { targetDF = dff }
    return targetDF
  }

	def generateFactDFwithInsertUpdateFlag(spark: SparkSession, src_data: DataFrame): (DataFrame) = {
			import spark.sqlContext.implicits._
			val log = LogManager.getRootLogger

			log.setLevel(Level.INFO)

			val srcPKCols = spark.sparkContext.getConf.get("spark.source.primaryKeyColumns")

			val srcColsPKList = srcPKCols.split(",")

			val srcPKColSEQ = srcColsPKList.map(x => col(x)).toSeq

			val srcAllColList = spark.sparkContext.getConf.get("spark.source.columns")

			val pondSelectList = spark.sparkContext.getConf.get("spark.pond.allColumns").trim() //spark.pond.Columns

			val srcAllColArray = srcAllColList.split(",")

			println("generateFactDFwithInsertUpdateFlag")
			val srcdfWithRowNumber = src_data
			println("#----------------------------------SRC DF with ROW NUMBER ------------------------#")
			//val colDataTypeSeq = src_data.head(1).map(x => x.toSeq(1))

			//var checkNullPrimarySkeyFlag=0
			println("##-----After filter assignment-------###")

	
			println("""#------------------Before Filter Excution-------------#""")

			val src_dedup_df = srcdfWithRowNumber //.filter($"rank" === 1)

			val pond_table = spark.sparkContext.getConf.get("spark.pond.database").trim() + "." + spark.sparkContext.getConf.get("spark.pond.table").trim()

			log.info("source table@@@@@@@@@@@@@@@" + pond_table)

			val pondSelectSQL = "select " + pondSelectList + " from " + pond_table

			val pondDataDF = spark.sql(pondSelectSQL)
			println(s"""#------------------------------Selected PonD Data for Comparision-----------------------------#""")
			val srcAllColSEQ = srcAllColArray.map(x => col(x)).toSeq

			var insertUpdateFlaggedDF: DataFrame = null;

			val srccolumns = src_dedup_df.columns

					val HashedSrcDF = src_dedup_df.withColumn("md5_hash", md5(concat_ws(",", src_data.select(srcAllColSEQ: _*).columns.map(c => col(c)): _*))).as("srh")

					val HashedSrcPrimaryDF = HashedSrcDF.withColumn("primaryhash", md5(concat_ws(",", src_dedup_df.select(srcPKColSEQ: _*).columns.map(c => col(c)): _*)))

					val HashedJoinedDF = HashedSrcPrimaryDF.join(pondDataDF, col("primaryhash") === col("pond_primaryhash"), "full")

					insertUpdateFlaggedDF = HashedJoinedDF.withColumn("insert_update_flag", when($"md5_hash" =!= $"pond_md5_hash", "U")
					    

							.when($"pond_md5_hash".isNull, "I").when($"pond_md5_hash" === $"md5_hash", "NC").when($"md5_hash".isNull, "NR").otherwise("N")) // md5_hash == pondhash NC// md5_hash is null NR //.when($"md5_hash".isNull and col(src_key_col)===col(tgt_key_col), "I").otherwise("UC"))
					//insertUpdateFlaggedDF.show()
					// }//END OF IF FOR SCD 1 WILL CHANGE FOR SCD2

					val w = Window.orderBy(insertUpdateFlaggedDF("insert_update_flag") desc)
					val insert_flagged_df = insertUpdateFlaggedDF.filter(col("insert_update_flag") === "I")
					.withColumn("row_number", row_number().over(w))
					.withColumn("upd_dt", current_timestamp())
					.withColumn("load_dt", current_timestamp())
					val update_flagged_df = insertUpdateFlaggedDF.filter(col("insert_update_flag") === "U")
					.withColumn("row_number", lit(0))

					.withColumn("upd_dt", current_timestamp())
					.withColumn("load_dt", when($"pond_load_dt".isNull, current_timestamp()).otherwise($"pond_load_dt"))
					val nc_nr_flagged_df = insertUpdateFlaggedDF.filter(col("insert_update_flag") === "NC" or col("insert_update_flag") === "NR")
					.withColumn("row_number", lit(0))

					.withColumn("upd_dt", when($"pond_upd_dt".isNull, current_timestamp()).otherwise($"pond_upd_dt"))
					.withColumn("load_dt", when($"pond_load_dt".isNull, current_timestamp()).otherwise($"pond_load_dt"))

					val dfs = Seq(insert_flagged_df, update_flagged_df, nc_nr_flagged_df)
					val insert_update_df = dfs.reduce(_ union _)
					return insert_update_df
	}

	def generateDFwithInsertUpdateFlag(spark: SparkSession, src_data: DataFrame): DataFrame = {

			import spark.sqlContext.implicits._
			val log = LogManager.getRootLogger
			log.setLevel(Level.INFO)

			val scdType = spark.sparkContext.getConf.get("spark.scdtype").trim()

			if (scdType.trim().toLowerCase().equals("type2")) 
			{
			  
      log.info("Slowly changing Dimension 2 slowlyChangingDimensionType2")
      val srcDateColumn = spark.sparkContext.getConf.get("spark.source.srcDateColumn").trim()

      var latestSeq = srcDateColumn;
      try {
        latestSeq = spark.sparkContext.getConf.get("spark.source.latestIdentifierColumns").trim()
      } catch {
        case e: NoSuchElementException => { latestSeq = srcDateColumn; log.info("#--------------------No columns to indentify latest record defaulting to Batchtime -------------------#") }
      }
      val latestIdcolSEQ = latestSeq.split(",").map(x => col(x)).toSeq

      /**
       * ########################  Target Table Processing ######################
       *    From the target table - Creating 2 Sets
       *   	1) pondDataDFHist having not high rec_end_dttm  2) pondDataDFCurr having high rec_end_dttm
       */

      val pond_table = spark.sparkContext.getConf.get("spark.pond.database").trim() + "." + spark.sparkContext.getConf.get("spark.pond.table").trim()

      val src_data1 = ConvertSchema(spark, src_data, pond_table)

      val final_cols_name = spark.catalog.listColumns(pond_table).select("name").collect().map(_.getAs[String]("name"))
      val pondColList = final_cols_name.mkString(",")
      val pondColListSel = final_cols_name.map(colName => col(colName)).toSeq
      val pondSelectList = final_cols_name.map(colName => colName + " as pond_" + colName).mkString(",")
      val pondSelectSQLHigh = "select " + pondSelectList + " from " + pond_table + " where to_date(rec_end_dttm) = to_date(\"9999-12-31\") "
      val pondDataDF = spark.sql(pondSelectSQLHigh)
      val pondSelectListNoHigh = final_cols_name.mkString(",")
      val final_cols = final_cols_name.mkString(",")
      val final_cols_list = final_cols.split(",")
      val final_col_seq = final_cols_list.map(x => col(x)).toSeq
      val pondDataDFCurrAllCols = pondDataDF
      val pondDataDFHistSql = "select " + pondSelectListNoHigh + " from " + pond_table + " where to_date(rec_end_dttm) != to_date(\"9999-12-31\") "
      val pondDataDFHist = spark.sql(pondDataDFHistSql)
      val surrogateKeyColumn = spark.sparkContext.getConf.get("spark.surrogate_key.column").trim()
      val max_var = spark.sql(s"SELECT nvl(max(  $surrogateKeyColumn ),0) as maxval FROM $pond_table").collect()(0).getLong(0)

      /**
       * ########################  Source Data  Preparation ######################
       *  Appending columns in source data ( primaryhash ,md5_hash, scd_hash, SrcType , isNext , CurrRec , TotCurr )
       *  Breaking the src data in 3 Sets - Set 1 , Set 2 , Set 3
       *
       */

      val srcPrimaryCol = spark.sparkContext.getConf.get("spark.source.primaryKeyColumns")
      val srcPKColSEQ = srcPrimaryCol.split(",").map(x => col(x)).toSeq
      val srcAllColList = spark.sparkContext.getConf.get("spark.source.columns")
      val srcAllCol = srcAllColList
      val srcAllColSeq = srcAllColList.split(",").map(x => col(x)).toSeq
      //val ExcludeSrcList = Array(srcDateColumn)
      var srcSCDCol = "";
      try {
        srcSCDCol = spark.sparkContext.getConf.get("spark.scd.columns").trim()
      } catch {
        case e: NoSuchElementException => { srcSCDCol = spark.sparkContext.getConf.get("spark.source.columns").trim(); log.info("#--------------------No columns to indentify latest record defaulting to Batchtime -------------------#") }
      }

      val srcSCDColSeq = srcSCDCol.split(",").map(x => col(x)).toSeq

      val srcHashedPrimary = src_data1.withColumn("primaryhash", md5(concat_ws(",", src_data1.select(srcPKColSEQ: _*).columns.map(x => col(x)): _*)))
      val srcHashedAll = srcHashedPrimary.withColumn("md5_hash", md5(concat_ws(",", src_data1.select(srcAllColSeq: _*).columns.map(x => col(x)): _*)))
      val srcSCDHashed = srcHashedAll.withColumn("scd_hash", md5(concat_ws(",", src_data1.select(srcSCDColSeq: _*).columns.map(x => col(x)): _*)))
      val w = Window.partitionBy(srcPKColSEQ: _*).orderBy(latestIdcolSEQ: _*)
      var randomNo = scala.util.Random

      /**
       * #############################    Removing Consecutive Duplicate records  ##################################
       * If source is having two consecutive same records then dedup is done ...Removing the latest duplicate record one
       */

      val RemoveSrcDupDF = srcSCDHashed.withColumn("lag_md5hash", lag("md5_hash", 1).over(w)).withColumn("lag_scdhash", lag("scd_hash", 1).over(w)).withColumn("dupflag", when($"md5_hash" === $"lag_md5hash", "Dup").otherwise("NoDup")).filter(col("dupflag") === "NoDup").drop("dupflag")

      /**
       * ###########################   Removing in-Between Updated Records ###################
       *  Using lag_md5hash and lag_scdhash  : Mark the src records ( SrcType ) into SCDU and U  ; The calculating CurrRec1 , TotCurr1
       *  Removing all the U records coming in between - only the last U record is kept
       *  SrcType for 1st record is SCDU only
       */

      val RemoveSrcUpdateDF = RemoveSrcDupDF.withColumn("SrcType", when($"scd_hash" === $"lag_scdhash" and $"md5_hash" =!= $"lag_md5hash", "U").otherwise("SCDU")).drop("lag_md5hash", "lag_scdhash")
      RemoveSrcUpdateDF.repartition(5).persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)

      val temp_table1Rnd = "temp_table1" + randomNo.nextInt(10000)
      RemoveSrcUpdateDF.createOrReplaceTempView(temp_table1Rnd)

      val temp_table1SQL = "select " + srcPrimaryCol + ", md5_hash, primaryhash,scd_hash,SrcType," + srcDateColumn + ", row_number() over (partition by primaryhash order by " + srcDateColumn + ") as CurrRec1 , count(primaryhash) over (partition by primaryhash) as TotCurr1 from " + temp_table1Rnd

      val joinDF1 = spark.sql(temp_table1SQL)

      val filterAllU = joinDF1.filter(not(col("SrcType") === "U" and col("CurrRec1") > lit(1) and col("TotCurr1") =!= col("CurrRec1")))
      val pondDataDFCurrReqdCols = pondDataDFCurrAllCols.select("pond_primaryhash", "pond_rec_start_dttm", "pond_rec_end_dttm", "pond_md5_hash", "pond_scd_hash", "pond_upd_dt", "pond_load_dt", "pond_etl_ld_status", "pond_" + surrogateKeyColumn + "")

      //	======================= For Crm opppo

      val joinedTgtDF = filterAllU.join(pondDataDFCurrReqdCols, col("primaryhash") === col("pond_primaryhash"), "full")

      val ActualDF = joinedTgtDF.filter(col("pond_rec_start_dttm") < col(srcDateColumn) or col("pond_rec_start_dttm").isNull or col(srcDateColumn).isNull or col("TotCurr1") === col("CurrRec1")).drop("CurrRec1", "TotCurr1", "SrcType")
      /**
       * ################  After Filtering the in-between U records ########################
       *  Calculating the CurrRec , TotCurr ,  LeadSrcType , isNext
       */

      val temp_table2Rnd = "temp_table2" + randomNo.nextInt(10000)
      ActualDF.createOrReplaceTempView(temp_table2Rnd)

      /*   val temp_table2SQL="select *,row_number() over (partition by primaryhash order by '$srcDateColumn') as CurrRec  from "+ temp_table2Rnd +" A inner join (select count(*) as TotCurr,primaryhash as temp2_primaryhash from "+ temp_table2Rnd +" group by primaryhash) B on A.primaryhash=B.temp2_primaryhash"
						val joinDF=spark.sql(temp_table2SQL).drop("temp2_primaryhash")
						.withColumn("LeadSrcType", lead("SrcType", 1).over(w))
						.withColumn("isNext", lead(srcDateColumn, 1).over(w))*/
      val temp_table2SQL = "select * , case when md5_hash != lag(md5_hash) over (partition by nvl(primaryhash,pond_primaryhash) order by " + srcDateColumn + ") and  scd_hash != lag(scd_hash) over (partition by  nvl(primaryhash,pond_primaryhash) order by " + srcDateColumn + ") then 'SCDU' else 'U' end as SrcType, row_number() over (partition by nvl(primaryhash,pond_primaryhash) order by " + srcDateColumn + ") as CurrRec, count(primaryhash) over (partition by primaryhash) as TotCurr from " + temp_table2Rnd
      val joinDF = spark.sql(temp_table2SQL).withColumn("LeadSrcType", lead("SrcType", 1).over(w)).withColumn("isNext", lead(srcDateColumn, 1).over(w))

      /**
       * ###############  Breaking the entire incoming source DataFrame in 3 Sets #############
       *   Set 1  - 1st Rec - SCDU ( srcType )
       *   Set 2  - SCDU2   ( Recalculating the Next date for Set 2 )
       *   Set 3  - U ( will be the last record )
       */

      val lastUpdateDF = joinDF.filter(col("SrcType") === "U" and col("CurrRec") > lit(1))

      /**
       * ################# Processing Set 2 #################
       *    Recalculating the Next date for Set 2
       */

      val SCDUset2DF = joinDF.filter(col("SrcType") === "SCDU" and col("CurrRec") > lit(1)).drop("isNext").withColumn("isNextSet2", lead(srcDateColumn, 1).over(w)).withColumnRenamed("isNextSet2", "isNext").withColumn("insert_update_flag", lit(null)).withColumn("tag_flag", lit(null))

      val scdu_set2 = SCDUset2DF.withColumn("rec_start_dttm", col(srcDateColumn)).withColumn("rec_end_dttm", when(col("isNext").isNotNull, col("isNext") - expr("INTERVAL 1 seconds")).otherwise(lit("9999-12-31 00:00:00").cast(TimestampType))).withColumn("upd_dt", current_timestamp()).withColumn("load_dt", current_timestamp()).withColumn("etl_ld_status", lit("Set2")).withColumn("generate_surr_key", lit("REQ"))

      /**
       * ######################  Joining only the 1st source record with target record #################
       *  Joining  only the first source record and target data and appending flags ( I , NC , NR , SCDU , U)
       * 	 I - new data, NC - no change , NR  - not received new data , SCDU - change in scd2 cols, Update - change in columns which is not scd2 )
       */
      val firstDF = joinDF.filter(col("CurrRec") === lit(1))
      val insertUpdateFirstFlaggedDF = firstDF.withColumn("insert_update_flag", when($"scd_hash" =!= $"pond_scd_hash" and $"pond_rec_start_dttm" < col(srcDateColumn), "SCDU").when($"pond_md5_hash".isNull, "I").when($"pond_md5_hash" === $"md5_hash", "NC").when($"md5_hash".isNull, "NR").when(($"scd_hash" === $"pond_scd_hash" and $"md5_hash" =!= $"pond_md5_hash") or ($"md5_hash" =!= $"pond_md5_hash"), "U").otherwise("N"))

      val tagFlaggedDF = insertUpdateFirstFlaggedDF.withColumn("tag_flag", when($"insert_update_flag" === "U" and col("TotCurr") === lit(1), "U1")
        .when($"insert_update_flag" === "U" and col("TotCurr") > lit(1) and col("LeadSrcType") === "U", "ULeadU")
        .when($"insert_update_flag" === "U" and col("TotCurr") > lit(1) and col("LeadSrcType") =!= "U", "ULeadNotU")
        .when($"insert_update_flag" === "I" and col("TotCurr") === lit(1), "I1")
        .when($"insert_update_flag" === "I" and col("TotCurr") > lit(1) and col("LeadSrcType") === "U", "ILeadU")
        .when($"insert_update_flag" === "I" and col("TotCurr") > lit(1) and col("LeadSrcType") =!= "U", "ILeadNotU")
        .when($"insert_update_flag" === "NR", "NR")
        .when($"insert_update_flag" === "NC" and col("TotCurr") === lit(1), "NC1")
        .when($"insert_update_flag" === "NC" and col("TotCurr") > lit(1) and col("LeadSrcType") === "U", "NCleadU")
        .when($"insert_update_flag" === "NC" and col("TotCurr") > lit(1) and col("LeadSrcType") =!= "U", "NCLeadNotU")
        .when($"insert_update_flag" === "SCDU" and col("TotCurr") === lit(1), "SCDU1")
        .when($"insert_update_flag" === "SCDU" and col("TotCurr") > lit(1) and col("LeadSrcType") === "U", "SCDULeadU")
        .when($"insert_update_flag" === "SCDU" and col("TotCurr") > lit(1) and col("LeadSrcType") =!= "U", "SCDULeadNotU")
        .otherwise("N"))

      tagFlaggedDF.printSchema

      val wc = Window.partitionBy(lit(0)).orderBy(lit(0))

      // By Default we are caching the tagFlaggedDF DataFrame
      var cacheFlag = "True";
      try {
        cacheFlag = spark.sparkContext.getConf.get("spark.source.cacheFlag").trim()
      } catch {
        case e: NoSuchElementException => { cacheFlag = "True"; log.info("#--------------------No columns to identify so caching the df by default-------------------#") }
      }

      if (cacheFlag.trim().toUpperCase().equals("TRUE")) {
        tagFlaggedDF.cache()
      }

      val update_flagged_df1 = tagFlaggedDF.filter(col("tag_flag") === "U1" or col("tag_flag") === "ULeadU" or col("tag_flag") === "ULeadNotU").withColumn("rec_start_dttm", $"pond_rec_start_dttm").withColumn("rec_end_dttm", when($"tag_flag" === "ULeadNotU", col("isNext") - expr("INTERVAL 1 seconds")).otherwise($"pond_rec_end_dttm")).withColumn("upd_dt", $"pond_upd_dt").withColumn("load_dt", $"pond_load_dt").withColumn("etl_ld_status", col("tag_flag")).withColumn("generate_surr_key", lit("NotREQ")).withColumn(surrogateKeyColumn, col("pond_" + surrogateKeyColumn))

      val nc_flagged_df_NC1 = tagFlaggedDF.filter(col("tag_flag") === "NC1" or col("tag_flag") === "NCleadU" or col("tag_flag") === "NCLeadNotU").withColumn("pond_" + surrogateKeyColumn, col("pond_" + surrogateKeyColumn)).withColumn("pond_rec_start_dttm", $"pond_rec_start_dttm").withColumn("pond_rec_end_dttm", when(col("tag_flag") === "NCLeadNotU", col("isNext") - expr("INTERVAL 1 seconds")).otherwise($"pond_rec_end_dttm")).withColumn("pond_upd_dt", $"pond_upd_dt").withColumn("pond_load_dt", $"pond_load_dt").withColumn("pond_etl_ld_status", col("tag_flag")).withColumn("generate_surr_key", lit("NotREQ"))

      val insert_flagged_df = tagFlaggedDF.filter(col("tag_flag") === "I1" or col("tag_flag") === "ILeadU" or col("tag_flag") === "ILeadNotU").withColumn("rec_start_dttm", col(srcDateColumn)).withColumn("rec_end_dttm", when(col("tag_flag") === "ILeadNotU", col("isNext") - expr("INTERVAL 1 seconds")).otherwise(lit("9999-12-31 00:00:00").cast(TimestampType))).withColumn("upd_dt", current_timestamp()).withColumn("load_dt", current_timestamp()).withColumn("etl_ld_status", col("tag_flag")).withColumn("generate_surr_key", lit("REQ"))

      val nr_flagged_df = tagFlaggedDF.filter(col("tag_flag") === "NR").withColumn("pond_" + surrogateKeyColumn, col("pond_" + surrogateKeyColumn)).withColumn("pond_rec_start_dttm", when($"pond_rec_start_dttm".isNull, col(srcDateColumn)).otherwise($"pond_rec_start_dttm")).withColumn("pond_rec_end_dttm", when($"pond_rec_end_dttm".isNull, lit("9999-12-31 00:00:00").cast(TimestampType)).otherwise($"pond_rec_end_dttm")).withColumn("pond_upd_dt", when($"pond_upd_dt".isNull, current_timestamp()).otherwise($"pond_upd_dt")).withColumn("pond_load_dt", when($"pond_load_dt".isNull, current_timestamp()).otherwise($"pond_load_dt")).withColumn("pond_etl_ld_status", col("insert_update_flag")).withColumn("generate_surr_key", lit("NotReq"))

      val update_flagged_df_SCDU_new = tagFlaggedDF.filter(col("tag_flag") === "SCDU1" or col("tag_flag") === "SCDULeadU" or col("tag_flag") === "SCDULeadNotU").withColumn("rec_start_dttm", col(srcDateColumn)).withColumn("rec_end_dttm", when(col("tag_flag") === "SCDULeadNotU", col("isNext") - expr("INTERVAL 1 seconds")).otherwise(lit("9999-12-31 00:00:00").cast(TimestampType))).withColumn("upd_dt", current_timestamp()).withColumn("load_dt", current_timestamp()).withColumn("etl_ld_status", col("tag_flag")).withColumn("generate_surr_key", lit("REQ"))

      val update_flagged_df_SCDU_target = tagFlaggedDF.filter(col("tag_flag") === "SCDU1" or col("tag_flag") === "SCDULeadU" or col("tag_flag") === "SCDULeadNotU").withColumn("pond_" + surrogateKeyColumn, col("pond_" + surrogateKeyColumn)).withColumn("pond_rec_start_dttm", when($"pond_rec_start_dttm".isNull, col(srcDateColumn)).otherwise($"pond_rec_start_dttm")).withColumn("pond_rec_end_dttm", when(col(srcDateColumn).isNotNull, col(srcDateColumn) - expr("INTERVAL 1 seconds")).otherwise(lit("9999-12-31 00:00:00").cast(TimestampType))).withColumn("pond_upd_dt", current_timestamp()).withColumn("pond_load_dt", current_timestamp()).withColumn("pond_etl_ld_status", col("pond_etl_ld_status")).withColumn("generate_surr_key", lit("NotREQ"))

      // data populating from target

      /*val	hist_flagged_df= pondDataDFHist.filter(col("insert_update_flag") === "HIST")
						.withColumn("row_number", lit(0))
						.withColumn("pond_" + surrogateKeyColumn, when(col("pond_" + surrogateKeyColumn).isNull, (col("row_number").cast(IntegerType) + lit(max_var).cast(IntegerType))).otherwise(col("pond_" + surrogateKeyColumn)))
						.withColumn("pond_rec_start_dttm", $"pond_rec_start_dttm")
						.withColumn("pond_rec_end_dttm", when($"pond_rec_end_dttm".isNull, lit("9999-12-31 00:00:00").cast(TimestampType)).otherwise($"pond_rec_end_dttm"))
						.withColumn("pond_upd_dt", when($"pond_upd_dt".isNull, current_timestamp()).otherwise($"pond_upd_dt"))
						.withColumn("pond_load_dt", when($"pond_load_dt".isNull, current_timestamp()).otherwise($"pond_load_dt"))
						.withColumn("pond_etl_ld_status",col("insert_update_flag")).drop("insert_update_flag","row_number")
*/

      /**
       *  Combining Data and then generating Surrogate key
       */

      val dfs_group1_set1 = Seq(insert_flagged_df, update_flagged_df_SCDU_new, scdu_set2)
      val dfs_group1_set1_union = dfs_group1_set1.reduce(_ union _)

      // Adding surrogate key in set1
      val final_Group1_Surrgate_set1 = dfs_group1_set1_union.withColumn( "row_number1", row_number().over(Window.orderBy(srcPKColSEQ: _*).orderBy("rec_end_dttm") ) ).withColumn(surrogateKeyColumn, col("row_number1").cast(IntegerType) + lit(max_var).cast(IntegerType)).drop("row_number1")

      val Set1Update = Seq(final_Group1_Surrgate_set1, update_flagged_df1)
      val Set1Update_Union = Set1Update.reduce(_ union _).select(surrogateKeyColumn, "rec_start_dttm", "rec_end_dttm", "upd_dt", "load_dt", "etl_ld_status", "primaryhash", "md5_hash", "scd_hash", "TotCurr")

      //Joining to Source to get all relevant data
      Set1Update_Union.createOrReplaceTempView("Set1Tbl")
      RemoveSrcDupDF.createOrReplaceTempView("RemoveSrcDupDFTbl")
      val srcAllColSel = srcAllCol.split(",").map(Colname => "src." + Colname).mkString(",")
      val Set1Cols = surrogateKeyColumn + ",rec_start_dttm,rec_end_dttm,upd_dt,load_dt,etl_ld_status,primaryhash,md5_hash,scd_hash"
      val Set1ColsSel = Set1Cols.split(",").map(Colname => "set1." + Colname).mkString(",")

      val FinalSet1Src = spark.sql("""select distinct """ + srcAllColSel + """,""" + Set1ColsSel + """ from RemoveSrcDupDFTbl src join Set1Tbl set1 on src.md5_hash = set1.md5_hash where src.src_date >= set1.rec_start_dttm or set1.TotCurr = 1""").select(pondColListSel: _*)
      FinalSet1Src.repartition(5).persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)

      /**
       *  Combining Set2
       */
      val dfs_group2 = Seq(update_flagged_df_SCDU_target, nc_flagged_df_NC1, nr_flagged_df)
      val final_group2 = dfs_group2.reduce(_ union _).select("pond_rec_start_dttm", "pond_rec_end_dttm", "pond_upd_dt", "pond_load_dt", "pond_etl_ld_status", "pond_primaryhash", "pond_md5_hash", "pond_scd_hash", "pond_" + surrogateKeyColumn + "")

      final_group2.createOrReplaceTempView("final_group2Tbl")
      val set2ColsSel = final_group2.columns.map(colName => "set2." + colName + " as " + colName.split("pond_")(1)).mkString(",")
      //Remove the set2 columns from pondSelectList which are present in set2
      val set2ColList = "pond_rec_start_dttm,pond_rec_end_dttm,pond_upd_dt,pond_load_dt,pond_etl_ld_status,pond_primaryhash,pond_md5_hash,pond_scd_hash"
      val pondSelectListSet2Sel = pondDataDFCurrAllCols.columns.diff(final_group2.columns).map(colName => "pond." + colName + " as " + colName.split("pond_")(1)).mkString(",")

      pondDataDFCurrAllCols.createOrReplaceTempView("pondTbl")
      val FinalSet2Src = spark.sql("""select distinct """ + pondSelectListSet2Sel + """,""" + set2ColsSel + """ from pondTbl pond join final_group2Tbl set2 on pond.pond_md5_hash = set2.pond_md5_hash""").select(pondColListSel: _*)
      FinalSet2Src.repartition(5).persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)

      //for update take only the data with high end date and join to the update record
      var FinalDF = spark.emptyDataFrame

      if (!(lastUpdateDF.head(1).isEmpty)) {
        val FinalSet1SrcHigh = FinalSet1Src.filter(col("rec_end_dttm") === lit("9999-12-31 00:00:00").cast(TimestampType))
        val FinalSet1SrcNoHigh = FinalSet1Src.filter(col("rec_end_dttm") =!= lit("9999-12-31 00:00:00").cast(TimestampType))
        val FinalSet2SrcHigh = FinalSet2Src.filter(col("rec_end_dttm") === lit("9999-12-31 00:00:00").cast(TimestampType))
        val FinalSet2SrcNoHigh = FinalSet2Src.filter(col("rec_end_dttm") =!= lit("9999-12-31 00:00:00").cast(TimestampType))

        val SetHighUpdate = Seq(FinalSet1SrcHigh, FinalSet2SrcHigh)
        val SetHighUpdateAll = SetHighUpdate.reduce(_ union _)
        SetHighUpdateAll.createOrReplaceTempView("SetHighUpdateAllDFTbl")

        lastUpdateDF.createOrReplaceTempView("lastUpdateDFTbl")
        val lastUpdateDFColsSel = "set1.pond_" + surrogateKeyColumn + " as " + surrogateKeyColumn + ",set1.md5_hash, set1.primaryhash, set1.scd_hash, set1.pond_rec_start_dttm as rec_start_dttm, set1.pond_rec_end_dttm as rec_end_dttm, current_timestamp() as upd_dt, set1.pond_load_dt as load_dt, 'U' as etl_ld_status"

        //join lastupdate to Src
        val lastUpdateDFSrcDF = spark.sql("""select distinct """ + srcAllColSel + """,""" + lastUpdateDFColsSel + """ from RemoveSrcDupDFTbl src join lastUpdateDFTbl set1 where src.md5_hash = set1.md5_hash""").select(pondColListSel: _*)
        lastUpdateDFSrcDF.createOrReplaceTempView("lastUpdateDFSrcDFTbl")

        //join lastupdateSrc to the FinalHighSet
        val SetHighUpdateAllCols = SetHighUpdateAll.columns.map(colName => "highDF." + colName + " as highDF_" + colName).mkString(", ")
        //val SetHighUpdateAllCols = SetHighUpdateAll.columns.filter(x=>(x == "md5_hash")).map(colName => "highDF."+colName +" as highDF_"+colName).mkString(", ")
        val lastUpdateDFSrcCols = lastUpdateDFSrcDF.columns.map(colName => "lastUpd." + colName + " as updDF_" + colName).mkString(", ")
        //val lastUpdateDFSrcCols = lastUpdateDFSrcDF.columns.filter(x=>(x == "md5_hash")).map(colName => "lastUpd."+colName+" as updDF_"+colName).mkString(", ")
        val SetHighUpdateAllColsSeq = SetHighUpdateAll.columns.map(colName => "highDF_" + colName).map(colName => col(colName))
        val lastUpdateDFSrcColsSeq = lastUpdateDFSrcDF.columns.map(colName => "updDF_" + col(colName)).map(colName => col(colName))

        //val UpdDFJoinHighDF = spark.sql("""select """+SetHighUpdateAllCols+""","""+lastUpdateDFSrcCols+ """ from SetHighUpdateAllDFTbl highDF join lastUpdateDFSrcDFTbl lastUpd on highDF.md5_hash = lastUpd.md5_hash""")

        val tmpSetHighUpdateAllCols = spark.sql("""select """ + SetHighUpdateAllCols + """ from SetHighUpdateAllDFTbl highDF""")
        val tmplastUpdateDFSrcCols = spark.sql("""select """ + lastUpdateDFSrcCols + """ from lastUpdateDFSrcDFTbl lastUpd""")
        tmpSetHighUpdateAllCols.createOrReplaceTempView("tmpSetHighUpdateAllCols")
        tmplastUpdateDFSrcCols.createOrReplaceTempView("tmplastUpdateDFSrcCols")
        val tmpSetHighUpdateAllColsStr = tmpSetHighUpdateAllCols.columns.map(colName => "highDF." + colName).mkString(", ")
        val tmplastUpdateDFSrcColsStr = tmplastUpdateDFSrcCols.columns.map(colName => "lastUpd." + colName).mkString(", ")

        val UpdDFJoinHighDF = spark.sql("""select """ + tmpSetHighUpdateAllColsStr + """,""" + tmplastUpdateDFSrcColsStr + """ from tmpSetHighUpdateAllCols highDF left join tmplastUpdateDFSrcCols lastUpd on highDF.highDF_scd_hash = lastUpd.updDF_scd_hash""")

        //val UpdDFJoinHighDF = spark.sql("""select """+SetHighUpdateAllCols+""","""+lastUpdateDFSrcCols+ """ from SetHighUpdateAllDFTbl highDF left join lastUpdateDFSrcDFTbl lastUpd on highDF.md5_hash = lastUpd.md5_hash""")

        UpdDFJoinHighDF.createOrReplaceTempView("""UpdDFJoinHighDFTbl""")
        val SetHighUpdateHighColsSel = UpdDFJoinHighDF.columns.filter(colName => colName.startsWith("highDF_")).map(colName => colName + " as " + colName.split("highDF_")(1)).mkString(",")

        //start dttm has to be picked up from src record calculation
        val SetHighUpdateUpdColsSel1 = UpdDFJoinHighDF.columns.filter(colName => colName != "updDF_rec_start_dttm").filter(colName => colName.startsWith("updDF_")).map(colName => colName + " as " + colName.split("updDF_")(1)).mkString(",")
        val SetHighUpdateUpdColsSel2 = ",highDF_rec_start_dttm as rec_start_dttm"
        val SetHighUpdateUpdColsSel = SetHighUpdateUpdColsSel1 + SetHighUpdateUpdColsSel2
        //val SetHighUpdateUpdColsSel = UpdDFJoinHighDF.columns.filter(colName => colName.startsWith("updDF_")).map(colName => colName +" as "+colName.split("updDF_")(1)).mkString(",")

        val UpdDFJoinHighDFHigh = spark.sql("""Select """ + SetHighUpdateHighColsSel + """ from UpdDFJoinHighDFTbl where updDF_primaryhash is Null""").select(pondColListSel: _*)
        val UpdDFJoinHighDFUpd = spark.sql("""Select """ + SetHighUpdateUpdColsSel + """ from UpdDFJoinHighDFTbl where updDF_primaryhash is Not Null""").select(pondColListSel: _*)

        //all datasets together
        //val FinalDFSeq = Seq(UpdDFJoinHighDFHigh, UpdDFJoinHighDFUpd, FinalSet1SrcNoHigh, FinalSet2SrcNoHigh)
        val FinalDFSeq = Seq(UpdDFJoinHighDFHigh, UpdDFJoinHighDFUpd, FinalSet1SrcNoHigh, FinalSet2SrcNoHigh, pondDataDFHist)
        FinalDF = FinalDFSeq.reduce(_ union _).select(pondColListSel: _*)

      } else {
        val FinalDFSeq = Seq(FinalSet1Src, FinalSet2Src, pondDataDFHist)
        FinalDF = FinalDFSeq.reduce(_ union _).select(pondColListSel: _*)
      }
      var landingSql = "True"
      try {
        landingSql = spark.sparkContext.getConf.get("spark.source.landingsql").trim()
      } catch {
        case e: NoSuchElementException => { landingSql = "True"; log.info("#--------------------No Landing Table query  -------------------#") }
      }
      
      
      if(landingSql!="True")
      {
      val srcData= spark.sql(landingSql) 
      val srcPrimaryCol = spark.sparkContext.getConf.get("spark.source.primaryKeyColumns")
      val srcPKColSEQ = srcPrimaryCol.split(",").map(x => col(x)).toSeq
      srcData.printSchema()
      val srcHashedPrimary = srcData.withColumn("srcprimaryhash", md5(concat_ws(",", src_data1.select(srcPKColSEQ: _*).columns.map(x => col(x)): _*)))
      
      var FinalData = FinalDF.as("FinalDF").join(srcHashedPrimary.as("srcHashedPrimary"),$"FinalDF.primaryhash" === $"srcHashedPrimary.srcprimaryhash", "left").select($"FinalDF.*",$"srcHashedPrimary.srcprimaryhash").withColumn("new_rec_end_dttm", when($"primaryhash" === $"srcprimaryhash" and $"rec_end_dttm" === "9999-12-31 00:00:00",current_timestamp()-expr("INTERVAL 10 minutes")).otherwise(FinalDF("rec_end_dttm"))).drop(col("rec_end_dttm")).withColumnRenamed("new_rec_end_dttm","rec_end_dttm").select(pondColListSel: _*)
      
      
      return FinalData
        
      }
      
      else 
      {
        FinalDF.show(false)
        return FinalDF
        
      }

      // end of scd Type 2

    
			  
			}

			/**
			 * Start of SCD Type 1
			 */


			val srcPKCols = spark.sparkContext.getConf.get("spark.source.primaryKeyColumns")

					val srcAllColList = spark.sparkContext.getConf.get("spark.source.columns")

					val pondSelectList = spark.sparkContext.getConf.get("spark.pond.allColumns").trim() //spark.pond.Columns

					var latestSeq = "batchtime";
			try {
				latestSeq = spark.sparkContext.getConf.get("spark.source.latestIdentifierColumns").trim()
			} catch {
			case e: NoSuchElementException => { latestSeq = "batchtime"; log.info("#--------------------No columns to indentify latest record defaulting to Batchtime -------------------#") }

			}
			val srcAllColArray = srcAllColList.split(",")

					val srcColsPKList = srcPKCols.split(",")
					val latestIdcolSEQ = latestSeq.split(",").map(x => col(x).desc).toSeq
					val srcPKColSEQ = srcColsPKList.map(x => col(x)).toSeq
					val window = Window
					.partitionBy(srcPKColSEQ: _*)
					.orderBy(latestIdcolSEQ: _*)
					val srcdfWithRowNumber = src_data.withColumn("rank", row_number().over(window))
					//log.info("#----------------------------------SRC DF with ROW NUMBER ------------------------#")

					val src_dedup_df = srcdfWithRowNumber.filter($"rank" === 1)
					//src_dedup_df.filter($"agentId"==="7101742").show(10,false)
					//src_dedup_df.cache() ///CACHED Source DF

					  src_dedup_df.show(false)

					val pond_table = spark.sparkContext.getConf.get("spark.pond.database").trim() + "." + spark.sparkContext.getConf.get("spark.pond.table").trim()

					log.info("source table@@@@@@@@@@@@@@@" + pond_table)

					val pondSelectSQL = "select " + pondSelectList + " from " + pond_table
println(pondSelectSQL)
					val pondDataDF = spark.sql(pondSelectSQL)
					pondDataDF.show(false)
					//pondDataDF.where($"pond_res_guest_id" === "16329").show(false)

					val srcAllColSEQ = srcAllColArray.map(x => col(x)).toSeq

					var insertUpdateFlaggedDF: DataFrame = null;

					val srccolumns = src_dedup_df.columns

							log.info("XXXXXXXXXXXXXXXXXXXXXXXX--------------------INSIDE IF SCD1------------------XXXXXXXXXXXXXXXXXXXXXXXXXXX")
							val HashedSrcDF = src_dedup_df.withColumn("md5_hash", md5(concat_ws(",", src_data.select(srcAllColSEQ: _*).columns.map(c => col(c)): _*))).as("srh")
							val HashedSrcPrimaryDF = HashedSrcDF.withColumn("primaryhash", md5(concat_ws(",", src_dedup_df.select(srcPKColSEQ: _*).columns.map(c => col(c)): _*)))
							val HashedJoinedDF = HashedSrcPrimaryDF.join(pondDataDF, col("primaryhash") === col("pond_primaryhash"), "full")


							println("#-----FROM POND-----##")
							//pondDataDF.filter($"pond_src_agent_id"==="7101742").show(10,false)
							println("#-------post join with pond -----#")
							//HashedJoinedDF.filter($"pond_src_agent_id"==="7101742").show(10,false)
							insertUpdateFlaggedDF = HashedJoinedDF.withColumn("insert_update_flag", when($"md5_hash" =!= $"pond_md5_hash", "U")
							

									.when($"pond_md5_hash".isNull, "I").when($"pond_md5_hash" === $"md5_hash", "NC").when($"md5_hash".isNull, "NR").otherwise("N")) // md5_hash == pondhash NC// md5_hash is null NR //.when($"md5_hash".isNull and col(src_key_col)===col(tgt_key_col), "I").otherwise("UC"))
									
							//insertUpdateFlaggedDF.where($"pond_res_guest_id" === "16329").show(false)
							// }//END OF IF FOR SCD 1 WILL CHANGE FOR SCD2
							val surrogateKeyColumn = spark.sparkContext.getConf.get("spark.surrogate_key.column").trim()

							val max_var = spark.sql(s"SELECT nvl(max(  $surrogateKeyColumn ),0) as maxval FROM $pond_table").
							collect()(0).getLong(0)
							
							

							val w = Window.orderBy(insertUpdateFlaggedDF("insert_update_flag") desc)

							var preVoyageFlag = "NotPreVoyage";
      					try {
      						preVoyageFlag = spark.sparkContext.getConf.get("spark.source.prevoyageflag").trim()
      					} catch {
      					case e: NoSuchElementException => { preVoyageFlag = "NotPreVoyage"; log.info("#--------------------No columns to decide prevoyage dims or not  -------------------#") }
      
      					}

					 		var insert_update_df=spark.emptyDataFrame
      					
							if (preVoyageFlag.trim().toLowerCase().equals("prevoyage")) {
								insert_update_df=preVoyagefunc(spark,insertUpdateFlaggedDF)
							}

							else
							{

								val insert_flagged_df = insertUpdateFlaggedDF.filter(col("insert_update_flag") === "I")
										.withColumn("row_number", row_number().over(w))
										.withColumn(surrogateKeyColumn, when(col("pond_" + surrogateKeyColumn).isNull, (col("row_number").cast(LongType) + lit(max_var).cast(LongType))).otherwise(col("pond_" + surrogateKeyColumn)))
										.withColumn("upd_dt", current_timestamp())
										.withColumn("load_dt", current_timestamp())

										val update_flagged_df = insertUpdateFlaggedDF.filter(col("insert_update_flag") === "U")
										.withColumn("row_number", lit(0))
										.withColumn(surrogateKeyColumn, when(col("pond_" + surrogateKeyColumn).isNull, (col("row_number").cast(LongType) + lit(max_var).cast(LongType))).otherwise(col("pond_" + surrogateKeyColumn)))
										.withColumn("upd_dt", current_timestamp())
										.withColumn("load_dt", when($"pond_load_dt".isNull, current_timestamp()).otherwise($"pond_load_dt"))

										val nc_nr_flagged_df = insertUpdateFlaggedDF.filter(col("insert_update_flag") === "NC" or col("insert_update_flag") === "NR")
										.withColumn("row_number", lit(0))
										.withColumn(surrogateKeyColumn, when(col("pond_" + surrogateKeyColumn).isNull, (col("row_number").cast(LongType) + lit(max_var).cast(LongType))).otherwise(col("pond_" + surrogateKeyColumn)))
										.withColumn("upd_dt", when($"pond_upd_dt".isNull, current_timestamp()).otherwise($"pond_upd_dt"))
										.withColumn("load_dt", when($"pond_load_dt".isNull, current_timestamp()).otherwise($"pond_load_dt"))

										
										val dfs = Seq(insert_flagged_df, update_flagged_df, nc_nr_flagged_df)
									  insert_update_df = dfs.reduce(_ union _)
							}
							
							println("#--------------------------------------Before the End of core function----------------#")
							//insert_update_df.filter($"agentId"==="7101742").show(10,false)
							return insert_update_df



	}
	
          	def preVoyagefunc(spark: SparkSession, insertUpdateFlaggedDF: DataFrame): DataFrame={
          	  
              import spark.sqlContext.implicits._
            	val log = LogManager.getRootLogger
            	log.setLevel(Level.INFO)
          	
          	  val pond_table = spark.sparkContext.getConf.get("spark.pond.database").trim() + "." + spark.sparkContext.getConf.get("spark.pond.table").trim()
          		val surrogateKeyColumn = spark.sparkContext.getConf.get("spark.surrogate_key.column").trim()

							val max_var = spark.sql(s"SELECT nvl(max(  $surrogateKeyColumn ),0) as maxval FROM $pond_table").
							collect()(0).getLong(0)
							val w = Window.orderBy(insertUpdateFlaggedDF("insert_update_flag") desc)
							
							val insert_flagged_df = insertUpdateFlaggedDF.filter(col("insert_update_flag") === "I")
										.withColumn("row_number", row_number().over(w))
										.withColumn(surrogateKeyColumn, when(col("pond_" + surrogateKeyColumn).isNull, (col("row_number").cast(LongType) + lit(max_var).cast(LongType))).otherwise(col("pond_" + surrogateKeyColumn)))
										.withColumn("upd_dt", current_timestamp())
										.withColumn("load_dt", current_timestamp())
										.withColumn("etl_ld_status",col("insert_update_flag"))

							val update_flagged_df = insertUpdateFlaggedDF.filter(col("insert_update_flag") === "U")
										.withColumn("row_number", lit(0))
										.withColumn(surrogateKeyColumn, when(col("pond_" + surrogateKeyColumn).isNull, (col("row_number").cast(LongType) + lit(max_var).cast(LongType))).otherwise(col("pond_" + surrogateKeyColumn)))
										.withColumn("upd_dt", current_timestamp())
										.withColumn("load_dt", when($"pond_load_dt".isNull, current_timestamp()).otherwise($"pond_load_dt"))
										.withColumn("etl_ld_status",col("insert_update_flag"))

							val nc_nr_flagged_df = insertUpdateFlaggedDF.filter(col("insert_update_flag") === "NC" or col("insert_update_flag") === "NR")
										.withColumn("row_number", lit(0))
										.withColumn(surrogateKeyColumn, when(col("pond_" + surrogateKeyColumn).isNull, (col("row_number").cast(LongType) + lit(max_var).cast(LongType))).otherwise(col("pond_" + surrogateKeyColumn)))
										.withColumn("upd_dt", when($"pond_upd_dt".isNull, current_timestamp()).otherwise($"pond_upd_dt"))
										.withColumn("load_dt", when($"pond_load_dt".isNull, current_timestamp()).otherwise($"pond_load_dt"))
										.withColumn("etl_ld_status",col("insert_update_flag"))

										val dfs = Seq(insert_flagged_df, update_flagged_df, nc_nr_flagged_df)
										val insert_update_df = dfs.reduce(_ union _)
          	    
										
										return insert_update_df
                    	} 
          	
 
	def slowlyChangingDimensionType2(spark: SparkSession, src_data: DataFrame): DataFrame = {
			val df = spark.sql("")
					return df
	}
	def factLoad(spark: SparkSession, src_data: DataFrame): Unit = {
			import spark.sqlContext.implicits._
			val sc = spark.sparkContext
			val sqlContext = new org.apache.spark.sql.SQLContext(sc)
			sqlContext.setConf("hive.exec.dynamic.partition", "true")
			sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
			spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
			val log = LogManager.getRootLogger
			log.info("#--------------------------------------------In THE FACT----------------------------------------------#")
			log.setLevel(Level.INFO)
			val frameworkEnv = spark.sparkContext.getConf.get("spark.frameworkEnv").trim()
			val curr_date = current_date()
			val today = ZonedDateTime.now(ZoneId.of("UTC"))
			val dateformatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
			val curr_date_str = dateformatter format today
			//val src_df_count = src_data     //#----addtional check can be disabled after the job is stable---#
			var fact_type = "TRANS";
			try {
				fact_type = spark.sparkContext.getConf.get("spark.factType").trim()
			} catch {
			case e: NoSuchElementException => { fact_type = "TRANS"; log.info("#--------------------No Fact Type Found Defaulting to Transactional Fact Type Add property (spark.FactType 'Accu/Trans') for changing -------------------#") }

			}
			if (frameworkEnv.trim().toUpperCase().equals("SHORE")) {
				log.info("SHORE Environment")
				if (fact_type.trim().toUpperCase().equals("TRANS")) {
					val insertFactDF = src_data
							.withColumn("upd_dt", current_timestamp())
							.withColumn("load_dt", current_timestamp())
							.withColumn("part_dt", curr_date)

							insertFactDF.createOrReplaceTempView("final_view")
							val final_cols = spark.sparkContext.getConf.get("spark.target.final_collist")

							val finalColSQL = "Select " + final_cols + " from final_view"
							val finalDF = spark.sql(finalColSQL)
							//finalDF.printSchema()

							val pond_table = spark.sparkContext.getConf.get("spark.pond.database").trim() + "." + spark.sparkContext.getConf.get("spark.pond.table").trim()
							finalDF.repartition(15).write.mode("Append").insertInto(pond_table)
							spark.sql("REFRESH TABLE " + pond_table)
							log.info("REFRESH TABLE " + pond_table)
		                //Bigquery transaction fact external table reflects latest data. No need to refresh or need alter table in the code.
						
				} else if (fact_type.trim().toUpperCase().equals("ACCU")) {
					print("#----------------------------------Accumulative Fact -------------------------#")
					log.info("#----------------------------------Accumulative Fact -------------------------#")
					import spark.sqlContext.implicits._
					println("X---------------------------Executing on shore----------------------------X")
					//var sql_connection: Connection = null
					val insertUpdateFlagDF = generateFactDFwithInsertUpdateFlag(spark, src_data)
					val final_cols = spark.sparkContext.getConf.get("spark.target.final_collist")
					val srcPKCols = spark.sparkContext.getConf.get("spark.source.primaryKeyColumns")
					val srcColsPKList = srcPKCols.split(",")

					val srcPKColSEQ = srcColsPKList.map(x => col(x)).toSeq
					
					println("srcPKColSEQ ...." + srcPKColSEQ)
	
          val filterExprFailed = src_data.select(srcPKColSEQ: _*).schema.fields.map(f => (if (f.dataType.toString() == "StringType") f.name.toString() +" is null" else if (f.dataType.toString() == "TimestampType") f.name.toString() +" is null" else if (f.dataType.toString() == "IntegerType") f.name.toString() +"=-1" else if (f.dataType.toString() == "LongType") f.name.toString() +"=-1" else null)).mkString(" or ")

          val filterExprPassed = src_data.select(srcPKColSEQ: _*).schema.fields.map(f => (if (f.dataType.toString() == "StringType") f.name.toString() +" is not null" else if (f.dataType.toString() == "TimestampType") f.name.toString() +" is not null" else if (f.dataType.toString() == "IntegerType") f.name.toString() +"!=-1" else if (f.dataType.toString() == "LongType") f.name.toString() +"!=-1" else null)).mkString(" and ")	
          
    
					println("#------------------------Filter Expression In Main-----------------------#")
					println("filterExprPassed "+filterExprFailed)
					println("filterExprPassed  "+filterExprPassed)
					val insertUpdateFlaggedDF = insertUpdateFlagDF
					.withColumn("upd_dt", current_timestamp())
					.withColumn("load_dt",when($"pond_load_dt".isNull, current_timestamp()).otherwise($"pond_load_dt"))
					.withColumn("part_dt", curr_date)

					insertUpdateFlaggedDF.show(2, false)
				
					println("Before Delete DF")

					log.setLevel(Level.INFO)

			
					log.info("XXXXXXXXXXXXXXXXXXXXXXXX------------------AUDIT COLUMN APPENDING COMPLETE--------------------XXXXXXXXXXXXXXXXXXXXXXXXXXX")

					println("#------------------------------------------------FINAL DATAFRAME-----------------------------------------------#")

					val final_cols_list = final_cols.split(",")
					val final_col_seq = final_cols_list.map(x => col(x)).toSeq

					println("X---------------------------SHORE SIDE ENTIRE DF----------------------------X")

					println("X---------------------------SHORE SIDE DF----------------------------X")

					val pond_table = spark.sparkContext.getConf.get("spark.pond.database").trim() + "." + spark.sparkContext.getConf.get("spark.pond.table").trim()
					val error_table = spark.sparkContext.getConf.get("spark.pond.database").trim() + "." + spark.sparkContext.getConf.get("spark.pond.table").trim() + "_err"
					
                    println("Error table name .... "+error_table)
					println("XXXXXXXXXXXXXXXXXXXXXXXX------------------AUDIT COLUMN APPENDING  COMPLETE--------------------XXXXXXXXXXXXXXXXXXXXXXXXXXX")
					log.info("XXXXXXXXXXXXXXXXXXXXXXXX------------------AUDIT COLUMN APPENDING COMPLETE--------------------XXXXXXXXXXXXXXXXXXXXXXXXXXX")

					insertUpdateFlaggedDF.createOrReplaceTempView("final_view")

					log.info("sorce table@@@@@@@@@@@@@@@" + pond_table)

					val pondSelectSQL = "select pond.* from " + pond_table + " pond inner join final_view fv on ((insert_update_flag='NR' or insert_update_flag='NC') and pond.primaryhash == fv.pond_primaryhash )"

					val pondDataNCNRDF = spark.sql(pondSelectSQL)
					print(s"#------------------------------Test Dataframe $pondSelectSQL ----------------------#")

					log.info("sorce table@@@@@@@@@@@@@@@" + pond_table)
					val final_df_sql = "Select " + final_cols + " from final_view where insert_update_flag='I' or insert_update_flag='U'"
					val finalIUDF = spark.sql(final_df_sql)
					val dfs = Seq(finalIUDF, pondDataNCNRDF)
					val final_temp_df = dfs.reduce(_ union _)
					final_temp_df.printSchema()
					println("X--------------------------Final Insert Update DF----------------X")
					//postgresinsertUpdateDf

					println("#------------------------------------------------FINAL DATAFRAME-----------------------------------------------#")

					//final_temp_df.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
					final_temp_df.repartition(15).write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
					
					spark.sql("REFRESH TABLE " + pond_table)
					println("REFRESH TABLE " + pond_table)

					val tempLocation = spark.sparkContext.getConf.get("spark.target.temp").trim()
					val BQTempExtnTbl = spark.sparkContext.getConf.get("spark.bq.dataset").trim() + "." + spark.sparkContext.getConf.get("spark.bq.temptblname").trim()
					val BQViewName = spark.sparkContext.getConf.get("spark.bq.viewname").trim()
					log.info(s"Alter View $BQViewName to point $BQTempExtnTbl ")
					val temp_qry=s"CREATE OR REPLACE VIEW $BQViewName AS SELECT * FROM $BQTempExtnTbl"
						try {
						    val bigquery = BigQueryOptions.getDefaultInstance().getService()
                            val config = QueryJobConfiguration.newBuilder(temp_qry).build()
							val job = bigquery.create(JobInfo.of(config))
							log.info("**Bigquery Accumulative Fact Load **" + "job val= " + job);
									if (job.getStatus().getError() != null) 
								     	{  println("Job create view failed ..."+ job.getStatus().getError())
							  throw new RuntimeException(String.format("Job %s ended with error %s", job.getJobId(), 
                   job.getStatus().getError().getMessage()))    }
	              	    	        else println("view executed ")
                }
    							catch {
    				       //Handle errors for BQ
    						case e: BigQueryException =>
    								{ log.info("******************in the catch of Bigquery Fact Load ******************");
        									e.printStackTrace(); }
    								case e: Exception =>
    								{ log.info("******************in the catch of BigQuery Fact Load ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace", e); }
    								print("BigQuery Failed")
    					   }
	
					val final_df = spark.read.parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
					
					//final_df.filter(filterExprPassed).show(false)
					//final_df.filter(filterExprFailed).show(false)
					println("#---------------------------------------------Data Integity Checking---------------------#")

					final_df.filter(filterExprPassed).createOrReplaceTempView("final_check")

					val intCheckDF = spark.sql("select count(*), " + srcPKCols + " from final_check group by " + srcPKCols + " having count(*)>1 ")
					println("select count(*), " + srcPKCols + " from final_check group by " + srcPKCols + " having count(*)>1 ", intCheckDF.count)
					if (intCheckDF.head(1).isEmpty == false) {
						println("#---------------------------------------------Data Integity Checked Failed Duplicates introduced---------------------#")
						log.info("#---------------------------------------------Data Integity Checked Failed Duplicates introduced ---------------------#")
						throw new Exception("Data Integity Checked Failed Duplicates introduced");
					}

					println("#---------------------------------------------Data Integity Checked Passed---------------------#")
					final_df.filter(filterExprPassed).write.mode("Overwrite").insertInto(pond_table)
					final_df.filter(filterExprFailed).write.mode("Overwrite").insertInto(error_table)
					spark.sql("REFRESH TABLE " + pond_table)
					println("REFRESH TABLE " + pond_table)
					spark.sql("REFRESH TABLE " + error_table)
					println("REFRESH TABLE " + error_table)
					
		
					
					val finalLocation = spark.sparkContext.getConf.get("spark.target.location").trim()
					val finalErrorLocation = spark.sparkContext.getConf.get("spark.target.location").trim() + "_err"
					val BQPermExtnTbl = spark.sparkContext.getConf.get("spark.bq.dataset").trim() + "." + spark.sparkContext.getConf.get("spark.bq.permtblname").trim()
					val BQErrorTableName = spark.sparkContext.getConf.get("spark.bq.dataset").trim() + "." + spark.sparkContext.getConf.get("spark.bq.permtblname").trim() + "_err"
					log.info(s"Alter View $BQViewName to point $BQPermExtnTbl ")
					val perm_qry=s"CREATE OR REPLACE VIEW $BQViewName AS SELECT * FROM $BQPermExtnTbl"
					 try {
										
					  val bigquery = BigQueryOptions.getDefaultInstance().getService()
                      val config = QueryJobConfiguration.newBuilder(perm_qry).build()
					  val job = bigquery.create(JobInfo.of(config))
					  log.info("**Bigquery Accumulative Fact Load **" + "job val= " + job);
											if (job.getStatus().getError() != null) 
								     	{  println("Job create view failed ..."+ job.getStatus().getError())
							  throw new RuntimeException(String.format("Job %s ended with error %s", job.getJobId(), 
                   job.getStatus().getError().getMessage()))    }
	              	    	        else println("view executed ")
	                	}
    				catch {
    				       //Handle errors for BQ
    						case e: BigQueryException =>
    								{ log.info("******************in the catch of Bigquery Fact Load ******************");
        									e.printStackTrace(); }
    								case e: Exception =>
    								{ log.info("******************in the catch of BigQuery Fact Load ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace", e); }
    								print("BigQuery Failed")
    					   }

								//final_df.write.mode("Overwrite").insertInto(pond_table)
								log.info(s"XXXXXXXXXXXXXXXXXXXXXXXX--------------------$pond_table loaded in Spark------------------XXXXXXXXXXXXXXXXXXXXXXXXXXX")
	
					if (src_data.filter(filterExprFailed).head(1).isEmpty == false) {
						println("#---------------------------There are records with no matches for primary skeys------------------#")
						log.info("#---------------------------There are records with no matches for primary skeys------------------#")
						src_data.filter(filterExprFailed).show(false)
						sys.exit(1)
					}

			} else if (fact_type.trim().toUpperCase().equals("SNAPSHOT")) {
				  
				  var part_col = "part_dt";
			try {
				part_col = spark.sparkContext.getConf.get("spark.partition.column").trim()
			} catch {
			case e: NoSuchElementException => { part_col = "part_dt"; log.info("#--------------------No Partition Column Found Defaulting to part_dt Add property (spark.partition.column 'partiton_columnname') for changing -------------------#") }

			}
				  
				  val insertFactDF = src_data
							.withColumn("upd_dt", current_timestamp())
							.withColumn("load_dt", current_timestamp())
							.withColumn(part_col, curr_date)

                            val latest_part_dt=java.time.LocalDate.now.toString

							insertFactDF.createOrReplaceTempView("final_view")
							val final_cols = spark.sparkContext.getConf.get("spark.target.final_collist")

							val finalColSQL = "Select " + final_cols + " from final_view"
						    val final_temp_df = spark.sql(finalColSQL)
							//finalDF.printSchema()

							val pond_table = spark.sparkContext.getConf.get("spark.pond.database").trim() + "." + spark.sparkContext.getConf.get("spark.pond.table").trim()
							val finalLocation = spark.sparkContext.getConf.get("spark.target.location").trim()
							final_temp_df.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
							val final_df = spark.read.parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
						   spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
										
						final_df.repartition(15).write.mode("Overwrite").insertInto(pond_table)
							
						spark.sql("REFRESH TABLE " + pond_table)
							log.info("REFRESH TABLE " + pond_table)
			
                           //  final_df.write.mode(org.apache.spark.sql.SaveMode.Overwrite).parquet(finalLocation + "/" + curr_date)
                             // final_df.write.mode(org.apache.spark.sql.SaveMode.Overwrite).parquet(finalLocation + "/" +part_col+"="+latest_part_dt)
 							/*	val insertString = s"""alter table $pond_table add IF NOT EXISTS partition($part_col='$latest_part_dt') location '$finalLocation/$part_col=$latest_part_dt'""".stripMargin
								println(insertString)
								spark.sql(insertString)
								spark.sql("refresh table " + pond_table)
								spark.sql("Msck repair table " + pond_table)*/
					}else {

					println("Invalid Fact Type")
					log.info("Invalid Fact Type")
					sys.exit(1)

				}
			} else if (frameworkEnv.trim().toUpperCase().equals("SHIP")) {
				println("SHIP Environment")

				//src_data.show()


				//println("SHIP Environment")

				//src_data.show()
				if (fact_type.trim().toUpperCase().equals("TRANS")) {
					val insertFactDF = src_data
							.withColumn("upd_dt", current_timestamp())
							.withColumn("load_dt", current_timestamp())
							.withColumn("part_dt", curr_date)

							insertFactDF.createOrReplaceTempView("final_view")
							//insertFactDF.printSchema()

							val final_cols = spark.sparkContext.getConf.get("spark.target.final_collist")

							val finalColSQL = "Select " + final_cols + " from final_view"
							val finalDF = spark.sql(finalColSQL)
							finalDF.printSchema()

							val pond_table = spark.sparkContext.getConf.get("spark.pond.database").trim() + "." + spark.sparkContext.getConf.get("spark.pond.table").trim()
							//finalDF.write.partitionBy("part_dt").mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
							finalDF.write.mode("Append").insertInto(pond_table)
							spark.sql("REFRESH TABLE " + pond_table)
							println("REFRESH TABLE " + pond_table)
							val connection_url = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.postgresql.url").trim() + spark.sparkContext.getConf.get("spark.postgresql.database").trim())

							val user_name = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.postgresql.user").trim())
							val pass_word = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.postgresql.password").trim())
							var sql_connection: Connection = null

							val tempLocation = spark.sparkContext.getConf.get("spark.target.temp").trim()
							val finalLocation = spark.sparkContext.getConf.get("spark.target.location").trim()
							val url: String = spark.sparkContext.getConf.get("spark.postgresql.url").trim() + spark.sparkContext.getConf.get("spark.postgresql.database")
							val tableName: String = spark.sparkContext.getConf.get("spark.postgresql.schema").trim() + "." + spark.sparkContext.getConf.get("spark.postgresql.tablename")

							val user: String = user_name.value
							val password: String = pass_word.value

							val properties = new Properties()
							properties.setProperty("user", user)
							properties.setProperty("password", password)
							properties.put("driver", "org.postgresql.Driver")
							finalDF.write.mode(SaveMode.Append).jdbc(url, tableName, properties)
				} else if (fact_type.trim().toUpperCase().equals("ACCU")) {
					print("#----------------------------------Accumulative Fact -------------------------#")
					log.info("#----------------------------------Accumulative Fact -------------------------#")
					import spark.sqlContext.implicits._
					println("X---------------------------Executint on ship----------------------------X")
					//var sql_connection: Connection = null
					val insertUpdateFlagDF = generateFactDFwithInsertUpdateFlag(spark, src_data)
					val srcPKCols = spark.sparkContext.getConf.get("spark.source.primaryKeyColumns")
					val srcColsPKList = srcPKCols.split(",")

					val srcPKColSEQ = srcColsPKList.map(x => col(x)).toSeq
				
					val filterExprFailed = src_data.select(srcPKColSEQ: _*).schema.fields.map(f => (if (f.dataType.toString() == "StringType") f.name.toString() +" is null" else if (f.dataType.toString() == "TimestampType") f.name.toString() +" is null" else if (f.dataType.toString() == "IntegerType") f.name.toString() +"=-1" else if (f.dataType.toString() == "LongType") f.name.toString() +"=-1" else null)).mkString(" or ")

          val filterExprPassed = src_data.select(srcPKColSEQ: _*).schema.fields.map(f => (if (f.dataType.toString() == "StringType") f.name.toString() +" is not null" else if (f.dataType.toString() == "TimestampType") f.name.toString() +" is not null" else if (f.dataType.toString() == "IntegerType") f.name.toString() +"!=-1" else if (f.dataType.toString() == "LongType") f.name.toString() +"!=-1" else null)).mkString(" and ")
					
					println("Filter Passed#---------------------------------" + filterExprPassed + "----------------------------------#")
					println("Filter Failed#---------------------------------" + filterExprFailed + "----------------------------------#")
					val curr_date = current_date()
					val insertUpdateFlaggedDF = insertUpdateFlagDF
					.withColumn("upd_dt", current_timestamp())
					.withColumn("load_dt", when($"pond_load_dt".isNull, current_timestamp()).otherwise($"pond_load_dt"))
					.withColumn("part_dt", curr_date)

					//insertUpdateFlaggedDF.show(12, false)
					///insertUpdateFlaggedDF.printSchema()
					println("#------------------------------------------------Updating Postgres Table-----------------------------------------------#")

					val connection_url = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.postgresql.url").trim() + spark.sparkContext.getConf.get("spark.postgresql.database").trim())

					val user_name = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.postgresql.user").trim())
					val pass_word = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.postgresql.password").trim())
					var sql_connection: Connection = null

					println("Before Delete DF")

					val deleteDF = insertUpdateFlaggedDF.filter(col("insert_update_flag") === "U").select("primaryhash")
					//deleteDF.show(12,false)
					println("Post Delete DF")
					val target_table = spark.sparkContext.getConf.get("spark.postgresql.schema").trim() + "." + spark.sparkContext.getConf.get("spark.postgresql.tablename").trim()

					log.setLevel(Level.INFO)

					log.info("target table@@@@@@@@@@@@@@@" + target_table)
					println("target table@@@@@@@@@@@@@@@" + target_table)
					//deleteDF.printSchema()
					//deleteDF.show(12, false)
					deleteDF.rdd.foreachPartition { rows =>
					Class.forName("org.postgresql.Driver");

					sql_connection = DriverManager.getConnection(connection_url.value, user_name.value, pass_word.value)

							val primary_col = "primaryhash"
							rows.foreach { row =>
							var primaryhash_in = row.getString(0)
							println(s"DELETE FROM  $target_table Where $primary_col ='$primaryhash_in' ")
							// val del_query =s"DELETE FROM  $target_table Where $primary_col ='$primaryhash_in' "
							val prepare_statement_add_column = sql_connection.prepareStatement(s"DELETE FROM  $target_table Where $primary_col ='$primaryhash_in' ")
							prepare_statement_add_column.executeUpdate()

							prepare_statement_add_column.close()
					}
					//sql_connection.commit(); Auto Commit Is enabled
					sql_connection.close()
					}
					println("XXXXXXXXXXXXXXXXXXXXXXXX------------------DELETION OF UPDATED RECORDS FROM POSTGRES COMPLETE--------------------XXXXXXXXXXXXXXXXXXXXXXXXXXX")
					log.info("XXXXXXXXXXXXXXXXXXXXXXXX------------------DELETION OF UPDATED RECORDS FROM POSTGRES COMPLETE--------------------XXXXXXXXXXXXXXXXXXXXXXXXXXX")

					val postgresinsertUpdateDf = insertUpdateFlaggedDF.filter(col("insert_update_flag") === "U" or col("insert_update_flag") === "I") //.select("primaryhash")//.dropDuplicates("primaryhash") //.collect().mkString("'", "', '", "'").replace("]","").replace("[","")
					postgresinsertUpdateDf.show(false)
					log.info("XXXXXXXXXXXXXXXXXXXXXXXX------------------AUDIT COLUMN APPENDING COMPLETE--------------------XXXXXXXXXXXXXXXXXXXXXXXXXXX")

					println("#------------------------------------------------FINAL DATAFRAME-----------------------------------------------#")
					val final_cols = spark.sparkContext.getConf.get("spark.target.final_collist")
					val final_cols_list = final_cols.split(",")
					val final_col_seq = final_cols_list.map(x => col(x)).toSeq

					val url: String = spark.sparkContext.getConf.get("spark.postgresql.url").trim() + spark.sparkContext.getConf.get("spark.postgresql.database")
					val tableName: String = spark.sparkContext.getConf.get("spark.postgresql.schema").trim() + "." + spark.sparkContext.getConf.get("spark.postgresql.tablename")
					val ErrortableName: String = spark.sparkContext.getConf.get("spark.postgresql.schema").trim() + "." + spark.sparkContext.getConf.get("spark.postgresql.tablename") + "_err"

					val user: String = user_name.value
					val password: String = pass_word.value

					val properties = new Properties()
					properties.setProperty("user", user)
					properties.setProperty("password", password)
					properties.put("driver", "org.postgresql.Driver")

					println("X---------------------------ship SIDE ENTIRE DF----------------------------X")

					println("X---------------------------ship SIDE DF----------------------------X")

					val pond_table = spark.sparkContext.getConf.get("spark.pond.database").trim() + "." + spark.sparkContext.getConf.get("spark.pond.table").trim()
					val err_table = spark.sparkContext.getConf.get("spark.pond.database").trim() + "." + spark.sparkContext.getConf.get("spark.pond.table").trim() + "_err"
					println("XXXXXXXXXXXXXXXXXXXXXXXX------------------AUDIT COLUMN APPENDING COMPLETE--------------------XXXXXXXXXXXXXXXXXXXXXXXXXXX")
					log.info("XXXXXXXXXXXXXXXXXXXXXXXX------------------AUDIT COLUMN APPENDING COMPLETE--------------------XXXXXXXXXXXXXXXXXXXXXXXXXXX")

					insertUpdateFlaggedDF.createOrReplaceTempView("final_view")

					log.info("sorce table@@@@@@@@@@@@@@@" + pond_table)

					val pondSelectSQL = "select pond.* from " + pond_table + " pond inner join final_view fv on ((insert_update_flag='NR' or insert_update_flag='NC') and pond.primaryhash == fv.pond_primaryhash )"

					val pondDataNCNRDF = spark.sql(pondSelectSQL)
					print(s"#------------------------------Test Dataframe $pondSelectSQL ----------------------#")

					log.info("sorce table@@@@@@@@@@@@@@@" + pond_table)
					val final_df_sql = "Select " + final_cols + " from final_view where insert_update_flag='I' or insert_update_flag='U'"
					val finalIUDF = spark.sql(final_df_sql)
					val dfs = Seq(finalIUDF, pondDataNCNRDF)
					val final_temp_df = dfs.reduce(_ union _)
					//final_temp_df.printSchema()
					println("X--------------------------Final Insert Update DF----------------X")
					//postgresinsertUpdateDf
					postgresinsertUpdateDf.createOrReplaceTempView("final_view_postgres")
					val final_postgredf_sql = "Select " + final_cols + " from final_view_postgres as final_view"

					println("X-------------------------------postgres printschema----------------------------X")
					val final_postgredf = spark.sql(final_postgredf_sql)
					final_postgredf.show(false)
					//final_postgredf.printSchema()
					final_temp_df.show()
					//print("So basically we ned to focus on postres seperately")
					println("#------------------------------------------------FINAL DATAFRAME-----------------------------------------------#")
					final_postgredf.filter(filterExprPassed).write.mode(SaveMode.Append).jdbc(url, tableName, properties)
					final_postgredf.filter(filterExprFailed).write.mode(SaveMode.Append).jdbc(url, ErrortableName, properties)
					final_temp_df.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
					spark.sql("REFRESH TABLE " + pond_table)
					println("REFRESH TABLE " + pond_table)

					val final_df = spark.read.parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
					//final_df.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.location").trim())

					final_df.filter(filterExprPassed).write.mode("Overwrite").insertInto(pond_table)
					final_df.filter(filterExprFailed).write.mode("Overwrite").insertInto(err_table)
					spark.sql("REFRESH TABLE " + pond_table)
					println("REFRESH TABLE " + pond_table)
					//src_data.filter(filterExprPassed).show(false)}
					if (src_data.filter(filterExprFailed).head(1).isEmpty == false) {
						println("#---------------------------There are no matches for primary skeys------------------#")
						log.info("#---------------------------There are no matches for primary skeys------------------#")
						src_data.filter(filterExprFailed).show(false)
						sys.exit(1)
					}
				} else {

					println("Invalid Fact Type")
					log.info("Invalid Fact Type")
					sys.exit(1)

				}

			} else {
				log.info("Invalid Environment")
			}

	}

	def loadDimFact(spark: SparkSession, src_data: DataFrame): Unit = {
			import spark.sqlContext.implicits._
			
  		val log = LogManager.getRootLogger
  		spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
			val EmptyDF = spark.emptyDataFrame
			//EmptyDF.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
			log.setLevel(Level.INFO)
			var ladFlag ="false";
			try {
				ladFlag = spark.sparkContext.getConf.get("spark.ladjob.flag").trim()
			} catch {
			case e: NoSuchElementException => { ladFlag = "false"; log.info("#--------------------No LAD FLAG Found Defaulting to false-------------------#") }

			}
			val frameworkEnv = spark.sparkContext.getConf.get("spark.frameworkEnv").trim()
					val tableType = spark.sparkContext.getConf.get("spark.tableType").trim()

					val final_cols = spark.sparkContext.getConf.get("spark.target.final_collist")
					val final_cols_list = final_cols.split(",")
					val final_col_seq = final_cols_list.map(x => col(x)).toSeq
					if (src_data.head(1).isEmpty == false) {
						if (tableType.trim().toUpperCase().equals("FACT")) {
							log.info("Load Fact Here")
							println("#-----------load fact lad flag is ------------------#",ladFlag)
							if (ladFlag.trim().toUpperCase().equals("TRUE"))
							{
							  println("Inside the call to factLoadLAD ... ")
								factLoadLAD(spark, src_data)
							  }
							else{
								factLoad(spark, src_data)
							}
						} else {
							if (frameworkEnv.trim().toUpperCase().equals("SHIP")) {
								var debug_flag = "False";
								try {
									debug_flag = spark.sparkContext.getConf.get("spark.debug.flag").trim()
								} catch {
								case e: NoSuchElementException => { debug_flag = "False"; log.info("#--------------------No Debug Flag No debug  -------------------#") }

								}
								import spark.sqlContext.implicits._
								println("X---------------------------Executint on ship----------------------------X")
								val surrogateKeyColumn = spark.sparkContext.getConf.get("spark.surrogate_key.column").trim()
								//var sql_connection: Connection = null
								val insertSourceUpdateFlaggedDF = generateDFwithInsertUpdateFlag(spark, src_data)
								val tempTable = "insertSourceUpdateFlaggedDF_" + spark.sparkContext.getConf.get("spark.postgresql.tablename").trim()
								//
								var tempLocationInter = "/data/apps/nbx/temp/" + tempTable;
								try {
									tempLocationInter = spark.sparkContext.getConf.get("spark.inprocess.tempLocation").trim()
								} catch {
								case e: NoSuchElementException => { tempLocationInter = "/data/apps/nbx/temp/" + tempTable; log.info("#--------------------No Inprocess temp location found using default location -------------------#") }

								}
								EmptyDF.write.mode("Overwrite").parquet(tempLocationInter) //remove
								//spark.sparkContext.getConf.get("spark.target.temp").trim()+"/"+tempTable

								insertSourceUpdateFlaggedDF.write.mode("Overwrite").parquet(tempLocationInter)
								//insertSourceUpdateFlaggedDF.createOrReplaceTempView("insertSourceUpdateFlaggedDF")

								//insertSourceUpdateFlaggedDF.registerTempTable(tempTable)
								val insertUpdateFlaggedDF = spark.read.parquet(tempLocationInter.trim())
								// spark.sql(s"""select * from $tempTable""")
								println("#----------------------------------------------Insert Update DF----------------------------------#")
								//insertUpdateFlaggedDF.filter(col(surrogateKeyColumn) === 2920).show(false)}

								println("#------------------------------------------------Updating Postgres Table-----------------------------------------------#")

								val connection_url = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.postgresql.url").trim() + spark.sparkContext.getConf.get("spark.postgresql.database").trim())

								val user_name = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.postgresql.user").trim())
								val pass_word = spark.sparkContext.broadcast(spark.sparkContext.getConf.get("spark.postgresql.password").trim())
								var sql_connection: Connection = null

								println("Before Delete DF")

								val deleteDF = insertUpdateFlaggedDF.filter(col("insert_update_flag") === "U").select("primaryhash")
								//deleteDF.show(12,false)
								println("#----------------------------------------------IdeleteDFe DF----------------------------------#")
								//deleteDF.filter(col(surrogateKeyColumn)===2920).show(false)}
								println("Post Delete DF")
								val target_table = spark.sparkContext.getConf.get("spark.postgresql.schema").trim() + "." + spark.sparkContext.getConf.get("spark.postgresql.tablename").trim()

								log.setLevel(Level.INFO)

								log.info("target table@@@@@@@@@@@@@@@" + target_table)
								println("target table@@@@@@@@@@@@@@@" + target_table)
								deleteDF.printSchema()
								if (debug_flag.trim().toUpperCase().equals("TRUE")) {
									deleteDF.show(false)
								}
								deleteDF.rdd.foreachPartition { rows =>
								Class.forName("org.postgresql.Driver");

								sql_connection = DriverManager.getConnection(connection_url.value, user_name.value, pass_word.value)

										val primary_col = "primaryhash"
										rows.foreach { row =>
										var primaryhash_in = row.getString(0)
										println(s"DELETE FROM  $target_table Where $primary_col ='$primaryhash_in' ")
										// val del_query =s"DELETE FROM  $target_table Where $primary_col ='$primaryhash_in' "
										val prepare_statement_add_column = sql_connection.prepareStatement(s"DELETE FROM  $target_table Where $primary_col ='$primaryhash_in' ")
										prepare_statement_add_column.executeUpdate()

										prepare_statement_add_column.close()
								}
								//sql_connection.commit(); Auto Commit Is enabled
								sql_connection.close()
								}
								//sys.exit(1)
								println("XXXXXXXXXXXXXXXXXXXXXXXX------------------DELETION OF UPDATED RECORDS FROM POSTGRES COMPLETE--------------------XXXXXXXXXXXXXXXXXXXXXXXXXXX")
								log.info("XXXXXXXXXXXXXXXXXXXXXXXX------------------DELETION OF UPDATED RECORDS FROM POSTGRES COMPLETE--------------------XXXXXXXXXXXXXXXXXXXXXXXXXXX")

								val postgresinsertUpdateDf = insertUpdateFlaggedDF.filter(col("insert_update_flag") === "U" or col("insert_update_flag") === "I") //.select("primaryhash")//.dropDuplicates("primaryhash") //.collect().mkString("'", "', '", "'").replace("]","").replace("[","")

								log.info("XXXXXXXXXXXXXXXXXXXXXXXX------------------AUDIT COLUMN APPENDING COMPLETE--------------------XXXXXXXXXXXXXXXXXXXXXXXXXXX")
								println("#----------------------------------------------postgresinsertUpdateDf DF----------------------------------#")
								println("#------------------------------------------------FINAL DATAFRAME-----------------------------------------------#")
								// final_df.show()

								val url: String = spark.sparkContext.getConf.get("spark.postgresql.url").trim() + spark.sparkContext.getConf.get("spark.postgresql.database")
								val tableName: String = spark.sparkContext.getConf.get("spark.postgresql.schema").trim() + "." + spark.sparkContext.getConf.get("spark.postgresql.tablename")

								val user: String = user_name.value
								val password: String = pass_word.value

								val properties = new Properties()
								properties.setProperty("user", user)
								properties.setProperty("password", password)
								properties.put("driver", "org.postgresql.Driver")

								println("X---------------------------ship SIDE ENTIRE DF----------------------------X")

								println("X---------------------------ship SIDE DF----------------------------X")

								val pond_table = spark.sparkContext.getConf.get("spark.pond.database").trim() + "." + spark.sparkContext.getConf.get("spark.pond.table").trim()

								println("XXXXXXXXXXXXXXXXXXXXXXXX------------------AUDIT COLUMN APPENDING COMPLETE--------------------XXXXXXXXXXXXXXXXXXXXXXXXXXX")
								log.info("XXXXXXXXXXXXXXXXXXXXXXXX------------------AUDIT COLUMN APPENDING COMPLETE--------------------XXXXXXXXXXXXXXXXXXXXXXXXXXX")

								insertUpdateFlaggedDF.createOrReplaceTempView("final_view")

								log.info("sorce table@@@@@@@@@@@@@@@" + pond_table)

								val pondSelectSQL = "select pond.* from " + pond_table + " pond inner join final_view fv on ((insert_update_flag='NR' or insert_update_flag='NC') and pond.primaryhash == fv.pond_primaryhash )"

								val pondDataNCNRDF = spark.sql(pondSelectSQL)
								print(s"#------------------------------Test Dataframe $pondSelectSQL ----------------------#")

								log.info("sorce table@@@@@@@@@@@@@@@" + pond_table)
								val final_df_sql = "Select " + final_cols + " from final_view where insert_update_flag='I' or insert_update_flag='U'"
								val finalIUDF = spark.sql(final_df_sql)
								val dfs = Seq(finalIUDF, pondDataNCNRDF)
								val final_temp_df = dfs.reduce(_ union _)
								final_temp_df.printSchema()
								println("X--------------------------Final Insert Update DF----------------X")
								//postgresinsertUpdateDf
								postgresinsertUpdateDf.createOrReplaceTempView("final_view_postgres")
								val final_postgredf_sql = "Select " + final_cols + " from final_view_postgres as final_view"

								println("X-------------------------------postgres printschema----------------------------X")
								val final_postgredf = spark.sql(final_postgredf_sql)
								println("#----------------------------------------------final_postgredf DF----------------------------------#")
								//final_postgredf.filter(col(surrogateKeyColumn) === 2920).show(false)}
								//final_postgredf.show(12,false)
								//final_postgredf.printSchema()
								if (debug_flag.trim().toUpperCase().equals("TRUE")) {
									final_temp_df.show()
								}
								//print("So basically we ned to focus on postres seperately")
								println("#------------------------------------------------FINAL DATAFRAME-----------------------------------------------#")
								EmptyDF.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
								final_temp_df.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
								spark.sql("REFRESH TABLE " + pond_table)
								println("REFRESH TABLE " + pond_table)
								val final_df = spark.read.parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
								//final_df.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.location").trim())
								final_postgredf.write.mode(SaveMode.Append).jdbc(url, tableName, properties)
								final_df.write.mode("Overwrite").insertInto(pond_table)
								println("#----------------------------------------------final_df DF----------------------------------#")
								if (debug_flag.trim().toUpperCase().equals("TRUE")) {
									final_df.show(false)
								}
								spark.sql("REFRESH TABLE " + pond_table)
								println("REFRESH TABLE " + pond_table)

							} 

							/**
							 * Updating for scd2 
							 */

							else if (frameworkEnv.trim().toUpperCase().equals("SHORE")) {

								var scdType = "type1";
								try {
									scdType = spark.sparkContext.getConf.get("spark.scdtype").trim()
								} catch {
								case e: NoSuchElementException => { scdType = "type1"; log.info("#--------------------No columns to decide scd so default is scd1  -------------------#") }
								}


								import spark.sqlContext.implicits._
								log.info("X---------------------------Executint on Shore----------------------------X")
								//var sql_connection: Connection = null
								val insertUpdateFlaggedDF = generateDFwithInsertUpdateFlag(spark, src_data)

								log.info("X---------------------------SHORE SIDE ENTIRE DF----------------------------X")
								log.info("X---------------------------SHORE SIDE DF----------------------------X")

								val pond_table = spark.sparkContext.getConf.get("spark.pond.database").trim() + "." + spark.sparkContext.getConf.get("spark.pond.table").trim()

								log.info("XXXXXXXXXXXXXXXXXXXXXXXX------------------AUDIT COLUMN APPENDING COMPLETE--------------------XXXXXXXXXXXXXXXXXXXXXXXXXXX")
								log.info("XXXXXXXXXXXXXXXXXXXXXXXX------------------AUDIT COLUMN APPENDING COMPLETE--------------------XXXXXXXXXXXXXXXXXXXXXXXXXXX")

								insertUpdateFlaggedDF.createOrReplaceTempView("final_view")

								var final_temp_df=spark.emptyDataFrame
								var write_flag=1

								if(scdType.trim().toLowerCase().equals("type2"))
								{
								  println("Inside Scd type 2 code for prevoyage")
									final_temp_df=spark.sql("select * from final_view")
								}

								if(scdType.trim().toLowerCase().equals("type1"))
								{

								   println("Inside Scd type 1 code")
									log.info("sorce table@@@@@@@@@@@@@@@" + pond_table)
									val pondSelectSQL = "select pond.* from " + pond_table + " pond inner join final_view fv on ((insert_update_flag='NR' or insert_update_flag='NC') and pond.primaryhash == fv.pond_primaryhash )"

									val pondDataNCNRDF = spark.sql(pondSelectSQL)
									print(s"#------------------------------Test Dataframe $pondSelectSQL ----------------------#")

									log.info("sorce table@@@@@@@@@@@@@@@" + pond_table)

									val final_df_sql = "Select " + final_cols + " from final_view where insert_update_flag='I' or insert_update_flag='U'"
									val finalIUDF = spark.sql(final_df_sql)
								
									if (finalIUDF.head(1).isEmpty){
										write_flag=0
									}

									println("#---------------insert Update DF---------")
									//finalIUDF.filter($"src_agent_id"==="7101742").show(12,false)
									if(write_flag==1){
										val dfs = Seq(finalIUDF, pondDataNCNRDF)
												 final_temp_df = dfs.reduce(_ union _)
												final_temp_df.printSchema()
												//final_temp_df.show()
												println("#------------------------------------------------FINAL DATAFRAME-----------------------------------------------#")
												//final_temp_df.filter($"src_agent_id"==="7101742").show(12,false)
												log.info("#------------------------------------------------FINAL DATAFRAME-----------------------------------------------#")
												//sys.exit(1)

									}  
								}
								println("value of write flag "+ write_flag)
								
								final_temp_df.show(2,false)
								
								if(write_flag==1){
								  
								final_temp_df.repartition(15).write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())


								spark.sql("REFRESH TABLE " + pond_table)
								log.info("REFRESH TABLE " + pond_table)

					          val tempLocation = spark.sparkContext.getConf.get("spark.target.temp").trim()     
										val BQTempExtnTbl = spark.sparkContext.getConf.get("spark.bq.dataset").trim() + "." + spark.sparkContext.getConf.get("spark.bq.temptblname").trim()
										val BQViewName = spark.sparkContext.getConf.get("spark.bq.viewname").trim()
										log.info(s"Alter View $BQViewName to point $BQTempExtnTbl ")
										val temp_qry=s"CREATE OR REPLACE VIEW $BQViewName AS SELECT * FROM $BQTempExtnTbl"
										try {
										
										  val bigquery = BigQueryOptions.getDefaultInstance().getService()
                      val config = QueryJobConfiguration.newBuilder(temp_qry).build()
										  val job = bigquery.create(JobInfo.of(config))
										if (job.getStatus().getError() != null) 
								     	{  println("Job create view failed ..."+ job.getStatus().getError())
							  throw new RuntimeException(String.format("Job %s ended with error %s", job.getJobId(), 
                   job.getStatus().getError().getMessage()))    }
	              	    	        else println("view executed ")
						
                  		}
    								catch {
    									//Handle errors for BQ
    								case e: BigQueryException =>
    								{ log.info("******************in the catch of Bigquery Dimension Load ******************"); e.printStackTrace(); }
    								case e: Exception =>
    								{ log.info("******************in the catch of BigQuery Dimension Load ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace", e); }
    								print("BigQuery Failed")
    								}

								
										val final_df = spark.read.parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
										final_df.repartition(15).write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.location").trim())
										//final_df.show()
										spark.sql("REFRESH TABLE " + pond_table)
										log.info("REFRESH TABLE " + pond_table)
										val finalLocation = spark.sparkContext.getConf.get("spark.target.location").trim()
										val BQPermExtnTbl = spark.sparkContext.getConf.get("spark.bq.dataset").trim() + "." + spark.sparkContext.getConf.get("spark.bq.permtblname").trim()
									  log.info(s"Alter View $BQViewName to point $BQPermExtnTbl ")
										val perm_qry=s"CREATE OR REPLACE VIEW $BQViewName AS SELECT * FROM $BQPermExtnTbl"
									 try {
										
										  val bigquery = BigQueryOptions.getDefaultInstance().getService()
										  val config = QueryJobConfiguration.newBuilder(perm_qry).build()
										  val job = bigquery.create(JobInfo.of(config))
													if (job.getStatus().getError() != null) 
								     	{  println("Job create view failed ..."+ job.getStatus().getError())
							  throw new RuntimeException(String.format("Job %s ended with error %s", job.getJobId(), 
                   job.getStatus().getError().getMessage()))    }
	              	    	        else println("view executed ")
                  		}
    								catch {
    									//Handle errors for BQ
    								case e: BigQueryException =>
    								{ log.info("******************in the catch of Bigquery Dimension Load ******************"); e.printStackTrace(); }
    								case e: Exception =>
    								{ log.info("******************in the catch of BigQuery Dimension Load ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace", e); }
    								print("BigQuery Failed")
    								}

								//final_df.write.mode("Overwrite").insertInto(pond_table)
								log.info(s"XXXXXXXXXXXXXXXXXXXXXXXX--------------------$pond_table loaded in Spark------------------XXXXXXXXXXXXXXXXXXXXXXXXXXX")

								}
																else
																{
																	println(s"""#------------------------------------------No new records received-----------------------------------------#""")
																}
								//spark.read.parquet(spark.sparkContext.getConf.get("spark.target.location").trim()).printSchema()

							} // end of shore 

							else { log.info("Invalid Framework Environment") }
						}


					}

	}  // end of loadDimFact 

}
