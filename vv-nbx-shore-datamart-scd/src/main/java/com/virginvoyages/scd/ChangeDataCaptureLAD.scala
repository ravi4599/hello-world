package com.virginvoyages.scd
import java.util.Date
import java.sql._;
import java.sql.SQLException;
import org.apache.spark.sql.functions.hash
import java.util.Properties
import org.apache.spark.sql.SaveMode
import java.sql.Timestamp

import org.apache.spark.SparkContext

import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType, DateType, LongType };

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

import org.apache.spark.sql.{ Column, DataFrame }

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.functions._
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryException
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.Job
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.QueryJobConfiguration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import java.util.Calendar


object ChangeDataCaptureLAD {

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

    //    try{
    //    val checkDF=src_data.filter(filterExpr)
    //    }
    //    catch{
    //       case e: NullPointerException =>{checkNullPrimarySkeyFlag=1;}
    //    }
    //    //checkDF.show(false)
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

  def factLoadLAD(spark: SparkSession, src_data: DataFrame): Unit = {

     println("entered in factLoadLAD ......+++")
    val log = LogManager.getRootLogger
    log.info("#--------------------------------------------In THE LAD FACT----------------------------------------------#")
    println("#--------------------------- Late Arriving Dimensions------------------#")
    log.setLevel(Level.INFO)
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    var debug_flag = "False";
    try {
      debug_flag = spark.sparkContext.getConf.get("spark.debug.flag").trim()
    } catch {
      case e: NoSuchElementException => { debug_flag = "False"; log.info("#--------------------No Debug Flag No debug  -------------------#") }
    }

    import spark.sqlContext.implicits._
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    val frameworkEnv = spark.sparkContext.getConf.get("spark.frameworkEnv").trim()
    val curr_date = current_date()
    val today = ZonedDateTime.now(ZoneId.of("UTC"))
    val dateformatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val curr_date_str = dateformatter format today

    /**
     * Determining the type of Fact Job - Trans and Accu
     * 	 By Default if we dont specify spark.factType ; it is Trans
     */

    var fact_type = "TRANS";
    try {
      fact_type = spark.sparkContext.getConf.get("spark.factType").trim()
    } catch {
      case e: NoSuchElementException => { fact_type = "TRANS"; log.info("#--------------------No Fact Type Found Defaulting to Transactional Fact Type Add property (spark.FactType 'Accu/Trans') for changing -------------------#") }

    }
    var ladremovalFlag = "false";
    try {
      ladremovalFlag = spark.sparkContext.getConf.get("spark.ladremoval.flag").trim()
    } catch {
      case e: NoSuchElementException => { ladremovalFlag = "false"; log.info("#--------------------No LAD FLAG Found Defaulting to false-------------------#") }
    }

    var lad_table = spark.sparkContext.getConf.get("spark.lad.srctable").trim() + "_lad"
    try {
      lad_table = spark.sparkContext.getConf.get("spark.lad.tgttable").trim()
    } catch {
      case e: NoSuchElementException => { lad_table = spark.sparkContext.getConf.get("spark.lad.srctable").trim() + "_lad"; log.info("#--------------------No LAD FLAG Found Defaulting to false-------------------#") }
    }

    /**
     * Shore LAD JOB
     *   Trans Fact
     */

    if (frameworkEnv.trim().toUpperCase().equals("SHORE")) {
      log.info("SHORE Environment")
      if (fact_type.trim().toUpperCase().equals("TRANS")) {
        log.info("SHORE Environment Trans ")
        val insertFactDF = src_data
          .withColumn("upd_dt", current_timestamp())
          .withColumn("load_dt", current_timestamp())
          .withColumn("part_dt", curr_date)

        insertFactDF.createOrReplaceTempView("final_view")
        val final_cols = spark.sparkContext.getConf.get("spark.target.final_collist")

        val LADPKCols = spark.sparkContext.getConf.get("spark.lad.columns")
        val LADColsPKList = LADPKCols.split(",")
        val LADPKColSEQ = LADColsPKList.map(x => col(x)).toSeq

        // Changes Introduced in PI 11

        val LADfilterExprFailed = src_data.select(LADPKColSEQ: _*).schema.fields.map(f => (if (f.dataType.toString() == "StringType") f.name.toString() + " is null" else if (f.dataType.toString() == "IntegerType") f.name.toString() + "=-1" else if (f.dataType.toString() == "LongType") f.name.toString() + "=-1" else if (f.dataType.toString() == "TimestampType") f.name.toString() + " is null" else if (f.dataType.toString() == "BooleanType") f.name.toString() + " is null" else null)).mkString(" or ")
        log.info ("LADfilterExprFailed ..."+LADfilterExprFailed)
        val LADfilterExprPassed = src_data.select(LADPKColSEQ: _*).schema.fields.map(f => (if (f.dataType.toString() == "StringType") f.name.toString() + " is not null" else if (f.dataType.toString() == "IntegerType") f.name.toString() + "!=-1" else if (f.dataType.toString() == "LongType") f.name.toString() + "!=-1" else if (f.dataType.toString() == "TimestampType") f.name.toString() + " is not null" else if (f.dataType.toString() == "BooleanType") f.name.toString() + " is not null" else null)).mkString(" and ")
        log.info ("LADfilterExprPassed ..."+LADfilterExprPassed)
        val primaryValuesFailed = insertFactDF.filter(LADfilterExprFailed)
		log.info ("primaryValuesFailed ..."+primaryValuesFailed)
        primaryValuesFailed.createOrReplaceTempView("primaryValuesFailedVw")
        val finalColSQL = "Select " + final_cols + " from final_view"
		log.info ("finalColSQL ... "+finalColSQL)
        val finalDF = spark.sql(finalColSQL).filter(LADfilterExprPassed)
     
        if (debug_flag.trim().toUpperCase().equals("TRUE")) {
          println("#---------------------------------------finalDF trans shore-----------------#")
          finalDF.show(2, false)
          finalDF.printSchema
          println("==============finalDF count after filtering =================" + finalDF.count)
        }


        val pond_table = spark.sparkContext.getConf.get("spark.pond.database").trim() + "." + spark.sparkContext.getConf.get("spark.pond.table").trim()

        finalDF.repartition(15).write.mode("Append").insertInto(pond_table)
        spark.sql("REFRESH TABLE " + pond_table)
        log.info("REFRESH TABLE " + pond_table)
  
        val LADfailedDF = src_data.filter(LADfilterExprFailed)
		println(" LADfailedDF .........")
		LADfailedDF.show(2)

        println("#--------------------------- Late Arriving Dimensions------------------#")
        if (LADfailedDF.head(1).isEmpty == false) {
          println("#---------------------------There are records with possible Late Arriving Dimensions------------------#")
          log.info("#---------------------------There are records with possible Late Arriving Dimensions------------------#")
  
          // Changes Introduced in PI 11.6
          val srcPKAllCol = spark.sparkContext.getConf.get("spark.lad.srctablekey")
          val srcPKAllColSeq = srcPKAllCol.split(",").map(x => col(x)).toSeq
          val srctableDF = spark.sql("select * from " + spark.sparkContext.getConf.get("spark.lad.srctable"))
		  log.info(" srctableDF....."+srctableDF)
		  
          val srcPKtableDF = srctableDF.withColumn("src_primaryhash", md5(concat_ws(",", srctableDF.select(srcPKAllColSeq: _*).columns.map(x => col(x)): _*)))
            .as("srcLAD")
          println("srcPKtableDF .....")			
          srcPKtableDF.show(2)
          val LADFailedPK = LADfailedDF.withColumn("lad_primaryhash", md5(concat_ws(",", LADfailedDF.select(srcPKAllColSeq: _*).columns.map(x => col(x)): _*))).as("failedLAD")
			println("LADFailedPK ....." + LADFailedPK)			
       
          /**
           * Inner jOin between failed Lad dataframe and src parse table and
           * Selecting all records from parse driving table
           * In Lad Removal Process source should be the lad source table
           */

          val tempLADDF = srcPKtableDF.join(LADFailedPK, col("src_primaryhash") === col("lad_primaryhash"), "inner")
            .drop("src_primaryhash").select($"srcLAD.*")

          // till here

          if (debug_flag.trim().toUpperCase().equals("TRUE")) {
            println("#---------------------------------------tempLADDF trans shore writing failed recods to lad table-----------------#")
            tempLADDF.show(2, false)
            tempLADDF.printSchema
            println("==============tempLADDF count (failed recods to lad table) =================" + tempLADDF.count)
          }

          tempLADDF.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim() + "_lad")
          val finalLADDF = spark.read.parquet(spark.sparkContext.getConf.get("spark.target.temp").trim() + "_lad")
          if (ladremovalFlag.trim().toUpperCase().equals("TRUE")) {
            //factLoadLAD(spark, src_data)
            finalLADDF.write.mode("Overwrite").insertInto(lad_table)
          } else {
            finalLADDF.write.mode("Append").insertInto(lad_table)
          }

        } // end of if 
        else {
          println("#---------------------------LAD Dataframe Empty.....There are no records with possible Late Arriving Dimensions------------------#")
          log.info("#---------------------------LAD Dataframe Empty.....There are no records with possible Late Arriving Dimensions------------------#")

          val table1 = sqlContext.table(lad_table).limit(0)
          table1.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim() + "_lad")
          val tempLADDFF = spark.read.parquet(spark.sparkContext.getConf.get("spark.target.temp").trim() + "_lad")
          if (ladremovalFlag.trim().toUpperCase().equals("TRUE")) {
            tempLADDFF.write.mode("Overwrite").insertInto(lad_table)
          }

        } // end of else

      } // end of shore trans fact
      /** Shore Lad Fact
       *  Accu Fact
       */ else if (fact_type.trim().toUpperCase().equals("ACCU")) {
        println("#---------------------------------- Shore Accumulative Fact -------------------------#")
        log.info("#---------------------------------- Shore Accumulative Fact -------------------------#")
        import spark.sqlContext.implicits._
        println("X---------------------------Executing on shore----------------------------X")
        //var sql_connection: Connection = null
        val insertUpdateFlagDF = generateFactDFwithInsertUpdateFlag(spark, src_data)
        val final_cols = spark.sparkContext.getConf.get("spark.target.final_collist")
        val srcPKCols = spark.sparkContext.getConf.get("spark.source.primaryKeyColumns")
        val srcColsPKList = srcPKCols.split(",")

        val srcPKColSEQ = srcColsPKList.map(x => col(x)).toSeq
        val LADPKCols = spark.sparkContext.getConf.get("spark.lad.columns")
        val LADColsPKList = LADPKCols.split(",")
        val LADPKColSEQ = LADColsPKList.map(x => col(x)).toSeq

        val LADfilterExprFailed = src_data.select(LADPKColSEQ: _*).schema.fields.map(f => (if (f.dataType.toString() == "StringType") f.name.toString() + " is null" else if (f.dataType.toString() == "IntegerType") f.name.toString() + "=-1" else if (f.dataType.toString() == "LongType") f.name.toString() + "=-1" else if (f.dataType.toString() == "TimestampType") f.name.toString() + " is null" else if (f.dataType.toString() == "BooleanType") f.name.toString() + " is null" else null)).mkString(" or ")
			log.info("LADfilterExprFailed ......."+ LADfilterExprFailed)

        val LADfilterExprPassed = src_data.select(LADPKColSEQ: _*).schema.fields.map(f => (if (f.dataType.toString() == "StringType") f.name.toString() + " is not null" else if (f.dataType.toString() == "IntegerType") f.name.toString() + "!=-1" else if (f.dataType.toString() == "LongType") f.name.toString() + "!=-1" else if (f.dataType.toString() == "TimestampType") f.name.toString() + " is not null" else if (f.dataType.toString() == "BooleanType") f.name.toString() + " is not null" else null)).mkString(" and ")
			log.info("LADfilterExprPassed ......."+ LADfilterExprPassed)


        val LADfailedDF = src_data.filter(LADfilterExprFailed).as("failedLAD")
		println("LADfailedDF ....goes to Lad table")
		LADfailedDF.show(false)
		
        val primaryValuesFailed = src_data.filter(LADfilterExprFailed).dropDuplicates()
		//.selectExpr(spark.sparkContext.getConf.get("spark.lad.srctablekey"))
        println("primaryValuesFailed ....")
		primaryValuesFailed.show(5)
		
        primaryValuesFailed.createOrReplaceTempView("primaryValuesFailedVw")

        println("#------------------------Filter Expression In Main-----------------------#")

        val insertUpdateFlaggedDF = insertUpdateFlagDF
          .withColumn("upd_dt", current_timestamp())
          .withColumn("load_dt", current_timestamp())
          .withColumn("part_dt", curr_date)
      println("#------------------------insertUpdateFlaggedDF values-----------------------#")
        insertUpdateFlaggedDF.show(12, false)
      
        var sql_connection: Connection = null

        println("Before Delete DF")

        log.setLevel(Level.INFO)

        //log.info("target table@@@@@@@@@@@@@@@" + target_table)
        //println("target table@@@@@@@@@@@@@@@" + target_table)

        log.info("XXXXXXXXXXXXXXXXXXXXXXXX------------------AUDIT COLUMN APPENDING COMPLETE--------------------XXXXXXXXXXXXXXXXXXXXXXXXXXX")

        println("#------------------------------------------------FINAL DATAFRAME-----------------------------------------------#")

        val final_cols_list = final_cols.split(",")
        val final_col_seq = final_cols_list.map(x => col(x)).toSeq

        println("X---------------------------SHORE SIDE ENTIRE DF----------------------------X")

        println("X---------------------------SHORE SIDE DF----------------------------X")

        val pond_table = spark.sparkContext.getConf.get("spark.pond.database").trim() + "." + spark.sparkContext.getConf.get("spark.pond.table").trim()

        println("XXXXXXXXXXXXXXXXXXXXXXXX------------------AUDIT COLUMN APPENDING COMPLETE--------------------XXXXXXXXXXXXXXXXXXXXXXXXXXX")
        log.info("XXXXXXXXXXXXXXXXXXXXXXXX------------------AUDIT COLUMN APPENDING COMPLETE--------------------XXXXXXXXXXXXXXXXXXXXXXXXXXX")

        log.info("sorce table@@@@@@@@@@@@@@@" + pond_table)

        insertUpdateFlaggedDF.createOrReplaceTempView("final_view")
        val pondSelectSQL = "select pond.* from " + pond_table + " pond inner join final_view fv on ((insert_update_flag='NR' or insert_update_flag='NC') and pond.primaryhash == fv.pond_primaryhash )"
		log.info("pondSelectSQL -------"+pondSelectSQL)
        val pondDataNCNRDF = spark.sql(pondSelectSQL)
        print(s"#------------------------------Test Dataframe $pondSelectSQL ----------------------#")

        log.info("sorce table@@@@@@@@@@@@@@@" + pond_table)

        val final_df_sql = "Select " + final_cols + " from final_view where insert_update_flag='I' or insert_update_flag='U'"

        val finalIUDF = spark.sql(final_df_sql).filter(LADfilterExprPassed)

        val dfs = Seq(finalIUDF, pondDataNCNRDF)
        val final_temp_df = dfs.reduce(_ union _)

        if (debug_flag.trim().toUpperCase().equals("TRUE")) {
          println("#---------------------------------------final_temp_df  writing to fact table-----------------#")
          final_temp_df.show(2, false)
          final_temp_df.printSchema
          println("==============final_temp_df count  =================" + final_temp_df.count)
        }

        println("X--------------------------Final Insert Update DF----------------X")
        println("#------------------------------------------------FINAL DATAFRAME-----------------------------------------------#")

        final_temp_df.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim())
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
        println("#---------------------------------------------Data Integity Checking---------------------#")

        final_df.createOrReplaceTempView("final_spark_check")

        val intChecksparkDF = spark.sql("select count(*), " + srcPKCols + " from final_spark_check group by " + srcPKCols + " having count(*)>1 ")
        println("select count(*), " + srcPKCols + " from final_spark_check group by " + srcPKCols + " having count(*)>1 ")
        if (intChecksparkDF.head(1).isEmpty == false) {
          println("#------------------------------------intChecksparkDF---------Data Integity Checked Failed Duplicates introduced---------------------#")
          log.info("#----------------------------------intChecksparkDF----------Data Integity Checked Failed Duplicates introduced ---------------------#")
          throw new Exception("Data Integity Checked Failed Duplicates introduced");
        }

        println("#---------------------------------------------Data Integity Checked Passed---------------------#")
        //finalPassedDF.repartition(15).write.mode("Append").insertInto(pond_table)
        final_df.write.mode("Overwrite").insertInto(pond_table)
        //final_df.filter(filterExprFailed).write.mode("Overwrite").insertInto(error_table)
        spark.sql("REFRESH TABLE " + pond_table)
        println("REFRESH TABLE " + pond_table)
					val finalLocation = spark.sparkContext.getConf.get("spark.target.location").trim()
					val finalErrorLocation = spark.sparkContext.getConf.get("spark.target.location").trim() + "_lad"
					val BQPermExtnTbl = spark.sparkContext.getConf.get("spark.bq.dataset").trim() + "." + spark.sparkContext.getConf.get("spark.bq.permtblname").trim()
					val BQErrorTableName = spark.sparkContext.getConf.get("spark.bq.dataset").trim() + "." + spark.sparkContext.getConf.get("spark.bq.permtblname").trim() + "_lad"
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
        val lad_table = spark.sparkContext.getConf.get("spark.lad.srctable").trim() + "_lad"
        println("#--------------------------- Late Arriving Dimensions------------------#")
        if (LADfailedDF.head(1).isEmpty == false) {
          println("#---------------------------There are records with possible Late Arriving Dimensions------------------#")
          log.info("#---------------------------There are records with possible Late Arriving Dimensions------------------#")
      
          /**
           * Joining the primary_hash of src parse table with the primary_hash of LADfailedDF
           *  To fetch the failed lad records
           */

          val srcPKAllCol = spark.sparkContext.getConf.get("spark.lad.srctablekey")
          val srcPKAllColSeq = srcPKAllCol.split(",").map(x => col(x)).toSeq
          val srctableDF = spark.sql("select * from " + spark.sparkContext.getConf.get("spark.lad.srctable"))
          val srcPKtableDF = srctableDF.withColumn("src_primaryhash", md5(concat_ws(",", srctableDF.select(srcPKAllColSeq: _*).columns.map(x => col(x)): _*)))
            .as("srcLAD")

          val LADFailedPK = LADfailedDF.withColumn("lad_primaryhash", md5(concat_ws(",", LADfailedDF.select(srcPKAllColSeq: _*).columns.map(x => col(x)): _*)))

          /**
           * Inner jOin between failed Lad dataframe and src parse table and
           * Selecting all records from parse driving table
           * In Lad Removal Process source should be the lad source table
           */

          val tempLADDF = srcPKtableDF.join(LADFailedPK, col("src_primaryhash") === col("lad_primaryhash"), "inner")
            .drop("src_primaryhash").select($"srcLAD.*")

          tempLADDF.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim() + "_lad")
          val finalLADDF = spark.read.parquet(spark.sparkContext.getConf.get("spark.target.temp").trim() + "_lad")
          if (ladremovalFlag.trim().toUpperCase().equals("TRUE")) {

            finalLADDF.write.mode("Overwrite").insertInto(lad_table)
          } else {
            finalLADDF.write.mode("Append").insertInto(lad_table)
          }
        } //end of if
        else {
          println("#---------------------------LAD Dataframe Empty.....There are no records with possible Late Arriving Dimensions------------------#")
          log.info("#---------------------------LAD Dataframe Empty.....There are no records with possible Late Arriving Dimensions------------------#")

          val table1 = sqlContext.table(lad_table).limit(0)
          table1.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim() + "_lad")
          val tempLADDFF = spark.read.parquet(spark.sparkContext.getConf.get("spark.target.temp").trim() + "_lad")
          if (ladremovalFlag.trim().toUpperCase().equals("TRUE")) {
            tempLADDFF.write.mode("Overwrite").insertInto(lad_table)
          }

        } // end of else

       }
	  else {

        println("Invalid Fact Type")
        log.info("Invalid Fact Type")
        sys.exit(1)

	  }
    } /**   SHip Env
     *    LAD Fact 
     */ else if (frameworkEnv.trim().toUpperCase().equals("SHIP")) {
      println("SHIP Environment")
      if (fact_type.trim().toUpperCase().equals("TRANS")) {
        val insertFactDF = src_data
          .withColumn("upd_dt", current_timestamp())
          .withColumn("load_dt", current_timestamp())
          .withColumn("part_dt", curr_date)

        insertFactDF.createOrReplaceTempView("final_view")

        val final_cols = spark.sparkContext.getConf.get("spark.target.final_collist")
        val finalColSQL = "Select " + final_cols + " from final_view"
        val finalDF = spark.sql(finalColSQL)
        if (debug_flag.trim().toUpperCase().equals("TRUE")) {
          println("#---------------------------------------finalDF Trans ship-----------------#")
          finalDF.show(2, false)
          finalDF.printSchema
          println("==============finalDF count Trans ship =================" + finalDF.count)
        }

        val pond_table = spark.sparkContext.getConf.get("spark.pond.database").trim() + "." + spark.sparkContext.getConf.get("spark.pond.table").trim()
        val LADPKCols = spark.sparkContext.getConf.get("spark.lad.columns")
        val LADColsPKList = LADPKCols.split(",")

        val LADPKColSEQ = LADColsPKList.map(x => col(x)).toSeq

        //        val LADfilterExprFailed = finalDF.select(LADPKColSEQ: _*).schema.fields
        //          .map(f => f.name.toString() + "=" + (if (f.dataType.toString() == "IntegerType") -1 else if (f.dataType.toString() == "LongType") -1 else null)).mkString(" or ")
        //        val LADfilterExprPassed = finalDF.select(LADPKColSEQ: _*).schema.fields
        //          .map(f => f.name.toString() + "!=" + (if (f.dataType.toString() == "IntegerType") -1 else if (f.dataType.toString() == "LongType") -1 else null)).mkString(" and ")

        // Changes Introduced in PI 11

        val LADfilterExprFailed = src_data.select(LADPKColSEQ: _*).schema.fields.map(f => (if (f.dataType.toString() == "StringType") f.name.toString() + " is null" else if (f.dataType.toString() == "IntegerType") f.name.toString() + "=-1" else if (f.dataType.toString() == "LongType") f.name.toString() + "=-1" else if (f.dataType.toString() == "TimestampType") f.name.toString() + " is null" else if (f.dataType.toString() == "BooleanType") f.name.toString() + " is null" else null)).mkString(" or ")

        val LADfilterExprPassed = src_data.select(LADPKColSEQ: _*).schema.fields.map(f => (if (f.dataType.toString() == "StringType") f.name.toString() + " is not null" else if (f.dataType.toString() == "IntegerType") f.name.toString() + "!=-1" else if (f.dataType.toString() == "LongType") f.name.toString() + "!=-1" else if (f.dataType.toString() == "TimestampType") f.name.toString() + " is not null" else if (f.dataType.toString() == "BooleanType") f.name.toString() + " is not null" else null)).mkString(" and ")
        finalDF.filter(LADfilterExprPassed).repartition(15).write.mode("Append").insertInto(pond_table)
        spark.sql("REFRESH TABLE " + pond_table)
        println("REFRESH TABLE " + pond_table)
        val LADfailedDF = src_data.filter(LADfilterExprFailed)
        println("#--------------------------- Late Arriving Dimensions------------------#")

        if (LADfailedDF.head(1).isEmpty == false) {
          println("#---------------------------There are records with possible Late Arriving Dimensions------------------#")
          log.info("#---------------------------There are records with possible Late Arriving Dimensions------------------#")
        
          //In Lad Removal Process source should be the lad source table

          // Changes Introduced in PI 11.6
          val srcPKAllCol = spark.sparkContext.getConf.get("spark.lad.srctablekey")
          val srcPKAllColSeq = srcPKAllCol.split(",").map(x => col(x)).toSeq
          val srctableDF = spark.sql("select * from " + spark.sparkContext.getConf.get("spark.lad.srctable"))
          val srcPKtableDF = srctableDF.withColumn("src_primaryhash", md5(concat_ws(",", srctableDF.select(srcPKAllColSeq: _*).columns.map(x => col(x)): _*)))
            .as("srcLAD")

          val LADFailedPK = LADfailedDF.withColumn("lad_primaryhash", md5(concat_ws(",", LADfailedDF.select(srcPKAllColSeq: _*).columns.map(x => col(x)): _*))).as("failedLAD")

          /**
           * Inner jOin between failed Lad dataframe and src parse table and
           * Selecting all records from parse driving table
           * In Lad Removal Process source should be the lad source table
           */

          val tempLADDF = srcPKtableDF.join(LADFailedPK, col("src_primaryhash") === col("lad_primaryhash"), "inner")
            .drop("src_primaryhash").select($"srcLAD.*")

          // till here

          if (debug_flag.trim().toUpperCase().equals("TRUE")) {
            println("#---------------------------------------tempLADDF trans shore writing failed recods to lad table-----------------#")
            tempLADDF.show(2, false)
            tempLADDF.printSchema
            println("==============tempLADDF count (failed recods to lad table) =================" + tempLADDF.count)
          }

          tempLADDF.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim() + "_lad")
          val finalLADDF = spark.read.parquet(spark.sparkContext.getConf.get("spark.target.temp").trim() + "_lad")
          if (ladremovalFlag.trim().toUpperCase().equals("TRUE")) {
            finalLADDF.write.mode("Overwrite").insertInto(lad_table)
          } else {
            finalLADDF.write.mode("Append").insertInto(lad_table)
          }

        } // end of if  
        else {
          println("#---------------------------LAD Dataframe Empty.....There are no records with possible Late Arriving Dimensions------------------#")
          log.info("#---------------------------LAD Dataframe Empty.....There are no records with possible Late Arriving Dimensions------------------#")

          val table1 = sqlContext.table(lad_table).limit(0)
          table1.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim() + "_lad")
          val tempLADDFF = spark.read.parquet(spark.sparkContext.getConf.get("spark.target.temp").trim() + "_lad")
          if (ladremovalFlag.trim().toUpperCase().equals("TRUE")) {
            tempLADDFF.write.mode("Overwrite").insertInto(lad_table)
          }

        } // end of else

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
        finalDF.filter(LADfilterExprPassed).write.mode(SaveMode.Append).jdbc(url, tableName, properties)
      } /** SHIp LAD Job
       *  FOR Accu Fact
       */ else if (fact_type.trim().toUpperCase().equals("ACCU")) {
        print("#----------------------------------Accumulative Fact -------------------------#")
        log.info("#----------------------------------Accumulative Fact -------------------------#")
        import spark.sqlContext.implicits._
        println("X---------------------------Executing on ship----------------------------X")
        val insertUpdateFlagDF = generateFactDFwithInsertUpdateFlag(spark, src_data)
        val srcPKCols = spark.sparkContext.getConf.get("spark.source.primaryKeyColumns")
        val srcColsPKList = srcPKCols.split(",")

        val srcPKColSEQ = srcColsPKList.map(x => col(x)).toSeq

        val filterExprFailed = src_data.select(srcPKColSEQ: _*).schema.fields
          .map(f => f.name.toString() + "=" + (if (f.dataType.toString() == "IntegerType") -1 else if (f.dataType.toString() == "LongType") -1 else null)).mkString(" or ")
        val filterExprPassed = src_data.select(srcPKColSEQ: _*).schema.fields
          .map(f => f.name.toString() + "!=" + (if (f.dataType.toString() == "IntegerType") -1 else if (f.dataType.toString() == "LongType") -1 else null)).mkString(" and ")
        println("Filter Passed#---------------------------------" + filterExprPassed + "----------------------------------#")
        println("Filter Failed#---------------------------------" + filterExprFailed + "----------------------------------#")

        val curr_date = current_date()
        val insertUpdateFlaggedDF = insertUpdateFlagDF
          .withColumn("upd_dt", current_timestamp())
          .withColumn("load_dt", current_timestamp())
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
           rows.foreach {row =>
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
        val LADPKCols = spark.sparkContext.getConf.get("spark.lad.columns")
        val LADColsPKList = LADPKCols.split(",")
        val LADPKColSEQ = LADColsPKList.map(x => col(x)).toSeq

        //        val LADfilterExprFailed = final_df.select(LADPKColSEQ: _*).schema.fields
        //          .map(f => f.name.toString() + "=" + (if (f.dataType.toString() == "IntegerType") -1 else if (f.dataType.toString() == "LongType") -1 else null)).mkString(" or ")
        //        val LADfilterExprPassed = final_df.select(LADPKColSEQ: _*).schema.fields
        //          .map(f => f.name.toString() + "!=" + (if (f.dataType.toString() == "IntegerType") -1 else if (f.dataType.toString() == "LongType") -1 else null)).mkString(" and ")

        val LADfilterExprFailed = src_data.select(LADPKColSEQ: _*).schema.fields.map(f => (if (f.dataType.toString() == "StringType") f.name.toString() + " is null" else if (f.dataType.toString() == "IntegerType") f.name.toString() + "=-1" else if (f.dataType.toString() == "LongType") f.name.toString() + "=-1" else if (f.dataType.toString() == "TimestampType") f.name.toString() + " is null" else if (f.dataType.toString() == "BooleanType") f.name.toString() + " is null" else null)).mkString(" or ")

        val LADfilterExprPassed = src_data.select(LADPKColSEQ: _*).schema.fields.map(f => (if (f.dataType.toString() == "StringType") f.name.toString() + " is not null" else if (f.dataType.toString() == "IntegerType") f.name.toString() + "!=-1" else if (f.dataType.toString() == "LongType") f.name.toString() + "!=-1" else if (f.dataType.toString() == "TimestampType") f.name.toString() + " is not null" else if (f.dataType.toString() == "BooleanType") f.name.toString() + " is not null" else null)).mkString(" and ")
        val LADfailedDF = src_data.filter(LADfilterExprFailed).as("failedLAD")
        //val lad_table = spark.sparkContext.getConf.get("spark.lad.srctable").trim() + "_lad"
        println("#--------------------------- Late Arriving Dimensions------------------#")
        if (LADfailedDF.head(1).isEmpty == false) {

          println("#---------------------------There are records with possible Late Arriving Dimensions------------------#")
          log.info("#---------------------------There are records with possible Late Arriving Dimensions------------------#")
          //          val ladsrcprimarykey = "srcLAD." + spark.sparkContext.getConf.get("spark.lad.srctablekey")
          //          val ladfailedprimarykey = "failedLAD." + spark.sparkContext.getConf.get("spark.lad.srctablekey")
          //          val srctableDF = spark.sql("select * from " + spark.sparkContext.getConf.get("spark.lad.srctable")).as("srcLAD")
          //          val tempLADDF = srctableDF.join(LADfailedDF, col(ladsrcprimarykey) === col(ladfailedprimarykey), "inner").select($"srcLAD.*")
          //          In Lad Removal Process source should be the lad source table

          // Changes Introduced in PI 11.6
          val srcPKAllCol = spark.sparkContext.getConf.get("spark.lad.srctablekey")
          val srcPKAllColSeq = srcPKAllCol.split(",").map(x => col(x)).toSeq
          val srctableDF = spark.sql("select * from " + spark.sparkContext.getConf.get("spark.lad.srctable"))
          val srcPKtableDF = srctableDF.withColumn("src_primaryhash", md5(concat_ws(",", srctableDF.select(srcPKAllColSeq: _*).columns.map(x => col(x)): _*)))
            .as("srcLAD")

          val LADFailedPK = LADfailedDF.withColumn("lad_primaryhash", md5(concat_ws(",", LADfailedDF.select(srcPKAllColSeq: _*).columns.map(x => col(x)): _*))).as("failedLAD")

          /**
           * Inner jOin between failed Lad dataframe and src parse table and
           * Selecting all records from parse driving table
           * In Lad Removal Process source should be the lad source table
           */

          val tempLADDF = srcPKtableDF.join(LADFailedPK, col("src_primaryhash") === col("lad_primaryhash"), "inner")
            .drop("src_primaryhash").select($"srcLAD.*")

          // till here

          if (debug_flag.trim().toUpperCase().equals("TRUE")) {
            println("#---------------------------------------tempLADDF trans shore writing failed recods to lad table-----------------#")
            tempLADDF.show(2, false)
            tempLADDF.printSchema
            println("==============tempLADDF count (failed recods to lad table) =================" + tempLADDF.count)
          }

          tempLADDF.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim() + "_lad")
          val finalLADDF = spark.read.parquet(spark.sparkContext.getConf.get("spark.target.temp").trim() + "_lad")
          if (ladremovalFlag.trim().toUpperCase().equals("TRUE")) {
            finalLADDF.write.mode("Overwrite").insertInto(lad_table)
          } else {
            finalLADDF.write.mode("Append").insertInto(lad_table)
          }
        } else {
          println("#---------------------------LAD Dataframe Empty.....There are no records with possible Late Arriving Dimensions------------------#")
          log.info("#---------------------------LAD Dataframe Empty.....There are no records with possible Late Arriving Dimensions------------------#")

          val table1 = sqlContext.table(lad_table).limit(0)
          table1.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.temp").trim() + "_lad")
          val tempLADDFF = spark.read.parquet(spark.sparkContext.getConf.get("spark.target.temp").trim() + "_lad")
          if (ladremovalFlag.trim().toUpperCase().equals("TRUE")) {
            tempLADDFF.write.mode("Overwrite").insertInto(lad_table)
          }

        } // end of else

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

}
