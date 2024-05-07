package com.virginvoyages.shore.facts

import java.util.Date
import com.virginvoyages.metadataframework.ManageMetadata

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
//import com.mart.dim.ChangeDataCapture.slowlyChangingDimension
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


object SailorServiceFactLoad {
  
  
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
    //spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    //import org.apache.spark.sql.functions._
    import sqlContext.implicits._
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
    try {
      //val whereClause = s""" where 1=1 """
        val whereClause = s""" where batchtime>= '$batch_start_tme' and batchtime<='$batch_end_tme' and part_date>='$part_read_start' and part_date<='$part_read_end'"""
		println(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
		log.info(s"""#---------------------------Starting the Execution--for $whereClause ----------------#""")
      
		  var srctable  = spark.sql("select * from vv_db.hvtb_parse_seaware_sailorattribution "+ whereClause)
		  srctable.createOrReplaceTempView("srctable")
		  
      val typeValue = spark.sql(""" Select ( row_number() over(partition by rowid,voyageid,paramtype order by paramname)) as rnum, rowid,names,paramtype,paramname, paramvalue,voyageid from srctable where lower(paramtype) not in ("operator","operatory","timestamp","res_id") """)
      
//println(typeValue.count())
//typeValue.show(100,false)

      typeValue.createOrReplaceTempView("typeVal")

      val typeValRec1 = spark.sql(""" Select rowid,names,paramtype,paramvalue,voyageid from typeVal where rnum =1 """)
      //println(typeValRec1.count())
      //typeValRec1.show(100,false)
      typeValRec1.createOrReplaceTempView("typeValRec1Tbl")

      val typeValRec2 = spark.sql(""" Select rowid,names,paramtype,paramvalue,voyageid from typeVal where rnum =2 """)
       // println(typeValRec2.count())
      //typeValRec2.show(100,false)
      typeValRec2.createOrReplaceTempView("typeValRec2Tbl")

      val operatorVal = spark.sql(""" Select rowid,names,paramtype,paramvalue,voyageid from srctable where lower(paramtype) =  "operator" or lower(paramtype) =  "operatory" """)
       //println(operatorVal.count())
      operatorVal.createOrReplaceTempView("operatorValTbl")
      //operatorVal.show(100,false)

      val timeStampVal = spark.sql(""" Select rowid,names,paramtype,paramvalue,voyageid from srctable where lower(paramtype) =  "timestamp" """)
      timeStampVal.createOrReplaceTempView("timeStampValTbl")
       //println(timeStampVal.count())
      //timeStampVal.show(100,false)
      
      val residVal = spark.sql(""" Select rowid,names,paramtype,paramvalue,voyageid from srctable where lower(paramtype) =  "res_id" """)
      residVal.createOrReplaceTempView("residVal")
      //println(residVal.count())
      //residVal.show(100,false)
      
   
     val invtotal1 = spark.sql(""" Select rowid,names,paramtype,paramvalue,voyageid,paramname from srctable where lower(paramtype) =  "invoice_total" and paramname =  "Old Invoice Total" """)
	  
      invtotal1.createOrReplaceTempView("invtotal1")
	  
	  val invtotal2 = spark.sql(""" Select rowid,names,paramtype,paramvalue,voyageid,paramname from srctable where lower(paramtype) =  "invoice_total" and paramname =  "New Invoice Total" """)
      invtotal2.createOrReplaceTempView("invtotal2")
	  
   
     /*val chgHistory = spark.sql(""" Select rec1.rowid change_id, rec1.names as attrib_name, rec1.paramtype, 
  rec1.paramvalue as new_value, rec1.voyageid, rec2.paramvalue as old_value, 
  opr.paramvalue as changed_by, cast(from_unixtime(unix_timestamp(tme.paramvalue, "MM/dd/yyyy hh:mm:ss a")) as timestamp) as changed_timestamp
   ,res.paramvalue as src_res_id,inv1.paramvalue as old_invoice_total,inv2.paramvalue as new_invoice_total  from  typeValRec1Tbl rec1 left join typeValRec2Tbl rec2 on
    rec1.rowid = rec2.rowid and rec1.paramtype = rec2.paramtype and 
    rec1.voyageid = rec2.voyageid and rec1.names = rec2.names left join operatorValTbl 
    opr on rec1.rowid = opr.rowid and rec1.names = opr.names and 
    rec1.voyageid = opr.voyageid left join timeStampValTbl tme on rec1.rowid = tme.rowid 
    and rec1.names = tme.names and rec1.voyageid = tme.voyageid left join residVal res on rec1.rowid = res.rowid and rec1.names = res.names and rec1.voyageid = res.voyageid left join invtotal1 inv1 on rec1.rowid = inv1.rowid and rec1.names = inv1.names and rec1.voyageid = inv1.voyageid
left join invtotal2 inv2 on rec1.rowid = inv2.rowid and rec1.names = inv2.names and rec1.voyageid = inv2.voyageid """)*/
      
      var chgHistory = spark.sql(""" Select distinct src.rowid change_id, src.names as attrib_name, src.paramtype, 
        rec1.paramvalue as new_value, src.voyageid, rec2.paramvalue as old_value, 
        opr.paramvalue as changed_by, cast(from_unixtime(unix_timestamp(tme.paramvalue, "MM/dd/yyyy hh:mm:ss a")) as timestamp) as changed_timestamp
         ,res.paramvalue as src_res_id,inv1.paramvalue as old_invoice_total,inv2.paramvalue as new_invoice_total  from srctable src left join typeValRec1Tbl rec1 on src.rowid=rec1.rowid and src.paramtype = rec1.paramtype and 
          src.voyageid = rec1.voyageid and src.names = rec1.names left join typeValRec2Tbl rec2 on
          src.rowid = rec2.rowid and src.paramtype = rec2.paramtype and 
          src.voyageid = rec2.voyageid and src.names = rec2.names left join operatorValTbl 
          opr on src.rowid = opr.rowid and src.names = opr.names and 
          src.voyageid = opr.voyageid left join timeStampValTbl tme on src.rowid = tme.rowid 
          and src.names = tme.names and src.voyageid = tme.voyageid left join residVal res on src.rowid = res.rowid and src.names = res.names and src.voyageid = res.voyageid left join invtotal1 inv1 on src.rowid = inv1.rowid and src.names = inv1.names and src.voyageid = inv1.voyageid
      left join invtotal2 inv2 on src.rowid = inv2.rowid and src.names = inv2.names and src.voyageid = inv2.voyageid where src.names !='ATTRIBUTION_TRANSACTION' """)

      
      val chgHistory_increased = spark.sql(""" select * from (Select distinct src.rowid change_id, src.names as attrib_name, src.paramtype, 
        rec1.paramvalue as new_value, src.voyageid, rec2.paramvalue as old_value, 
        opr.paramvalue as changed_by, cast(from_unixtime(unix_timestamp(tme.paramvalue, "MM/dd/yyyy hh:mm:ss a")) as timestamp) as changed_timestamp
         ,res.paramvalue as src_res_id,inv1.paramvalue as old_invoice_total,inv2.paramvalue as new_invoice_total ,row_number() over(partition by src.rowid,tme.paramvalue order by cast(from_unixtime(unix_timestamp(tme.paramvalue, "MM/dd/yyyy hh:mm:ss a")) as timestamp)) as rk   from srctable src left join typeValRec1Tbl rec1 on src.rowid=rec1.rowid and src.paramtype = rec1.paramtype and 
          src.voyageid = rec1.voyageid and src.names = rec1.names left join typeValRec2Tbl rec2 on
          src.rowid = rec2.rowid and src.paramtype = rec2.paramtype and 
          src.voyageid = rec2.voyageid and src.names = rec2.names left join operatorValTbl 
          opr on src.rowid = opr.rowid and src.names = opr.names and 
          src.voyageid = opr.voyageid left join timeStampValTbl tme on src.rowid = tme.rowid 
          and src.names = tme.names and src.voyageid = tme.voyageid left join residVal res on src.rowid = res.rowid and src.names = res.names and src.voyageid = res.voyageid left join invtotal1 inv1 on src.rowid = inv1.rowid and src.names = inv1.names and src.voyageid = inv1.voyageid
      left join invtotal2 inv2 on src.rowid = inv2.rowid and src.names = inv2.names and src.voyageid = inv2.voyageid where src.names in ('ATTRIBUTION_INVOICEDECREASED','ATTRIBUTION_INVOICEINCREASED')) where rk > 1  """)

      chgHistory.join(chgHistory_increased,chgHistory("change_id") === chgHistory_increased("change_id") and chgHistory("attrib_name") === chgHistory_increased("attrib_name"),"leftanti")
        
      /* spark.sql(""" Select distinct src.rowid change_id, src.names as attrib_name, src.paramtype, 
        rec1.paramvalue as new_value, src.voyageid, rec2.paramvalue as old_value, 
        opr.paramvalue as changed_by, cast(from_unixtime(unix_timestamp(tme.paramvalue, "MM/dd/yyyy hh:mm:ss a")) as timestamp) as changed_timestamp
         ,res.paramvalue as src_res_id,inv1.paramvalue as old_invoice_total,inv2.paramvalue as new_invoice_total  from srctable src left join typeValRec1Tbl rec1 on src.rowid=rec1.rowid and src.paramtype = rec1.paramtype and 
          src.voyageid = rec1.voyageid and src.names = rec1.names left join typeValRec2Tbl rec2 on
          src.rowid = rec2.rowid and src.paramtype = rec2.paramtype and 
          src.voyageid = rec2.voyageid and src.names = rec2.names left join operatorValTbl 
          opr on src.rowid = opr.rowid and src.names = opr.names and 
          src.voyageid = opr.voyageid left join timeStampValTbl tme on src.rowid = tme.rowid 
          and src.names = tme.names and src.voyageid = tme.voyageid left join residVal res on src.rowid = res.rowid and src.names = res.names and src.voyageid = res.voyageid left join invtotal1 inv1 on src.rowid = inv1.rowid and src.names = inv1.names and src.voyageid = inv1.voyageid
      left join invtotal2 inv2 on src.rowid = inv2.rowid and src.names = inv2.names and src.voyageid = inv2.voyageid where src.names ='ATTRIBUTION_INVOICEDECREASED' """).createOrReplaceTempView("chgHistory_invoicedecreased")

     spark.sql("""Select if(a.change_id is null,b.change_id,a.change_id) as change_id ,if(a.attrib_name is null,b.attrib_name,a.attrib_name) as attrib_name,if(a.paramtype is null,b.paramtype,a.paramtype) as paramtype,if(a.new_value is null,b.new_value,a.new_value) as new_value,if(a.voyageid is null,b.voyageid,a.voyageid) as voyageid,if(a.old_value is null,b.old_value,a.old_value) as old_value ,if(a.changed_by is null,b.changed_by,a.changed_by) as changed_by,if(a.changed_timestamp is null,b.changed_timestamp,a.changed_timestamp) as changed_timestamp,if(a.src_res_id is null,b.src_res_id,a.src_res_id) as src_res_id,if(a.old_invoice_total is null,b.old_invoice_total,a.old_invoice_total) as old_invoice_total, if(a.new_invoice_total is null,b.new_invoice_total,a.new_invoice_total) as new_invoice_total  from chgHistory_invoiceincreased a outer join chgHistory_invoicedecreased b on a.changed_timestamp = b.changed_timestamp where a.changed_timestamp!=b.changed_timestamp """)*/
      
      
val chgHistoryFinal =chgHistory.select("change_id","src_res_id","attrib_name","paramtype","old_value","new_value","changed_by","changed_timestamp","old_invoice_total","new_invoice_total").sort($"change_id".desc)//,(unix_timestamp($"changed_timestamp", "MM/dd/yyyy HH:mm:ss") * 1000).cast("changed_timestamp"),$"changed_timestamp")
        
         
         chgHistoryFinal.show(1,false)
         
         //println(chgHistoryFinal.printSchema)

       if (!chgHistoryFinal.head(1).isEmpty) {
      loadDimFact(spark: SparkSession, chgHistoryFinal)
        // chgHistoryFinal.repartition(15).write.mode("Overwrite").parquet("gs://test_jars/BRD_test/Sarang/test_data")
         }
	   ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark);
	  

    } catch {

      case e: SQLException => {
        ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
        log.info("******************in the catch of SailorServiceFactLoad******************");
        e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace", e);
      }

      case e: Exception =>
        {
          ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
          log.info("******************in the catch of SailorServiceFactLoad ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace", e);
        }
        println("#----------------------------Process Has Failed---------------------------#")
        System.exit(1)
      //exit(1);
    }

  }
  
}