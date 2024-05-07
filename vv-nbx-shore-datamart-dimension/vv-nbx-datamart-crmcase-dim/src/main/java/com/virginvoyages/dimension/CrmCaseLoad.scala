package com.virginvoyages.dimension
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
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact
object CrmCaseLoad {
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
//
//spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
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



import spark.sqlContext.implicits._
val whereClause = s""" where batchtime>= '$batch_start_tme' and batchtime<='$batch_end_tme' and part_date>='$part_read_start' and part_date<='$part_read_end'"""
println(s"""#---------------------------Starting the Execution--for whereClause ----------------#""")
log.info(s"""#---------------------------Starting the Execution--for whereClause ----------------#""")




val stageSailDimDF = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").trim().replace("*whereclause*", whereClause))
/*.replace("*whereclause*", whereClause)*/
/*.trim()+whereClause)*/
/* val stageVoyageDF = spark.sql(spark.sparkContext.getConf.get("spark.source.sql"))*/
stageSailDimDF.show(4,false)

val interDF=stageSailDimDF.select($"id",$"contactid",$"accountid",$"type",$"casenumber",$"reason",$"createddate",$"closeddate",$"caseownerid__c",$"casesubreason__c",$"case_resolution__c",$"case_sub_resolution__c",$"complaint_type__c",$"origin",$"subject",$"complaint__c",$"request_assistance_queue__c",$"request_assistance_reason__c",$"external_case_resolution__c",$"external_case_sub_resolution__c",$"case_activity_counter__c",$"reopened__c",$"escalated__c",$"total_case_resolution_time__c",$"external_escalation__c",$"status",$"ownerid",$"first_contact_resolution_chat__c",$"assetid",$"booked_activities__c",$"booking_payments__c",$"businesshoursid",$"caseaudience__c",$"count_child_cases__c",$"count_closed_cases__c",$"createdbyid",$"description",$"entitlementid",$"escalation_reason__c",$"isclosed",$"isclosedoncreate",$"isdeleted",$"isescalated",$"isstopped",$"last_interaction_date__c",$"lastmodifiedbyid",$"lastmodifieddate",$"link__c",$"milestonestatus",$"navigatorname__c",$"parentid",$"priority",$"productid",$"recordtypeid",$"reservation_number__c",$"sailor_email__c",$"sailor_name__c",$"ship__c",$"slaexitdate",$"slastartdate",$"sub_category__c",$"suppliedcompany",$"suppliedemail",$"suppliedname",$"suppliedphone",$"systemmodstamp",$"transferred_to__c",$"voyage__c",$"webfname__c",$"weblname__c",$"Case_Owner_Queue__c",$"Booking_Flow__c",$"Case_Owner__c",$"Escalated_Flag__c",$"Reopened_Count__c",$"First_CaseOwnerQueue__c",$"lastmodifieddate" as "src_date")

val required_columns = spark.sparkContext.getConf.get("spark.source.columns").split(",")



val missing_columns = ArrayBuffer[String]()
val avaliable_columns = ArrayBuffer[String]()
def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess



for (col <- required_columns) {



if (hasColumn(interDF, col)) {
println(col, "column exists", avaliable_columns.toString)
println("column exists", avaliable_columns.length)
log.info(col, "column exists", avaliable_columns.toString, avaliable_columns.length)
println(avaliable_columns.length, "lenthg")
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
val stage_final_df = missing_columns.foldLeft(interDF)((df, c) =>
df.withColumn(s"$c", lit(0)))
//stage_final_df.show(5,false)



println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxx-------------------------------Source-Stage Case------------------------------------xxxxxxxxxxxxxxxxxxxxxx")

if (!stage_final_df.head(1).isEmpty) {
loadDimFact(spark: SparkSession, stage_final_df)
}
println("after SCD")
log.info("after SCD")
ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
} catch {



case e: SQLException => { ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("******************in the catch of Package Dimension Load ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace", e); }



case e: Exception =>
{ ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark); log.info("******************in the catch of Crm Case Dimension Load ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace", e); }
//case e: SQLException => println("#----------------------------Process Has Failed---------------------------#")
System.exit(1)
//exit(1);
}



}



}