package com.virginvoyages.invoke.api

import org.apache.spark.storage.StorageLevel
import scala.collection.immutable.Map
import scala.util.parsing.json._
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml.XmlReader
import scala.collection.Seq
import java.sql.Struct
import org.apache.hadoop.hdfs.util.Diff.ListType
import scala.collection.mutable.WrappedArray
import org.scalatest.tagobjects.Disk

object SailDiningApi {

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)
      
  def main(args: Array[String]): Unit = {
    
  val findAllocDetailsUDF = udf(findAllocDetails)
  val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate() 
 def checkArray(df: DataFrame, colname: String): Boolean = {
      df.schema(colname).dataType match {
        case ArrayType(_, _) => return true
        case _               => return false
      }
    }

  def parseSailDetail(responseDF: DataFrame, configMap: Broadcast[Map[String, String]]): DataFrame = {
      log.info("Inside Df method")
      var dfnoEmpty = responseDF.filter(col("response") =!= "")
      import spark.implicits._
       var xmlStringRDD = dfnoEmpty
      .selectExpr("concat('<response> ',response,'</response>')").map(r => r.getString(0)).rdd
      var masterDF = new XmlReader().xmlRdd(spark.sqlContext, xmlStringRDD) 

      masterDF = masterDF.withColumn("AvailDining", masterDF.col("GetAvailDinings_OUT.AvailDinings.AvailDining"))

      if (checkArray(masterDF, "AvailDining")) {
        masterDF = masterDF.withColumn("AvailDining_explode", explode_outer(masterDF.col("AvailDining")))
        masterDF = masterDF.withColumn("Ship", masterDF.col("AvailDining_explode.Ship"))
                           .withColumn("Restaurant",masterDF.col("AvailDining_explode.Restaurant"))
                           .withColumn("DateTime",masterDF.col("AvailDining_explode.DateTime"))
                           .withColumn("DiningKind",masterDF.col("AvailDining_explode.DiningKind"))
                           .withColumn("Availability",masterDF.col("AvailDining_explode.Availability"))
                           .withColumn("test1",lit(monotonically_increasing_id))//monotonically_increasing_id)
      } 
      masterDF = masterDF.drop("AvailDining_explode","GetAvailDinings_OUT","AvailDining")
     var allocDF = new XmlReader().xmlRdd(spark.sqlContext, xmlStringRDD)
     allocDF = allocDF.withColumn("AllocTotal",
         allocDF.col("GetAvailDinings_OUT.AvailDinings.AvailDining.AllocTotals.AllocTotal"))
      if (checkArray(allocDF, "AllocTotal")) {
        allocDF = allocDF.withColumn("AllocTotal_explode", explode(allocDF.col("AllocTotal")))
        allocDF = allocDF.withColumn("AllocTotal", allocDF.col("AllocTotal_explode"))
                          .withColumn("wtl_abs", findAllocDetailsUDF(allocDF.col("AllocTotal_explode.Abs"),lit(0)))
                         .withColumn("wtl_wgt", findAllocDetailsUDF(allocDF.col("AllocTotal_explode.Wgt"),lit(0)))
                         .withColumn("gty_abs", findAllocDetailsUDF(allocDF.col("AllocTotal_explode.Abs"),lit(1)))
                         .withColumn("gty_wgt", findAllocDetailsUDF(allocDF.col("AllocTotal_explode.Wgt"),lit(1)))
                         .withColumn("ok_abs", findAllocDetailsUDF(allocDF.col("AllocTotal_explode.Abs"),lit(2)))
                         .withColumn("ok_wgt", findAllocDetailsUDF(allocDF.col("AllocTotal_explode.Wgt"),lit(2)))
                          .withColumn("test",lit(monotonically_increasing_id))//monotonicallyIncreasingId   monotonically_increasing_id
                         .drop("AllocTotal","AllocTotal_explode","AllocTotals_explode","AllocTotals","GetAvailDinings_OUT")
      }
        val mastAllocDF1 = allocDF.join(masterDF,allocDF.col("test")===masterDF.col("test1"),"inner")
                                  .drop("test")
       var tablesDF = new XmlReader().xmlRdd(spark.sqlContext, xmlStringRDD)  
             tablesDF =  tablesDF.withColumn("Tables",tablesDF.col("GetAvailDinings_OUT.AvailDinings.AvailDining.Tables"))
         
         if (checkArray(tablesDF, "Tables")) {
           tablesDF= tablesDF.withColumn("Tables_explode", explode_outer(tablesDF.col("Tables"))) 
           tablesDF = tablesDF.withColumn("TabTotal", tablesDF.col("Tables_explode.Total"))
                             .withColumn("TabAvlBase", tablesDF.col("Tables_explode.AvlBase"))
                             .withColumn("TabAvlParty", tablesDF.col("Tables_explode.AvlParty"))
                             .withColumn("test",lit(monotonically_increasing_id))
                             .drop("Tables_explode","Tables","GetAvailDinings_OUT")
         }
        val mastAllocDF2 = tablesDF.join(mastAllocDF1,tablesDF.col("test")===mastAllocDF1.col("test1"),"inner")
                                   .drop("test")
        var tablesUsageDF = new XmlReader().xmlRdd(spark.sqlContext, xmlStringRDD)  
       tablesUsageDF  = tablesUsageDF.withColumn("TabUsageTotals",tablesUsageDF.col("GetAvailDinings_OUT.AvailDinings.AvailDining.Tables.UsageTotals"))
               if(checkArray(tablesUsageDF, "TabUsageTotals")) {
          tablesUsageDF = tablesUsageDF.withColumn("TabUsageTotals_explode",explode_outer(tablesUsageDF.col("TabUsageTotals"))) 
          tablesUsageDF=tablesUsageDF.withColumn("TabUsageAbs", tablesUsageDF.col("TabUsageTotals_explode.Abs"))
                             .withColumn("TabUsageWgt", tablesUsageDF.col("TabUsageTotals_explode.Wgt"))
                             .withColumn("test",lit(monotonically_increasing_id))
                             .drop("TabUsageTotals_explode","TabUsageTotals","GetAvailDinings_OUT")
               }
          val mastAllocDF3 = tablesUsageDF.join(mastAllocDF2,tablesUsageDF.col("test")===mastAllocDF2.col("test1"),"inner")
                                 .drop("test")
         var seatsDF = new XmlReader().xmlRdd(spark.sqlContext, xmlStringRDD)
          seatsDF = seatsDF.withColumn("Seats", seatsDF.col("GetAvailDinings_OUT.AvailDinings.AvailDining.Seats"))
                                        println("test--3")
            if(checkArray(seatsDF, "Seats")) {
         seatsDF = seatsDF.withColumn("Seats_explode",explode_outer(seatsDF.col("Seats")))
         seatsDF = seatsDF.withColumn("SeatsTotal",seatsDF.col("Seats_explode.Total"))
                          .withColumn("SeatsAvlBase",seatsDF.col("Seats_explode.AvlBase"))
                          .withColumn("SeatsAvlParty",seatsDF.col("Seats_explode.AvlParty"))
                          .withColumn("test",lit(monotonically_increasing_id))
                          .drop("Seats_explode","Seats","GetAvailDinings_OUT")
            }
        val mastAllocDF4 = seatsDF.join(mastAllocDF3,seatsDF.col("test")===mastAllocDF3.col("test1"),"inner")
                                   .drop("test")
          var seatsUsageDF = new XmlReader().xmlRdd(spark.sqlContext, xmlStringRDD) 
         seatsUsageDF = seatsUsageDF.withColumn("SeatsUsageTotals",seatsUsageDF.col("GetAvailDinings_OUT.AvailDinings.AvailDining.Seats.UsageTotals"))
           if(checkArray(seatsUsageDF, "SeatsUsageTotals")) {
         seatsUsageDF = seatsUsageDF.withColumn("SeatsUsageTotals_explode",explode_outer(seatsUsageDF.col("SeatsUsageTotals")))
         seatsUsageDF = seatsUsageDF.withColumn("SeatsUsageAbs",seatsUsageDF.col("SeatsUsageTotals_explode.Abs").cast(IntegerType)) 
                                    .withColumn("SeatsUsageWgt",seatsUsageDF.col("SeatsUsageTotals_explode.Wgt").cast(IntegerType)) 
                                    .withColumn("test",lit(monotonically_increasing_id))
                                    .drop("SeatsUsageTotals_explode","SeatsUsageTotals","GetAvailDinings_OUT")
           }
        val mastAllocDF5 = seatsUsageDF.join(mastAllocDF4,seatsUsageDF.col("test")===mastAllocDF4.col("test1"),"inner")
                                        .drop("test")
    val mastAllocDF6 = mastAllocDF5.dropDuplicates()
    return mastAllocDF6
        .selectExpr("Ship", "Restaurant", "DateTime", "DiningKind", "Availability",
            "wtl_abs","wtl_wgt","gty_abs","gty_wgt","ok_abs","ok_wgt","TabTotal", "TabAvlBase", "TabAvlParty",
            "TabUsageAbs","TabUsageWgt","SeatsTotal","SeatsAvlBase","SeatsAvlParty","SeatsUsageAbs","SeatsUsageWgt")
    }
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    val sailingDiningFact =  spark.sparkContext.getConf.get("spark.target.table").trim() 
    val curr_date = current_date()

    val sailDiningLandingTable = spark.sparkContext.getConf.get("spark.landing.table").trim()
    println("sailDiningLandingTable=="+sailDiningLandingTable)
    import spark.implicits._
    val responseDF = spark.sql("select * from " + sailDiningLandingTable)
    responseDF.persist(StorageLevel.MEMORY_AND_DISK)
    var parseDF = parseSailDetail(responseDF, sparkConfiguration)
    responseDF.unpersist()
    parseDF =parseDF.withColumnRenamed("Ship", "Ship_Api")
     
    val shipfacility = spark.sql("select ship_code,facility_code,facility_id from vv_db.hvtb_nbx_core_spf_ship_facility_lkp")
    parseDF = parseDF.join(shipfacility,(parseDF.col("Ship_Api")===shipfacility("ship_code") && parseDF.col("Restaurant")===shipfacility("facility_code")),"left")
                         .drop("ship_code","facility_code")
    
     val sailshipDF = spark.sql("select sh.ship ,sd.sail_id ,sd.sail_date_from ,sh.ship_id ,sd.sail_date_to from vv_db.hvtb_nbx_core_sw_sail_dim sd join vv_db.hvtb_nbx_core_sw_ship_dim sh on sd.ship_id = sh.ship_id where sd.rec_end_dttm = '9999-12-31'")
    val parseFilterDF1 =parseDF.join(sailshipDF,parseDF.col("Ship_Api")===sailshipDF.col("ship"),"inner")
                       .where(parseDF.col("DateTime") between(sailshipDF.col("sail_date_from"),sailshipDF.col("sail_date_to")))
                       .drop("sail_date_from","sail_date_to","Ship_Api")
                       
     val parseFilterDF2 = parseFilterDF1
                       .withColumnRenamed("facility_id","src_facility_id")
                       .withColumn("snapshot_date",current_date())
                       .withColumnRenamed("DiningKind","dining_kind")
                       .withColumnRenamed("Availability","dining_availability")
                       .withColumn("wtl_abs", when(col("wtl_abs") isNotNull,col("wtl_abs").cast(IntegerType)).otherwise(lit(null)))
                       .withColumn("wtl_wgt", when(col("wtl_wgt") isNotNull,col("wtl_wgt").cast(IntegerType)).otherwise(lit(null)))
                         .withColumn("gty_abs", when(col("gty_abs") isNotNull,col("gty_abs").cast(IntegerType)).otherwise(lit(null)))
                         .withColumn("gty_wgt", when(col("gty_wgt") isNotNull,col("gty_wgt").cast(IntegerType)).otherwise(lit(null)))
                         .withColumn("ok_abs", when(col("ok_abs") isNotNull,col("ok_abs").cast(IntegerType)).otherwise(lit(null)))
                         .withColumn("ok_wgt", when(col("ok_wgt") isNotNull,col("ok_wgt").cast(IntegerType)).otherwise(lit(null)))
                       .withColumnRenamed("TabTotal" ,"total_tables")
                       .withColumnRenamed("TabAvlBase","avlbase_tables")
                       .withColumnRenamed("TabAvlParty","avlparty_tables")
                       .withColumnRenamed("TabUsageAbs","usagetotals_abs_tables")
                       .withColumnRenamed("TabUsageWgt","usagetotals_wgt_tables")
                       .withColumnRenamed("SeatsTotal","total_seats")
                       .withColumnRenamed("SeatsAvlBase","avlbase_seats")
                       .withColumnRenamed("SeatsAvlParty","avlparty_seats")
                       .withColumnRenamed("SeatsUsageAbs","usagetotals_abs_seats")
                       .withColumnRenamed("SeatsUsageWgt","usagetotals_wgt_seats")
                        .withColumn("sail_id", col("sail_id"))
                       .withColumn("etl_ld_dt", current_timestamp())
                       .withColumn("etl_upd_dt", current_timestamp())
                        .withColumnRenamed("DateTime","dining_datetime_slot")
                       .drop("ship","ship_id","Restaurant")
        val parseFilterDF3= parseFilterDF2
                          .withColumn("total_tables",when(col("total_tables") isNotNull,col("total_tables").cast(IntegerType)).otherwise(lit(null)))
                          .withColumn("avlbase_tables",when(col("avlbase_tables") isNotNull,col("avlbase_tables").cast(IntegerType)).otherwise(lit(null))) 
                          .withColumn("avlparty_tables",when(col("avlparty_tables") isNotNull,col("avlparty_tables").cast(IntegerType)).otherwise(lit(null)))
                          .withColumn("usagetotals_abs_tables",when(col("usagetotals_abs_tables") isNotNull,col("usagetotals_abs_tables").cast(IntegerType)).otherwise(lit(null))) 
                          .withColumn("usagetotals_wgt_tables",when(col("usagetotals_wgt_tables") isNotNull,col("usagetotals_wgt_tables").cast(IntegerType)).otherwise(lit(null)))
                          .withColumn("total_seats",when(col("total_seats") isNotNull,col("total_seats").cast(IntegerType)).otherwise(lit(null)))
                          .withColumn("avlbase_seats",when(col("avlbase_seats") isNotNull,col("avlbase_seats").cast(IntegerType)).otherwise(lit(null)))
                          .withColumn("avlparty_seats",when(col("avlparty_seats") isNotNull,col("avlparty_seats").cast(IntegerType)).otherwise(lit(null)))
                          .withColumn("usagetotals_abs_seats",when(col("usagetotals_abs_seats") isNotNull,col("usagetotals_abs_seats").cast(IntegerType)).otherwise(lit(null)))
                          .withColumn("usagetotals_wgt_seats",when(col("usagetotals_wgt_seats") isNotNull,col("usagetotals_wgt_seats").cast(IntegerType)).otherwise(lit(null)))
                          .withColumn("dining_datetime_slot", col("dining_datetime_slot").cast(TimestampType))
       parseFilterDF3.write.mode("Overwrite").parquet(spark.sparkContext.getConf.get("spark.target.final").trim())
     //parseFilterDF.write.mode("Overwrite").insertInto(spark.sparkContext.getConf.get("spark.target.table").trim())
    spark.stop()
  }
  val findAllocDetails  = (alloctot : scala.collection.mutable.WrappedArray[java.math.BigDecimal],measure:Int)=>{
   alloctot(measure).toBigInteger()
}
}