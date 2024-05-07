package com.virginvoyages.shore.dim
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
import org.apache.spark.sql.functions.unix_timestamp

object WearableDimLoad {
  
  def main(args: Array[String]) {
    
    
     val log = LogManager.getRootLogger
     log.setLevel(Level.INFO)
  
     
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

    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    
    
    try{
      

      
      
      var trackableDf  =  spark.sql(spark.sparkContext.getConf.get("spark.trackable.query").trim())
      
      var shipmentDf  =  spark.sql(spark.sparkContext.getConf.get("spark.shipment.query").trim())
      
      var shippingCompanyDf  =  spark.sql(spark.sparkContext.getConf.get("spark.shipping.query").trim())
      
      var trackableShipmentDf  =  spark.sql(spark.sparkContext.getConf.get("spark.trackableshipment.query").trim())
      
      var guestFolioDf  =  spark.sql(spark.sparkContext.getConf.get("spark.guestfolio.query").trim())
      
      var guestrelDf = spark.sql(spark.sparkContext.getConf.get("spark.seaware.guest.rel").trim())
      
      var swToVxpWearablesDf = spark.sql(spark.sparkContext.getConf.get("spark.swtovxp.wearable.query").trim())
      
      var crossIdRefDf = spark.sql(spark.sparkContext.getConf.get("spark.crossidref.query").trim())
      
      var coreCrmaccDf = spark.sql(spark.sparkContext.getConf.get("spark.core.crm.account.sql").trim())
      
      var guestDimDf = spark.sql(spark.sparkContext.getConf.get("spark.source.guest.dim.sql").trim())
      
      var coreGuestRel = spark.sql(spark.sparkContext.getConf.get("spark.core.res.guest.rel.sql").trim())
      
      var resDimDf = spark.sql(spark.sparkContext.getConf.get("spark.core.reservation.dim.sql").trim())
      
      var bookedCabinDf = spark.sql(spark.sparkContext.getConf.get("spark.core.booked.cabin.reservation.dim.sql").trim())
        
      var cabinMasterDf = spark.sql(spark.sparkContext.getConf.get("spark.core.cabin.master.dim.sql").trim())
      
      var joinedDf = guestrelDf.join(crossIdRefDf,guestrelDf.col("vxp_reservation_guest_id") === crossIdRefDf.col("reservationguestid"),"inner")
                               .join(trackableDf ,crossIdRefDf.col("trackableid") === trackableDf.col("trackableid")  && trackableDf.col("isdeleted") === false,"inner")
                               .join(trackableShipmentDf, trackableDf.col("trackableid") === trackableShipmentDf.col("trackableid") && trackableShipmentDf.col("isdeleted") === false , "left" )
                               .join(shipmentDf, trackableShipmentDf.col("shipmentid") === shipmentDf.col("shipmentid") && shipmentDf.col("isdeleted") === false , "left" )
                               .join(shippingCompanyDf, shipmentDf.col("shippingcompanyid") === shippingCompanyDf.col("shippingcompanyid") &&  shippingCompanyDf.col("isdeleted") === false,"left")
                               .join(guestFolioDf ,guestFolioDf.col("reservationguestid") ===  crossIdRefDf.col("reservationguestid") && guestFolioDf.col("isdeleted")  === false ,"left")
                               .join(coreCrmaccDf , guestrelDf.col("client_id") === coreCrmaccDf.col("Client_ID__C"), "left")
                               .join(guestDimDf, guestrelDf.col("src_guest_id") === guestDimDf.col("src_guest_id"),"left")
                               .join(coreGuestRel,guestDimDf.col("guest_id") === coreGuestRel.col("res_guest_rel_guest_id") ,"left")
                               .join(resDimDf,coreGuestRel.col("res_guest_rel_res_id") === resDimDf.col("res_dim_res_id"),"left")
                               .join(bookedCabinDf,resDimDf.col("res_dim_res_id") === bookedCabinDf.col("cabin_dim_res_id"),"left")
                               .join(cabinMasterDf,bookedCabinDf.col("cabin_dim_cabin_id") === cabinMasterDf.col("cabin_master_cabin_id") ,"left")
                               .join(swToVxpWearablesDf,guestrelDf.col("vxp_guest_id") === swToVxpWearablesDf.col("mxp_guest_id"),"left")
                               .withColumn("name_to_be_itched",when(coreCrmaccDf.col("preferred_name__c") === "" ,coreCrmaccDf.col("firstname")).otherwise(coreCrmaccDf.col("preferred_name__c")))
                               .withColumn("wearable_colour",when(resDimDf.col("vip_status").like("%VIP%") ,lit("Black"))
                                                             .when(cabinMasterDf.col("deck_number") >= 5 && cabinMasterDf.col("deck_number") <= 10 ,lit("Coral"))
                                                             .when(cabinMasterDf.col("deck_number") >= 11 && cabinMasterDf.col("deck_number") <= 15,lit("Seaspray")).otherwise(lit(null)))
                               .withColumn("rec_start_dttm" , current_timestamp())
                               .withColumn("rec_end_dttm",lit("9999-12-31 00:00:00").cast(TimestampType))
                               .withColumn("wearable_rfid",trackableDf.col("rfid"))
                               .withColumn("charge_id",guestFolioDf.col("accesscardnumber"))
                               //.withColumn("tracking_number",shipmentDf.col("trackingnumber"))
                               .withColumn("shipping_company",shippingCompanyDf.col("name"))
                               .withColumn("shippment_status",shipmentDf.col("statuscode"))
                               //.withColumn("batchtime" ,lit(start_execution_time) )
                               //.withColumn("batchtime",guestrelDf.col("loadtimestamp") )
                               .select(col("rec_start_dttm"),col("rec_end_dttm"),col("wearable_rfid"),guestrelDf.col("src_guest_id"),col("charge_id"),
                                        col("name_to_be_itched"), col("wearable_colour"),shipmentDf.col("trackingnumber"),col("shipping_company"),col("shippment_status"),
                                        swToVxpWearablesDf.col("mailing_address_line1"),swToVxpWearablesDf.col("mailing_address_line2"),swToVxpWearablesDf.col("mailing_address_line3"),
                                        swToVxpWearablesDf.col("mailing_address_city"),swToVxpWearablesDf.col("mailing_address_state"),swToVxpWearablesDf.col("mailing_address_zip"),swToVxpWearablesDf.col("mailing_address_countrycode"),
                                        trackableDf.col("batchtime"))
                                      
                                        
     
      
      
      
      //joinedDf.show(20,false)
      
      loadDimFact(spark: SparkSession, joinedDf)
       
  
      
    }
    catch {

      case e: Exception =>
        {
          log.info("in the catch of updateStatus ****************** ")
          e.printStackTrace()
          throw new Exception("General Exception..please check the stacktrace")
        }
    }
     
     
  }
  
}