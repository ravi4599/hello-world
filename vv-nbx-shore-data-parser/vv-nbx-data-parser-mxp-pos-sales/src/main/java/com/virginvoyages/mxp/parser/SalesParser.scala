package com.virginvoyages.mxp.parser


import org.apache.spark.SparkConf
import java.sql.SQLException

import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.Row

import org.apache.spark.sql.functions.{ avg, explode, concat, lit, trim }
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.to_json
import java.sql.Struct
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.Column
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Column, DataFrame }
import scala.collection.mutable.ArrayBuffer

import java.text.SimpleDateFormat

//XML validator imports
import org.apache.spark.SparkConf
import java.io._
import java.nio.charset.StandardCharsets
import org.apache.spark.SparkFiles
import scala.util.Try
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

import com.virginvoyages.metadataframework.ManageMetadata

object SalesParser {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    import spark.implicits._

    var batchInstanceId : String = null
    var batchId : String = null

    try {

      sqlContext.setConf("hive.exec.dynamic.partition", "true")
      sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
      val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
    metadata.productIterator.foreach(println)

      val batchStartTime = metadata._3
       batchId = metadata._1
       batchInstanceId = metadata._2
      val batchEndTime = metadata._4
      val partStartTime = metadata._5.toString()
      val partEndTime = metadata._6.toString()
      val startExecutionTime=metadata._7.toString()
      val partDate=metadata._8.toString()

      var salesdf = spark.sql(spark.sparkContext.getConf.get("spark.source.table")).where($"BatchTime" >= lit(batchStartTime).cast(TimestampType) && $"BatchTime" <= lit(batchEndTime).cast(TimestampType) and ($"part_date".between(partStartTime, partEndTime)))
      var shipcode=salesdf.select("ShipCode").toString()

      var salesmsg = salesdf.select("Message").rdd.map(x => x.toString)
      var salesData = spark.read.json(salesmsg).withColumn("BatchTime",lit(startExecutionTime)).withColumn("VoyageId", lit(spark.sparkContext.getConf.get("spark.voyage.id"))).withColumn("Part_date",lit(partDate))
      .withColumn("ShipCode", lit(shipcode).cast(StringType))
      if(!salesData.head(1).isEmpty){
        
      salesData = salesData.withColumn("Sales_Id", salesData.col("id"))
        .withColumn("Sales_Guid", when(salesData.col("guid").isNotNull, salesData.col("guid")).otherwise(lit(null)))
        .withColumn("sales_BusinessUnitId", when(salesData.col("businessUnitId").isNotNull, salesData.col("businessUnitId")).otherwise(lit(null)))
        .withColumn("Sales_ShipCode", when(salesData.col("shipCode").isNotNull, salesData.col("shipCode")).otherwise(lit(null)))      
        .withColumn("Sales_VoyageNumber", when(salesData.col("voyageNumber").isNotNull, salesData.col("voyageNumber")).otherwise(lit(null)))
        .withColumn("Sales_receiptNr", when(salesData.col("receiptNr").isNotNull, salesData.col("receiptNr")).otherwise(lit(null)))
        .withColumn("Sales_operatorNr", when(salesData.col("operatorNr").isNotNull, salesData.col("operatorNr")))
        .withColumn("Sales_accountNr", when(salesData.col("accountNr").isNotNull, salesData.col("accountNr")).otherwise(lit(null)))
        .withColumn("Sales_ChargeId", when(salesData.col("chargeId").isNotNull, salesData.col("chargeId")).otherwise(lit(null)))
        .withColumn("Sales_Date", when(salesData.col("salesDate").isNotNull, salesData.col("salesDate")).otherwise(lit(null)).cast(TimestampType))
        .withColumn("Sales_Time", when(salesData.col("salesTime").isNotNull, salesData.col("salesTime")).otherwise(lit(null)).cast(TimestampType))
        .withColumn("Sales_OutletId", when(salesData.col("salesOutletId").isNotNull, salesData.col("salesOutletId")).otherwise(lit(null)))
        .withColumn("Sales_OutletName", when(salesData.col("salesOutletName").isNotNull, salesData.col("salesOutletName")).otherwise(lit(null)))
        .withColumn("Sales_CheckStatus", when(salesData.col("checkStatus").isNotNull, salesData.col("checkStatus")).otherwise(lit(null)))
        .withColumn("Sales_synchronized", when(salesData.col("synchronized").isNotNull, salesData.col("synchronized")).otherwise(lit(null)))
        .withColumn("Sales_Created", when(salesData.col("created").isNotNull, salesData.col("created")).otherwise(lit(null)).cast(TimestampType))
        .withColumn("Sales_VoidedParent", when(salesData.col("voidedParent").isNotNull, salesData.col("voidedParent")).otherwise(lit(null)))
        .withColumn("Sales_VoidReason", when(salesData.col("voidReason").isNotNull, salesData.col("voidReason")).otherwise(lit(null)))
		//.withColumn("ShipCode", when(salesData.col("ShipCode").isNotNull, salesData.col("ShipCode")).otherwise(lit(null)))


      val SalesItem = salesData.select("Sales_Id", "Sales_Guid", "sales_BusinessUnitId", "Sales_ShipCode","Sales_VoyageNumber", "Sales_receiptNr", "Sales_operatorNr", "Sales_accountNr", "Sales_ChargeId", "Sales_Date", "Sales_Time", "Sales_OutletId", "Sales_OutletName", "Sales_CheckStatus", "Sales_synchronized", "Sales_Created", "Sales_VoidedParent", "Sales_VoidReason", "VoyageID","BatchTime","ShipCode", "Part_date" )
       
  
      SalesItem.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.salesitem.table"))

      if (hasColumn(salesData, "checkDetails")) {
        if (checkArray(salesData, "checkDetails")) {
          salesData = salesData.withColumn("checkDetails", explode_outer($"checkDetails"))
          salesData = salesData.withColumn(
            "checkDetailsID", when(salesData.col("checkDetails.id").isNotNull, salesData.col("checkDetails.id")).otherwise(lit(null)))
            .withColumn("checkDetailsguid", when(salesData.col("checkDetails.guid").isNotNull, salesData.col("checkDetails.guid")).otherwise(lit(null)))
            .withColumn("checkDetailspluId", when(salesData.col("checkDetails.pluId").isNotNull, salesData.col("checkDetails.pluId")).otherwise(lit(null)))
            .withColumn("checkDetailspluNr", when(salesData.col("checkDetails.pluNr").isNotNull, salesData.col("checkDetails.pluNr")).otherwise(lit(null)))
            .withColumn("checkDetailspluDesc", when(salesData.col("checkDetails.pluDesc").isNotNull, salesData.col("checkDetails.pluDesc")).otherwise(lit(null)))
            .withColumn("checkDetailSalesQty", when(salesData.col("checkDetails.salesQty").isNotNull, salesData.col("checkDetails.salesQty")).otherwise(lit(null)))
            .withColumn("checkDetailUnitPrice", when(salesData.col("checkDetails.unitPrice").isNotNull, salesData.col("checkDetails.unitPrice")).otherwise(lit(null)))
            .withColumn("checkDetailTaxAmount", when(salesData.col("checkDetails.taxAmount").isNotNull, salesData.col("checkDetails.taxAmount")).otherwise(lit(null)))
            .withColumn("costOfGoods", when(salesData.col("checkDetails.costOfGood").isNotNull, salesData.col("checkDetails.costOfGood")).otherwise(lit(null)))
            .withColumn("dishCodes", when(salesData.col("checkDetails.dishCode").isNotNull, salesData.col("checkDetails.dishCode")).otherwise(lit(null)))
            .withColumn("ownerOperators", when(salesData.col("checkDetails.ownerOperators").isNotNull, salesData.col("checkDetails.ownerOperators")).otherwise(lit(null)))
            .withColumn("Discount_Amount", when(salesData.col("checkDetails.discount.amount").isNotNull, salesData.col("checkDetails.discount.amount")).otherwise(lit(null)))
            .withColumn("Discount_pluId", when(salesData.col("checkDetails.discount.pluId").isNotNull, salesData.col("checkDetails.discount.pluId")).otherwise(lit(null)))
            .withColumn("Discount_TypeId", when(salesData.col("checkDetails.discount.typeId").isNotNull, salesData.col("checkDetails.discount.typeId")).otherwise(lit(null)))
            .withColumn("Discount_Value", when(salesData.col("checkDetails.discount.value").isNotNull, salesData.col("checkDetails.discount.value")).otherwise(lit(null)))

        } else {
          salesData = salesData.withColumn("checkDetailsID", lit(null))
            .withColumn("checkDetailsguid", lit(null))
            .withColumn("checkDetailspluId", lit(null))
            .withColumn("checkDetailspluNr", lit(null))
            .withColumn("checkDetailspluDesc", lit(null))
            .withColumn("checkDetailSalesQty", lit(null))
            .withColumn("checkDetailUnitPrice", lit(null))
            .withColumn("checkDetailTaxAmount", lit(null))
            .withColumn("costOfGoods", lit(null))

        }

        val checkDetails = salesData.select("checkDetailsID", "checkDetailsguid", "checkDetailspluId", "checkDetailspluNr", "checkDetailspluDesc", "checkDetailSalesQty", "checkDetailUnitPrice", "checkDetailTaxAmount", "Sales_Guid", "Discount_Amount", "Discount_pluId", "Discount_TypeId", "Discount_Value", "VoyageID","BatchTime","ShipCode","Part_date" )
          

        checkDetails.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.checkdetails.table"))

        if (hasColumn(salesData, "costOfGoods")) {
          if (checkArray(salesData, "costOfGoods")) {
            salesData = salesData.withColumn("costOfGood", explode_outer($"costOfGoods"))
          } else {
            salesData = salesData.withColumn("costOfGood", $"costOfGoods")
          }
          salesData = salesData.withColumn(
            "price", when(salesData.col("costOfGood.price").isNotNull, salesData.col("costOfGood.price")).otherwise(lit(null)))
            .withColumn("validFrom", when(salesData.col("costOfGood.validFrom").isNotNull, salesData.col("costOfGood.validFrom")).otherwise(lit(null)).cast(TimestampType))
            .withColumn("validTo", when(salesData.col("costOfGood.validTo").isNotNull, salesData.col("costOfGood.validTo")).otherwise(lit(null)).cast(TimestampType))
            .withColumn("currencyId", when(salesData.col("costOfGood.currencyId").isNotNull, salesData.col("costOfGood.currencyId")).otherwise(lit(null)))
            .withColumn("costOfGood_guid", when(salesData.col("costOfGood.guid").isNotNull, salesData.col("costOfGood.guid")).otherwise(lit(null)))
            .withColumn("lastChanged", when(salesData.col("costOfGood.lastChanged").isNotNull, salesData.col("costOfGood.lastChanged")).otherwise(lit(null)).cast(TimestampType))
            .withColumn("checkDetailsguid", when(salesData.col("checkDetails.guid").isNotNull, salesData.col("checkDetails.guid")).otherwise(lit(null)))


        } else {
          salesData = salesData.withColumn("price", lit(null))
            .withColumn("validFrom", lit(null))
            .withColumn("validTo", lit(null))
            .withColumn("currencyId", lit(null))
            .withColumn("costOfGood_guid", lit(null))
            .withColumn("lastChanged", lit(null))

        }

        val cGoodData = salesData.select("price", "validFrom", "validTo", "currencyId", "costOfGood_guid", "lastChanged", "checkDetailsguid", "Sales_Guid", "VoyageID", "BatchTime","ShipCode","Part_date" )//.where($"BatchTime" >= lit(batch_start_time).cast(TimestampType) && $"BatchTime" <= lit(batch_end_time).cast(TimestampType) and ($"part_date".between(part_start_time, part_end_time)))


     
        cGoodData.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.costgood.table"))

        if (hasColumn(salesData, "dishCodes")) {
          if (checkArray(salesData, "dishCodes")) {
            salesData = salesData.withColumn("dishCode", explode_outer($"dishCodes"))
          } else {
            salesData = salesData.withColumn("dishCode", $"dishCodes")
          }
          salesData = salesData.withColumn(
            "dishCodeId", when(salesData.col("dishCode.id").isNotNull, salesData.col("dishCode.id")).otherwise(lit(null)))
            .withColumn("dishCodeType", when(salesData.col("dishCode.type").isNotNull, salesData.col("dishCode.type")).otherwise(lit(null)))
            .withColumn("Sales_Guid", when(salesData.col("guid").isNotNull, salesData.col("guid")).otherwise(lit(null)))
        } else {
          salesData = salesData.withColumn("dishCodeId", lit(null))
            .withColumn("dishCodeType", lit(null))
            .withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
        }

        val dishcode = salesData.select("dishCodeId", "dishCodeType", "VoyageID", "Part_date", "BatchTime","ShipCode")

        if (hasColumn(salesData, "ownerOperators")) {
          if (checkArray(salesData, "ownerOperators")) {
            salesData = salesData.withColumn("ownerOper", explode_outer($"ownerOperators"))
          } else {
            salesData = salesData.withColumn("ownerOper", $"ownerOperators")
          }
          salesData = salesData.withColumn(
            "operatorNr", when(salesData.col("ownerOper.Operator_Nr").isNotNull, salesData.col("ownerOper.Operator_Nr")).otherwise(lit(null)))
            .withColumn("Sales_Guid", when(salesData.col("guid").isNotNull, salesData.col("guid")).otherwise(lit(null)))


        } else {
          salesData = salesData.withColumn("operatorNr", lit(null))

        }
        val operatorData = salesData.select("operatorNr", "VoyageID", "Part_date", "BatchTime","ShipCode")

      }
      
      }
      ManageMetadata.updateStatus(batchInstanceId, batchId, "Successful", spark) 

    } catch {

      case e: Exception =>
        {
          log.info("in the catch of updateStatus ****************** ")
          e.printStackTrace()
          throw new Exception("General Exception..please check the stacktrace")
        }
        ManageMetadata.updateStatus(batchInstanceId, batchId, "Failed", spark) 
    }

  }

  def checkArray(df: DataFrame, colname: String): Boolean = {

    df.schema(colname).dataType match {
      case ArrayType(_, _) => return true
      case _               => return false
    }
  }

  def checkStructType(df: DataFrame, colname: String): Boolean = {
    df.schema(colname).dataType match {
      case StructType(_) => return true
      case _             => return false
    }
  }

  def checkStringType(df: DataFrame, colname: String): Boolean = {
    df.schema(colname).dataType match {
      case StringType => return true
      case _          => return false
    }
  }

  def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
}

