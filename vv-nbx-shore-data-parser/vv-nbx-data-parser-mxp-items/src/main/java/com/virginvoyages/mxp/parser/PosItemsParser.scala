package com.virginvoyages.mxp.parser
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

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
import org.apache.spark.SparkConf
import java.io._
import java.nio.charset.StandardCharsets
import org.apache.spark.SparkFiles
import scala.util.Try
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

import com.virginvoyages.metadataframework.ManageMetadata

object PosItemsParser {

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

    var batchInstanceId: String = null
    var batchId: String = null
    try {

      val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
      metadata.productIterator.foreach(println)

      val batchStartTime = metadata._3
      batchId = metadata._1
      batchInstanceId = metadata._2
      val batchEndTime = metadata._4
      val partStartTime = metadata._5.toString()
      val partEndTime = metadata._6.toString()
      val startExecutionTime = metadata._7.toString()
      val partDate = metadata._8.toString()

      var posItemsSourceDf = spark.sql(spark.sparkContext.getConf.get("spark.source.table"))

      var Data = posItemsSourceDf.select("Message", "BatchTime", "ShipCode", "Part_Date").where($"BatchTime" >= lit(batchStartTime).cast(TimestampType) && $"BatchTime" <= lit(batchEndTime).cast(TimestampType) and ($"part_date".between(partStartTime, partEndTime)))
      if (!Data.head(1).isEmpty) {
        var shipcode = Data.select("ShipCode").first().getString(0)
        var posItemsDf = Data.select("Message").rdd.map { x => x.toString }

        var posItemsMsg = spark.read.json(posItemsDf).withColumn("BatchTime", lit(startExecutionTime)).withColumn("VoyageId", lit(spark.sparkContext.getConf.get("spark.voyage.id"))).withColumn("partDate", lit(partDate))

        posItemsMsg = posItemsMsg.withColumn("ShipCode", lit(shipcode).cast(StringType))
        posItemsMsg.printSchema

        if (hasColumn(posItemsMsg, "salesGroup")) {
          posItemsMsg = posItemsMsg.withColumn("salesGroup", explode_outer($"salesGroup"))

          /*      posItemsMsg = posItemsMsg.withColumn("salesGroup_number", when(posItemsMsg.col("salesGroup.number").isNotNull, posItemsMsg.col("salesGroup.number")).otherwise(lit(null)))
              .withColumn("salesGroup_name", when(posItemsMsg.col("salesGroup.name").isNotNull, posItemsMsg.col("salesGroup.name")).otherwise(lit(null)))
              .withColumn("salesGroup_guid", when(posItemsMsg.col("salesGroup.guid").isNotNull, posItemsMsg.col("salesGroup.guid")).otherwise(lit(null)))
              .withColumn("salesGroup_enableCrewDiscounts", when(posItemsMsg.col("salesGroup.enableCrewDiscounts").isNotNull, posItemsMsg.col("salesGroup.enableCrewDiscounts")).otherwise(lit(null)))
              */
          if (hasColumn(posItemsMsg, "salesGroup.number")) {
            posItemsMsg = posItemsMsg
              .withColumn("salesGroup_number", when(posItemsMsg.col("salesGroup.number").isNotNull, posItemsMsg.col("salesGroup.number")).otherwise(lit(null)))
          } else { posItemsMsg = posItemsMsg.withColumn("salesGroup_number", lit(null)) }

          if (hasColumn(posItemsMsg, "salesGroup.name")) {
            posItemsMsg = posItemsMsg
              .withColumn("salesGroup_name", when(posItemsMsg.col("salesGroup.name").isNotNull, posItemsMsg.col("salesGroup.name")).otherwise(lit(null)))
          } else { posItemsMsg = posItemsMsg.withColumn("salesGroup_name", lit(null)) }

          if (hasColumn(posItemsMsg, "salesGroup.guid")) {
            posItemsMsg = posItemsMsg
              .withColumn("salesGroup_guid", when(posItemsMsg.col("salesGroup.guid").isNotNull, posItemsMsg.col("salesGroup.guid")).otherwise(lit(null)))
          } else { posItemsMsg = posItemsMsg.withColumn("salesGroup_guid", lit(null)) }

          if (hasColumn(posItemsMsg, "salesGroup.enableCrewDiscounts")) {
            posItemsMsg = posItemsMsg
              .withColumn("salesGroup_enableCrewDiscounts", when(posItemsMsg.col("salesGroup.enableCrewDiscounts").isNotNull, posItemsMsg.col("salesGroup.enableCrewDiscounts")).otherwise(lit(null)))
          } else { posItemsMsg = posItemsMsg.withColumn("salesGroup_enableCrewDiscounts", lit(null)) }

          if (hasColumn(posItemsMsg, "salesGroup.generatedDate")) {
            posItemsMsg = posItemsMsg
              .withColumn("generated_date", when(posItemsMsg.col("salesGroup.generatedDate").isNotNull, posItemsMsg.col("salesGroup.generatedDate")).otherwise(lit(null)))
          } else { posItemsMsg = posItemsMsg.withColumn("generated_date", lit(null)) }

          val posSalesGroup = posItemsMsg.select("VoyageId", "BatchTime", "salesGroup_number", "salesGroup_name", "salesGroup_guid", "salesGroup_enableCrewDiscounts", "generated_date", "ShipCode", "partDate")
          posSalesGroup.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.salesgroup.table"))

          //	posSalesGroup.show()
          println("Pos salesgroup data has been written")

          if (hasColumn(posItemsMsg, "salesGroup.categories")) {
            posItemsMsg = posItemsMsg.withColumn("explode_category", when(posItemsMsg.col("salesGroup.categories").isNotNull, posItemsMsg.col("salesGroup.categories")).otherwise(lit(null)))

            if (checkArray(posItemsMsg, "explode_category")) {

              posItemsMsg = posItemsMsg.withColumn("categories", explode_outer($"explode_category"))

              /*  posItemsMsg = posItemsMsg.withColumn("categories_number", when(posItemsMsg.col("categories.number").isNotNull, posItemsMsg.col("categories.number")).otherwise(lit(null)))
                .withColumn("categories_name", when(posItemsMsg.col("categories.name").isNotNull, posItemsMsg.col("categories.name")).otherwise(lit(null)))
                .withColumn("categories_guid", when(posItemsMsg.col("categories.guid").isNotNull, posItemsMsg.col("categories.guid")).otherwise(lit(null)))
*/
              if (hasColumn(posItemsMsg, "categories.number")) {
                posItemsMsg = posItemsMsg
                  .withColumn("categories_number", when(posItemsMsg.col("categories.number").isNotNull, posItemsMsg.col("categories.number")).otherwise(lit(null)))
              } else { posItemsMsg = posItemsMsg.withColumn("categories_number", lit(null)) }

              if (hasColumn(posItemsMsg, "categories.name")) {
                posItemsMsg = posItemsMsg
                  .withColumn("categories_name", when(posItemsMsg.col("categories.name").isNotNull, posItemsMsg.col("categories.name")).otherwise(lit(null)))
              } else { posItemsMsg = posItemsMsg.withColumn("categories_name", lit(null)) }

              if (hasColumn(posItemsMsg, "categories.guid")) {
                posItemsMsg = posItemsMsg
                  .withColumn("categories_guid", when(posItemsMsg.col("categories.guid").isNotNull, posItemsMsg.col("categories.guid")).otherwise(lit(null)))
              } else { posItemsMsg = posItemsMsg.withColumn("categories_guid", lit(null)) }

            }

          } else {
            posItemsMsg = posItemsMsg.withColumn("categories_number", lit(null))
              .withColumn("categories_name", lit(null))
              .withColumn("categories_guid", lit(null))

          }
          val posCategories = posItemsMsg.select("VoyageId", "BatchTime", "salesGroup_guid", "categories_number", "categories_name", "categories_guid", "generated_date", "ShipCode", "partDate")
          posCategories.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.categories.table"))

         // posCategories.show()
          println("Pos categories data has been written")
          if (hasColumn(posItemsMsg, "categories.pluGroups")) {

            posItemsMsg = posItemsMsg.withColumn("explode_plugroup", when(posItemsMsg.col("categories.pluGroups").isNotNull, posItemsMsg.col("categories.pluGroups")).otherwise(lit(null)))

            if (checkArray(posItemsMsg, "explode_plugroup")) {

              posItemsMsg = posItemsMsg.withColumn("pluGroups", explode_outer($"explode_plugroup"))
              /*      posItemsMsg = posItemsMsg.withColumn("pluGroups_nr", when(posItemsMsg.col("pluGroups.nr").isNotNull, posItemsMsg.col("pluGroups.nr")).otherwise(lit(null)))
                .withColumn("pluGroups_name", when(posItemsMsg.col("pluGroups.name").isNotNull, posItemsMsg.col("pluGroups.name")).otherwise(lit(null)))
                .withColumn("pluGroups_guid", when(posItemsMsg.col("pluGroups.guid").isNotNull, posItemsMsg.col("pluGroups.guid")).otherwise(lit(null)))
             */
              if (hasColumn(posItemsMsg, "pluGroups.nr")) {
                posItemsMsg = posItemsMsg
                  .withColumn("pluGroups_nr", when(posItemsMsg.col("pluGroups.nr").isNotNull, posItemsMsg.col("pluGroups.nr")).otherwise(lit(null)))
              } else { posItemsMsg = posItemsMsg.withColumn("pluGroups_nr", lit(null)) }

              if (hasColumn(posItemsMsg, "pluGroups.name")) {
                posItemsMsg = posItemsMsg
                  .withColumn("pluGroups_name", when(posItemsMsg.col("pluGroups.name").isNotNull, posItemsMsg.col("pluGroups.name")).otherwise(lit(null)))
              } else { posItemsMsg = posItemsMsg.withColumn("pluGroups_name", lit(null)) }
              if (hasColumn(posItemsMsg, "pluGroups.guid")) {
                posItemsMsg = posItemsMsg
                  .withColumn("pluGroups_guid", when(posItemsMsg.col("pluGroups.guid").isNotNull, posItemsMsg.col("pluGroups.guid")).otherwise(lit(null)))
              } else { posItemsMsg = posItemsMsg.withColumn("pluGroups_guid", lit(null)) }

            }

          } else {
            posItemsMsg = posItemsMsg.withColumn("pluGroups_nr", lit(null))
              .withColumn("pluGroups_name", lit(null))
              .withColumn("pluGroups_guid", lit(null))
          }

          val posPluGroups = posItemsMsg.select("VoyageId", "BatchTime", "salesGroup_guid", "categories_guid", "pluGroups_nr", "pluGroups_name", "pluGroups_guid", "generated_date", "ShipCode", "partDate")

          posPluGroups.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.plugroup.table"))

          //posPluGroups.show()
          println("Pos plugroups data has been written")
          if (hasColumn(posItemsMsg, "pluGroups.items")) {

            posItemsMsg = posItemsMsg.withColumn("explode_items", when(posItemsMsg.col("pluGroups.items").isNotNull, posItemsMsg.col("pluGroups.items")).otherwise(lit(null)))

            if (checkArray(posItemsMsg, "explode_items")) {
              posItemsMsg = posItemsMsg.withColumn("items", explode_outer($"explode_items"))

              /*   posItemsMsg = posItemsMsg
                .withColumn("items_number", when(posItemsMsg.col("items.number").isNotNull, posItemsMsg.col("items.number")).otherwise(lit(null)))
                .withColumn("items_guid", when(posItemsMsg.col("items.guid").isNotNull, posItemsMsg.col("items.guid")).otherwise(lit(null)))
                .withColumn("items_name", when(posItemsMsg.col("items.name").isNotNull, posItemsMsg.col("items.name")).otherwise(lit(null)))
                .withColumn("items_active", when(posItemsMsg.col("items.active").isNotNull, posItemsMsg.col("items.active")).otherwise(lit(null)))
              */
              if (hasColumn(posItemsMsg, "items.number")) {
                posItemsMsg = posItemsMsg.withColumn("items_number", when(posItemsMsg.col("items.number").isNotNull, posItemsMsg.col("items.number")).otherwise(lit(null)))
              } else {
                posItemsMsg = posItemsMsg.withColumn("items_number", lit(null))
              }

              if (hasColumn(posItemsMsg, "items.guid")) {
                posItemsMsg = posItemsMsg.withColumn("items_guid", when(posItemsMsg.col("items.guid").isNotNull, posItemsMsg.col("items.guid")).otherwise(lit(null)))
              } else {
                posItemsMsg = posItemsMsg.withColumn("items_guid", lit(null))
              }

              if (hasColumn(posItemsMsg, "items.name")) {
                posItemsMsg = posItemsMsg.withColumn("items_name", when(posItemsMsg.col("items.name").isNotNull, posItemsMsg.col("items.name")).otherwise(lit(null)))
              } else {
                posItemsMsg = posItemsMsg.withColumn("items_name", lit(null))
              }
              if (hasColumn(posItemsMsg, "items.active")) {
                posItemsMsg = posItemsMsg.withColumn("items_active", when(posItemsMsg.col("items.active").isNotNull, posItemsMsg.col("items.active")).otherwise(lit(null)))
              } else {
                posItemsMsg = posItemsMsg.withColumn("items_active", lit(null))
              }

            }

          } else {
            posItemsMsg = posItemsMsg.withColumn("items_number", lit(null))
              .withColumn("items_guid", lit(null))
              .withColumn("items_name", lit(null))
              .withColumn("items_active", lit(null))

          }

          val items = posItemsMsg.select("VoyageId", "BatchTime", "salesGroup_guid", "categories_guid", "pluGroups_guid", "items_number", "items_guid", "items_name", "items_active", "generated_date", "ShipCode", "partDate")

          items.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.items.table"))
         // items.show()
          println("Pos item data has been written")
          if (hasColumn(posItemsMsg, "items.prices")) {

            posItemsMsg = posItemsMsg.withColumn("explode_prices", when(posItemsMsg.col("items.prices").isNotNull, posItemsMsg.col("items.prices")).otherwise(lit(null)))
            if (checkArray(posItemsMsg, "explode_prices")) {

              posItemsMsg = posItemsMsg.withColumn("prices", explode_outer($"explode_prices"))

              /*      posItemsMsg = posItemsMsg.withColumn("prices_guid", when(posItemsMsg.col("prices.guid").isNotNull, posItemsMsg.col("prices.guid")).otherwise(lit(null)))
                .withColumn("prices_price", when(posItemsMsg.col("prices.price").isNotNull, posItemsMsg.col("prices.price")).otherwise(lit(null)))
                .withColumn("prices_validFrom", when(posItemsMsg.col("prices.validFrom").isNotNull, posItemsMsg.col("prices.validFrom")).otherwise(lit(null)))
                .withColumn("prices_validTo", when(posItemsMsg.col("prices.validTo").isNotNull, posItemsMsg.col("prices.validTo")).otherwise(lit(null)))
                .withColumn("prices_currencyId", when(posItemsMsg.col("prices.currencyId").isNotNull, posItemsMsg.col("prices.currencyId")).otherwise(lit(null)))
                .withColumn("prices_active", when(posItemsMsg.col("prices.active").isNotNull, posItemsMsg.col("prices.active")).otherwise(lit(null)))

              */
              if (hasColumn(posItemsMsg, "prices.guid")) {
                posItemsMsg = posItemsMsg.withColumn("prices_guid", when(posItemsMsg.col("prices.guid").isNotNull, posItemsMsg.col("prices.guid")).otherwise(lit(null)))
              } else {
                posItemsMsg = posItemsMsg.withColumn("prices_guid", lit(null))
              }
              if (hasColumn(posItemsMsg, "prices.price")) {
                posItemsMsg = posItemsMsg.withColumn("prices_price", when(posItemsMsg.col("prices.price").isNotNull, posItemsMsg.col("prices.price")).otherwise(lit(null)))
              } else {
                posItemsMsg = posItemsMsg.withColumn("prices_price", lit(null))
              }
              if (hasColumn(posItemsMsg, "prices.validFrom")) {
                posItemsMsg = posItemsMsg.withColumn("prices_validFrom", when(posItemsMsg.col("prices.validFrom").isNotNull, posItemsMsg.col("prices.validFrom")).otherwise(lit(null)))
              } else {
                posItemsMsg = posItemsMsg.withColumn("prices_validFrom", lit(null))
              }
              if (hasColumn(posItemsMsg, "prices.validTo")) {
                posItemsMsg = posItemsMsg.withColumn("prices_validTo", when(posItemsMsg.col("prices.validTo").isNotNull, posItemsMsg.col("prices.validTo")).otherwise(lit(null)))
              } else {
                posItemsMsg = posItemsMsg.withColumn("prices_validTo", lit(null))
              }
              if (hasColumn(posItemsMsg, "prices.currencyId")) {
                posItemsMsg = posItemsMsg.withColumn("prices_currencyId", when(posItemsMsg.col("prices.currencyId").isNotNull, posItemsMsg.col("prices.currencyId")).otherwise(lit(null)))
              } else {
                posItemsMsg = posItemsMsg.withColumn("prices_currencyId", lit(null))
              }
              if (hasColumn(posItemsMsg, "prices.active")) {
                posItemsMsg = posItemsMsg.withColumn("prices_active", when(posItemsMsg.col("prices.active").isNotNull, posItemsMsg.col("prices.active")).otherwise(lit(null)))
              } else {
                posItemsMsg = posItemsMsg.withColumn("prices_active", lit(null))
              }

              if (hasColumn(posItemsMsg, "prices.shipCode")) {
                posItemsMsg = posItemsMsg.withColumn("ship_code", when(posItemsMsg.col("prices.shipCode").isNotNull, posItemsMsg.col("prices.shipCode")).otherwise(lit(null)))
              } else {
                posItemsMsg = posItemsMsg.withColumn("ship_code", lit(null))
              }

            }

          } else {
            posItemsMsg = posItemsMsg.withColumn("prices_guid", lit(null))
              .withColumn("prices_price", lit(null))
              .withColumn("prices_validFrom", lit(null))
              .withColumn("prices_validTo", lit(null))
              .withColumn("prices_currencyId", lit(null))
              .withColumn("prices_active", lit(null))
              .withColumn("ship_code", lit(null))
          }

          val prices = posItemsMsg.select("VoyageId", "BatchTime", "salesGroup_guid", "categories_guid", "pluGroups_guid", "items_guid", "prices_guid", "prices_price", "prices_validFrom", "prices_validTo", "prices_currencyId", "prices_active", "ship_code", "generated_date", "ShipCode", "partDate")

          prices.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.prices.table"))
         // prices.show()
//          posItemsMsg.show(20, false)
          println("Pos price data has been written")

        } else {

          log.info("salesGroup message blank")

        }
      }

      ManageMetadata.updateStatus(batchInstanceId, batchId, "Successful", spark)

      def checkArray(df: DataFrame, colname: String): Boolean = {

        df.schema(colname).dataType match {
          case ArrayType(_, _) => return true
          case _               => return false
        }
      }

      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

    } catch {

      case e: Exception =>
        {

          ManageMetadata.updateStatus(batchInstanceId, batchId, "Failed", spark)
          log.info("in the catch of updateStatus ******************")
          e.printStackTrace()
          throw new Exception("General Exception..please check the stacktrace")
        }

    }

  }
}