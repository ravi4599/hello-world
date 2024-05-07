package com.virginvoyages.parser
import org.apache.spark.SparkConf
import java.sql.SQLException
//import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
//import org.apache.spark.streaming.Seconds
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
import org.apache.spark.sql.types.IntegerType

import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.Column
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
//import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Column, DataFrame }
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat

//XML validator imports
import org.apache.spark.SparkConf
import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import org.xml.sax.SAXException;
import java.io._
import java.nio.charset.StandardCharsets
import org.apache.spark.SparkFiles
import scala.util.Try
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import com.virginvoyages.metadataframework.ManageMetadata

object RTSStatusDetailParser 
{
def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    //val streamingContext = new StreamingContext(spark.sparkContext, Seconds(20))
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    import spark.implicits._

    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    var batch_instance_id1: String = null
    var batch_id1: String = null
	
	 val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
    metadata.productIterator.foreach(println)

    val batch_start_time = metadata._3
    val batch_end_time = metadata._4
    val part_start_time = metadata._5.toString()
    val part_end_time = metadata._6.toString()
    val start_execution_time = metadata._7.toString()
    val part_write_date = metadata._8.toString()
    batch_instance_id1 = metadata._2
    batch_id1 = metadata._1

    try {
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
      //val whereClause = s""" where batchtime>= '$batchtStartTime' and batchtime<='$batchEndTime' and Part_Date>='$partReadStart' and Part_Date<='$partEndStart'"""

      //println(whereClause)
      val statusDf = spark.sql(spark.sparkContext.getConf.get("spark.source.table").trim()) //+ whereClause)
      var data = statusDf.select("Message", "BatchTime", "Part_Date").where($"BatchTime" >= lit(batch_start_time).cast(TimestampType) && $"BatchTime" <= lit(batch_end_time).cast(TimestampType) and ($"part_date".between(part_start_time, part_end_time)))
      val statusMsg = data.select("Message").rdd.map(x => x.toString)
      var statusData = spark.read.json(statusMsg)

        //statusData.printSchema

       // statusData.show(false)
if (!statusData.head(1).isEmpty)
 {
 
       if (hasColumn(statusData, "pl.checkinGuestDetails")) {
 
                                   statusData = statusData.withColumn("checkinGuestDetails", explode_outer($"pl.checkinGuestDetails"))
					  
					   if (hasColumn(statusData, "checkinGuestDetails.guestId"))
					   {
					   statusData = statusData.withColumn("guestid", when(statusData.col("checkinGuestDetails.guestId").isNotNull, statusData.col("checkinGuestDetails.guestId")).otherwise(lit(null)))} 
	            	   else 
					   {
                       statusData = statusData.withColumn("guestid", lit(null))
                       }
					   //7th July
					   
					      if (hasColumn(statusData, "checkinGuestDetails.reservationId"))
					   {
					   statusData = statusData.withColumn("reservationId", when(statusData.col("checkinGuestDetails.reservationId").isNotNull, statusData.col("checkinGuestDetails.reservationId")).otherwise(lit(null)))} 
	            	   else 
					   {
                       statusData = statusData.withColumn("reservationId", lit(null))
                       }
					   
		
					      if (hasColumn(statusData, "checkinGuestDetails.reservationInfo.voyageNumber"))
					   {
					   statusData = statusData.withColumn("voyageNumber", when(statusData.col("checkinGuestDetails.reservationInfo.voyageNumber").isNotNull, statusData.col("checkinGuestDetails.reservationInfo.voyageNumber")).otherwise(lit(null)))} 
	            	   else 
					   {
                       statusData = statusData.withColumn("voyageNumber", lit(null))
                       }
					   
					       if (hasColumn(statusData, "checkinGuestDetails.reservationInfo.stateroom"))
					   {
					   statusData = statusData.withColumn("stateroom", when(statusData.col("checkinGuestDetails.reservationInfo.stateroom").isNotNull, statusData.col("checkinGuestDetails.reservationInfo.stateroom")).otherwise(lit(null)))} 
	            	   else 
					   {
                       statusData = statusData.withColumn("stateroom", lit(null))
                       }
					   
		//7th july ends here
		
		               if (hasColumn(statusData, "checkinGuestDetails.reservationGuestId"))
					   {
                       statusData = statusData.withColumn("reservationguestid", when(statusData.col("checkinGuestDetails.reservationGuestId").isNotNull, statusData.col("checkinGuestDetails.reservationGuestId")).otherwise(lit(null)))
                        } 
						else 
						{
                       statusData = statusData.withColumn("reservationguestid", lit(null))
					   }
					   
					   if (hasColumn(statusData, "checkinGuestDetails.reservationInfo.reservationNumber"))
					   {
                       statusData = statusData.withColumn("reservationnumber", when(statusData.col("checkinGuestDetails.reservationInfo.reservationNumber").isNotNull, statusData.col("checkinGuestDetails.reservationInfo.reservationNumber")).otherwise(lit(null)))
                       }
					   else
					   {
                       statusData = statusData.withColumn("reservationnumber", lit(null))
                       }
					   
					   if (hasColumn(statusData, "checkinGuestDetails.reservationInfo.embarkDate"))
					   {
                       statusData = statusData.withColumn("reservationembarkdate", when(statusData.col("checkinGuestDetails.reservationInfo.embarkDate").isNotNull, statusData.col("checkinGuestDetails.reservationInfo.embarkDate")).otherwise(lit(null)))
					   } 
					   else
					   {
                       statusData = statusData.withColumn("reservationembarkdate", lit(null))
                       }
					   
					   if (hasColumn(statusData, "checkinGuestDetails.reservationInfo.debarkDate")) {
                       statusData = statusData.withColumn("reservationdebarkdate", when(statusData.col("checkinGuestDetails.reservationInfo.debarkDate").isNotNull, statusData.col("checkinGuestDetails.reservationInfo.debarkDate")).otherwise(lit(null)))
                       } else {
                       statusData = statusData.withColumn("reservationdebarkdate", lit(null))
                       }
					  

					  /*additional columns added as part of 13.1 from checkindetails.reservationinfo*/
					  
					  				   if (hasColumn(statusData, "checkinGuestDetails.reservationInfo.lastModificationTime")) {
          statusData = statusData
            .withColumn("lastmodificationtime", when(statusData.col("checkinGuestDetails.reservationInfo.lastModificationTime").isNotNull, statusData.col("checkinGuestDetails.reservationInfo.lastModificationTime")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("lastmodificationtime", lit(null))
		  }
		  
					  
					  					   if (hasColumn(statusData, "checkinGuestDetails.reservationInfo.shipCode")) {
          statusData = statusData
            .withColumn("shipcode", when(statusData.col("checkinGuestDetails.reservationInfo.shipCode").isNotNull, statusData.col("checkinGuestDetails.reservationInfo.shipCode")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("shipcode", lit(null))
		  }
					   
					   if (hasColumn(statusData, "checkinGuestDetails.reservationInfo.statusCode")) {
          statusData = statusData
            .withColumn("statuscode", when(statusData.col("checkinGuestDetails.reservationInfo.statusCode").isNotNull, statusData.col("checkinGuestDetails.reservationInfo.statusCode")).otherwise(lit(null)))

        } 
		else {
          statusData = statusData.withColumn("statuscode", lit(null))
		  }
		  
		  if (hasColumn(statusData, "checkinGuestDetails.reservationInfo.paymentStatusCode")) {
          statusData = statusData
            .withColumn("paymentstatuscode", when(statusData.col("checkinGuestDetails.reservationInfo.paymentStatusCode").isNotNull, statusData.col("checkinGuestDetails.reservationInfo.paymentStatusCode")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("paymentstatuscode", lit(null))
		  }
		  
		  if (hasColumn(statusData, "checkinGuestDetails.reservationInfo.guestModerationStatus")) {
          statusData = statusData
            .withColumn("guestmoderationstatus", when(statusData.col("checkinGuestDetails.reservationInfo.guestModerationStatus").isNotNull, statusData.col("checkinGuestDetails.reservationInfo.guestModerationStatus")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("guestmoderationstatus", lit(null))
		  }
		  
		  if (hasColumn(statusData, "checkinGuestDetails.reservationInfo.guestOverallDocumentStatus")) {
          statusData = statusData
            .withColumn("guestoveralldocumentstatus", when(statusData.col("checkinGuestDetails.reservationInfo.guestOverallDocumentStatus").isNotNull, statusData.col("checkinGuestDetails.reservationInfo.guestOverallDocumentStatus")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("guestoveralldocumentstatus", lit(null))
		  }
		  
		   if (hasColumn(statusData, "checkinGuestDetails.reservationInfo.isGuestOnBoard")) {
          statusData = statusData
            .withColumn("isguestonboard", when(statusData.col("checkinGuestDetails.reservationInfo.isGuestOnBoard").isNotNull, statusData.col("checkinGuestDetails.reservationInfo.isGuestOnBoard")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("isguestonboard", lit(null))
		  }
		  
		   if (hasColumn(statusData, "checkinGuestDetails.reservationInfo.isValidated")) {
          statusData = statusData
            .withColumn("isvalidated", when(statusData.col("checkinGuestDetails.reservationInfo.isValidated").isNotNull, statusData.col("checkinGuestDetails.reservationInfo.isValidated")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("isvalidated", lit(null))
		  }
		  
		   if (hasColumn(statusData, "checkinGuestDetails.reservationInfo.isAssociatedFlightBooking")) {
          statusData = statusData
            .withColumn("isassociatedflightbooking", when(statusData.col("checkinGuestDetails.reservationInfo.isAssociatedFlightBooking").isNotNull, statusData.col("checkinGuestDetails.reservationInfo.isAssociatedFlightBooking")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("isassociatedflightbooking", lit(null))
		  }
		  
		  if (hasColumn(statusData, "checkinGuestDetails.reservationInfo.isItineraryRulesApplied")) {
          statusData = statusData
            .withColumn("isitineraryrulesapplied", when(statusData.col("checkinGuestDetails.reservationInfo.isItineraryRulesApplied").isNotNull, statusData.col("checkinGuestDetails.reservationInfo.isItineraryRulesApplied")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("isitineraryrulesapplied", lit(null))
		  }
		  
		  if (hasColumn(statusData, "checkinGuestDetails.completedCheckinStatus.SECURITY_PHOTO")) {
          statusData = statusData
            .withColumn("checkinguestdetails_security_photo", when(statusData.col("checkinGuestDetails.completedCheckinStatus.SECURITY_PHOTO").isNotNull, statusData.col("checkinGuestDetails.completedCheckinStatus.SECURITY_PHOTO")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("checkinguestdetails_security_photo", lit(null))
		  }
		  
		  if (hasColumn(statusData, "checkinGuestDetails.completedCheckinStatus.HEALTH_QUESTIONS")) {
          statusData = statusData
            .withColumn("health_questions", when(statusData.col("checkinGuestDetails.completedCheckinStatus.HEALTH_QUESTIONS").isNotNull, statusData.col("checkinGuestDetails.completedCheckinStatus.HEALTH_QUESTIONS")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("health_questions", lit(null))
		  }
		  
		  if (hasColumn(statusData, "checkinGuestDetails.completedCheckinStatus.EMERGENCY_CONTACT")) {
          statusData = statusData
            .withColumn("checkinguestdetails_emergency_contact", when(statusData.col("checkinGuestDetails.completedCheckinStatus.EMERGENCY_CONTACT").isNotNull, statusData.col("checkinGuestDetails.completedCheckinStatus.EMERGENCY_CONTACT")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("checkinguestdetails_emergency_contact", lit(null))
		  }
		  
		  if (hasColumn(statusData, "checkinGuestDetails.completedCheckinStatus.TRAVEL_DOCUMENTS")) {
          statusData = statusData
            .withColumn("checkinguestdetails_travel_documents", when(statusData.col("checkinGuestDetails.completedCheckinStatus.TRAVEL_DOCUMENTS").isNotNull, statusData.col("checkinGuestDetails.completedCheckinStatus.TRAVEL_DOCUMENTS")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("checkinguestdetails_travel_documents", lit(null))
		  }
		  
		  if (hasColumn(statusData, "checkinGuestDetails.completedCheckinStatus.POST_VOYAGE")) {
          statusData = statusData
            .withColumn("post_voyage", when(statusData.col("checkinGuestDetails.completedCheckinStatus.POST_VOYAGE").isNotNull, statusData.col("checkinGuestDetails.completedCheckinStatus.POST_VOYAGE")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("post_voyage", lit(null))
		  }
		  
		  if (hasColumn(statusData, "checkinGuestDetails.completedCheckinStatus.EMBARKATION_SLOT")) {
          statusData = statusData
            .withColumn("checkinguestdetails_embarkation_slot", when(statusData.col("checkinGuestDetails.completedCheckinStatus.EMBARKATION_SLOT").isNotNull, statusData.col("checkinGuestDetails.completedCheckinStatus.EMBARKATION_SLOT")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("checkinguestdetails_embarkation_slot", lit(null))
		  }
		  
		  if (hasColumn(statusData, "checkinGuestDetails.completedCheckinStatus.VOYAGE_CONTRACT")) {
          statusData = statusData
            .withColumn("checkinguestdetails_voyage_contract", when(statusData.col("checkinGuestDetails.completedCheckinStatus.VOYAGE_CONTRACT").isNotNull, statusData.col("checkinGuestDetails.completedCheckinStatus.VOYAGE_CONTRACT")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("checkinguestdetails_voyage_contract", lit(null))
		  }
		  
		   if (hasColumn(statusData, "checkinGuestDetails.completedCheckinStatus.PAYMENT_METHOD")) {
          statusData = statusData
            .withColumn("checkinguestdetails_payment_method", when(statusData.col("checkinGuestDetails.completedCheckinStatus.PAYMENT_METHOD").isNotNull, statusData.col("checkinGuestDetails.completedCheckinStatus.PAYMENT_METHOD")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("checkinguestdetails_payment_method", lit(null))
		  }
		  
		  if (hasColumn(statusData, "checkinGuestDetails.completedCheckinStatus.PREGNANCY")) {
          statusData = statusData
            .withColumn("checkinguestdetails_pregnancy", when(statusData.col("checkinGuestDetails.completedCheckinStatus.PREGNANCY").isNotNull, statusData.col("checkinGuestDetails.completedCheckinStatus.PREGNANCY")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("checkinguestdetails_pregnancy", lit(null))
		  }
		  
		   if (hasColumn(statusData, "checkinGuestDetails.percentageCheckinStatus.SECURITY_PHOTO")) {
          statusData = statusData
            .withColumn("percentagecheckinstatus_security_photo", when(statusData.col("checkinGuestDetails.percentageCheckinStatus.SECURITY_PHOTO").isNotNull, statusData.col("checkinGuestDetails.percentageCheckinStatus.SECURITY_PHOTO")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("percentagecheckinstatus_security_photo", lit(null))
		  }
		  
		  if (hasColumn(statusData, "checkinGuestDetails.percentageCheckinStatus.EMERGENCY_CONTACT")) {
          statusData = statusData
            .withColumn("percentagecheckinstatus_emergency_contact", when(statusData.col("checkinGuestDetails.percentageCheckinStatus.EMERGENCY_CONTACT").isNotNull, statusData.col("checkinGuestDetails.percentageCheckinStatus.EMERGENCY_CONTACT")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("percentagecheckinstatus_emergency_contact", lit(null))
		  }
		  
		  if (hasColumn(statusData, "checkinGuestDetails.percentageCheckinStatus.TRAVEL_DOCUMENTS")) {
          statusData = statusData
            .withColumn("percentagecheckinstatus_travel_documents", when(statusData.col("checkinGuestDetails.percentageCheckinStatus.TRAVEL_DOCUMENTS").isNotNull, statusData.col("checkinGuestDetails.percentageCheckinStatus.TRAVEL_DOCUMENTS")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("percentagecheckinstatus_travel_documents", lit(null))
		  }
		  
		  if (hasColumn(statusData, "checkinGuestDetails.percentageCheckinStatus.EMBARKATION_SLOT")) {
          statusData = statusData
            .withColumn("percentagecheckinstatus_embarkation_slot", when(statusData.col("checkinGuestDetails.percentageCheckinStatus.EMBARKATION_SLOT").isNotNull, statusData.col("checkinGuestDetails.percentageCheckinStatus.EMBARKATION_SLOT")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("percentagecheckinstatus_embarkation_slot", lit(null))
		  }
		  
		  if (hasColumn(statusData, "checkinGuestDetails.percentageCheckinStatus.PAYMENT_METHOD")) {
          statusData = statusData
            .withColumn("percentagecheckinstatus_payment_method", when(statusData.col("checkinGuestDetails.percentageCheckinStatus.PAYMENT_METHOD").isNotNull, statusData.col("checkinGuestDetails.percentageCheckinStatus.PAYMENT_METHOD")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("percentagecheckinstatus_payment_method", lit(null))
		  }
		  
		    if (hasColumn(statusData, "checkinGuestDetails.percentageCheckinStatus.VOYAGE_CONTRACT")) {
          statusData = statusData
            .withColumn("percentagecheckinstatus_voyage_contract", when(statusData.col("checkinGuestDetails.percentageCheckinStatus.VOYAGE_CONTRACT").isNotNull, statusData.col("checkinGuestDetails.percentageCheckinStatus.VOYAGE_CONTRACT")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("percentagecheckinstatus_voyage_contract", lit(null))
		  }
		  
		  if (hasColumn(statusData, "checkinGuestDetails.totalPercentageCheckinStatus")) {
          statusData = statusData
            .withColumn("checkinguestdetails_totalPercentageCheckinStatus", when(statusData.col("checkinGuestDetails.totalPercentageCheckinStatus").isNotNull, statusData.col("checkinGuestDetails.totalPercentageCheckinStatus")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("checkinguestdetails_totalPercentageCheckinStatus", lit(null))
		  }

		  if (hasColumn(statusData, "n")) {
          statusData = statusData
            .withColumn("n", when(statusData.col("n").isNotNull, statusData.col("n")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("n", lit(null))
		  }
		  
		   if (hasColumn(statusData, "ts")) {
          statusData = statusData
            .withColumn("ts", when(statusData.col("ts").isNotNull, statusData.col("ts")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("ts", lit(null))
		  }
		  
		  
		  if (hasColumn(statusData, "checkinGuestDetails.travelInfo.preCruiseInfo.prePostCruiseId")) {
          statusData = statusData.withColumn("precruiseinfo_prepostcruiseid", when(statusData.col("checkinGuestDetails.travelInfo.preCruiseInfo.prePostCruiseId").isNotNull, statusData.col("checkinGuestDetails.travelInfo.preCruiseInfo.prePostCruiseId")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("precruiseinfo_prepostcruiseid", lit(null))

        }
		
		if (hasColumn(statusData, "checkinGuestDetails.travelInfo.preCruiseInfo.travelByDetails.transportationTypeCode")) {
          statusData = statusData.withColumn("precruiseinfo_transportationtypecode", when(statusData.col("checkinGuestDetails.travelInfo.preCruiseInfo.travelByDetails.transportationTypeCode").isNotNull, statusData.col("checkinGuestDetails.travelInfo.preCruiseInfo.travelByDetails.transportationTypeCode")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("precruiseinfo_transportationtypecode", lit(null))

        }
		

        if (hasColumn(statusData, "checkinGuestDetails.travelInfo.preCruiseInfo.flightDetails")) {
		
		print("inside precruiseinfo")
		  
            statusData = statusData.withColumn("flightDetails", explode_outer($"checkinGuestDetails.travelInfo.preCruiseInfo.flightDetails"))
			
            if (hasColumn(statusData, "flightDetails.flightNumber")) {
              statusData = statusData.withColumn("precruiseinfo_flightnumber", when(statusData.col("flightDetails.flightNumber").isNotNull, statusData.col("flightDetails.flightNumber")).otherwise(lit(null)))

            } else {
              statusData = statusData.withColumn("precruiseinfo_flightnumber", lit(null))

            }
			
			//7th July
			
			  if (hasColumn(statusData, "flightDetails.departureAirportCode")) {
              statusData = statusData.withColumn("precruiseinfo_departureairportcode", when(statusData.col("flightDetails.departureAirportCode").isNotNull, statusData.col("flightDetails.departureAirportCode")).otherwise(lit(null)))

            } else {
              statusData = statusData.withColumn("precruiseinfo_departureairportcode", lit(null))

            }
			
			  if (hasColumn(statusData, "flightDetails.departureCity")) {
              statusData = statusData.withColumn("precruiseinfo_departurecity", when(statusData.col("flightDetails.departureCity").isNotNull, statusData.col("flightDetails.departureCity")).otherwise(lit(null)))

            } else {
              statusData = statusData.withColumn("precruiseinfo_departurecity", lit(null))

            }
			
			 if (hasColumn(statusData, "flightDetails.departureTime")) {
              statusData = statusData.withColumn("precruiseinfo_departuretime", when(statusData.col("flightDetails.departureTime").isNotNull, statusData.col("flightDetails.departureTime")).otherwise(lit(null)))

            } else {
              statusData = statusData.withColumn("precruiseinfo_departuretime", lit(null))

            }
			
			 if (hasColumn(statusData, "flightDetails.arrivalAirportCode")) {
              statusData = statusData.withColumn("precruiseinfo_arrivalairportcode", when(statusData.col("flightDetails.arrivalAirportCode").isNotNull, statusData.col("flightDetails.arrivalAirportCode")).otherwise(lit(null)))

            } else {
              statusData = statusData.withColumn("precruiseinfo_arrivalairportcode", lit(null))

            }
			
			if (hasColumn(statusData, "flightDetails.arrivalCity")) {
              statusData = statusData.withColumn("precruiseinfo_arrivalcity", when(statusData.col("flightDetails.arrivalCity").isNotNull, statusData.col("flightDetails.arrivalCity")).otherwise(lit(null)))

            } else {
              statusData = statusData.withColumn("precruiseinfo_arrivalcity", lit(null))

            }
			
			if (hasColumn(statusData, "flightDetails.isDeleted")) {
              statusData = statusData.withColumn("precruiseinfo_isdeleted", when(statusData.col("flightDetails.isDeleted").isNotNull, statusData.col("flightDetails.isDeleted")).otherwise(lit(null)))

            } else {
              statusData = statusData.withColumn("precruiseinfo_isdeleted", lit(null))

            }
			//7thJuly ends here
			
            if (hasColumn(statusData, "flightDetails.arrivalTime")) {
              statusData = statusData
                .withColumn("precruiseinfo_arrivaltime", when(statusData.col("flightDetails.arrivalTime").isNotNull, statusData.col("flightDetails.arrivalTime")).otherwise(lit(null)))

            } else {
              statusData = statusData.withColumn("precruiseinfo_arrivaltime", lit(null))

            }

            if (hasColumn(statusData, "flightDetails.departureTime")) {
              statusData = statusData
                .withColumn("precruiseinfo_departuretime", when(statusData.col("flightDetails.departureTime").isNotNull, statusData.col("flightDetails.departureTime")).otherwise(lit(null)))

            } else {
              statusData = statusData.withColumn("precruiseinfo_departuretime", lit(null))

            }
			
			if (hasColumn(statusData, "flightDetails.airlineCode")) {
              statusData = statusData
                .withColumn("precruiseinfo_airlinecode", when(statusData.col("flightDetails.airlineCode").isNotNull, statusData.col("flightDetails.airlineCode")).otherwise(lit(null)))

            } else {
              statusData = statusData.withColumn("precruiseinfo_airlinecode", lit(null))

            }
			
		
		  }
         else {

             statusData = statusData.withColumn("precruiseinfo_flightnumber",lit(null))
            .withColumn("precruiseinfo_arrivaltime", lit(null))
            .withColumn("precruiseinfo_airlinecode", lit(null))

        }
      
	   if (hasColumn(statusData, "checkinGuestDetails.travelInfo.postCruiseInfo.prePostCruiseId")) {
          statusData = statusData
            .withColumn("postcruiseinfo_prepostcruiseid", when(statusData.col("checkinGuestDetails.travelInfo.postCruiseInfo.prePostCruiseId").isNotNull, statusData.col("checkinGuestDetails.travelInfo.postCruiseInfo.prePostCruiseId")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("postcruiseinfo_prepostcruiseid", lit(null))

        }
		
		
        if (hasColumn(statusData, "checkinGuestDetails.travelInfo.postCruiseInfo.flightDetails")) {

        statusData = statusData.withColumn("flightDetails", explode_outer($"checkinGuestDetails.travelInfo.postCruiseInfo.flightDetails"))
       

        if (hasColumn(statusData, "flightDetails.flightNumber")) {
          statusData = statusData
            .withColumn("postcruiseinfo_flightnumber", when(statusData.col("flightDetails.flightNumber").isNotNull, statusData.col("flightDetails.flightNumber")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("postcruiseinfo_flightnumber", lit(null))

        }
		
		//7th July 
		
		if (hasColumn(statusData, "flightDetails.departureAirportCode")) {
          statusData = statusData
            .withColumn("postcruiseinfo_departureairportcode", when(statusData.col("flightDetails.departureAirportCode").isNotNull, statusData.col("flightDetails.departureAirportCode")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("postcruiseinfo_departureairportcode", lit(null))

        }
		
		if (hasColumn(statusData, "flightDetails.departureCity")) {
          statusData = statusData
            .withColumn("postcruiseinfo_departurecity", when(statusData.col("flightDetails.departureCity").isNotNull, statusData.col("flightDetails.departureCity")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("postcruiseinfo_departurecity", lit(null))

        }
				
		
		if (hasColumn(statusData, "flightDetails.arrivalAirportCode")) {
          statusData = statusData
            .withColumn("postcruiseinfo_arrivalairportcode", when(statusData.col("flightDetails.arrivalAirportCode").isNotNull, statusData.col("flightDetails.arrivalAirportCode")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("postcruiseinfo_arrivalairportcode", lit(null))

        }
		
		if (hasColumn(statusData, "flightDetails.arrivalCity")) {
          statusData = statusData
            .withColumn("postcruiseinfo_arrivalcity", when(statusData.col("flightDetails.arrivalCity").isNotNull, statusData.col("flightDetails.arrivalCity")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("postcruiseinfo_arrivalcity", lit(null))

        }
		
		
		if (hasColumn(statusData, "flightDetails.arrivalTime")) {
          statusData = statusData
            .withColumn("postcruiseinfo_arrivaltime", when(statusData.col("flightDetails.arrivalTime").isNotNull, statusData.col("flightDetails.arrivalTime")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("postcruiseinfo_arrivaltime", lit(null))

        }
		
		
		if (hasColumn(statusData, "flightDetails.isDeleted")) {
          statusData = statusData
            .withColumn("postcruiseinfo_isdeleted", when(statusData.col("flightDetails.isDeleted").isNotNull, statusData.col("flightDetails.isDeleted")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("postcruiseinfo_isdeleted", lit(null))

        }

		 //7th July ends here

        if (hasColumn(statusData, "flightDetails.flyingOutPlanCode")) {
          statusData = statusData
            .withColumn("postcruiseinfo_flyingoutplancode", when(statusData.col("flightDetails.flyingOutPlanCode").isNotNull, statusData.col("flightDetails.flyingOutPlanCode")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("postcruiseinfo_flyingoutplancode", lit(null))

        }
        if (hasColumn(statusData, "flightDetails.isOpted")) {
          statusData = statusData
            .withColumn("postcruiseinfo_isopted", when(statusData.col("flightDetails.isOpted").isNotNull, statusData.col("flightDetails.isOpted")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("postcruiseinfo_isopted", lit(null))

        }
		if (hasColumn(statusData, "flightDetails.departureTime")) {
          statusData = statusData
            .withColumn("postcruiseinfo_departureTime", when(statusData.col("flightDetails.departureTime").isNotNull, statusData.col("flightDetails.departureTime")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("postcruiseinfo_departureTime", lit(null))

        }
		
		if (hasColumn(statusData, "flightDetails.airlineCode")) {
          statusData = statusData
            .withColumn("postcruiseinfo_airlinecode", when(statusData.col("flightDetails.airlineCode").isNotNull, statusData.col("flightDetails.airlineCode")).otherwise(lit(null)))

        } else {
          statusData = statusData.withColumn("postcruiseinfo_airlinecode", lit(null))

        }
		
        }
		else {
           statusData = statusData
            .withColumn("postcruiseinfo_flightnumber",lit(null))
            .withColumn("postcruiseinfo_flyingoutplancode", lit(null))
            .withColumn("postcruiseinfo_isopted", lit(null))
			.withColumn("postcruiseinfo_departureTime", lit(null))
			.withColumn("postcruiseinfo_airlinecode", lit(null))
        }
		
		
		
		
        statusData = statusData.withColumn("BatchTime", lit(start_execution_time).cast(TimestampType))
          .withColumn("Part_date", to_date(lit(part_write_date)))
          .withColumn("VoyageID", lit(spark.sparkContext.getConf.get("spark.voyage.id")))
		  .withColumn("ts", col("ts").cast(TimestampType))
		  .withColumn("lastmodificationtime", col("lastmodificationtime").cast(TimestampType))
		  
		  
        
		statusData.createOrReplaceTempView("finaldf") 
		var finalData = spark.sql("select * from finaldf where GuestId is not null and ReservationNumber is not null")
		
		var checkinguest = finalData.select("GuestId","reservationId", "ReservationNumber", "reservationguestid","voyageNumber","stateroom", "reservationembarkdate", "reservationdebarkdate","statuscode","paymentstatuscode","guestmoderationstatus", "guestoveralldocumentstatus","isguestonboard","isvalidated","isassociatedflightbooking","isitineraryrulesapplied","checkinguestdetails_security_photo","health_questions","checkinguestdetails_emergency_contact","checkinguestdetails_travel_documents","post_voyage","checkinguestdetails_embarkation_slot","checkinguestdetails_voyage_contract","checkinguestdetails_payment_method","checkinguestdetails_pregnancy","percentagecheckinstatus_security_photo","percentagecheckinstatus_emergency_contact","percentagecheckinstatus_travel_documents","percentagecheckinstatus_embarkation_slot","percentagecheckinstatus_payment_method","percentagecheckinstatus_voyage_contract","checkinguestdetails_totalpercentagecheckinstatus","lastmodificationtime","n","precruiseinfo_prepostcruiseid","postcruiseinfo_prepostcruiseid","ts","BatchTime", "VoyageID","shipcode", "Part_date")
		checkinguest.printSchema
		
	    //checkinguest.createOrReplaceTempView("checkinguesttemp")
		//var checkinguestfinal = spark.sql("select * from checkinguesttemp where GuestId is not null and ReservationNumber is not null")
		
		checkinguest.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.checkinGuestDetail.target.table"))
		
		var precruiseinfo = finalData.select("GuestId", "ReservationNumber","precruiseinfo_transportationtypecode","precruiseinfo_flightnumber", "precruiseinfo_arrivaltime", "precruiseinfo_departuretime","precruiseinfo_airlinecode","n","precruiseinfo_departureairportcode","precruiseinfo_departurecity","precruiseinfo_arrivalairportcode","precruiseinfo_arrivalcity","precruiseinfo_isdeleted","precruiseinfo_prepostcruiseid","ts","BatchTime", "VoyageID","shipcode", "Part_date")
		precruiseinfo.printSchema
		//precruiseinfo.createOrReplaceTempView("precruiseinfotemp")
		//var precruiseinfo = spark.sql("select * from precruiseinfotemp where GuestId is not null and ReservationNumber is not null")
		
		precruiseinfo.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.precruiseinfo.target.table"))
		
		var postcruiseinfo = finalData.select("GuestId", "ReservationNumber","postcruiseinfo_flightnumber", "postcruiseinfo_flyingoutplancode", "postcruiseinfo_isopted","postcruiseinfo_departureTime","postcruiseinfo_airlinecode","n","postcruiseinfo_departureairportcode","postcruiseinfo_departurecity","postcruiseinfo_arrivalairportcode","postcruiseinfo_arrivalcity","postcruiseinfo_arrivaltime","postcruiseinfo_isdeleted","postcruiseinfo_prepostcruiseid","ts","BatchTime", "VoyageID","shipcode","Part_date")
		postcruiseinfo.printSchema
		//postcruiseinfo.createOrReplaceTempView("postcruiseinfotemp")
		//var postcruiseinfofinal = spark.sql("GuestId, cast(ReservationNumber as int) as ReservationNumber,postcruiseinfo_flightnumber, postcruiseinfo_flyingoutplancode, postcruiseinfo_isopted,n,postcruiseinfo_prepostcruiseid,BatchTime, VoyageID,shipcode,Part_date from postcruiseinfotemp")
		
		postcruiseinfo.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.postcruiseinfo.target.table"))
       
        ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
      }
      }
    } catch {

      case e: Exception =>

        {
          ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark)
		   
          log.info("in the catch of updateStatus ******************")

          e.printStackTrace()

          throw new Exception("General Exception..please check the stacktrace")

        }

    }

  }
}
