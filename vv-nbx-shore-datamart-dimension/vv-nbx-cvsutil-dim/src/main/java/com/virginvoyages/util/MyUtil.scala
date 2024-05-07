package com.virginvoyages.util

import java.sql.SQLException
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DataType._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._

import com.virginvoyages.metadataframework.ManageMetadata

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import scala.util.control._

object MyUtil {
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = sparkSession.sparkContext

    val sparkConfiguration = sc.broadcast(sc.getConf.getAll.toMap)
   
  
    import java.util.Calendar
    val now = Calendar.getInstance.getTime

    val date = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val todaydate = date.format(now)
   
      val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, sparkSession)
      //batch_instance_id1, batch_id1
      val batch_id1 = metadata._1
      val batch_instance_id1 = metadata._2
      val batchStartTme = metadata._3
      val batchEndTme = metadata._4
      val partReadStart = metadata._5

      val partReadEnd = metadata._6
      val startExecutionTime = metadata._7
      val part_write_date = metadata._8
      try {

        log.info("----------------------- Start logic from here-----------------------")

        sparkSession.conf.set("spark.debug.maxToStringFields", 1000)

        val prefixWithDate = sparkSession.sqlContext.sparkContext.getConf.getBoolean("spark.prefix_with_date", true)

        println("**** To Get The Agency List from Config ****")

        val Agency = sparkSession.sparkContext.getConf.get("spark.agency.list").trim()

        val AgencyArray = Agency.split(",")

        val temploc = sparkSession.sqlContext.sparkContext.getConf.get("spark.temp").trim()

        val baseFileName = sparkSession.sqlContext.sparkContext.getConf.get("spark.filename").trim()
        
        print("baseFileName .." +baseFileName)

       // for (agencyID <- AgencyArray) {
       //   val Agencyid = agencyID
          
          //val agency_name =List(378,483,3010,3011,3014,3044,5782,8161,12861,11877) zip List("imagine-cruising-limited","alfendo-ltd","virgin-atlantic","barrhead-travel-service-ltd","cruise-118","sunshine-cruise-holidays-limited","www-cruise-co-uk","aviate","travcorp-holdings-ltd","iglu-com-transport-ltd-netrates") toMap
          
          val agency_name =List(378,483,3010,3011,3014,3044,5782,8161,18448,5344,12861,11877) zip List("imagine-cruising-limited","alfendo-ltd","virgin-atlantic","barrhead-travel-service-ltd","cruise-118","sunshine-cruise-holidays-limited","www-cruise-co-uk","aviate","our-vacation-centre-tour-operator","cruise-circle-travel-circle","travcorp-holdings-ltd","iglu-com-transport-ltd-netrates") toMap
          
           
          for ((agency_id,agency_name_v) <- agency_name){
          println(s"The Agency Name is: $agency_name_v")
            agency_name_v
            
          val whereClause = "where prd.agency_id = " + agency_id
          
          println(" WHere clause values    "+whereClause)

          log.info("----------------------- Executing Source Query-----------------------")
          val MYDF = sparkSession.sql(sparkSession.sparkContext.getConf.get("spark.query").replace("*whereclause*", whereClause))
           println("MYDF value......" )
          MYDF.show(2,false)
         // val temppath = temploc + "-" + agencyID + "/temp"
          
          val temppath = temploc + "-" + agency_name_v + "/temp"
          println(temppath)

          MYDF.coalesce(1).write.format("csv").option("header", "true").mode("Overwrite").save(temppath)
          println("*** File has been written in Temp Path ***" + temppath )

          val src = new Path(temppath)
          val conf = sparkSession.sparkContext.hadoopConfiguration
          val fs = src.getFileSystem(conf)
          val status = fs.listStatus(src).map(_.getPath.toString)
          val csvfiles = status.filter(line => line.contains("csv"))
          val sourcePath = csvfiles(0).toString
          println("Source_path**   " + sourcePath) 
         
          

          // for ((k,v) <- agency_name) println(s"value: $v")
          
          //val filename = baseFileName + "_" + agencyID

          val filename = baseFileName + "_" + agency_name_v
          
          print("FileName for Agency.." + filename)
          
          //val destFile = temploc + "-" + agencyID
          
          val destFile = temploc + "-" + agency_name_v

          val dest = new Path(destFile)

          val targetPath = s"${destFile}/${filename}.csv"
          println("targetPath   ==> " + targetPath)
          val targetHadoopPath = new Path(targetPath)
          if (fs.exists(targetHadoopPath)) {
            fs.delete(targetHadoopPath, true)
          }

          fs.rename(new Path(sourcePath), targetHadoopPath);
        }
    //  }

        ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", sparkSession)
        log.info(todaydate + "----------------------- End logic from here-----------------------")
      } catch {
        case e: SQLException => {
          ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", sparkSession);
          e.printStackTrace();
          log.info(todaydate + "connectioin issue..please check service");
        }
        case e: Exception => {
          ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", sparkSession);
          throw new Exception("General Exception..please check the stacktrace")
        }
      }
     sparkSession.stop()
    log.info(todaydate + "------  Spark Connection close-----------------------------")
  }
}