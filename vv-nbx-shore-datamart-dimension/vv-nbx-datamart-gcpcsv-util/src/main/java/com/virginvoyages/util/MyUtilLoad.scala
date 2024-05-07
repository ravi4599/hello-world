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
//import com.virginvoyages.metadataframework.ManageExecution
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import scala.util.control._
/*
 * Adding MetaData Logging Code and Execution Check code Based on Changes.
 * Edited in Sprint 10.2
 * Edited By Aaron Rebello
 * Original Author Enrique
 *
 * */
object MyUtil {

	val log = LogManager.getRootLogger
			log.setLevel(Level.INFO)

			def main(args: Array[String]) {

		val sparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
				val sc = sparkSession.sparkContext

				val sparkConfiguration = sc.broadcast(sc.getConf.getAll.toMap)

				println( "After the loop" );
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

			// The output file is generated with name defined in `spark.filename`. If `spark.prefix_with_date`
			// configured as true (default value), it's prefixed with the current date in the format `yyyyMMdd`
			val baseFileName = sparkSession.sqlContext.sparkContext.getConf.get("spark.filename").trim()
			val prefixWithDate = sparkSession.sqlContext.sparkContext.getConf.getBoolean("spark.prefix_with_date", true)
			val fileName = (if (prefixWithDate) DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now) else "").concat(baseFileName)

			val temploc = sparkSession.sqlContext.sparkContext.getConf.get("spark.temp").trim()
			val finalloc = sparkSession.sqlContext.sparkContext.getConf.get("spark.location").trim()
			val src = new Path(sparkSession.sqlContext.sparkContext.getConf.get("spark.temp").trim())
			val dest = new Path(sparkSession.sqlContext.sparkContext.getConf.get("spark.location").trim())

			val conf = sparkSession.sparkContext.hadoopConfiguration
			val fs = src.getFileSystem(conf)

			val MYDF = sparkSession.sql(sparkSession.sqlContext.sparkContext.getConf.get("spark.query").trim())
			//MYDF.count()
			//MYDF.show(10, false)
			//log.info("We reached the mydf:"+ MYDF.count())
			if(MYDF.count()!=0) {
			MYDF.coalesce(1).write.format("csv").option("header", "true").mode("Overwrite").save(temploc)
			
			val status = fs.listStatus(src).map(_.getPath.toString)
			val csvfiles = status.filter(line => line.contains("csv"))
			val sourcePath = csvfiles(0).toString
      
			val targetPath = s"${finalloc}/${fileName}.csv"
			
			println("==>" + targetPath)
			val targetHadoopPath = new Path(targetPath)
			if (fs.exists(targetHadoopPath)) {
				fs.delete(targetHadoopPath, true)
			}

			fs.rename(new Path(sourcePath), targetHadoopPath);
			}
			else {
			  log.info("There is no data")
			}
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
		{
			//ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "N/A", sparkSession);
			println(s"""#----------------------Job did not execute for the aforementioned reason--------------------------#""")
		}

		sparkSession.stop()
		log.info(todaydate + "------  Spark Connection close-----------------------------")
	}
}