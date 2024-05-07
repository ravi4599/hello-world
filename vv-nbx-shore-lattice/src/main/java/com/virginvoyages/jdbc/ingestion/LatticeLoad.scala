package com.virginvoyages.jdbc.ingestion

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import com.virginvoyages.metadataframework.ManageMetadata
import org.apache.spark.sql.types.{ StringType, TimestampType }
import org.apache.spark.sql.functions._
import java.sql.SQLException
import org.apache.spark._
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import org.apache.hadoop.fs._
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.ZonedDateTime
import java.time.Instant
import java.time.Duration  

object LatticeLoad {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    var batchInstanceId: String = null
    var batchId: String = null
    try {
      sqlContext.setConf("hive.exec.dynamic.partition", "true")
      sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
      val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
/*************************************calling metadata framework*********************************************/
      val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)

      metadata.productIterator.foreach(println)
      batchId = metadata._1
      batchInstanceId = metadata._2
      val batchStartTime = metadata._3
      val batchEndTime = metadata._4
      val startExecutionTiime = metadata._7
      val partWriteDate = metadata._8

      val voyageId = spark.sparkContext.getConf.get("spark.voyage.id")

     
      import org.apache.spark.sql.types.{ StringType, TimestampType, IntegerType }
      import org.apache.spark.sql.functions.{ unix_timestamp, to_date }
	  
       val selectQuery = """(SELECT DISTINCT CONCAT(p1.PERSON_FIRST_NAME,' ',p1.PERSON_LAST_NAME) as "Name", po.POSITION_NAME as "Title",    lower(pa.PERSON_E_MAIL) as "Email", FORMAT(p1.PERSON_DOB, 'MM/dd/yyyy') as "Birth Date", CASE WHEN p1.PERSON_GENDER = 'F' THEN 'Female' WHEN p1.PERSON_GENDER = 'M' THEN 'Male' ELSE 'Non Binary' END AS "Gender", FORMAT(p1.PERSON_FIRST_STAY, 'MM/dd/yyyy') as "Start Date", CASE WHEN p1.ACTIVE = 1 THEN 'Active' ELSE 'Inactive' END AS "Status", dep.ORG_DEPT_NAME as "Department", '' as "Manager Email", p1.PERSON_ID as "Employee ID", c2.COUNTRY_NAME as "Nationality", CASE WHEN pb.ORG_UNIT_ID = 2 THEN 'Scarlet Lady' WHEN pb.ORG_UNIT_ID = 3 THEN 'Shipyard' WHEN pb.ORG_UNIT_ID = 4 THEN 'Valiant Lady' ELSE 'Unknown' END AS "Work Location" FROM dbo.Persons p1 JOIN ( SELECT max(pb2.ARRIVAL_DATE) as "LatestArrivalDate", pb2.PERSON_ID FROM dbo.Person_Booking pb2 WHERE pb2.MANIFEST_TYPE = 'E' and pb2.PERSON_BOOKING_STATUS_ID = 1 and pb2.REC_DELETED = 0 GROUP BY pb2.PERSON_ID ) pbsub ON p1.PERSON_ID = pbsub.PERSON_ID JOIN dbo.Person_Booking pb ON pbsub.PERSON_ID = pb.PERSON_ID and pbsub.LatestArrivalDate = pb.ARRIVAL_DATE JOIN dbo.Person_Address pa ON p1.PERSON_ID = pa.PERSON_ID JOIN dbo.Positions po ON pb.POSITION_ID = po.POSITION_ID JOIN dbo.Org_Depts dep ON po.DEPARTMENT_ID = dep.ORG_DEPT_NAME_ID JOIN dbo.Countries c2 ON c2.COUNTRY_ID = p1.PERSON_NATIONALITY_COUNTRY_ID WHERE 1=1 and p1.ACTIVE = 1 and p1.PERSON_TYPE_ID = 2 and p1.PERSON_FIRST_STAY <= GETDATE() and pa.PERSON_IS_MAILING_ADDRESS = 1 and pa.PERSON_ADDRESS_TYPE_ID = 1 and pb.REC_DELETED = 0 and pa.PERSON_E_MAIL is not null ORDER BY Department, Name OFFSET 0 ROWS) as lattice"""
      
       //Connecting to source database
      val data = spark.read.format(sparkConfiguration.value.get("spark.src.con.format").get).options(Map("url" -> sparkConfiguration.value.get("spark.src.con.url").get, "user" -> sparkConfiguration.value.get("spark.src.user").get, "password" -> sparkConfiguration.value.get("spark.src.password").get,
        "dbtable" -> selectQuery, "driver" -> sparkConfiguration.value.get("spark.src.con.driver").get)).load()
      data.show
       
      /*  val data = spark.read.format("jdbc").options(Map("url" ->":5432/SEAWARE", "user" -> "rep_user", "password" ->"reportdev@321",
        "dbtable" -> "\"SEAWARE\".\"REVENUE_TARGET_VALUE\"", "driver" -> "org.postgresql.Driver")).load()*/
			
    val fileName=spark.sqlContext.sparkContext.getConf.get("spark.target.filename").concat("_").concat(DateTimeFormatter.ofPattern("MMddyyyy").format(LocalDateTime.now))
			
			val temploc = spark.sqlContext.sparkContext.getConf.get("spark.temp").trim()
			val finalloc = spark.sqlContext.sparkContext.getConf.get("spark.target.location").trim()
			val src = new Path(spark.sqlContext.sparkContext.getConf.get("spark.temp").trim())
			val dest = new Path(spark.sqlContext.sparkContext.getConf.get("spark.target.location").trim())
			val bckloc= spark.sqlContext.sparkContext.getConf.get("spark.target.backuplocation").trim()

      
			val conf = spark.sparkContext.hadoopConfiguration
			val fs = src.getFileSystem(conf)
			
			data.coalesce(1).write.format("csv").option("header", "true").mode("Overwrite").save(temploc)
			data.coalesce(1).write.format("csv").option("header","true").mode("append").save(bckloc)
			
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
			
		//data.write.mode("overwrite").insertInto(spark.sparkContext.getConf.get("spark.target.location"))
		//data.write.mode("overwrite").insertInto(spark.sparkContext.getConf.get("spark.target.bkplocation"))
		//  s3://vv-dev-emr-cluster/data/landing/Betterup/
		//  Backup target: s3://vv-dev-emr-cluster/data/landing/bkupBetterup/<date>
      
      //function call to update status as successful
      ManageMetadata.updateStatus(batchInstanceId, batchId, "Successful", spark)
    } catch {

      case e: SQLException => {
        ManageMetadata.updateStatus(batchInstanceId, batchId, "Failed", spark)
        log.info("***************in the catch of Konami Ingestion ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace")
      }
      case e: Exception =>
        {
          ManageMetadata.updateStatus(batchInstanceId, batchId, "Failed", spark)
          log.info("***************in the catch of Konami Ingestion ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace")
        }
        System.exit(1)

    }
  }
}
