package com.virginvoyages.shore.facts

import java.sql.SQLException

import com.virginvoyages.metadataframework.ManageMetadata
import com.virginvoyages.scd.ChangeDataCapture.loadDimFact
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession

object WellFactLoad {
  def getSparkSession(): SparkSession = {
        SparkSession
          .builder()
          .enableHiveSupport()
          .getOrCreate()
    }

    def main(args: Array[String]) {
        val log = LogManager.getRootLogger
        log.setLevel(Level.INFO)

        val spark = getSparkSession()
        val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
        spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
        spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

        /** ***********************************calling metadata framework *********************************************/
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
            
			
			//val whereClause = s""" where rg.batchtime>= '$batch_start_tme' and rg.batchtime<='$batch_end_tme' and rg.part_date >='$part_read_start' and rg.part_date <='$part_read_end'"""
							//println(s"""===========Starting the execution for $whereClause""")
							//log.info(s"""===========Starting the execution for $whereClause""")
			
            val sourceDf = spark.sql(spark.sparkContext.getConf.get("spark.source.sql").trim())

            if (!sourceDf.head(1).isEmpty) {
                log.info("Loading request fact table")
                loadDimFact(spark: SparkSession, sourceDf)
            }

            log.info("No incremental data found")
            log.info("Updating Metadata framework")
            ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
        } catch {
            case e: SQLException => {
                ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
                log.info("******************in the catch of Voyage Well Fact Load ******************");
                e.printStackTrace();
                throw new Exception("SQL Exception..please check the stacktrace", e);
            }
            println("#----------------------------Process Has Failed---------------------------#")
            System.exit(1)
            spark.stop()
        }
    }
}