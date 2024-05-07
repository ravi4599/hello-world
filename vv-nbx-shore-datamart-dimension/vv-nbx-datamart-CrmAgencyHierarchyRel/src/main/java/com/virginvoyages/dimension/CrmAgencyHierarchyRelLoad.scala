package com.virginvoyages.dimension

import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.DriverManager
import scala.collection.mutable.ListBuffer

case class AgencyHierarchy(
  agency_id: String,
  child_agency_id: String,
  agency_name: String,
  child_agency_name: String,
  seaware_agency_id: String,
  child_seaware_agency_id: String,
  immediate_parent_agency_id: String,
  level: Int
)

object CrmAgencyHierarchyRelLoad {
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def agencyRec(cachedJoin: DataFrame, varAgencyId: String, Level: Int, AgencyId: String, AgencyName: String, SeawareAgencyId: String): ListBuffer[AgencyHierarchy] = {
    val results = new ListBuffer[AgencyHierarchy]()
    val currentAgency = cachedJoin.where(s"""AgencyId = \"$varAgencyId\" AND AccountRecordType = 'Agency'""").first
    val currentResult = AgencyHierarchy(
      agency_id = AgencyId,
      child_agency_id = currentAgency.getAs("AgencyId"),
      agency_name = AgencyName,
      child_agency_name = currentAgency.getAs("AgencyName"),
      seaware_agency_id = SeawareAgencyId,
      child_seaware_agency_id = currentAgency.getAs("SeawareAgencyId"),
      immediate_parent_agency_id = currentAgency.getAs("ParentId"),
      level = Level
    )
    results += currentResult

    val children = cachedJoin.where(s"""ParentId = \"${currentResult.child_agency_id}\" AND ParentId <> '' AND AccountRecordType = 'Agency'""").collect()
    children.foreach(c => {
      // println("Children")
      results.appendAll(agencyRec(cachedJoin, c.getAs("AgencyId"), Level + 1, AgencyId, AgencyName, SeawareAgencyId))
    })
    results
  }

  def getSparkSession() = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    spark
  }

  def main(args: Array[String]) {
    val spark = getSparkSession()

    spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)

    try {
       val cachedJoin = spark.sql(s"""select AgencyId, AgencyName, SeawareAgencyId, acc.AccountRecordType, ParentId from ( select id as AgencyId, name as AgencyName, seaware_agency_id__c as SeawareAgencyId, parentid as ParentId, recordtypeId,AccountRecordType from vv_db.hvtb_nbx_core_crm_account)acc left join ( select name, id from vv_db.hvtb_parse_sfdc_contact)contact on acc.recordtypeId = contact.id
        """).cache()

      val seedAgencies = cachedJoin.where("AccountRecordType = 'Agency'").collect()
      val result = new ListBuffer[AgencyHierarchy]()
      seedAgencies.foreach(agency => {
        result.appendAll(agencyRec(cachedJoin, agency.getAs("AgencyId"), 0,
          agency.getAs("AgencyId"), agency.getAs("AgencyName"), agency.getAs("SeawareAgencyId")))
      })

      val tempDF = spark.createDataFrame(result)
      tempDF.show(false)

      val targetTemp = spark.sparkContext.getConf.get("spark.target.temp").trim()
      tempDF.repartition(15).write.mode("Overwrite").parquet(targetTemp)

      val pond_table = spark.sparkContext.getConf.get("spark.pond.database").trim() + "." + spark.sparkContext.getConf.get("spark.pond.table").trim()
      spark.sql("REFRESH TABLE " + pond_table)

      
      val connectionURL = spark.sparkContext.getConf.get("spark.metadata.con.url").trim()
      val user = spark.sparkContext.getConf.get("spark.metadata.user").trim()
      val passWord = spark.sparkContext.getConf.get("spark.metadata.password").trim()
      val sqlConnection = DriverManager.getConnection(connectionURL, user, passWord)
	  
     // val bqTableName = spark.sparkContext.getConf.get("spark.bq.schema").trim() + "." + spark.sparkContext.getConf.get("spark.bq.tablename").trim()
      
	  //val AlterBigQueryToTemp = sqlConnection.prepareStatement(s"Alter Table $bqTableName SET LOCATION '$targetTemp' ")
      //AlterBigQueryToTemp.executeUpdate()
      //AlterBigQueryToTemp.close()

      val targetFinal = spark.sparkContext.getConf.get("spark.target.location").trim()
      val finalDF = spark.read.parquet(targetTemp)
      finalDF.repartition(15).write.mode("Overwrite").parquet(targetFinal)
      spark.sql("REFRESH TABLE " + pond_table)

     // val AlterBigQueryToFinal = sqlConnection.prepareStatement(s"Alter Table $bqTableName SET LOCATION '$targetFinal' ")
     // AlterBigQueryToFinal.executeUpdate()
	  //AlterBigQueryToFinal.close()
	  
      sqlConnection.close()
    } catch {
      case e: Exception => {
        e.printStackTrace();
        throw new Exception("General Exception..please check the stacktrace")
      }
    }

    spark.stop()
  }
}