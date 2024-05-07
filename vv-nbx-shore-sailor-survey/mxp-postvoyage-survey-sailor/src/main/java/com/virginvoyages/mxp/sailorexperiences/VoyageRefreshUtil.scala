package com.virginvoyages.mxp.sailorexperiences
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.sql.SQLException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.virginvoyages.metadataframework.ManageMetadata
object VoyageRefreshUtil {

  /**
   * Initilize thte logger
   */
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]) {

     val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
    
  //  val sparkSession = SparkSession.builder().getOrCreate()
    //val sc = sparkSession.sparkContext
   // val sparkConfiguration = sc.broadcast(sc.getConf.getAll.toMap)
    val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
    //batch_instance_id1, batch_id1
    val batch_id1 = metadata._1
    val batch_instance_id1 = metadata._2
    val srcPKCols = sc.getConf.get("spark.source.primaryKeyColumns")
    val srcColsPKList = srcPKCols.split(",")
    val srcPKColSEQ = srcColsPKList.map(x => col(x)).toSeq
    try {

      /**
       * Acxiom Newsletter Nov data
       */
      val windowSpec = Window.partitionBy("SELECTEDSAILINGGROUP_VOYAGEID").orderBy(col("SELECTEDSAILINGGROUP_START").asc,col("SELECTEDSAILINGGROUP_END").desc)
      log.info(s"[START]----- reading phoenix table ${spark.sparkContext.getConf.get("spark.reservation.table.name")} as dataframe")
      /*val seawareReservationDF = sparkSession.sqlContext.read.format("org.apache.phoenix.spark")
        .option("inferSchema", "true")
        .option("table", sparkSession.sqlContext.sparkContext.getConf.get("spark.reservation.table.name"))
        .option("header", "true")
        .option("zkUrl", sparkSession.sqlContext.sparkContext.getConf.get("spark.zkurl")).load()*/
      log.info("---------------------------------------------------------  dataframe call  --------------------------------------------------")

      import spark.sqlContext.implicits._

      log.info("distinctVoyages:::::::")
      var distinctVoyages = spark.sql("""select  pkgdim.package_code as SELECTEDSAILINGGROUP_VOYAGEID, ship.ship as CRUISELINE_SHIPCODE, to_date(sail.sail_date_from) as SELECTEDSAILINGGROUP_START, to_date(sail.sail_date_to) as SELECTEDSAILINGGROUP_END,to_timestamp(current_timestamp(),'dd-MM-yyyy HH24:mi:ss') as  LOADDATETIME, 'N' as IsPostVygFlag,'N' as IsFirstMtFlag,'N' as IsRemiFlag, md5(concat(pkgdim.package_code,ship.ship,to_date(sail.sail_date_from),to_date(sail.sail_date_to))) as md5SailorSurvey 
    from vv_db.hvtb_nbx_core_sw_package_dim pkgdim join vv_db.hvtb_nbx_core_sw_sail_dim sail on pkgdim.src_sail_id  = sail.src_sail_id
    join vv_db.hvtb_nbx_core_sw_ship_dim ship on sail.ship_id = ship.ship_id
    where pkgdim.rec_end_dttm = '9999-12-31 00:00:00' and pkgdim.package_class = 'VOYAGE'
    and sail.rec_end_dttm = '9999-12-31 00:00:00'
    and pkgdim.is_active  = 'Y'
    and sail.is_active = 'Y'
    and to_date(sail.sail_date_from) >= to_date(current_timestamp()) """)
    
    
 //seawareReservationDF.select("SELECTEDSAILINGGROUP_VOYAGEID", "CRUISELINE_SHIPCODE", "SELECTEDSAILINGGROUP_START", "SELECTEDSAILINGGROUP_END").filter(!(isnull($"SELECTEDSAILINGGROUP_VOYAGEID"))).distinct().withColumn("row_number", row_number.over(windowSpec)).withColumn("startdatestr2date", (col("SELECTEDSAILINGGROUP_START").cast("date"))).withColumn("enddatestr2date", (col("SELECTEDSAILINGGROUP_END").cast("date"))).withColumn("current_date", current_date()).where(col("row_number") === 1).drop("row_number").withColumn("md5SailorSurvey", md5(concat_ws(",", seawareReservationDF.select(srcPKColSEQ: _*).columns.map(c => col(c)): _*)))
      log.info("voyageSailorDf:::::::")
       val pguser=spark.sparkContext.getConf.get("spark.target.user").trim()
       val pgpassword=spark.sparkContext.getConf.get("spark.target.password").trim()
       val jdbcUrl = spark.sparkContext.getConf.get("spark.target.con.url").trim() 
       val tableName =  spark.sparkContext.getConf.get("spark.target.ops.table").trim()
       val jdbcDriver = spark.sparkContext.getConf.get("spark.target.con.driver").trim()
       val con_format = spark.sparkContext.getConf.get("spark.target.con.format").trim()
       val op_mode = spark.sparkContext.getConf.get("spark.target.ops.table.mode").trim()
          
      
     /* var voyageSailorDf = spark.sqlContext.read.format("org.apache.phoenix.spark").option("inferSchema", "true").option("table", spark.sqlContext.sparkContext.getConf.get("spark.voyage.table")).option("header", "true").option("zkUrl", spark.sqlContext.sparkContext.getConf.get("spark.zkurl")).load() */

 var voyageSailorDf = spark.read.format(con_format).option("driver",jdbcDriver).option("dbtable",tableName).option("url",jdbcUrl)
        .option("user",pguser).option("password",pgpassword).load()
        voyageSailorDf.show(2)

      log.info("voyageSailorDf.head(1).isEmpty:::::::" + voyageSailorDf.head(1).isEmpty)
      if (voyageSailorDf.head(1).isEmpty) {
        log.info("before write:::::::")
        //drop("startdatestr2date","enddatestr2date","current_date")        
        distinctVoyages = distinctVoyages.select("SELECTEDSAILINGGROUP_VOYAGEID", "CRUISELINE_SHIPCODE", "SELECTEDSAILINGGROUP_START", "SELECTEDSAILINGGROUP_END", "LOADDATETIME", "IsPostVygFlag","IsFirstMtFlag","IsRemiFlag","md5SailorSurvey")
        distinctVoyages = distinctVoyages.withColumn("SELECTEDSAILINGGROUP_START",col("SELECTEDSAILINGGROUP_START").cast(StringType)).withColumn("SELECTEDSAILINGGROUP_END",col("SELECTEDSAILINGGROUP_END").cast(StringType))
             distinctVoyages.write
        .format(con_format)
        .mode("Append")
        .option("driver",jdbcDriver)
        .option("dbtable", tableName)
        .option("url", jdbcUrl)
        .option("user", pguser)
        .option("password", pgpassword)
        .save
      } else {

       
        voyageSailorDf = voyageSailorDf.withColumn("row_number", row_number.over(windowSpec)).where(col("row_number") === 1).drop("row_number").distinct().withColumn("startdatestr2date", (col("SELECTEDSAILINGGROUP_START").cast("date"))).withColumn("enddatestr2date", (col("SELECTEDSAILINGGROUP_END").cast("date"))).withColumn("current_date", current_date().as("current_date"))
        voyageSailorDf = voyageSailorDf.where($"startdatestr2date" >= $"current_date")
       voyageSailorDf.show(2,false)
        voyageSailorDf.printSchema()

       // val newdataDf = distinctVoyages.alias("reserv").join(voyageSailorDf.alias("voyage"), (distinctVoyages("SELECTEDSAILINGGROUP_VOYAGEID") === voyageSailorDf("SELECTEDSAILINGGROUP_VOYAGEID")), "left").select(distinctVoyages("*")).where(col("voyage.SELECTEDSAILINGGROUP_VOYAGEID").isNull).select("SELECTEDSAILINGGROUP_VOYAGEID", "CRUISELINE_SHIPCODE", "SELECTEDSAILINGGROUP_START", "SELECTEDSAILINGGROUP_END", "LOADDATETIME", "IsPostVygFlag", "IsFirstMtFlag", "IsRemiFlag", "md5SailorSurvey")
        
        val newdataDf = distinctVoyages.alias("reserv").join(voyageSailorDf.alias("voyage"), (distinctVoyages("SELECTEDSAILINGGROUP_VOYAGEID") === voyageSailorDf("SELECTEDSAILINGGROUP_VOYAGEID")), "left").withColumn("update_flag", when(voyageSailorDf("SELECTEDSAILINGGROUP_VOYAGEID").isNull, "I")).filter(col("update_flag") === "I").select(distinctVoyages("*")).select("SELECTEDSAILINGGROUP_VOYAGEID", "CRUISELINE_SHIPCODE", "SELECTEDSAILINGGROUP_START", "SELECTEDSAILINGGROUP_END", "LOADDATETIME", "IsPostVygFlag", "IsFirstMtFlag", "IsRemiFlag", "md5SailorSurvey")
         newdataDf.show(2,false)
       // val newdataDf = spark.sql("""Select distinctVoyages.SELECTEDSAILINGGROUP_VOYAGEID,distinctVoyages.CRUISELINE_SHIPCODE,distinctVoyages.SELECTEDSAILINGGROUP_START,distinctVoyages.SELECTEDSAILINGGROUP_END,distinctVoyages.LOADDATETIME,distinctVoyages.IsPostVygFlag,distinctVoyages.IsFirstMtFlag,distinctVoyages.IsRemiFlag,distinctVoyages.md5SailorSurvey,case when voyageSailorDf.SELECTEDSAILINGGROUP_VOYAGEID is null then 'I' end as update_flag from distinctVoyages left join voyageSailorDf on distinctVoyages.SELECTEDSAILINGGROUP_VOYAGEID = voyageSailorDf.SELECTEDSAILINGGROUP_VOYAGEID""")

newdataDf.createOrReplaceTempView("newdataDftbl")

val newdatDffinal = spark.sql("select SELECTEDSAILINGGROUP_VOYAGEID,CRUISELINE_SHIPCODE, cast(SELECTEDSAILINGGROUP_START as string) as SELECTEDSAILINGGROUP_START, cast(SELECTEDSAILINGGROUP_END as string ) as SELECTEDSAILINGGROUP_END,LOADDATETIME,IsPostVygFlag,IsFirstMtFlag,IsRemiFlag,md5SailorSurvey from newdataDftbl")

//val newdataDfinal = spark.sql(" select SELECTEDSAILINGGROUP_VOYAGEID,CRUISELINE_SHIPCODE,cast(SELECTEDSAILINGGROUP_START as String) as SELECTEDSAILINGGROUP_START,cast(SELECTEDSAILINGGROUP_END as String) as SELECTEDSAILINGGROUP_END,LOADDATETIME,IsPostVygFlag,IsFirstMtFlag,IsRemiFlag,md5SailorSurvey from newdataDftbl")

//val newdataDfinal = newdatDf.withColumn("SELECTEDSAILINGGROUP_START",col("SELECTEDSAILINGGROUP_START").cast(StringType)).withColumn("SELECTEDSAILINGGROUP_START",col("SELECTEDSAILINGGROUP_END").cast(StringType))
newdatDffinal.printSchema()   

          /*newdataDf.write
          .format(sparkSession.sqlContext.sparkContext.getConf.get("spark.voyage.format"))
          .mode(sparkSession.sqlContext.sparkContext.getConf.get("spark.voyage.mode"))
          .option("table", sparkSession.sqlContext.sparkContext.getConf.get("spark.voyage.table"))
          .option("zkUrl", sparkSession.sqlContext.sparkContext.getConf.get("spark.zkurl"))
          .save()*/
        log.info("newdataDf:::::::")
        
        
        val updatedataDf=distinctVoyages.alias("reserv").join(voyageSailorDf.alias("voyage"),(distinctVoyages("SELECTEDSAILINGGROUP_VOYAGEID") === voyageSailorDf("SELECTEDSAILINGGROUP_VOYAGEID")), "full").withColumn("update_flag", when(voyageSailorDf("md5SailorSurvey") =!= distinctVoyages("md5SailorSurvey"), "U")).filter(col("update_flag") === "U").select(distinctVoyages("*")).select("SELECTEDSAILINGGROUP_VOYAGEID", "CRUISELINE_SHIPCODE", "SELECTEDSAILINGGROUP_START", "SELECTEDSAILINGGROUP_END", "LOADDATETIME", "IsPostVygFlag", "IsFirstMtFlag", "IsRemiFlag","md5SailorSurvey")

updatedataDf.createOrReplaceTempView("updatedataDftbl")

//val updatedatDffinal = spark.sql("""select SELECTEDSAILINGGROUP_VOYAGEID,CRUISELINE_SHIPCODE, cast(SELECTEDSAILINGGROUP_START as string) as SELECTEDSAILINGGROUP_START , cast(SELECTEDSAILINGGROUP_END as string) as SELECTEDSAILINGGROUP_END,LOADDATETIME,IsPostVygFlag,IsFirstMtFlag,IsRemiFlag,md5SailorSurvey from updatedataDftbl""")

val updatedatDffinal = spark.sql("""select SELECTEDSAILINGGROUP_VOYAGEID,CRUISELINE_SHIPCODE, cast(SELECTEDSAILINGGROUP_START as string) as SELECTEDSAILINGGROUP_START , cast(SELECTEDSAILINGGROUP_END as string) as SELECTEDSAILINGGROUP_END,cast(LOADDATETIME as timestamp),IsPostVygFlag,IsFirstMtFlag,IsRemiFlag,md5SailorSurvey from updatedataDftbl""")

updatedatDffinal.printSchema()
updatedatDffinal.show(2,false)

        log.info("updatedataDf:::::::")
        val newupdateDf = Seq(newdatDffinal,updatedatDffinal)
        val finalDf = newupdateDf.reduce(_ union _)
        log.info("finalDf:::::::")
       finalDf.write
        .format(con_format)
        .mode("Append")
        .option("driver",jdbcDriver)
        .option("dbtable", tableName)
        .option("url", jdbcUrl)
        .option("user", pguser)
        .option("password", pgpassword)
        .save

      }
      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)
    } catch {
      case e: SQLException => {
        e.printStackTrace(); log.info("Cloud SQL connectioin issue..please check Postgre service");

      }
      case e: Exception => { log.info("******************in the catch of VoyageRefreshUtil ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace") }
    }

    spark.stop()
  }
}
