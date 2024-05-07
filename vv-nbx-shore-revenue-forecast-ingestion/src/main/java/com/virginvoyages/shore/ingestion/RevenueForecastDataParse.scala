package com.virginvoyages.shore.ingestion
import java.net.URI

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.storage.StorageLevel

import com.virginvoyages.metadataframework.ManageMetadata

object RevenueForecastDataParse {

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
        var excelDF: DataFrame = null
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    val exceltabs = spark.sparkContext.getConf.get("spark.excel.tabs").split(",").toList
    val replaceColumnUDF = udf(replaceColumn)
    val findFileTimestampUDF = udf(findFileTimestamp)
    val inputfilepath = spark.sparkContext.getConf.get("spark.s3file.location").trim()
    try {
      if (FileSystem.get(new URI(inputfilepath), sc.hadoopConfiguration).exists(new Path(inputfilepath))) {
        println("Directory exist :::" + inputfilepath)
      }
      val files = FileSystem.get(new URI(inputfilepath), sc.hadoopConfiguration).listStatus(new Path(inputfilepath))
      val fileslist = files.map(_.getPath.getName).toList
      import spark.implicits._
      var fileslistDF = fileslist.toDF("filename")
      fileslistDF = fileslistDF.filter(col("filename").contains(".xlsx"))
      fileslistDF.createOrReplaceTempView("fileslistDFtbl")
      fileslistDF = fileslistDF.withColumn("timestamp", findFileTimestampUDF(col("filename")))
      val windowSpec = Window.orderBy(col("timestamp").desc)
      fileslistDF = fileslistDF.withColumn("row_number", row_number.over(windowSpec))
      val file_name = fileslistDF.filter(col("row_number") === 1).head().getString(0)
      /*time stamp code with spark sql
     val getFileNamedd = spark.sql("""select filenames, regexp_replace((substring_index((substring_index(filenames,'.',1)),'revenue_kpi_plan_forecast_upload_',-1)),'-','') as fff from fileslistDFtbl """)
     val getFileName1 = spark.sql(""" select filenames from (select filenames, row_number() OVER ( ORDER BY (regexp_replace((regexp_replace((substring_index((substring_index(filenames,'.',1)),'revenue_kpi_plan_forecast_upload_',-1)),'-','')),':','')) desc) as rownum from fileslistDFtbl ) outerQry where rownum =1 """)
     getFileName1.show(false)*/
      if (fileslistDF.filter(col("row_number") === 1).count() == 1) {
        println(" reading exceltabs:::::" + exceltabs)
        for (tabname <- exceltabs) {
          if (excelDF == null) {
            excelDF = readExcel(spark, inputfilepath + file_name, tabname)
          } else {
            /* union all excel tabs records */
            var tempdf = readExcel(spark, inputfilepath + file_name, tabname)
            tempdf = tempdf.withColumn("SAIL_DATE", col("SAIL_DATE").cast(TimestampType))
                           .withColumn("SAIL_DAYS", col("SAIL_DAYS").cast(IntegerType))
            excelDF = tempdf.unionAll(excelDF)
          }
        }

        var newcols = excelDF.columns
        println("count newcols before change::" + newcols.size)
        /* replacing dataframe columns with table columns */
        for ((col, i) <- newcols.zipWithIndex) {
          if (col.contains("0") || col.contains("1") || col.contains("2") || col.contains("3") || col.contains("4")
            || col.contains("5") || col.contains("6") || col.contains("7") || col.contains("8") || col.contains("9"))
            newcols(i) = "value_index_" + col.replace("-", "").replace(".00", "")
        }
        excelDF = excelDF.toDF(newcols: _*)
        var transposeDF = excelDF
        val sparkConfiguration = sc.broadcast(sc.getConf.getAll.toMap)
        val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
        val batchStartTme = metadata._3
        val part_write_date = metadata._8
        /* Adding batchtime and part date */
        excelDF = excelDF.withColumn("batchtime", lit(batchStartTme).cast(TimestampType))
                         .withColumn("part_date", lit(part_write_date).cast(DateType))
                         .withColumn("sail_date", col("sail_date").cast(TimestampType))
                         .withColumn("sail_days", col("sail_days").cast(IntegerType))
        val tableNames = spark.sparkContext.getConf.get("spark.target.landingtable").toString()
        val colList = spark.catalog.listColumns(tableNames).select("name").collect().map(_.getAs[String]("name")).mkString(",").trim
        val temp_tab = spark.sparkContext.getConf.get("spark.target.landingtable").replace('.', '_')
        excelDF.createOrReplaceTempView(temp_tab)
        val finaleQuery = s"""select """ + colList + s""" from $temp_tab"""
        val stageFinalDf = spark.sql(finaleQuery)
        stageFinalDf.show(false)
        /* populating records from source Excel to landing table */
        val coalsecval: Int = spark.sparkContext.getConf.get("spark.target.coalesce.value").toInt
        stageFinalDf.coalesce(coalsecval).write.mode(SaveMode.Overwrite).parquet(spark.sparkContext.getConf.get("spark.target.landinglocation"))
        /* calling transpose method */
        println("calling transpose method")
        var parseDF = transposeProcess(spark,transposeDF, spark.sparkContext.getConf.get("spark.static.columns").split(",").toSeq)
        parseDF = parseDF.withColumnRenamed("key", "value_index")
                         .withColumnRenamed("val", "value")
                         parseDF.show(false)
        /*calling UDF method */
                         println("calling UDF method")
        parseDF = parseDF.withColumn("value_index", replaceColumnUDF(col("value_index")))
        parseDF.show(false)
        var PresistparseDF = parseDF.persist(StorageLevel.MEMORY_AND_DISK)
        /* adding batchtime and part_date */
        PresistparseDF = PresistparseDF.withColumn("batchtime", lit(batchStartTme).cast(TimestampType))
                                       .withColumn("part_date", lit(part_write_date).cast(DateType))
                                       .withColumn("sail_date", col("sail_date").cast(TimestampType))
                                       .withColumn("sail_days", col("sail_days").cast(IntegerType))
        /* loading data in to parse table after transpose  */
        PresistparseDF.coalesce(coalsecval).write.mode(SaveMode.Overwrite).parquet(spark.sparkContext.getConf.get("spark.target.parselocation"))
      } else {
        println("Source Excel File not exist in s3")
      }
    } catch {
      case e: Exception => {
        log.info("******************in the catch of ForeCastRevenueIngestion ******************");
        e.printStackTrace();
        throw new Exception("General Exception..please check the stacktrace")
      }
    }
  }
  /* reading excel file from s3 location */
  def readExcel(spark: SparkSession, filePath: String, tab: String): DataFrame = {
    spark.read.format("com.crealytics.spark.excel")
      .option("dataAddress", s"'$tab'!")
      .option("header", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .option("inferSchema", "true")
      .option("addColorColumns", "false")
      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
      .option("maxRowsInMemory", 20)
      .load(filePath)
  }
  /* transpose method for dynamic columns */
  def transposeProcess(spark: SparkSession,df: DataFrame, by: Seq[String]): DataFrame = {
    import spark.implicits._
    val (cols, types) = df.dtypes.filter { case (c, _) => !by.contains(c) }.unzip
    require(types.distinct.size == 1, s"${types.distinct.toString}.length != 1")
    val kvs = explode(array(
      cols.map(c => struct(lit(c).alias("key"), col(c).alias("val"))): _*))
    val byExprs = by.map(col(_))

    df.select(byExprs :+ kvs.alias("_kvs"): _*)
      .select(byExprs ++ Seq($"_kvs.key", $"_kvs.val"): _*)
  }
  /* UDF for creating value_index column's values Ex: -110,109 */
  val replaceColumn = (str: String) => {
    str.replace("value_index_", "-")
  }
  /* UDF for finding latest file using file name*/
  val findFileTimestamp = (str: String) => {
    str.replace("revenue_kpi_plan_forecast_upload_", "")
       .replace(":", "").replace(".", "")
       .replace("-", "").replace("xlsx", "")
  }
}
