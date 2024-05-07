package com.virginvoyages.parse
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types.{ StructType, StructField, TimestampType, StringType, IntegerType, DateType, LongType };
import org.apache.spark.sql.DataFrame
import scala.util.Try
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import com.virginvoyages.metadataframework.ManageMetadata

object ParseFramework {

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
    var batch_instance_id1: String = null
    var batch_id1: String = null

    // def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
    try {
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

      val landSource = spark.sparkContext.getConf.get("spark.source.table")
      val temptab= landSource.replace("select * from ", "")
      val lndTblName="select * from " +temptab
      val tgtTblName = spark.sparkContext.getConf.get("spark.target.table")
      //val whereCond = "part_date = to_date('2020-11-03')"
     // spark.sql(spark.sparkContext.getConf.get("spark.source.table"))
      val whereClause = s""" where batchtime>= '$batch_start_time' and batchtime<='$batch_end_time' and part_date >='$part_start_time' and part_date <='$part_end_time'"""
      var selectStr=""
      //var selectStr = spark.sparkContext.getConf.get("spark.source.table")
      //  .toString()
       if (whereClause.length == 0) { selectStr = lndTblName } else { selectStr = lndTblName + whereClause }

      println(selectStr)

      val lndTblDF = spark.sql(selectStr)
      if (!lndTblDF.head(1).isEmpty) {
        val lndTblMSG = lndTblDF.select("Message").map { x => x.toString }
        var tempDf = spark.read.json(lndTblMSG).drop("BatchTime", "Part_Date")

       
      val tableNames =  spark.sparkContext.getConf.get("spark.target.table").toString()
      
     
        val colList = spark.catalog.listColumns(tableNames).select("name").collect().map(_.getAs[String]("name")).mkString(",").trim

       // val finaleQuery1 = "select " + colList + " from " + tableNames
       // val stageFinalDf1 = spark.sql(finaleQuery1)
      //  stageFinalDf1.show(false)
        //val required_columns = spark.sparkContext.getConf.get("spark.parse.columns").split(",")
      val required_columns = colList.split(",")
        val missing_columns = ArrayBuffer[String]()
        val avaliable_columns = ArrayBuffer[String]()
        def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

        for (col <- required_columns) {

          if (hasColumn(tempDf, col)) {
            println(col, "column exists", avaliable_columns.toString)
            println("column exists", avaliable_columns.length)
            log.info(col, "column exists", avaliable_columns.toString, avaliable_columns.length)
            println(avaliable_columns.length, "lenthg")
            avaliable_columns.append(col)
          } else {
            println(col, "column missing", missing_columns.length)
            println(missing_columns.length, "length")
            log.info(col, "column exists", missing_columns.toString, missing_columns.length)
            println("column missing", missing_columns)
            missing_columns.append(col)
          }

        }

       /* print(missing_columns, "Here are the missing columns")
        var targetDF = missing_columns.foldLeft(tempDf)((df, c) =>
          df.withColumn(s"$c", lit(null)))

        //targetDF.show(false)
        var dataDf = targetDF.withColumn("BatchTime", lit(batch_start_time).cast(TimestampType)).withColumn("Part_Date", to_date(lit(part_write_date)))
        .withColumn("geocoordinate",lit("NA").cast(StringType))
        .withColumn("additionalinfo",lit("NA").cast(StringType))
        .withColumn("additionalinfo",lit("NA").cast(TimestampType))
        .withColumn("additionalinfo",lit("NA").cast(TimestampType))

        val temp_tab = spark.sparkContext.getConf.get("spark.target.table").replace('.', '_')
        dataDf.createOrReplaceTempView(temp_tab)
        val finaleQuery = s"""select """ + colList + s""" from $temp_tab"""

        println(finaleQuery)

        val stageFinalDf = spark.sql(finaleQuery)
       val tabl_name =spark.sparkContext.getConf.get("spark.target.table")
       println(tabl_name)
        stageFinalDf.show(false)
        stageFinalDf.printSchema
        
      val tab=spark.sparkContext.getConf.get("spark.target.table").toString()
      println(tab)
       // stageFinalDf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.table"))
        println("data written")
        //spark.sql(s"""select * from """+spark.sparkContext.getConf.get("spark.target.table")).show(false)
       // spark.sql(s"""select * from """+spark.sparkContext.getConf.get("spark.target.table")).printSchema
        log.info("Count of records : " +stageFinalDf.count()) 
             
        stageFinalDf.write.mode("Append").insertInto(spark.sparkContext.getConf.get("spark.target.table"))
             */
        print(missing_columns, "Here are the missing columns")
        var targetDF = missing_columns.foldLeft(tempDf)((df, c) =>
          df.withColumn(s"$c", lit(null)))
        
        targetDF.show(false)
        var dataDf = targetDF.withColumn("BatchTime", lit(batch_start_time).cast(TimestampType)).withColumn("Part_Date", to_date(lit(part_write_date)))

        val temp_tab = spark.sparkContext.getConf.get("spark.target.table").replace('.', '_')
        dataDf.createOrReplaceTempView(temp_tab)
        val finaleQuery = s"""select """ + colList + s""" from $temp_tab"""

        println(finaleQuery)

        val stageFinalDf = spark.sql(finaleQuery)
        
        stageFinalDf.printSchema
        
        stageFinalDf.show(false)
        stageFinalDf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.table"))

        
        
        
      }

      ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark)

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