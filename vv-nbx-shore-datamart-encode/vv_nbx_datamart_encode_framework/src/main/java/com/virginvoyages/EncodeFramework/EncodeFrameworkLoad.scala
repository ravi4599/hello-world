package com.virginvoyages.EncodeFramework

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.LogManager

import org.apache.spark.sql.types.{ StringType, TimestampType }
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import java.sql.SQLException
import org.apache.spark._
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import java.sql.SQLException
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Column

object EncodeFrameworkLoad {
  def main(args: Array[String]) {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    val EncodeFn = (inputStr: String) => {
    var outputStr = inputStr
    if (inputStr.length > 0) 
    {
      var StringtoProcess = inputStr
      if (inputStr.length <= 3) {StringtoProcess = inputStr.reverse.padTo(4,'0').reverse}
      val prefixSub = StringtoProcess.reverse.substring(0,2)
      val suffixSub = StringtoProcess.substring(0,2).reverse
      val padToDigits = StringtoProcess.reverse.padTo(7,'0').reverse
      val StrtoConvert = prefixSub+padToDigits+suffixSub
      outputStr = StrtoConvert.toLong.toHexString.toLowerCase
    }
    outputStr
   }

    val EncodeFnUDF = udf(EncodeFn)
    spark.udf.register("EncodeFnUDF", EncodeFn)
    
    try {
      val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
      val srctbl = spark.sparkContext.getConf.get("spark.source.table")
      val tgtTbl = spark.sparkContext.getConf.get("spark.target.encode.table")
 
 
 
 try{
				println("test_code")              
				val col_1 = spark.sparkContext.getConf.get("spark.source.columns")                                                     
				val selectStr2 = """Select """+""" * """+""" from """+ tgtTbl
				val df = spark.sql(selectStr2).limit(0).columns.toList	
				println(df)			
				val reqcol = df(1)
				println(reqcol)						  
				println(col_1)
				println(tgtTbl)
				val selectStr1 = """Select """ + col_1 +""" from """+ srctbl
				val sourceDf=spark.sql(selectStr1)  //.withColumn(col_1,when(col(col_1).rlike("[A-Za-z]"),lit(0)).when(col(col_1).isNull,lit(0)).when(col(col_1).contains("-"),lit(0)).when(col(col_1).contains("."),lit(0)).otherwise(col(col_1).cast("Integer")))
								
				println(selectStr1)
				val masterEncodeDf = spark.sql(selectStr2)
				println(selectStr2)
				val mergedEncodedData = sourceDf.join(masterEncodeDf,sourceDf(col_1)=== masterEncodeDf(col_1),"leftanti")
				
				
				if (mergedEncodedData.count > 0){
					val updatedEncodedData = mergedEncodedData.withColumn(reqcol,reverse(base64(encode(col(col_1),"UTF-16")))) 
//					val updatedEncodedData = mergedEncodedData.withColumn(reqcol,EncodeFnUDF(col(col_1))) 
//          updatedEncodedData.write.mode("overwrite").insertInto(tgtTbl)        
					updatedEncodedData.write.mode("append").insertInto(tgtTbl)                	
				}else{
					println("no new data")
				}     
        }catch{
          case e: SQLException =>
            { log.info("****************** SQLexception in the catch of Encode ******************"); e.printStackTrace(); }
          case e: Exception =>
            { log.info("******************Exception in the catch of Encode ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace", e); }
        }
    }catch {
      case e: SQLException =>{ 
		  log.info("***************SQLexception in the catch of Encode ******************"); e.printStackTrace(); throw new Exception("SQL Exception..please check the stacktrace")
      }case e: Exception =>{
          log.info("*************** Exception catch in the catch of Encode ******************"); e.printStackTrace(); throw new Exception("General Exception..please check the stacktrace")
      }
    }
        System.exit(1)
  }
}