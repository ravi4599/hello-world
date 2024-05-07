package com.virginvoyages.facts
import scala.collection.immutable.Map
import org.apache.spark.sql.expressions.Window
import scala.util.parsing.json._
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import com.virginvoyages.metadataframework.ManageMetadata
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import scala.util.Try
import scalaj.http.Http
import scalaj.http.HttpOptions
import com.squareup.okhttp.OkHttpClient
import com.squareup.okhttp.MediaType
import com.squareup.okhttp.RequestBody
import com.squareup.okhttp.Request;



object AgentHistoryFact {
  
   val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
 
    
   def main(args: Array[String]): Unit = {
     
   
     
  val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
    
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkConfiguration = spark.sparkContext.broadcast(spark.sparkContext.getConf.getAll.toMap)
     sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
     
     /*val startBatchTime = args(0)
     val endBatchTime = args(1)*/
	 val metadata = ManageMetadata.fetchBatchTime(sparkConfiguration, spark)
      metadata.productIterator.foreach(println)
      val batch_id1 = metadata._1
      val batch_instance_id1 = metadata._2
      val startBatchTime  = metadata._3
      val endBatchTime = metadata._4
     //yyyy-MM-dd'T'23:59:59 2020-03-17T00:00:00
     val batch_s_date = startBatchTime.substring(0, 10)
    val batch_s_time = startBatchTime.substring(11)
    val batch_e_date = endBatchTime.substring(0, 10)
    val batch_e_time = endBatchTime.substring(11)
    
   
    
      val batch_api_start_datetime = batch_s_date+"T"+batch_s_time+"Z"
    val batch_api_end_datetime = batch_e_date+"T"+batch_e_time+"Z"
     
    log.info(batch_api_start_datetime)  
     log.info(batch_api_start_datetime)
     
    var accessToken: String = null;
  var contactResponse: String = null;
  var df = spark.emptyDataFrame
  
 accessToken=  getAccessToken(sparkConfiguration)
 
 contactResponse =callContactApi(accessToken,batch_api_start_datetime,batch_api_end_datetime,sparkConfiguration)
 log.info(contactResponse)
 
 df = parseContactResponse(contactResponse,sparkConfiguration)
 

 
 if(!df.head(1).isEmpty) {
 log.info(df.select("agentId").distinct().count())
 
df = df.select("agentId").distinct()

var result = Seq[Row]()

for (row <- df.rdd.collect)
     {
       val agentID = row.mkString(",").split(",")(0)
      val agentResponse = callAgentStateHistory(agentID.toLong , accessToken ,batch_api_start_datetime,batch_api_end_datetime,sparkConfiguration)
      result = result ++ Seq(Row(agentID,agentResponse))
      
     }
  val responseRDD = sc.parallelize(result)

  val schema = new StructType().add(StructField("agentID", StringType, false)).add(StructField("agentResponse", StringType, true))
  
  val responseDF = spark.createDataFrame(responseRDD, schema)
  
  
 /* Old Code -- Serialization Issue
 df = df.rdd.map{f =>
   //val contactID = f.getLong(0)
   val agentID = f.getLong(0)
   println(agentID)
   
   accessToken=  getAccessToken(sparkConfiguration)
   val agentResponse = callAgentStateHistory(agentID , accessToken ,batch_api_start_datetime,batch_api_end_datetime,sparkConfiguration)
  
   (agentID,agentResponse)
  }.toDF("agentID","agentResponse");*/
  
parseAgentResponse(responseDF,startBatchTime,sparkConfiguration)
ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark);
log.info("Completed Successfully")
  
 }
  
 else{
   log.info("No Agent ID's to be processed")
    ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
   
 }
       
def getAccessToken(configMap: Broadcast[Map[String, String]]): String = {
 
try{
  
 /*    val postData ="""{
    "grant_type" : "password",
    "username" : "ashwin.john@capgemini.com",
    "password" : "VV$Feb$022020",
    "scope" : ""}"""
     */
     val postData = configMap.value.get("spark.access.credentials").get
     log.info(postData)
     val httpClient = new OkHttpClient()

     val mediaType = MediaType.parse("text/plain");
   
        val body = RequestBody.create(mediaType,postData);
        
        val request = new Request.Builder()
                      //.url("https://api.incontact.com/InContactAuthorizationServer/Token")
                      .url(configMap.value.get("spark.access.token.url").get)
                      .method("POST", body)
                      //.addHeader("Authorization", "basic VlZfRGF0YUxha2UxQENhcGdlbWluaTE6NDU5NzI3Mw==")
                      .addHeader("Authorization", configMap.value.get("spark.auth.key").get )
                      .addHeader("Content-Type", "text/plain")
                      .build();  
       val response = httpClient.newCall(request).execute()
       
     
       accessToken = parseAccessToken(response.body().string())
       log.info(accessToken)
        
        return accessToken  
        
      } 
        catch{
        case e: Exception => {ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);e.printStackTrace(); throw new Exception("General Exception..please check stacktrace ") } 
      }
    }
  
def parseAccessToken(jsonStr: String) : String = { 
     
     log.info("IN parseAccessToken Method")
     
     var parseAccesToken:String = null;
     
     try {
       val result = JSON.parseFull(jsonStr) 
       
       result match {
                      case Some(map: Map[String, String]) => map.get("access_token") match {
                           case Some(ref) => parseAccesToken = ref
                           }
                     case None => log.info("Invalid Access Token")
                     }  
                 }        

     catch {  
     case e: Exception => {ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);e.printStackTrace(); 
     throw new Exception("Access Token received not in proper format")
     } } 
     return parseAccesToken 
}

def callContactApi(accessToken: String,StartTime : String,EndTime : String ,configMap: Broadcast[Map[String, String]]): String = {
  
  val httpClient = new OkHttpClient()

   
   
         val request = new Request.Builder()
                      .url("https://api-c29.incontact.com/inContactAPI/services/v17.0/contacts/completed?startDate="+StartTime+"&endDate="+EndTime+"&mediaTypeId=4")                    
                      .header("Authorization", "bearer "+accessToken )
                      .header("Content-Type", "application/json")
                      .header("Accept","application/json")
                      .build();  
       val response = httpClient.newCall(request).execute()

  
  return response.body().string()
}


def parseContactResponse(contactResponse : String,configMap: Broadcast[Map[String, String]]): DataFrame ={
  
  var jsonContactResponse = spark.read.json(Seq(contactResponse).toDS)
  
  var contactDf=spark.emptyDataFrame
  
  if(hasColumn(jsonContactResponse,"completedContacts")){
    if(checkArray(jsonContactResponse,"completedContacts")){
      
      jsonContactResponse = jsonContactResponse.withColumn("completedContacts_explode", explode_outer(jsonContactResponse.col("completedContacts")))
      
      if(!checkStringType(jsonContactResponse,"completedContacts_explode")){
      jsonContactResponse = jsonContactResponse.withColumn("agentId",jsonContactResponse.col("completedContacts_explode.agentId"))
                            .withColumn("contactId",jsonContactResponse.col("completedContacts_explode.contactId"))
   contactDf = jsonContactResponse.select("contactId","agentId").filter(jsonContactResponse.col("agentId").isNotNull)
      }
      }
    
    
  }
  
  return contactDf
}

def callAgentStateHistory(agentID : Long, accessToken: String ,StartTime :String ,EndTime : String,configMap: Broadcast[Map[String, String]]) : String = {
  
  //log.info(StartTime)
  val httpClient = new OkHttpClient()

   
   
         val request = new Request.Builder()
                      .url("https://api-c29.incontact.com/inContactAPI/services/v17.0/agents/"+agentID+"/state-history?startDate="+StartTime+"&endDate="+EndTime+"")                    
                      .header("Authorization", "bearer "+accessToken )
                      .header("Content-Type", "application/json")
                      .header("Accept","application/json")
                      .build();  
       val response = httpClient.newCall(request).execute()
  
  return response.body().string()
}
     def parseAgentResponse(df: DataFrame ,StartTime : String,configMap: Broadcast[Map[String, String]]): DataFrame = {
       
     
       var dfRdd = df.select("agentResponse").rdd.map{x => x.toString}
       
       var agentJson = spark.read.json(dfRdd)
       
    
       
       agentJson = agentJson.withColumn("id" , monotonically_increasing_id())
       var agentId = df.withColumn("id", monotonically_increasing_id())
       
       
       var w = Window.orderBy("id")
       
        agentJson = agentJson.withColumn("index", row_number().over(w)).drop("id") 
        agentId = agentId.withColumn("index", row_number().over(w)).drop("id")
       
        var agentStateHistory = agentJson.join(agentId, agentJson.col("index") === agentId.col("index"), "inner").select(agentId.col("agentID"),agentJson.col("agentStateHistory"))
       
        println("Data")
       agentStateHistory.show(10,false)
       agentStateHistory.printSchema
       
       
        
        
       if(hasColumn(agentStateHistory,"agentStateHistory")){
         
         if(checkArray(agentStateHistory,"agentStateHistory")){
           
         agentStateHistory = agentStateHistory.withColumn("agentStateHistory_explode", explode_outer(agentStateHistory.col("agentStateHistory"))) 
        agentStateHistory.printSchema
                  val typed=agentStateHistory.schema("agentStateHistory_explode").dataType.typeName == "string"
          println(typed)
           if(!checkStringType(agentStateHistory,"agentStateHistory_explode")){

             //agentStateHistory.printSchema
          agentStateHistory= agentStateHistory.withColumn("stateIndex", agentStateHistory.col("agentStateHistory_explode.stateIndex"))
                             .withColumn("agentSessionId", agentStateHistory.col("agentStateHistory_explode.agentSessionId"))
                             .withColumn("agentStateName", agentStateHistory.col("agentStateHistory_explode.agentStateName"))
                             .withColumn("startDate", agentStateHistory.col("agentStateHistory_explode.startDate"))
                             .withColumn("skillId", agentStateHistory.col("agentStateHistory_explode.skillId"))
                             .withColumn("fromAddress", agentStateHistory.col("agentStateHistory_explode.fromAddress"))
                             .withColumn("toAddress", agentStateHistory.col("agentStateHistory_explode.toAddress"))
                             .withColumn("outStateDescription", agentStateHistory.col("agentStateHistory_explode.outStateDescription"))
                             .withColumn("duration", agentStateHistory.col("agentStateHistory_explode.duration"))
               var agentState = agentStateHistory.select("agentID","stateIndex","agentSessionId","agentStateName","startDate","skillId","fromAddress","toAddress","outStateDescription","duration")
       

       
             loadAgentStateHistoryFact(agentState,StartTime,sparkConfiguration)
         
         }}
         
       }
       

       
       return agentStateHistory
     }
     
     
     def loadAgentStateHistoryFact(agentSrcDf : DataFrame , StartTime: String,configMap: Broadcast[Map[String, String]]): DataFrame = {
       
       val partDate = StartTime.substring(0,10) 
      
       var  agentDim = spark.sql(spark.sparkContext.getConf.get("spark.agent.dim.sql").trim())
       var skillDim = spark.sql(spark.sparkContext.getConf.get("spark.skill.dim.sql").trim())
       
   //var  agentDim = spark.sql("select cti_agent_skey,src_agent_id from vv_db.hvtb_nbx_core_cti_agent_dim")
   //var skillDim = spark.sql("select cti_skill_skey,src_skill_id from vv_db.hvtb_nbx_core_cti_skill_dim ")
       
     var agentFactDf =   agentSrcDf.join(agentDim, agentSrcDf.col("agentID") === agentDim.col("src_agent_id"),"left").select(agentSrcDf.col("*"),agentDim.col("cti_agent_skey"))
       

       
      var skillFactDf =  agentFactDf.join(skillDim, agentFactDf.col("skillId") === skillDim.col("src_skill_id") , "left").select(agentFactDf.col("*"),skillDim.col("*"))
       
     

      skillFactDf = skillFactDf.withColumn("etl_load_dt" , lit(StartTime).cast(TimestampType)).withColumn("batch_time", lit(StartTime).cast(TimestampType)).withColumn("part_date" , lit(to_date(col("batch_time"),"yyyy-MM-dd")))
                     .withColumn("cti_skill_skey_new",when( skillFactDf.col("cti_skill_skey").isNull,"-1").otherwise(skillFactDf.col("cti_skill_skey")))
                     .drop("cti_skill_skey")
                     .withColumnRenamed("cti_skill_skey_new", "cti_skill_skey")
      
      
      
      val finalDf = skillFactDf.select("cti_agent_skey","stateIndex","agentSessionId","agentStateName","startDate","cti_skill_skey","fromAddress","toAddress","outStateDescription","duration","etl_load_dt","batch_time","part_date")
      
      //finalDf.show(10,false)
      try{
      finalDf.write.mode("append").insertInto(spark.sparkContext.getConf.get("spark.target.fact.table"))
	   ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Successful", spark); 
      } catch {

      case e: Exception =>
        {
          log.info("in the catch of updateStatus ****************** ")
          ManageMetadata.updateStatus(batch_instance_id1, batch_id1, "Failed", spark);
          e.printStackTrace()
          throw new Exception("General Exception..please check the stacktrace")
        }
      }
      return skillFactDf
     }
     
     
     
     
     
     
     
     
     
     
   def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
       
       
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
  
  
   }
  
}