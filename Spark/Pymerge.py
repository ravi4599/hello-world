
#############################################################################
##Created on Mar 21 19:28:38 2018
##@author: Raghu Anumasa
## Merge Program to take an incoming file and do an upsert operation to a reference dataset
## It can handle Initial load where there is no reference file to compare
## It can do Insert or Merge based on Input parameters
## Kw Parameters: **{'referencefile':"adl://rccldatalakestore.azuredatalakestore.net/RaghavTest/file.csv",'sourcefile':"adl://rccldatalakestore.azuredatalakestore.net/RaghavTest/incomingfile.csv",'destfile' : "adl://rccldatalakestore.azuredatalakestore.net/RaghavTest/rccl",'operation' : "Insert",'filepartitionsize' : 4,'Loadtype':"Initial",'partitioncol':"ColumnNameForHiveTypeParitionedFolder",'tblkeycol':"ColumntoJoin"}) 

############################################################################

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
sc = SparkContext()
sqlContext = SQLContext(sc)

spark = SparkSession.builder \
     .appName("RCCLPySparkMerge")\
     .enableHiveSupport()\
     .getOrCreate()

from pyspark.sql.functions import *
import sys 





#class MergeClass:
#    """This is a class for Merging in staging layer."""
#    def __init__(self,referencefile,sourcefile,destfile,operation,filepartitionsize,Loadtype,partitioncol,tblkeycol,fielddm):

        
def PyMerge (referencefile,sourcefile,destfile,operation,filepartitionsize,Loadtype,partitioncol,tblkeycol,fielddm):       
            
            if(Loadtype=="Initial"):
            

                fulldf = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter",fielddm).load(sourcefile)
                #fulldf.show()
                fulldf.registerTempTable("SourceTbl")
                fulldf.repartition(4).write.format('com.databricks.spark.csv').mode('overwrite').option("header", "true").option("delimiter",fielddm).save(destfile)  #below for Hive folders              
                #fulldf.show()
                #newdf=fulldf.select(partitioncol, from_unixtime(unix_timestamp(partitioncol, 'MM/dd/yyy')).alias('New_date'))
                #newdf.registerTempTable("NewTbl")
                #newtable=spark.sql("Select *,year(New_date) as year,month(New_date)as month from NewTbl")
                #newtable.registerTempTable("partitiontbl")
                #finalquery="select F.*,year,month from SourceTbl F join partitiontbl P on F.{0}=P.{1}".format(partitioncol,partitioncol)
                #finaloutput=sqlContext.sql(finalquery)
                #finaloutput.write.partitionBy("year","month").format("csv").mode('overwrite').save(destfile,header = 'true')

                  

            
        
            else:
                Tgtdf = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter",fielddm).load(referencefile)
                newdf=Tgtdf.select(partitioncol, from_unixtime(unix_timestamp(partitioncol, 'MM/dd/yyy')).alias('New_date'))
                Tgtdf.registerTempTable("TgtTable")
                newdf.registerTempTable("NewTbl")
                newtable=spark.sql("Select *,year(New_date) as year,month(New_date)as month from NewTbl")
                newtable.registerTempTable("partitiontbl")
                Sourcedf = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter",fielddm).load(sourcefile)
                Sourcedf.registerTempTable("SourceTbl")
                
                if(operation=='Insert'):            
                    #perform the insert
                      
                    fulldf=Sourcedf.union(Tgtdf)
                    fulldf.registerTempTable("PreFinalTbl")
                    fulldf.repartition(4).write.format('com.databricks.spark.csv') \
  .mode('append').option("header", "true").option("delimiter",fielddm).mode('overwrite').save(destfile)
                    #if rccl wants Hive folder ##
                    #finalquery="select F.*,year,month from PreFinalTbl F join partitiontbl P on F.{0}=P.{1}".format(partitioncol,partitioncol)
                    #finaloutput=sqlContext.sql(finalquery)
                    #finaloutput.write.partitionBy("year","month").format("csv").mode('overwrite').save(destfile,header = 'true')
                    #Hive Folder ##
                    return fulldf.count()
                else:
                    print("Its a Merge Operation")
                    targettable=spark.sql("Select * from TgtTable")#check this condition
                    query = "SELECT T.* from TgtTable T LEFT JOIN SourceTbl S on T.{0}=S.{1} Where S.{2} iS  NULL ".format(tblkeycol,tblkeycol,tblkeycol)
                    existingdata=sqlContext.sql(query)
                    fulldf=existingdata.union(Sourcedf)
                    fulldf.repartition(filepartitionsize).write.format('com.databricks.spark.csv') \
  .mode('append').option("header", "true").option("delimiter",fielddm).mode('overwrite').save(destfile)
                    #if rccl wants Hive folder ##
                    #fulldf.registerTempTable("PreFinalTbl")
                    #finalquery="select F.*,year,month from PreFinalTbl F join partitiontbl P on F.{0}=P.{1}".format(partitioncol,partitioncol)
                    #finaloutput=sqlContext.sql(finalquery)
                    #finaloutput.write.partitionBy("year","month").format("csv").mode('overwrite').save(destfile,header = 'true')
                    
                    print("target table cnt" , targettable.count())
                    print("Source  table cnt" , Sourcedf.count())
                    print("Merged  table cnt" , fulldf.count())    
                    print("New records",(fulldf.count()-targettable.count()))
                    #finaloutput.show()
                    return fulldf.count()  

if __name__ == "__main__":
            referencefile=sys.argv[1]
            sourcefile=sys.argv[2]
            destfile=sys.argv[3]
            operation=sys.argv[4]
            filepartitionsize=int(sys.argv[5])
            Loadtype=sys.argv[6]
            partitioncol=sys.argv[7]
            tblkeycol=sys.argv[8]
            fielddm=sys.argv[9]    
            DoMerge= PyMerge(referencefile,sourcefile,destfile,operation,filepartitionsize,Loadtype,partitioncol,tblkeycol,fielddm)
