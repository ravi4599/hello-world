##################################################
# Sailor affinity attribute Transformation code  #                 
#                                                #
# Created by DS Team                             #
#                                                #
# Data Source - Acxiom                           #
#                                                #
##################################################


%pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import greatest
from pyspark.sql.functions import least
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import HiveContext
from pyspark import SparkContext
sc =SparkContext.getOrCreate()
hc = HiveContext(sc)
spark.catalog.clearCache()


###########Import the Acxiom data from HBase
#### Fetching string and boolean columns list
df_AcxiomRawData_colums = spark.read.csv("s3a://vv-dev-emr-cluster/tmp/Acxiom/acxiom_selected_columns_matching_to_bundles.csv", header=True)
df_AcxiomRawData_colums.cache()

#### Fetching the Acxiom master mapping list
df_attributes_mapping = spark.read.csv("s3a://vv-dev-emr-cluster/data/twitter/Acxiom_Master_MappingList_v2.csv",header=True)
df_attributes_mapping.cache()
df_attributes_mapping.show(df_attributes_mapping.count(),truncate=False)
#columns_list = df_AcxiomRawData_colums.select('SpecificColumnsTobeSelected').collect()

#### Fethcing data from HBase
df_AcxiomRawData=spark.read.format("org.apache.phoenix.spark").option("table", "HBT_NBX_ACXIOM").option("zkUrl", "ip-10-3-100-117.shoreside.virginvoyages.com:/hbase").load() 

######## Data Transformations
#Select only those columns which are matching to our Acxiom master mapping list bundles
var_StringColumns=[item[0] for item in df_AcxiomRawData_colums.select('SpecificColumnsTobeSelected').collect()]

#UNIQUE KEY COLUMN TO BE ADDED INSTEAD OF DOCUMENTID
var_StringColumns.append("CLIENT_ID")
df_AcxiomRawStringData=df_AcxiomRawData.select(var_StringColumns)

#Identify the boolean columns and get the attribute
df_boolColmns= df_attributes_mapping.select("Values_From_Data","IsBooleanColumn").where(col("IsBooleanColumn") != "NONE")

#Replacing boolean values with their affinities
for item in df_boolColmns.collect():
    print(item)
    colname=item[1]
    value=item[0]
    df_AcxiomRawStringData = df_AcxiomRawStringData.withColumn(colname+"_",when(col(colname) == True,value)).drop(colname)
df_AcxiomRawStringData.select("POLITICAL_CURRENTAFFAIRSANDPOLITICS_").show(truncate=False)
df_AcxiomRawStringData.select("TOBACCO_SMOKINGTOBACCO_").distinct().show(truncate=False)
df_AcxiomRawStringData.select("ALCOHOL_WINEINTEREST_").distinct().show(truncate=False)

###Transpose the data

def to_long(df, by):

    # Filter dtypes and split into column names and type description
    cols, dtypes = zip(*((c, t) for (c, t) in df.dtypes if c not in by))
    
    
    # Spark SQL supports only homogeneous columns
    assert len(set(dtypes)) == 1, "All columns have to be of the same type"

    # Create and explode an array of (column_name, column_value) structs
    kvs = explode(array([
      struct(lit(c).alias("key"), col(c).alias("val")) for c in cols
    ])).alias("kvs")

    return df.select(by + [kvs]).select(by + ["kvs.key", "kvs.val"])
    
df_AcxiomRawData_Transposed=to_long(df_AcxiomRawStringData,["CLIENT_ID"])
df_AcxiomRawData_Transposed.show()
#Remove [ from the column values
df_AcxiomRawData_Transposed=df_AcxiomRawData_Transposed.withColumn("val1",(regexp_replace(col("val"), "\\[", "")).alias("topic_no_syn"))
#Remove ] from the column values
df_AcxiomRawData_Transposed=df_AcxiomRawData_Transposed.withColumn("val3",(regexp_replace(col("val1"), "\\]", ""))).filter(col('val3')!="NULL")
#Remove ' from the column values
#df_AcxiomRawData_Transposed=df_AcxiomRawData_Transposed.withColumn("val3",(regexp_replace(col("val2"), "\\'", "")))
#df_AcxiomRawData_Transposed.columns

df_AcxiomRawData_Transposed.show()

#### Convert String into Array on Pipe Symbol

# Need not Convert Already Binary Columns- Hence Exclude the Binary Columns - Here smokingtobaco and wineinterest are binary as per data definition
df_AcxiomRawData_Transposed_NonBin=df_AcxiomRawData_Transposed
#df_AcxiomRawData_Transposed_NonBin.show()
df_AcxiomRawData_Transposed_NonBin_Split=df_AcxiomRawData_Transposed_NonBin.withColumn("SplitColumnValues",split(col("val3"), "\\|"))
df_AcxiomRawData_Transposed_NonBin_Split.show()


#### Generating the master list of client and affinity mapping
distinct_client_df = df_AcxiomRawData_Transposed_NonBin_Split.select("CLIENT_ID").withColumnRenamed("CLIENT_ID","main_client_id").distinct()
#distinct_client_df.show()
distinct_affinities_df = df_attributes_mapping.select("alternate").distinct().orderBy("alternate")
#distinct_affinities_df.show()
client_affinities_master_df = distinct_client_df.crossJoin(distinct_affinities_df)
client_affinities_master_df.show()
client_affinities_master_df= client_affinities_master_df.withColumn("MainAcxiomAffinity",concat(col('alternate'),lit("_acxiom"))).withColumn("newvalue",lit(0.0)).drop("alternate")

client_affinities_master_df.show()


####### Explode the Array into Rows

df_AcxiomRawData_Transposed_NonBin_Exploded=df_AcxiomRawData_Transposed_NonBin_Split.withColumn("ExplodedValues",explode(col('SplitColumnValues')))
df_AcxiomRawData_Transposed_NonBin_Exploded=df_AcxiomRawData_Transposed_NonBin_Exploded.withColumnRenamed("ExplodedValues","columnname1")

#AttributeName+AttributeValue would correspoond to an affinity
df_AcxiomRawData_Transposed_NonBin_Exploded=df_AcxiomRawData_Transposed_NonBin_Exploded.withColumn("columnname",concat(col('key'),lit("_"),col('columnname1'))) #.show(10)


df_join = df_AcxiomRawData_Transposed_NonBin_Exploded.join(df_attributes_mapping,df_AcxiomRawData_Transposed_NonBin_Exploded.columnname1==df_attributes_mapping.Values_From_Data,how='left')
print("non matching affinities:-")
df_join.select("columnname1","Values_From_Data","alternate").orderBy("Values_From_data").where(col("Values_From_data").isNull()).show(df_join.count(),truncate=False)
df_affinities_join = df_join.select("CLIENT_ID","alternate","columnname1","Values_From_data").orderBy("alternate").where(col("Values_From_data").isNotNull())
df_affinities_join=df_affinities_join.withColumn("AcxiomAffinity",concat(col('alternate'),lit("_acxiom"))).withColumn("value",lit(1.0))
df_affinities_join=df_affinities_join.select("CLIENT_ID","AcxiomAffinity","value")
df_affinities_join.show(10)
#joining the actual affinities with the master list and get the final df ready for pivoting
conditions = ((df_affinities_join.CLIENT_ID==client_affinities_master_df.main_client_id) &
              (df_affinities_join.AcxiomAffinity == client_affinities_master_df.MainAcxiomAffinity))
              
final_acxiom_df  =client_affinities_master_df.join(df_affinities_join,conditions, how="left")
final_acxiom_df =final_acxiom_df.withColumn("TotalValueMatched",when(col("AcxiomAffinity").isNotNull(),col("value")).otherwise(lit(0))).drop("AcxiomAffinity","value","newvalue","CLIENT_ID")
final_acxiom_df.show()
#print(df_affinities_join.count(), df_attributes_mapping.count())


##### Pivot the dataset

df_Acxiom_Joined_Affinity=final_acxiom_df.groupBy(['main_client_id']).pivot('MainAcxiomAffinity').avg('TotalValueMatched') 
df_Acxiom_Joined_Affinity_ScoringData=df_Acxiom_Joined_Affinity
df_Acxiom_Joined_Affinity_ScoringData=df_Acxiom_Joined_Affinity_ScoringData.where(col("main_client_id")!="0.0")

df_Acxiom_Joined_Affinity_ScoringData.where(col("main_client_id")!="0.0").count()

df_Acxiom_Joined_Affinity_ScoringData.columns


#df_Acxiom_Joined_Affinity_ScoringData.show()
df_Acxiom_Joined_Affinity_ScoringData.withColumnRenamed("main_client_id","sequence").createOrReplaceTempView("Acxiom_Transformed_July28_2018") 
hc.sql("create table ds_db.Acxiom_Transformed_July28_2018_hbt stored as TEXTFILE LOCATION 's3a://vv-dev-emr-cluster/surbhi_test_data/' as select * from Acxiom_Transformed_July28_2018")




############################################# spending selected column list  #########################################################################
df_Acxiom_spending_colums = spark.read.csv("s3a://vv-dev-emr-cluster/sailor_affinity/spending_selected_columns.csv", header=True)

############################################# Selecting spending columns and appending UNIQUE KEY COLUMN TO BE ADDED INSTEAD OF DOCUMENTID  #############################################
var_integerColumns_client=[item[0] for item in df_Acxiom_spending_colums.select('spending_columns').collect()]
var_integerColumns_client.append("CLIENT_ID")

df_spending=df_AcxiomRawData.select(var_integerColumns_client)

df_spending=df_spending.withColumn("SPENDING_CATEGORYSPEND_GIFT",df_spending.SPENDING_CATEGORYSPEND_GIFTS+df_spending.SPENDING_CATEGORYSPEND_SPECIALTYGIFTS).drop("SPENDING_CATEGORYSPEND_GIFTS","SPENDING_CATEGORYSPEND_SPECIALTYGIFTS")
df_spending=df_spending.withColumn("SPENDING_CATEGORYSPEND_HOME_FURNISHINGS",df_spending.SPENDING_CATEGORYSPEND_FURNITURE+df_spending.SPENDING_CATEGORYSPEND_HOMEFURNISHINGS+df_spending.SPENDING_CATEGORYSPEND_HOUSEWARES).drop("SPENDING_CATEGORYSPEND_FURNITURE","SPENDING_CATEGORYSPEND_HOMEFURNISHINGS","SPENDING_CATEGORYSPEND_HOUSEWARES")

############################################# spending selected column list #############################################################################
df_Acxiom_spending_updated_colums = spark.read.csv("s3a://vv-dev-emr-cluster/sailor_affinity/Spending_updated_columns.csv", header=True)


############################################# Fetching list of columns to use in greatest and least function #############################################
var_integerColumns=[item[0] for item in df_Acxiom_spending_updated_colums.select('spending_updated_columns').collect()]

df_spending=df_spending.withColumn("Max",greatest((*[col(x) for x in var_integerColumns])))
df_spending=df_spending.withColumn("Min",least((*[col(x) for x in var_integerColumns])))

############################################# Indexing ###################################################################################################
for field in df_spending.columns:
    if (field != 'CLIENT_ID') & (field != 'Max') :
        df_spending = df_spending.withColumn(field, when(col("Max")!=0,round( ( (col(field)-col("Min")) / (col("Max")-col("Min")) )*100  ,2)))

df_spending=df_spending.fillna(0.0) 



df_attributes_mapping = spark.read.csv("s3a://vv-dev-emr-cluster/sailor_affinity/Acxiom_Master_MappingList_v3.csv",header=True)
#df_attributes_mapping.show(10)

################################################ Identify the spending columns and get the attribute ####################################
df_spendingColumns= df_attributes_mapping.select("attribute","Is_SpendingColumn").where(col("Is_SpendingColumn") != "NONE")
df_spendingColumns.show(5)

#Replacing boolean values with their affinities
for item in df_spendingColumns.collect():
    print(item)
    colname=item[0]
    value=item[1]
    df_spending = df_spending.withColumn(colname+"_",col(value)).drop(value) 

#df_spending.show()
    
df_spending=df_spending.drop("Max","Min")
df_spending.createOrReplaceTempView("Acxiom_Transformed_Spending") 
#hc.sql("create table ds_db.Acxiom_Transformed_Spending_new  as select * from Acxiom_Transformed_Spending")
hc.sql("create table ds_db.Acxiom_Transformed_Spending_hbt stored as TEXTFILE LOCATION 's3a://vv-dev-emr-cluster/surbhi_test_data/' as select * from Acxiom_Transformed_Spending")