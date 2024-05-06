
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys, time
from pyspark import HiveContext
import traceback, logging
import sys,os

# Save the output as hive table


# Initialize Spark Session
def initSparkSession(appName):
    """
    Initializes the SparkSession
    :param appName  : name for the SparkSession
    :return         : initialized SparkSession
    """

    try:
        spark = SparkSession \
            .builder \
            .appName(appName) \
            .enableHiveSupport() \
            .getOrCreate()
        # spark.conf.set("spark.sql.shuffle.partitions", "50")

    except Exception as e:
        traceback.print_exc()
        raise TypeError(
            "Error Code 6: CDC module Error initializing SparkSession. Please review SparkSession configurations. ")

    return spark


def get_logger():
    """
    Create the logger which will be used for this script

    :return : configured logger
    """

    try:
        log = logging.getLogger('Spark')
        _h = logging.StreamHandler()
        _h.setFormatter(logging.Formatter("%(levelname)s  %(msg)s"))
        log.addHandler(_h)
        log.setLevel(logging.DEBUG)
        log.info("module imported and logger initialized")

    except Exception as e:
        traceback.print_exc()
        raise TypeError(
            "Error Code 7: Error initializing Logger. Please review Logger.")

    return log

def loadProcessDriver(spark, log, db_name, table_name):
    """
    Load data relevant to the current script from the Process Driver Table

    :param spark      : current SparkSession
    :param log        : current logger
    :param db_name    : name of the database which contains the data relevant to the current script
    :param table_name : name of CDC table name
    :return           : dictionary of data relevant to the current script in form of {VariableName:Value}
    """

    log.info("Reading data from Processdriver")
    query_string = "select VariableName,Value from {0} where processname='{1}'".format(db_name,table_name)
    log.info(query_string)
    data_dict = {}
    try:
        raw_data = spark.sql(query_string).collect()
        data = map(lambda x: (x[0], x[1]), raw_data)
        data_dict = dict(data)
    except Exception as e:
        traceback.print_exc()
        raise TypeError("Error Code 1: Please verify process driver query execution.")
    return data_dict


def initProcess(spark, processController, log):
    """
    Fetches the relevant parameters from the processController dictionary

    :param spark                : current SparkSession
    :param log                  : current logger
    :param processController    : dictionary of relevant parameters
    :return                     : Spark Dataframe
    """

    log.info("Fetch Parameters from Processor Driver")
    try:
        main_table_query = processController.get("main_table_query")
        incr_table_query = processController.get("incr_table_query")
        subtract_query = processController.get("subtract_query")

    except Exception as e:
        traceback.print_exc()
        raise TypeError("Error Code 3: Please verify driver table connection and entries.")

    return process(log, spark, main_table_query,incr_table_query,subtract_query)


def process(log,spark,main_table_query,incr_table_query,subtract_query):
    """
    Prepares data and gets synonyms

    :param log                  : current logger
    :param spark                : current SparkSession
    :param main_table_query     : query to get main table data
    :param incr_table_query     : query to get incr table data
    :param subtract_query       : subtract query

    :return                     : spark dataframe
    """

    try:
        log.info("Query Main table Date")
        #df_main_table = spark.sql(main_table_query)
        #df_main_table.registerTempTable("maintable")
        #df_main_table.show()


        log.info("Query incr table Data")
        df_temp_table = spark.sql(incr_table_query)
        #df_temp_table.registerTempTable("temptable")
        #df_temp_table.show()

        log.info("Query subtract Data")
        df_subtract = spark.sql(subtract_query)
        #df_subtract.show()

    except Exception as e:
        traceback.print_exc()
        raise TypeError("Error Code 3: Please verify connection to Hive tables.")

    try:
        log.info("union the subtract data with incr table data")
        df_union = df_subtract.union(df_temp_table)
        #df_union.show()

    except Exception as e:
        traceback.print_exc()
        raise TypeError("Error Code 3: Please verify that the data is proper.")

    log.info("Returning Dataframe...")
    return df_union


def store(log, spark, processController, data):
    """
    Saves the input Dataframe to the location(s) defined in the process driver table

    :param processController    : dictionary of relevant parameters
    :param log                  : current logger
    :param data                 : Spark Dataframe
    :return                     : N/A
    """

    try:
        path_temp = processController.get("temp_file_path")
        path_cdc_crm = processController.get("original_file_path")
    except Exception as e:
        traceback.print_exc()
        raise TypeError("Error Code 3: Please verify driver table connection and entries.")


    try:
        log.info("Save the results in s3 temp path %s" % path_temp)
        #log.info("Save the results in s3 original path %s" % path_cdc_crm)
        data.repartition(10).write.mode('overwrite').format("parquet").save(path_temp)
        #data.repartition(4).write.mode('overwrite').format("parquet").save(path_cdc_crm)
        time.sleep(20)
        print("WAIT COOMPLETE")
    except Exception as e:
        traceback.print_exc()
        raise TypeError("Error Code 3: Please verify connection to Hive and S3")

def stopSparkSession(log, spark):
    """ Shuts down the current SparkSession

    :param log      : current logger
    :param spark    : current SparkSession
    :return         : N/A
    """
    log.info("Shutting Down")
    spark.stop()


if __name__ == "__main__":
    spark = initSparkSession("CDC_crm")
    log = get_logger()
    tablename = sys.argv[1]
    processController = loadProcessDriver(spark, log, "vv_db.processdriver",tablename)
    crmdf = initProcess(spark, processController, log)
    store(log, spark, processController, crmdf)
    stopSparkSession(log, spark)

    
    
    



