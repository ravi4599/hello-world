#!/bin/bash
#------------------------
# SparkUnionDataFiles.sh
#------------------------

 usage(){
	echo "Usage: $0 [Single/Multiple Files S/M] [CSV Delimiter] [Application Name] [Spark Master] [Master File] [Delta File] [key Columns] [Update Timestamp Column] [Output Directory]"
	exit 1
 }

 if [[ "$#" -lt 9 ]]; then
    usage
 fi

# Spark Environment Variables
export SPARK_MAJOR_VERSION=2
export SPARK_HOME=/usr/hdp/current/spark2-client
export SPARK_CONF_DIR=/etc/spark2/conf
export HADOOP_CONF_DIR=/etc/hadoop/conf

# Spark submit command parameters
SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
SPARK_MASTER="yarn"
SPARK_DEPLOY_MODE="cluster"
YARN_QUEUE="default"

# Spark execution parameters
NUM_EXECUTOR="4"
EXECUTOR_CORES="10"
EXECUTOR_MEM="20g"

# Jars and name of the application jar file
APP_JAR="/home/deploy/spark-applications/jars/SparkMergeDataFiles-assembly-1.0V8.jar"
CLASS_NAME="main.scala.UnionDataFiles"

$SPARK_SUBMIT --master $SPARK_MASTER --deploy-mode $SPARK_DEPLOY_MODE --queue $YARN_QUEUE --num-executors $NUM_EXECUTOR --executor-cores $EXECUTOR_CORES --executor-memory $EXECUTOR_MEM  --class $CLASS_NAME $APP_JAR $@  

