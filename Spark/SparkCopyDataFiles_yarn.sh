#!/bin/bash
#------------------------
# SparkCopyDataFiles_yarn.sh
#------------------------

 usage(){
	echo "Usage: $0 [Single/Multiple Files S/M] [CSV Delimiter] [Application Name] [Spark Master] [Master File] [Output Directory] [encoding]"
	exit 1
 }

 if [[ "$#" -lt 7 ]]; then
    usage
 fi

# Spark Environment Variables
export SPARK_MAJOR_VERSION=2
export SPARK_HOME=/usr/hdp/current/spark2-client
export SPARK_CONF_DIR=/etc/spark2/conf
export HADOOP_CONF_DIR=/etc/hadoop/conf
dttm=`date +"%Y%m%d%H%M%S"`
cur_pid=`echo $$`
tmp_log_file="/tmp/"$3"_"$dttm"_"$cur_pid".log"

appl_name=${3}
jb_type="Copy"

sprk_jb_name=${appl_name}"_"${jb_type}



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
APP_JAR="/home/deploy/spark-applications/jars/SparkMergeDataFiles-assembly-1.0V9.jar"
CLASS_NAME="main.scala.CopyDataFiles"

$SPARK_SUBMIT --name ${sprk_jb_name} --master $SPARK_MASTER --deploy-mode $SPARK_DEPLOY_MODE --queue $YARN_QUEUE --num-executors $NUM_EXECUTOR --executor-cores $EXECUTOR_CORES --executor-memory $EXECUTOR_MEM  --class $CLASS_NAME $APP_JAR $@ > $tmp_log_file 2>&1

appl_id=`cat $tmp_log_file|grep 'YarnClientImpl: Submitted'|awk -F " " '{print $NF}'`
#echo $appl_id
#############check the status of the spark submit from yarn##############
#
######################################################################
final_state=`yarn application -status $appl_id|grep 'Final-State'|cut -d ":" -f2|tr -d " "`
echo $final_state" |${appl_id}"
if [  $final_state == "SUCCEEDED" ];then
#csv_cnt=`yarn logs -applicationId $appl_id -log_files stdout|grep 'CSV File Record Count'|cut -d ":" -f2|tr -d " "`
rm -f $tmp_log_file
else
rm -f $tmp_log_file
exit 1
fi
  

