#!/bin/bash
#------------------------
# SparkMergeDataFiles_yarn.sh
#------------------------

 usage(){
	echo "Usage: $0 [Single/Multiple Files S/M] [CSV Delimiter] [Application Name] [Spark Master] [Master File] [Delta File] [key Columns] [Update Timestamp Column] [Output Directory] [encoding]"
	exit 1
 }

 if [[ "$#" -ne 10 ]]; then
    usage
 fi

 
# Spark Environment Variables
#echo "the parmas $@"
export SPARK_MAJOR_VERSION=2
export SPARK_HOME=/usr/hdp/current/spark2-client
export SPARK_CONF_DIR=/etc/spark2/conf
export HADOOP_CONF_DIR=/etc/hadoop/conf
dttm=`date +"%Y%m%d%H%M%S"`
cur_pid=`echo $$`
tmp_log_file="/tmp/"$3"_"$dttm"_"$cur_pid".log"
output_dir=$5
appl_name=$3
jb_type="Merge"

sprk_jb_name=${appl_name}"_"${jb_type}


str1=`echo $5|cut -d/ -f1-3`
accnt_type=`echo $5|tr ':/.' ' '|awk '{print ($2)}'`

#str1="adl://developmentexcaliberdls.azuredatalakestore.net"
echo "master file is ${5%/*}"
final_stg_dir_tmp=`echo ${5%/*} | sed 's~LANDING~STAGING~;s~'$str1'~~'`
tmp=`echo $final_stg_dir_tmp |grep 'STAGING'|wc -l` 
if [ $tmp == 0 ]; then 
final_stg_dir_tmp=`echo $final_stg_dir_tmp|sed 's~/RAW~/RAW/STAGING~'`
fi
echo $final_srg_dir_tmp	
pat=`echo $final_stg_dir_tmp|grep -o "/"|wc -l`
if [ $pat -eq 5 ]; then 
final_stg_dir=`echo $final_stg_dir_tmp|rev| cut -d "/" -f1-|rev`"/"
else
final_stg_dir=`echo $final_stg_dir_tmp`
fi
#echo $final_stg_dir
intrm_dir=`echo $9 | sed 's~'$str1'~~'`
#echo $intrm_dir
# Spark submit command parameters
echo "az dls fs delete --account $accnt_type --path $final_stg_dir --recurse"

SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
SPARK_MASTER="yarn"
SPARK_DEPLOY_MODE="cluster"
YARN_QUEUE="default"

# Spark execution parameters
NUM_EXECUTOR="4"
EXECUTOR_CORES="10"
EXECUTOR_MEM="20g"

# Jars and name of the application jar file
#APP_JAR="/$HOME/spark-applications/jars/SparkMergeDataFiles-assembly-1.0.jar"
APP_JAR="/home/deploy/spark-applications/jars/SparkMergeDataFiles-assembly-1.0V9.jar"
CLASS_NAME="main.scala.MergeDataFiles"

$SPARK_SUBMIT --name ${sprk_jb_name} --master $SPARK_MASTER --deploy-mode $SPARK_DEPLOY_MODE --queue $YARN_QUEUE --num-executors $NUM_EXECUTOR --executor-cores $EXECUTOR_CORES --executor-memory $EXECUTOR_MEM  --class $CLASS_NAME $APP_JAR $@ > $tmp_log_file 2>&1 

appl_id=`cat $tmp_log_file|grep 'YarnClientImpl: Submitted'|awk -F " " '{print $NF}'`
#echo $appl_id
#############check the status of the spark submit from yarn##############
#
######################################################################
final_state=`yarn application -status $appl_id|grep 'Final-State'|cut -d ":" -f2|tr -d " "`
#echo $final_state
if [  $final_state == "SUCCEEDED" ];then
#csv_cnt=`yarn logs -applicationId $appl_id -log_files stdout|grep 'CSV File Record Count'|cut -d ":" -f2|tr -d " "`
rm -f $tmp_log_file
######################login to the azure ################################

az login --service-principal -u 9d5e2bc7-60ae-424d-91ec-21341b4ef221 -p gNacOfP9okqXd+XKL2BKp+oxa602PURubRyPu+c148A= --tenant 1caa43b8-bf09-48b6-9b3c-bd5a56fec019 > output_${appl_id}.txt
#################################delete destinatin#######################
echo "az dls fs delete --account $accnt_type --path $final_stg_dir --recurse"
az dls fs delete --account $accnt_type --path $final_stg_dir --recurse
#########################move the temporary folder to azure staging##########
az dls fs move --account $accnt_type --destination-path $final_stg_dir --source-path $intrm_dir --force

	if [ "$?" -eq "0" ];then
		echo "adl://${accnt_type}.azuredatalakestore.net/${final_stg_dir}""|${appl_id}"
		rm -f output_${appl_id1}.txt
	else
		echo "file upload unsuccessful"
		rm -f output_${appl_id}.txt
	fi
	#echo $csv_cnt"|Y"
	exit 0
else
rm -f $tmp_log_file
exit 1
fi



