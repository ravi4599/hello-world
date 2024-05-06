export SPARK_HOME=/opt/spark/
export PATH="$PATH:/opt/spark/bin"

export YARN_CONF_DIR=/opt/spark/conf
export HIVE_CONF_DIR=/opt/spark/conf
export HADOOP_CONF_DIR=/opt/spark/conf
export HADOOP_USER_NAME=hadoop


export HADOOP_HOME=/opt/spark/
export PATH="$PATH:/opt/spark/bin" 
echo "reading env var"

echo $SPARK_HOME
echo $PATH
echo $YARN_CONF_DIR
echo $HIVE_CONF_DIR
echo "completed reading env var"

echo "parameter 1 : "${1}
echo "parameter 2: "${2}
/opt/spark/bin/spark-submit --name ${1} --conf spark.yarn.maxAppAttempts=1 --num-executors 8 --driver-memory 16G --executor-memory 16G --executor-cores 6 --master yarn --deploy-mode cluster /data/apps/talend/shared/scripts/cap-bigdata-prod-pyspark/cdc_crm_emailresponsses.py ${2}