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
/opt/spark/bin/spark-submit --name ${1} --num-executors 2 --driver-memory 2G --executor-memory 2G --executor-cores 2 --master yarn --deploy-mode cluster /data/apps/talend/shared/scripts/cap-bigdata-prod-pyspark/cdc_crm.py ${2}