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
/opt/spark/bin/spark-submit --name Acxiom_Sailor_Attributes --conf spark.executor.memoryOverhead=10240 --jars /data/apps/talend/shared/scripts/cap-bigdata-prod-pyspark/lib/phoenix-spark-4.11.0-HBase-1.3.jar,/data/apps/talend/shared/scripts/cap-bigdata-prod-pyspark/lib/phoenix-4.11.0-HBase-1.3-client.jar --num-executors 20 --driver-memory 4G --executor-memory 8G --executor-cores 4 --master yarn --deploy-mode cluster /data/apps/talend/shared/scripts/cap-bigdata-prod-pyspark/src/sailor_attributes/acxiom_sailor_attributes.py $1 $2