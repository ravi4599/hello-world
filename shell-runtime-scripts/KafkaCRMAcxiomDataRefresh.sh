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
sh /opt/spark/bin/spark-submit --files /data/apps/talend/shared/scripts/cap-bigdata-acxiom-client-prod/config/global.properties --jars /data/apps/talend/shared/scripts/cap-bigdata-acxiom-client-prod/dsapi-0.0.1-SNAPSHOT-jar-with-dependencies.jar --class com.virginvoyages.sparkstreaming.KafkaCRMAcxiomDataRefresh --properties-file /data/apps/talend/shared/scripts/cap-bigdata-real-time-prod/config/vvconsumers.conf --master yarn --deploy-mode cluster --driver-memory 16g --num-executors 3 --executor-memory 8g --executor-cores 2 /data/apps/talend/shared/scripts/cap-bigdata-real-time-prod/VVSparkStreamingNBXApi-0.0.1-SNAPSHOT-jar-with-dependencies.jar
