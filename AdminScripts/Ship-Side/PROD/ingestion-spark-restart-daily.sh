#!/bin/bash

/usr/bin/kinit -kt /etc/security/keytabs/nbxservice.keytab nbx.service@AHOY.VIRGINVOYAGES.COM

MXPIngestionSalessubmit()
{
spark-submit --name MXPIngestionSales --class com.virginvoyages.kafka.ingestion.EnablerFramework --master yarn --deploy-mode cluster --num-executors 1  --driver-memory 2g --executor-memory 2g --executor-cores 1 --jars /data/apps/nbx/Metadata/virginvoyages_ship_metadata-0.0.1-SNAPSHOT-jar-with-dependencies.jar --properties-file /data/apps/nbx/MXP/ingestion/config/SrcSales_TgtSales /data/apps/nbx/MXP/ingestion/virginvoyages_ship_kafka_ingestion-0.0.1-SNAPSHOT-jar-with-dependencies.jar
}
MXPIngestionCruisesubmit()
{
spark-submit --name MXPIngestionCruise --class com.virginvoyages.kafka.ingestion.EnablerFramework --master yarn --deploy-mode cluster --num-executors 1  --driver-memory 2g --executor-memory 2g --executor-cores 1 --jars /data/apps/nbx/Metadata/virginvoyages_ship_metadata-0.0.1-SNAPSHOT-jar-with-dependencies.jar --properties-file /data/apps/nbx/MXP/ingestion/config/SrcCru_TgtCru /data/apps/nbx/MXP/ingestion/virginvoyages_ship_kafka_ingestion-0.0.1-SNAPSHOT-jar-with-dependencies.jar
}
GoSparkIngestionVenuesubmit()
{
spark-submit --name GoSparkIngestionVenue --class com.virginvoyages.kafka.ingestion.EnablerFramework --master yarn --deploy-mode cluster --num-executors 1  --driver-memory 2g --executor-memory 2g --executor-cores 1 --jars /data/apps/nbx/Metadata/virginvoyages_ship_metadata-0.0.1-SNAPSHOT-jar-with-dependencies.jar --properties-file /data/apps/nbx/MXP/ingestion/config/SrcGoVenueAvailKafka_TgtGoVenuAvailHive /data/apps/nbx/MXP/ingestion/virginvoyages_ship_kafka_ingestion-0.0.1-SNAPSHOT-jar-with-dependencies.jar
}
MXPIngestionGuestsubmit()
{
spark-submit --name MXPIngestionGuest --class com.virginvoyages.kafka.ingestion.EnablerFramework --master yarn --deploy-mode cluster --num-executors 1  --driver-memory 2g --executor-memory 2g --executor-cores 1 --jars /data/apps/nbx/Metadata/virginvoyages_ship_metadata-0.0.1-SNAPSHOT-jar-with-dependencies.jar --properties-file /data/apps/nbx/MXP/ingestion/config/SrcMXPGuest_TgtMXPGuest /data/apps/nbx/MXP/ingestion/virginvoyages_ship_kafka_ingestion-0.0.1-SNAPSHOT-jar-with-dependencies.jar
}
CMSActivityConsumersubmit()
{
spark-submit --name CMSActivityConsumer --conf spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.schedulerBacklogTimeout=10 --conf spark.dynamicAllocation.executorIdleTimeout=15 --conf spark.dynamicAllocation.maxExecutors=2 --conf spark.dynamicAllocation.initialExecutors=1 --class com.virginvoyages.ship.sparkstreaming.CMSActivityConsumer --properties-file /tmp/vvconsumerscms/vvconsumers.conf --master yarn --deploy-mode cluster --driver-memory 4g --executor-memory 4g --executor-cores 8 /tmp/vvconsumerscms/VV-NBX-Ship-RecommendationRealtime-0.0.1-SNAPSHOT-jar-with-dependencies.jar
}
ARS_Booking_Ingestionsubmit()
{
spark-submit --name ARS_Booking_Ingestion --conf spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.schedulerBacklogTimeout=10 --conf spark.dynamicAllocation.executorIdleTimeout=15 --conf spark.dynamicAllocation.maxExecutors=2 --conf spark.dynamicAllocation.initialExecutors=1 --class com.virginvoyages.ship.sparkstreaming.ArsBooking  --properties-file /data/apps/nbx/recommendation-realtime/vvconsumers_prod_ars.conf --master yarn  --deploy-mode cluster  --driver-memory 4g  --executor-memory 4g  --executor-cores 8 /data/apps/nbx/recommendation-realtime/VV-NBX-Ship-RecommendationRealtime-0.0.1-SNAPSHOT-jar-with-dependencies.jar
}

LOG_FILE="/data/admin-scripts/logs/ingestion-spark-restart-daily.sh.log"
INVOKE_DIR=$(dirname $0)
logit()
{
    echo "[${USER}][`date`] - ${*}" >> ${LOG_FILE}
}

sendemail()
{
echo $1, $2
#cat <<EOF | /usr/sbin/sendmail -t
#From:talendadmin-non-prod@virginvoyages.com
#To:virginnbxservicedesk.in@capgemini.com
#Subject:$1
#$2
#EOF
}

IFS=$'\n'

for job in `cat /data/admin-scripts/spark-ingestion-restart/spark-ingestion-app-restart-list.txt`
do
 flag=$(echo $job | cut -d '#' -f1)
 yarnname=$(echo $job | cut -d '#' -f2)
 trigger="$yarnname""submit"
 if [ "$flag" = "Y" ]; then
        response=$(yarn application -list | grep -i $yarnname | awk '{print $1,$2}')
        if [ -z "$response" ]; then
                echo "$yarnname was not running or something's wrong got following response $response"
                logit "$yarnname was not running or something's wrong got following response $response"
                sendemail "$yarnname was not running in $env" "$yarnname was not running in $env, kindly check this script will attempy to restart it now."
                                echo "Starting $yarnname"
                                logit "Starting $yarnname"
                                cd $INVOKE_DIR
                                $trigger > /dev/null 2>&1 &
                                echo "Job Triggered"
                                logit "$yarnname Triggered"
                                sleep 20
                                response3=$(yarn application -list | grep -i $yarnname |  awk '{print $1,$2}')
                                appid1=$(echo $response3 | tr -s "[:blank:]" | cut -d ' ' -f1)
                                echo "New application ID for job $yarnname is $appid1"
                                logit "New application ID for job $yarnname is $appid1"
                                sendemail "Started $yarnname in $env" "New application ID for job $yarnname is $appid1"
                else
                echo "$yarnname is running"
                echo "killing Job $yarnname"
                logit "$yarnname is running"
                logit "killing Job $yarnname"
                appid=$(echo $response | awk '{print $1}')
                appname=$(echo $response | awk '{print $2}')
                echo "Killing application $appid with name $appname"
                logit "Killing application $appid with name $appname"
                response1=$(yarn application -kill $appid)
                echo $response1
                logit $response1
                [ -z "$response" ] && echo "Killed"
                echo "Restarting $yarnname"
                logit "Restarting $yarnname"
                cd $INVOKE_DIR
                $trigger > /dev/null 2>&1 &
                echo "Job Triggered"
                logit "$yarnname Triggered"
                sleep 20
                response3=$(yarn application -list | grep -i $yarnname | awk '{print $1,$2}')
                appid1=$(echo $response3 | awk '{print $1}')
                echo "New application ID for job $yarnname is $appid1"
                logit "New application ID for job $yarnname is $appid1"
                sendemail "$yarnname Restarted in $env" "New application ID for job is $appid1 old app id was $appid"
        fi
 fi
done