#!/bin/sh

env=$2

#submitcmd()
#{
#spark-submit --name $yarnname --class com.virginvoyages.kafka.ingestion.EnablerFramework --master yarn --deploy-mode cluster --num-executors $numexecutors  --driver-memory $drivermem --executor-memory $executormem --executor-cores $execcores --jars /data/apps/talend/shared/scripts/nbx/Metadata/virginvoyages_shore_metadata-0.0.1-SNAPSHOT-jar-with-dependencies.jar --properties-file /data/apps/talend/shared/scripts/nbx/Ship-to-shore/config/$configfile /data/apps/talend/shared/scripts/nbx/Ship-to-shore/kafka_to_hive_sync-0.0.1-SNAPSHOT-jar-with-dependencies.jar
#}

#submitcmd()
#{
#spark-submit --name $yarnname --class com.virginvoyages.kafka.ingestion.EnablerFramework --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.schedulerBacklogTimeout=10 --conf spark.dynamicAllocation.executorIdleTimeout=15 --conf spark.dynamicAllocation.maxExecutors=5 --conf spark.dynamicAllocation.initialExecutors=1 --conf spark.shuffle.service.enabled=true --driver-memory $drivermem --executor-memory $executormem --executor-cores $execcores --jars /data/apps/talend/shared/scripts/nbx/Metadata/virginvoyages_shore_metadata-0.0.1-SNAPSHOT-jar-with-dependencies.jar --properties-file /data/apps/talend/shared/scripts/nbx/Ship-to-shore/config/$configfile /data/apps/talend/shared/scripts/nbx/Ship-to-shore/kafka_to_hive_sync-0.0.1-SNAPSHOT-jar-with-dependencies.jar
#}

submitcmd()
 {
  spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.schedulerBacklogTimeout=10 --conf spark.dynamicAllocation.executorIdleTimeout=15 --conf spark.dynamicAllocation.maxExecutors=$maxexec --conf spark.dynamicAllocation.initialExecutors=1 --driver-memory $drivmem --executor-memory $execmeme  --executor-cores 4 --name $yarnname $classvar $class $propvar $propertiesfile $jarvar $jars $binary
 }

source /home/talenduser/.bashrc
LOG_FILE="/data/apps/talend/shared/scripts/sprak-streaming-job-logs/restart-scripts.log"
INVOKE_DIR=$(dirname $0)
logit()
{
    echo "[${USER}][`date`] - ${*}" >> ${LOG_FILE}
}

sendemail()
{
#echo $1, $2
cat <<EOF | /usr/sbin/sendmail -t
From:talendadmin-prod-shore@virginvoyages.com
To:thimma-reddy.chinnaiahgari@capgemini.com
Subject:$1
$2
EOF
}
IFS=$'\n'
for job in `sed '1d' /data/apps/talend/shared/parameterfiles/ship2shore-app-restart-list-new.txt`
do
 flag=$(echo $job | cut -d '#' -f1)
 yarnname=$(echo $job | cut -d '#' -f2)
 maxexec=$(echo $job | cut -d '#' -f3)
 drivmem=$(echo $job | cut -d '#' -f4)
 execmeme=$(echo $job | cut -d '#' -f5)
 class=$(echo $job | cut -d '#' -f6)
 propertiesfile=$(echo $job | cut -d '#' -f7)
 jars=$(echo $job | cut -d '#' -f8)
 binary=$(echo $job | cut -d '#' -f9)

 [[ ! -z "$class" ]] && class=$class && classvar=--class || classvar=''
 [[ ! -z "$propertiesfile" ]] && propertiesfile=$propertiesfile && propvar=--properties-file || propvar=''
 [[ ! -z "$jars" ]] && jars=$jars && jarvar=--jars || jarvar=''

 if [ "$flag" = "Y" ]; then
        response=$(ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $1 "yarn application -list -appStates RUNNING -appTypes SPARK | grep -w $yarnname | cut -d ' ' -f1,2")
        if [ -z "$response" ]; then
                echo "$yarnname was not running or something's wrong got following response $response"
                logit "$yarnname was not running or something's wrong got following response $response"
                sendemail "$yarnname was not running in $env" "$yarnname was not running in $env, kindly check this script will attempy to restart it now."
                                echo "Starting $yarnname"
                                logit "Starting $yarnname"
                                cd $INVOKE_DIR
                                submitcmd > /dev/null 2>&1 &
                                echo "Job Triggered"
                                logit "$yarnname Triggered"
                                sleep 20
                                response3=$(ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $1 "yarn application -list -appStates RUNNING -appTypes SPARK | grep -w $yarnname | cut -d ' ' -f1")
                                appid1=$(echo $response3 | xargs | tr -s "[:blank:]" | cut -d ' ' -f1)
                                echo "New application ID for job $yarnname is $appid1"
                                logit "New application ID for job $yarnname is $appid1"
#                                sendemail "Started $yarnname in $env" "New application ID for job $yarnname is $appid1"
                else
                echo "$yarnname is running"
                echo "killing Job $yarnname"
                logit "$yarnname is running"
                logit "killing Job $yarnname"
                appid=$(echo $response | xargs | tr -s "[:blank:]" | cut -d ' ' -f1)
                appname=$(echo $response | xargs | tr -s "[:blank:]" | cut -d ' ' -f2)
                echo "Killing application $appid with name $appname"
                logit "Killing application $appid with name $appname"
                response1=$(ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $1 "yarn application -kill $appid")
                echo $response1
                logit $response1
                [ -z "$response" ] &&echo "Killed"
                echo "Restarting $yarnname"
                logit "Restarting $yarnname"
                cd $INVOKE_DIR
                submitcmd > /dev/null 2>&1 &
                echo "Job Triggered"
                logit "$yarnname Triggered"
                sleep 20
                response3=$(ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $1 "yarn application -list -appStates RUNNING -appTypes SPARK | grep -w $yarnname | cut -d ' ' -f1")
                appid1=$(echo $response3 | xargs | tr -s "[:blank:]" | cut -d ' ' -f1)
                echo "New application ID for job $yarnname is $appid1"
                logit "New application ID for job $yarnname is $appid1"
#                sendemail "$yarnname Restarted in $env" "New application ID for job is $appid1 old app id was $appid"
        fi
 fi
done