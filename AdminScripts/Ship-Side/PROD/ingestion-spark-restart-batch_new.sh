#!/bin/bash

env=$1

$(kinit -kt /etc/security/keytabs/nbxservice.keytab nbx.service@AHOY.VIRGINVOYAGES.COM)

LOG_FILE="/data/admin-scripts/logs/ingestion-restart-scripts.log"
INVOKE_DIR=$(dirname $0)
logit()
{
    echo "[${USER}][`date`] - ${*}" >> ${LOG_FILE}
}

sendemail()
{
#echo $1, $2
cat <<EOF | /usr/sbin/sendmail -t
From:nbxadmin-scl-prod@virginvoyages.com
To:zill.silveira@capgemini.com
Subject:$1
$2
EOF
}


submitcmd()
 {
  spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.schedulerBacklogTimeout=10 --conf spark.dynamicAllocation.executorIdleTimeout=15 --conf spark.dynamicAllocation.maxExecutors=$maxexec --conf spark.dynamicAllocation.initialExecutors=1 --driver-memory $drivmem --executor-memory $execmeme  --executor-cores 4 --name $yarnname $classvar $class $propvar $propertiesfile $jarvar $jars $binary
 }

IFS=$'\n'

batch=`cat /data/admin-scripts/files/ingestion-app-restart-batch-new.txt`
echo Currently running batch is :$batch
echo Killing jobs of Batch $batch


for job in `sed '1d' /data/admin-scripts/files/ingestion-app-restart-list-new.txt`
do
 jobbatch=$(echo $job | cut -d '#' -f1)
 flag=$(echo $job | cut -d '#' -f2)
 yarnname=$(echo $job | cut -d '#' -f3)

 if [ "$flag" = "Y" ] && [ "$batch" = "$jobbatch" ]; then
        response=$(yarn application -list | grep -wi $yarnname | cut -d ' ' -f1,2)
        if [ -z "$response" ]; then
                echo "$yarnname was not running or something's wrong got following response $response"
                logit "$yarnname was not running or something's wrong got following response $response"
#                sendemail "$yarnname was not running in $env" "$yarnname was not running in $env, kindly check this script will attempy to restart it now."
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
        fi
 fi
done
 if [ "$batch" -lt 6 ]; then
 batch=$((batch+1))
 echo updating batch file batch $batch
 echo $batch > /data/admin-scripts/files/ingestion-app-restart-batch-new.txt
 else
 batch=1
 echo $batch > /data/admin-scripts/files/ingestion-app-restart-batch-new.txt
 fi

for job in `sed '1d' /data/admin-scripts/files/ingestion-app-restart-list-new.txt`
do
 jobbatch=$(echo $job | cut -d '#' -f1)
 flag=$(echo $job | cut -d '#' -f2)
 yarnname=$(echo $job | cut -d '#' -f3)
 maxexec=$(echo $job | cut -d '#' -f4)
 drivmem=$(echo $job | cut -d '#' -f5)
 execmeme=$(echo $job | cut -d '#' -f6)
 class=$(echo $job | cut -d '#' -f7)
 propertiesfile=$(echo $job | cut -d '#' -f8)
 jars=$(echo $job | cut -d '#' -f9)
 binary=$(echo $job | cut -d '#' -f10)

 [[ ! -z "$class" ]] && class=$class && classvar=--class || classvar=''
 [[ ! -z "$propertiesfile" ]] && propertiesfile=$propertiesfile && propvar=--properties-file || propvar=''
 [[ ! -z "$jars" ]] && jars=$jars && jarvar=--jars || jarvar=''

 if [ "$flag" = "Y" ] && [ "$batch" = "$jobbatch" ]; then
                echo "Starting $yarnname"
                logit "Starting $yarnname"
                cd $INVOKE_DIR
                submitcmd > /dev/null 2>&1 &
#                submitcmd &
                echo "Job Triggered"
                logit "$yarnname Triggered"
                 sleep 10
                response3=$(yarn application -list | grep -w $yarnname | cut -d ' ' -f1)
                appid1=$(echo $response3 | tr -s "[:blank:]" | cut -d ' ' -f1)
                echo "New application ID for job $yarnname is $appid1"
                logit "New application ID for job $yarnname is $appid1"
#                sendemail "$yarnname Restarted in $env" "New application ID for job is $appid1 old app id was $appid"
 fi
done