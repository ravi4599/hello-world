#!/bin/sh
source /home/talenduser/.bashrc
LOG_FILE="/data/apps/talend/shared/scripts/sprak-streaming-job-logs/restart-scripts.log"
INVOKE_DIR=$(dirname $0)
logit()
{
    echo "[${USER}][`date`] - ${*}" >> ${LOG_FILE}
}

sendemail()
{
cat <<EOF | /usr/sbin/sendmail -t
From:talendadmin-non-prod@virginvoyages.com
To:virginnbxservicedesk.in@capgemini.com
Subject:$1
$2
EOF
}

for job in `cat /data/apps/talend/shared/parameterfiles/spark-streaming-app-restart-list.txt`
do
 yarnname=$(echo $job | cut -d '#' -f1)
 triggername=$(echo $job | cut -d '#' -f2)
 env=$(echo $job | cut -d '#' -f3)
 flag=$(echo $job | cut -d '#' -f4)
 if [ "$flag" = "Y" ]; then
        response=$(ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $1 "yarn application -list | grep $yarnname | cut -d ' ' -f1,2")
        if [ -z "$response" ]; then
                echo "$yarnname was not running or something's wrong got following response $response"
                logit "$yarnname was not running or something's wrong got following response $response"
                sendemail "$triggername was not running in $env" "$yarnname was not running in $env, kindly check this script will attempy to restart it now."
                                echo "Starting $yarnname"
                                logit "Starting $yarnname"
                                cd $INVOKE_DIR
                                sh ./invoke_realtime_consumers.sh $triggername $env | tee -a $LOG_FILE
                                echo "Job Triggered"
                                logit "$yarnname Triggered"
                                sleep 20
                                response3=$(ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $1 "yarn application -list | grep $yarnname | cut -d ' ' -f1")
                                appid1=$(echo $response3 | cut -d ' ' -f1)
                                echo "New application ID for job $yarnname is $appid1"
                                logit "New application ID for job $yarnname is $appid1"
#                                sendemail "Started $triggername in $env" "New application ID for job $yarnname is $appid1"
                else
                echo "$yarnname is running"
                echo "killing Job $yarnname"
                logit "$yarnname is running"
                logit "killing Job $yarnname"
                appid=$(echo $response | cut -d ' ' -f1)
                appname=$(echo $response | cut -d ' ' -f2)
                echo "Killing application $appid with name $appname"
                logit "Killing application $appid with name $appname"
                response1=$(ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $1 "yarn application -kill $appid")
                echo $response1
                logit $response1
                [ -z "$response" ] &&echo "Killed"
                echo "Restarting $yarnname"
                logit "Restarting $yarnname"
                cd $INVOKE_DIR
                sh ./invoke_realtime_consumers.sh $triggername $env | tee -a $LOG_FILE
                echo "Job Triggered"
                logit "$yarnname Triggered"
                sleep 20
                response3=$(ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $1 "yarn application -list | grep $yarnname | cut -d ' ' -f1")
                appid1=$(echo $response3 | cut -d ' ' -f1)
                echo "New application ID for job $yarnname is $appid1"
                logit "New application ID for job $yarnname is $appid1"
#                sendemail "$triggername Restarted in $env" "New application ID for job is $appid1 old app id was $appid"
        fi
 fi
done
