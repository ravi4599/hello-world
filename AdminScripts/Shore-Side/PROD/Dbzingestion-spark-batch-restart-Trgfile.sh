#!/bin/bash

#SCRIPT INPUTS
env=$1
batchfile=$2
joblistfile=$3
location=$4
totalbatches=$5
shoreEMRlogin=$6
current_date=$(date "+%Y-%m-%d")
#SETTING ENVIONMENT SPECIFIC VARIABLES
if [ "$location" = "Ship" ]; then
        $(kinit -kt /etc/security/keytabs/nbxservice.keytab nbx.service@VIRGINVOYAGES.QA.DEV)
        sendermail="nbxadmin-"$env"-ship@virginvoyages.com"
        LOG_FILE="/data/admin-scripts/logs/ingestion-restart-scripts-$current_date.log"
elif [ "$location" = "Shore" ]; then
        echo "No need for Kinit on Shore"
        source /home/talenduser/.bashrc
        sendermail="nbxadmin-"$env"-shore@virginvoyages.com"
        LOG_FILE="/data/apps/talend/shared/scripts/sprak-streaming-job-logs/restart-scripts-$current_date.log"
else
        echo "Incorrect Location it should be either Shore or Ship"
        exit 1
fi

#FUNCTION TO LOG EVENTS IN LOG FILE
INVOKE_DIR=$(dirname $0)
logit()
{
echo "[${USER}][`date`] - ${*}" >> ${LOG_FILE}
}

#FUNCTION TO SEND EMAILS
sendemail()
{
#echo $1, $2 , $3
cat <<EOF | /usr/sbin/sendmail -t
From:$3
To:thimma-reddy.chinnaiahgari@capgemini.com;zill.silveira@capgemini.com
Subject:$1
$2
EOF
}

#FUMCTION TO KILLS THE JOBS
parametercheckandforcekill()
{
configfile=$1
appid=$2
shoreEMRlogin=$3
yarnname=$4

configfilename=`basename $configfile`
config="spark.stop.trgFile"
config1="spark.trgFileName"
if [ -z $(grep "$config" "$configfile") ]; then
        echo "parameter not present in the file the job will not be able to exit notmal way";
        sendemail "$yarnname dosent have trgFile configuration in $location$env" "$yarnname dosent have trgFile configuration in $location$env" "$sendermail"
        if [ "$location" = "Ship" ]; then
                response1=$(yarn application -kill $appid)
                echo $response1
                [ -z "$response" ] && echo "Killed"
        else
                response1=$(ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $shoreEMRlogin "yarn application -kill $appid")
                echo $response1
                [ -z "$response" ] &&echo "Killed"
        fi
else
        echo "parameters  present in the properties file killing gracefully"
fi
}

#FUNCTION TO ADD TRIGGERFILE PARAMETERS TO CONFIGFILES
parametercheckandadd()
{
configfile=$1
configfilename=`basename $configfile`
config="spark.stop.trgFile"
config1="spark.trgFileName"
if [ -z $(grep "$config" "$configfile") ]; then echo -e "\nspark.stop.trgFile Y" >> $configfile; fi
if [ -z $(grep "$config1" "$configfile") ]; then echo -e "spark.trgFileName hdfs:///data/streamingtriggers/$configfilename.trg" >> $configfile; fi
}

#FUNCTION TO TRIGGER THE SPARK-SUBMITS
submitcmd()
{
spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.schedulerBacklogTimeout=10 --conf spark.dynamicAllocation.executorIdleTimeout=15 --conf spark.dynamicAllocation.maxExecutors=$maxexec --conf spark.dynamicAllocation.initialExecutors=1 --driver-memory $drivmem --executor-memory $execmeme --executor-cores 4 --name $yarnname $classvar $class $propvar $propertiesfile $jarvar $jars $binary
}

IFS=$'\n'

#RETRIVING CURRENTLY RUNNING BATCH FROM BATCH FILE
batch=`cat $batchfile`
echo Currently running batch is :$batch
echo Killing jobs of Batch $batch

#CHECKING IF JOBS IN PREVIOUS BATCH ARE RUNNING AND KILL IF THEY ARE
for job in `sed '1d' $joblistfile`
do
        jobbatch=$(echo $job | cut -d '#' -f1)
        flag=$(echo $job | cut -d '#' -f2)
        yarnname=$(echo $job | cut -d '#' -f3)
        propertiesfile=$(echo $job | cut -d '#' -f8)
        if [ "$flag" = "Y" ] && [ "$batch" = "$jobbatch" ]; then
                if [ "$location" = "Ship" ]; then
                        response=$(yarn application -list -appStates RUNNING ACCEPTED -appTypes SPARK | grep -wi $yarnname | cut -d ' ' -f1,2)
                elif [ "$location" = "Shore" ]; then
                        response=$(ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $shoreEMRlogin "yarn application -list -appStates RUNNING ACCEPTED -appTypes SPARK | grep -wi $yarnname | cut -d ' ' -f1,2")
                fi

                if [ -z "$response" ]; then
                        echo "$yarnname was not running or something's wrong got following response $response"
                        logit "$yarnname was not running or something's wrong got following response $response"
                        #Alerts handled my metadata
                        sendemail "$yarnname was not running in $location$env" "$yarnname was not running in $location$env, kindly check this script will attempy to restart it now." "$sendermail"
            else
                        echo "$yarnname is running"
                        echo "killing Job $yarnname"
                        logit "$yarnname is running"
                        logit "killing Job $yarnname"
                        appid=$(echo $response | xargs | tr -s "[:blank:]" | cut -d ' ' -f1)
                        parametercheckandforcekill "$propertiesfile" "$appid" "$shoreEMRlogin" "$yarnname"
                        hdfsfile=$(grep "$spark.trgFileName" "$propertiesfile" | awk '{ print $2}')
                        #CREATING THE TRIGGER FILE
                        if [ "$location" = "Ship" ]; then
                                hdfs dfs -mkdir -p hdfs:///data/streamingtriggers ; hdfs dfs -touchz $hdfsfile
                        elif [ "$location" = "Shore" ]; then
                                response=$(ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $shoreEMRlogin "sudo -u hadoop hdfs dfs -mkdir -p /data/streamingtriggers ; sudo -u hadoop hdfs dfs -touchz $hdfsfile")
                        fi
                fi
        fi
done

#INCREMENTING THE BATCH IN BATCHFILE
if [ "$batch" -lt "$totalbatches" ]; then
        batch=$((batch+1))
        echo updating batch file batch $batch
        echo $batch > $batchfile
else
        batch=1
        echo $batch > $batchfile
fi

#STARTING JOBS FOR CURRENT BATCH
for job in `sed '1d' $joblistfile`
do
        hashcount=$(echo "$job" | tr -cd '#' | wc -c)
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

        if [[ ("$hashcount" -eq 9) && ($jobbatch =~ ^[0-9]+$) && ("${flag}" =~ ^[YN]+$) && ("${maxexec}" =~ ^[0-9]+$) && ("${drivmem}" =~ ^[0-9]G+$) && ("${execmeme}" =~ ^[0-9]G+$) && ("$flag" = "Y") && ("$batch" = "$jobbatch") ]]; then
                echo "Starting $yarnname"
                logit "Starting $yarnname"
#INVOKING PATAMETER CHECK AND ADD FUNCTION
                parametercheckandadd "$propertiesfile"
                hdfsfile=$(grep "spark.trgFileName" "$propertiesfile" | awk '{ print $2}')

#CHECKING IF JOB IS NOT ALREADY RUNNING
                if [ "$location" = "Ship" ]; then
                        response=$(yarn application -list -appStates RUNNING ACCEPTED -appTypes SPARK | grep -wi $yarnname | cut -d ' ' -f1,2)
                elif [ "$location" = "Shore" ]; then
                        response=$(ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $shoreEMRlogin "yarn application -list -appStates RUNNING ACCEPTED -appTypes SPARK | grep -wi $yarnname | cut -d ' ' -f1,2")
                fi

#TRIGGERING THE JOB

                if [ -z "$response" ]; then
                        echo "$yarnname was not fond running triggering it"

#REMOVING THE TRIGGER FILE
                        if [ "$location" = "Ship" ]; then
                                hdfs dfs -rm $hdfsfile
                        elif [ "$location" = "Shore" ]; then
                                resp=$(ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $shoreEMRlogin "sudo -u hadoop hdfs dfs -rm $hdfsfile")
                        fi

                        cd $INVOKE_DIR
                        submitcmd > /dev/null 2>&1 &
                        #                   submitcmd &
                        echo "Job Triggered"
                        logit "$yarnname Triggered"
                else
                        echo "$yarnname is already running"
                        appid=$(echo $response | xargs | tr -s "[:blank:]" | cut -d ' ' -f1)
                        #sendemail "$yarnname already found running in $location$env" "$yarnname already found running with $appid" "$sendermail"
                fi

                sleep 10
                #GETTING APPLICATION ID OF THE NEW JOB
                if [ "$location" = "Ship" ]; then
                        response3=$(yarn application -list -appStates RUNNING ACCEPTED -appTypes SPARK | grep -wi $yarnname | cut -d ' ' -f1,2)
                elif [ "$location" = "Shore" ]; then
                        response3=$(ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $shoreEMRlogin "yarn application -list -appStates RUNNING ACCEPTED -appTypes SPARK | grep -wi $yarnname | cut -d ' ' -f1,2")
                fi

                appid1=$(echo $response3 | tr -s "[:blank:]" | cut -d ' ' -f1)
                echo "New application ID for job $yarnname is $appid1"
                logit "New application ID for job $yarnname is $appid1"
                #sendemail "$yarnname Restarted in $location$env" "New application ID for job is $appid1 old app id was $appid" "$sendermail"
    elif [ "$flag" = "N" ] && [ "$batch" = "$jobbatch" ]; then
                echo "Parameter File is invalid at $job"
                #sendemail "$yarnname Parameter File is invalid $location$env" "$yarnname Parameter File is invalid $job" "$sendermail"
        fi
done