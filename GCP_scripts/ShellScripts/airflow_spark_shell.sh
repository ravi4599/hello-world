#!/bin/bash
job_name=$1
codefile=$2
classname=$3
propfile=$4
jarfile=$5
conf_param=$6


MAIL_CMD="$(which mail)"
WHOAMI="vv-app-alert@virginvoyages.com"
EMAIL_PAGERDUTY="virginnbxservicedesk.in@capgemini.com,sarang.laxman@capgemini.com,w54y1dm66pd@bemodem.com,vvamsl1l2support.amer@capgemini.com,alexander.saip@virginvoyages.com,chris.robinson@virginvoyages.com,adel.elia@virginvoyages.com,enrique.altuna@virginvoyages.com"
EMAIL="virginnbxservicedesk.in@capgemini.com,sarang.laxman@capgemini.com,vvamsl1l2support.amer@capgemini.com,alexander.saip@virginvoyages.com,chris.robinson@virginvoyages.com,adel.elia@virginvoyages.com,enrique.altuna@virginvoyages.com"

resources()
{
check=$(grep -w "$1:$2" /data/admin-scripts/airflow_external/airflow_paramfile.txt)
param_name=$(if [ ! -z "$check" ]; then awk -F: -v job="$1" -v res="$2" '$1==job && $2==res {print $3}' /data/admin-scripts/airflow_external/airflow_paramfile.txt; else echo "$3"; fi )
}

parameters()
{
if [ "$#" != 0 ]; then  param_name="$2 $1"; else param_name=""; fi
}

if [ `yarn application -list -appStates RUNNING |grep  $job_name |sort -V -r  | head -1 | wc -l` != 0 ]
then
	echo "Job is running " 
	while : 
	do 
	
		if [ `yarn application -list -appStates RUNNING |grep  $job_name |sort -V -r  | head -1 | wc -l` -eq 0 ]
		then
		echo "Job Completed"
		break
		fi
	sleep 60	
	done
	if [ `yarn application -list -appStates FAILED |grep -i $job_name |sort -V -r  | head -1 | wc -l` -eq 0 ]
	then 
	exit 0 
	else 
	exit 0 
	fi
fi


resources $job_name executor_mem 4g; executor_mem="$param_name"
resources $job_name executor_core 4; executor_core="$param_name"
resources $job_name executor_num 4; executor_num="$param_name"
resources $job_name driver_mem 4g; driver_mem="$param_name"

if [[ "$job_name" != [Nn][Aa] ]]; then parameters $job_name --name; job_name="$param_name"; else job_name=" "; fi
if [[ "$codefile" != [Nn][Aa] ]]; then parameters $codefile; code_file="$param_name"; else code_file=" "; fi
if [[ "$classname" != [Nn][Aa] ]]; then parameters $classname --class; class_name="$param_name"; else class_name=" "; fi
if [[ "$propfile" != [Nn][Aa] ]]; then parameters $propfile --properties-file; prop_file="$param_name"; else prop_file=" "; fi
if [[ "$jarfile" != [Nn][Aa] ]]; then parameters $jarfile --jars; jar_file="$param_name"; else jar_file=" "; fi

echo "spark-submit $job_name --master yarn $prop_file  $conf_param $jar_file --num-executors $executor_num  --executor-cores $executor_core --executor-memory $executor_mem --driver-memory $driver_mem $class_name --deploy-mode cluster $code_file"

spark-submit $job_name --master yarn $prop_file  $conf_param $jar_file --num-executors $executor_num  --executor-cores $executor_core --executor-memory $executor_mem --driver-memory $driver_mem $class_name --deploy-mode cluster $code_file

error_code="$?"

if [ "$error_code" != 0 ]; then
app_id=$(curl -s GET "http://$HADOOPMASTER:8088/ws/v1/cluster/apps?states=FINISHED,FAILED,KILLED" | sed s#},{#}\\\n{#g   | sed "s/{\"apps\":{\"app\":\[//1"  | sed s#]}}##g | grep -i $job_name | jq .id | sort -r | head -n 1)
rm_tracking_url=$(curl -s GET "http://$HADOOPMASTER:8088/ws/v1/cluster/apps?states=FINISHED,FAILED,KILLED" | sed s#},{#}\\\n{#g   | sed "s/{\"apps\":{\"app\":\[//1"  | sed s#]}}##g | grep -i $app_id | jq .amContainerLogs)
#echo "Tracking_URL: $rm_tracking_url" |  mail -s "Airflow-Job $job_name failed in Shore CERT - $app_id" -r "vv-app-alert-non-prod@virginvoyages.com"  akshay.doifode@capgemini.com,zill.silveira@capgemini.com,umesh.sawant@capgemini.com,greeshma.girish@capgemini.com,aaron-a.rebello@capgemini.com,snehal.shetty@capgemini.com,ravi-teja.voleti@capgemini.com
app_name=$(echo $job_name |awk -F ' ' '{print $2}')
critical_check=$(grep -w "$app_name" /data/apps/scripts/pager_duty.txt)
pager_duty=$(if [ ! -z "$critical_check" ]; then echo $critical_check| awk -F, '{print $2}'; else echo "N"; fi )
alert_priority=$(if [ ! -z "$critical_check" ]; then echo $critical_check| awk -F, '{print $3}'; else echo "Low"; fi )
echo "${alert_priority} : Prod GCP Dag ${app_name} Task Failed"
if [ "$pager_duty" == 'Y' ]; then echo "$job_name has Failed in Airflow" | ${MAIL_CMD} -s "${alert_priority} : Prod GCP Dag ${app_name} Task Failed" -S from="${WHOAMI}" "${EMAIL_PAGERDUTY}"; else echo "$job_name has Failed in Airflow" | ${MAIL_CMD} -s "${alert_priority} : Prod GCP Dag Task Failed" -S from="${WHOAMI}" "${EMAIL}"; fi
fi

exit "$error_code"


