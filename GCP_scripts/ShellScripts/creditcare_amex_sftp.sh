#!/bin/bash

url=$1
username=$2
password=$3
#archive_path=$4
source_path=$4
destination_path=$5



#lftp sftp://$username:$password@$url

#date=`lftp sftp://$username:$password@$url -e 'ls outbox;exit' | tail -1 | awk -F ' ' '{print $9}'`

#echo $date

if [ $(lftp sftp://$username:$password@$url -e 'ls outbox;exit' | wc -l) -eq 0 ];
then
	echo "No File to extract"
else
	lftp sftp://$username:$password@$url -e 'mget -O outbox/* /home/airflow/creditcard/Amex/MCC/;exit'
fi


if [ `ls $source_path | wc -l` -ne 0 ];
then

for file_name in $source_path/*
do
	gsutil -m cp $file_name $destination_path/$file_name.csv
done
fi

