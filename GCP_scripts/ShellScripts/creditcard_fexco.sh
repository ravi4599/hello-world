#!/bin/bash

#url=$1
source_path=$1
destination_path=$2
archive_path=$3


echo "quit" | sftp -o IdentityFile=~/.ssh/fexco_rsa CUST_VVF@sftp.fexcodccapps.com


if [ $? -ne 0 ] ;then
exit 1
fi

file_name=`gsutil ls -l $archive_path | sort -k 2 | awk '{a[NR]=$0; delete a[NR-3]} END{print a[NR-1]}' |rev| cut -d "/" -f 1 | rev`

echo $file_name

if [ ! -z `echo "ls -rt Outgoing/* " | sftp -o IdentityFile=~/.ssh/fexco_rsa CUST_VVF@sftp.fexcodccapps.com | grep -A 100 $file_name | grep -v $file_name` ] ;
then
	#echo "We Found file"
	echo "ls -rt Outgoing/* " | sftp -o IdentityFile=~/.ssh/fexco_rsa CUST_VVF@sftp.fexcodccapps.com | grep -A 100 $file_name | grep -v $file_name > file_list.txt
elif [ `gsutil ls -l $archive_path | wc -l` -eq 0 ]; then
	#echo "No archive File"
	echo "ls -rt Outgoing/* " | sftp -o IdentityFile=~/.ssh/fexco_rsa CUST_VVF@sftp.fexcodccapps.com > file_list.txt
else 
	
	>file_list.txt	
fi



while IFS= read -r line; do
	echo "get $line $source_path" | sftp -o IdentityFile=~/.ssh/fexco_rsa CUST_VVF@sftp.fexcodccapps.com
        file_name=`echo $line|rev| cut -d "/" -f 1 | rev`
	gsutil -m cp $source_path/$file_name $destination_path
	if [ $? -eq 0 ] ;then
		rm $source_path/$file_name
	fi	
done < "file_list.txt"
