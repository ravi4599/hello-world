#!/usr/bin/bash
Hadoop_Master_IP=$1
Environment=$2

#table_list="hvtb_nbx_core_ga360_session_dim,hvtb_nbx_core_ga360_hit_dim,hvtb_nbx_core_ga360_session_fact,hvtb_nbx_core_ga360_hit_fact"
s3_path="s3://vv-$2-emr-cluster/data/core/ga_360/"
Paramfile=/data/apps/talend/shared/parameterfiles/seaware_parameterfile.txt #is this file configured in all environments

Partition=$(date '+%Y-%m-%d' --date "2 days ago")

hive_tables=("hvtb_nbx_core_ga360_session_dim" "hvtb_nbx_core_ga360_hit_dim" "hvtb_nbx_core_ga360_session_fact" "hvtb_nbx_core_ga360_hit_fact" )

if [ $3 != "" ] 
then 

     Partition=$3

    #echo "a is equal to b"
fi 
i=0 
schema="hive_schema_stg"

while [ $i -lt ${#hive_tables[@]} ] 
do

    echo ${hive_tables[$i]} 
	
	#Location="s3://vv-$Environment-emr-cluster/data/core/ga_360/${hive_tables[$i]}/sessiondate=${Partition}"    #$hive_tables[$i]" 
	Location="$s3_path${hive_tables[$i]}/sessiondate=${Partition}"
	PGDATABASE=$(cat $Paramfile | grep PGDATABASE | cut -d "|" -f2)
	PGHOST=$(cat $Paramfile | grep PGHOST | cut -d "|" -f2)
	PGPORT=$(cat $Paramfile | grep PGPORT | cut -d "|" -f2)
	PGUSER=$(cat $Paramfile | grep PGUSER | cut -d "|" -f2)
	PGPASSWORD=$(cat $Paramfile | grep PGPASSWORD | cut -d "|" -f2)
	
	query="ALTER TABLE $schema.${hive_tables[$i]} ADD IF NOT EXISTS PARTITION (sessiondate='$Partition') location '$Location'"
	
	echo "$query"
ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $1 << EOF #is this file configured in all environments
#!/bin/bash
export PGDATABASE=$PGDATABASE;
export PGHOST=$PGHOST;
export PGPORT=$PGPORT;
export PGUSER=$PGUSER;
export PGPASSWORD=$PGPASSWORD;
echo "$query;" > /tmp/ga360.sql
psql -t -v tblName='$2' -v partition='$3' -v location='$4' -f /tmp/ga360.sql
EOF
	
 
    i=`expr $i + 1` 
done

