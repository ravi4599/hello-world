#!/bin/bash
sleep 5m
sendemail()
{
#echo $1, $2
cat <<EOF | /usr/sbin/sendmail -t
From:talendadmin-non-prod@virginvoyages.com
#To:virginnbxservicedesk.in@capgemini.com
To:zill.silveira@capgemini.com
Subject:$1
$2
EOF
}

whitelist=$(echo $1:$2 | grep 'hvtb_nbx_core_pushtopic_campaign\|hvtb_nbx_core_pushtopic_accesskey\|hvtb_nbx_core_sw_package_dim_history\|hvtb_nbx_core_sw_booked_cabin_reservation_dim_history\|hvtb_nbx_core_sw_transaction_evt_history\|13sep2020\|seaware_ship_room_dim\|seaware_ship_room_request_dim')

if [ -z "$whitelist" ]
then
      echo "$1"
else
      exit 0
fi


table=$( echo "$1" | cut -d"." -f2 )

if [ -z "$table" ]
then
      sendemail "Redshift Table not provided for $2" "One of the redshift table is pointing to the temp location in $3 kindly check. $2 is the corresponding hive table"
      exit 1
else
      echo "$1"
fi


Paramfile=/data/apps/talend/shared/parameterfiles/red_secret.txt
PGDATABASE=$(cat $Paramfile | grep PGDATABASE | cut -d "|" -f2)
PGHOST=$(cat $Paramfile | grep PGHOST | cut -d "|" -f2)
PGPORT=$(cat $Paramfile | grep PGPORT | cut -d "|" -f2)
PGUSER=$(cat $Paramfile | grep PGUSER | cut -d "|" -f2)
PGPASSWORD=$(cat $Paramfile | grep PGPASSWORD | cut -d "|" -f2)
export PGDATABASE=$PGDATABASE;
export PGHOST=$PGHOST;
export PGPORT=$PGPORT;
export PGUSER=$PGUSER;
export PGPASSWORD=$PGPASSWORD;

COUNT_QUERY=`psql -c "select count(1) (select 1 from $1 limit 1)t"`

if [ $? == 0 ]
then
        TABLE_COUNT=`echo $COUNT_QUERY | awk '{print $3}'`
        if [ $TABLE_COUNT == 0 ]
        then
                sendemail "Table $1 has zero records in $3 Redshift" "Table $1 has zero records in Redshift in $3 kindly check"
        else
                echo "Count is fine"
#               sendemail "Table $1 has $TABLE_COUNT records in $3 Redshift" "Table $1 has $TABLE_COUNT records in $3 Redshift"
        fi
else
        echo "Please Check: Issue with Redshift"
        sendemail "Unable to query Redshift" "Count script was unable to query redshift in $3"
fi
