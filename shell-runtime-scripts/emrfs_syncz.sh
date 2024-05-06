#trap read debug

Paramfile=/data/apps/talend/shared/parameterfiles/seaware_parameterfile.txt
PGDATABASE=$(cat $Paramfile | grep PGDATABASE | cut -d "|" -f2)
PGHOST=$(cat $Paramfile | grep PGHOST | cut -d "|" -f2)
PGPORT=$(cat $Paramfile | grep PGPORT | cut -d "|" -f2)
PGUSER=$(cat $Paramfile | grep PGUSER | cut -d "|" -f2)
PGPASSWORD=$(cat $Paramfile | grep PGPASSWORD | cut -d "|" -f2)
now="$(date)"
echo "Script started at" "$now"
echo userandserver=$1
echo s3path=$2
echo hdfspath=$3

getenv="$( echo "${2}"|cut -d "-" -f2)"
echo $getenv

case "$2" in
*/)
    echo ""
    ;;
*)
    echo "S3 path doesn't have a trailing slash"; exit 1
    ;;
esac

case "$3" in
*/)
    echo ""
    ;;
*)
    echo "HDFS path doesn't have a trailing slash"; exit 1
    ;;
esac

if [ ${#2} -ge 5 ] && [ ${#3} -ge 5 ]
then
s3p=$(echo $2 | rev | cut -d '/' -f2-12 | rev)
s3pt="$s3p""_temp"
s3pt_merge="$s3p""_merge"

hdfspath=$3
hdfspathrev=$(echo $hdfspath | rev | cut -d '/' -f2-12 | rev)
hdfspathmerge="$hdfspathrev""_merge/"
currdt=`date`
echo "starting merge of hdfs files at " $currdt
echo "source: " $hdfspathrev " destination: " $hdfspathmerge
ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $1 << EOF
#!/bin/bash
echo "hdfs dfs -mkdir -p $hdfspathmerge"
sudo -u hadoop hdfs dfs -mkdir -p $hdfspathmerge
echo "hadoop hdfs dfs -rm -r -skipTrash $hdfspathmerge"
sudo -u hadoop hdfs dfs -rm -r -skipTrash $hdfspathmerge
EOF

n=0
until [ "$n" -ge 2 ]
do
        timeout 60m spark-submit --conf spark.yarn.maxAppAttempts=1 --name copyAndMerge --executor-cores 2 --master yarn --driver-memory 8G --executor-memory 12G --deploy-mode cluster /data/apps/talend/shared/scripts/copyAndMerge.py $hdfspath $hdfspathmerge && Fail=N && break
		Fail=Y
        n=$((n+1))
        echo "Failed $n time, Retrying in 30 seconds"
        sleep 30
done

currdt=`date`
echo "ending merge of hdfs files at" $currdt
if [ "$Fail" = "Y" ]
then
 echo "Copy and Merge job Failed"
 exit 1
fi

factcheck=$(echo $s3p | rev | cut -d '/' -f1,2 | rev | grep fact)
if [ -z "$factcheck" ]
then
      fact=N
      echo "Processing Table"
      hivetable=$(echo $s3p | rev | cut -d '/' -f1 | rev)
      ht=vv_db.$hivetable
      echo hivetable=$ht
      query='ALTER TABLE '$ht' SET LOCATION ''"'$s3pt'"'''
      query1='ALTER TABLE '$ht' SET LOCATION ''"'$s3p'"'''
      query2="alter table :tblName set location :'location'"
      query3="select tablename from SVV_EXTERNAL_TABLES where location = :'location' and schemaname='hive_schema_stg'"
      ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $1 << EOF
                #!/bin/bash
                export PGDATABASE=$PGDATABASE;
                export PGHOST=$PGHOST;
                export PGPORT=$PGPORT;
                export PGUSER=$PGUSER;
                export PGPASSWORD=$PGPASSWORD;
                echo "emrfs delete $s3p"
                #emrfs delete $s3p
                                n=0
                                until [ "\$n" -ge 50 ]
                                do
                                        timeout 10  emrfs delete $s3p && break
                                        n=\$((n+1))
                                        echo "Failed \$n time, Retrying in 2 seconds"
                                        sleep 2
                                done
                echo "emrfs delete $s3pt"
                #emrfs delete $s3pt
                                n=0
                                until [ "\$n" -ge 50 ]
                                do
                                        timeout 10  emrfs delete $s3pt && break
                                        n=\$((n+1))
                                        echo "Failed \$n time, Retrying in 2 seconds"
                                        sleep 2
                                done
                echo "Delete temp files in S3"
                echo "hadoop fs -mkdir -p $s3pt"
                sudo -u hadoop hadoop fs -mkdir $s3pt
                echo "aws s3 rm $s3pt/*.parquet"
                aws s3 rm --recursive --exclude "*" --include "*.parquet*" $s3pt
                echo "Copy files from core hdfs to s3"
                echo "hadoop distcp -Dmapreduce.map.memory.mb=4028 -Dyarn.app.mapreduce.am.resource.mb=4028 -m 3 $hdfspathmerge* $s3pt/"
                sudo -u hadoop hadoop distcp -Dmapreduce.map.memory.mb=4028 -Dyarn.app.mapreduce.am.resource.mb=4028 -m 3 $hdfspathmerge* $s3pt/
                sleep 5
                echo '$query;' > /tmp/kpi$ht.hql
                echo "$query2;" > /tmp/kpi$ht.sql
                echo "$query3;" > /tmp/kpi1$ht.sql
				if [ "$hivetable" = "hvtb_nbx_core_sw_sail_dim" ]; then
                    			beeline -u jdbc:hive2://localhost:10000/default -n hadoop -f /tmp/kpi$ht.hql
				fi
                psql -t -v location='$s3p' -f /tmp/kpi1$ht.sql > /tmp/kpi$ht.txt
                echo "emrfs delete $s3p"
                #emrfs delete $s3p
                                n=0
                                until [ "\$n" -ge 50 ]
                                do
                                        timeout 10  emrfs delete $s3p && break
                                        n=\$((n+1))
                                        echo "Failed \$n time, Retrying in 2 seconds"
                                        sleep 2
                                done
                echo "emrfs delete $s3pt"
                #emrfs delete $s3pt
                                n=0
                                until [ "\$n" -ge 50 ]
                                do
                                        timeout 10  emrfs delete $s3pt && break
                                        n=\$((n+1))
                                        echo "Failed \$n time, Retrying in 2 seconds"
                                        sleep 2
                                done
EOF
           redt="$(ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $1 "head -1 /tmp/kpi"$ht".txt" | tr -d ' ')"
           rt=hive_schema_stg.$redt
           echo "redshifttable=$rt"
           ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $1 << EOF
                        #!/bin/bash
                        export PGDATABASE=$PGDATABASE;
                        export PGHOST=$PGHOST;
                        export PGPORT=$PGPORT;
                        export PGUSER=$PGUSER;
                        export PGPASSWORD=$PGPASSWORD;
                        psql -v tblName='$rt' -v location='$s3pt' -f /tmp/kpi$ht.sql
                        echo "tables renamed"
                        #sleep 1m
                        echo "hadoop fs -mkdir -p $s3p"
                        sudo -u hadoop hadoop fs -mkdir -p $s3p
                        echo "aws s3 sync $s3pt/* $s3p/"
                        aws s3 sync --delete $s3pt $s3p
                        echo "data deleted from core and copied from temp"
                        echo '$query1;' > /tmp/kpi$ht.hql
						if [ "$hivetable" = "hvtb_nbx_core_sw_sail_dim" ]; then
                         			   beeline -u jdbc:hive2://localhost:10000/default -n hadoop -f /tmp/kpi$ht.hql
						fi
                        psql -v tblName='$rt' -v location='$s3p' -f /tmp/kpi$ht.sql
                        echo "emrfs delete $s3p"
                        #emrfs delete $s3p
                                                n=0
                                                until [ "\$n" -ge 50 ]
                                                do
                                                        timeout 10  emrfs delete $s3p && break
                                                        n=\$((n+1))
                                                        echo "Failed \$n time, Retrying in 2 seconds"
                                                        sleep 2
                                                done
                        echo "emrfs delete $s3pt"
                        #emrfs delete $s3pt
                                                n=0
                                                until [ "\$n" -ge 50 ]
                                                do
                                                        timeout 10  emrfs delete $s3pt && break
                                                        n=\$((n+1))
                                                        echo "Failed \$n time, Retrying in 2 seconds"
                                                        sleep 2
                                                done
EOF

if [ "$rt" = "hive_schema_stg.seaware_booked_cabin_reservation_dim" ]; then
echo "Triggering SparkCode that Invokes Tableau Refresh"
rt1="$( echo "$rt"|cut -d "." -f2)"
rtf=seaware.$rt1

nohup sh /data/apps/talend/shared/scripts/tableau_api_trigger.sh $rtf $getenv >/dev/null 2>&1 &
fi

else
      fact=Y
      echo "Procesing Fact Table"
          hivetable=$(echo $s3p | rev | cut -d '/' -f2 | rev)
          ht=vv_db.$hivetable
          echo hivetable=$ht
          folder=$(echo $s3p | rev | cut -d '/' -f2-10 | rev)
          ft="$folder"_temp
          var=$(echo $s3p | rev | cut -d '/' -f1 | rev)
          folder1="$folder"_temp/$var
          query='ALTER TABLE '$ht' SET LOCATION ''"'$ft'"'''
          query1='ALTER TABLE '$ht' SET LOCATION ''"'$folder'"'''
          query2="alter table :tblName set location :'location'"
          query3="select tablename from SVV_EXTERNAL_TABLES where location = :'location' and schemaname='hive_schema_stg'"
          ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $1 << EOF
                        #!/bin/bash
                        export PGDATABASE=$PGDATABASE;
                        export PGHOST=$PGHOST;
                        export PGPORT=$PGPORT;
                        export PGUSER=$PGUSER;
                        export PGPASSWORD=$PGPASSWORD;
                        echo "emrfs delete $ft"
                        #emrfs delete $ft
                                                n=0
                                                until [ "\$n" -ge 50 ]
                                                do
                                                        timeout 10  emrfs delete $ft && break
                                                        n=\$((n+1))
                                                        echo "Failed \$n time, Retrying in 2 seconds"
                                                        sleep 2
                                                done
                        echo "emrfs delete $folder"
                        #emrfs delete $folder
                                                n=0
                                                until [ "\$n" -ge 50 ]
                                                do
                                                        timeout 10  emrfs delete $folder && break
                                                        n=\$((n+1))
                                                        echo "Failed \$n time, Retrying in 2 seconds"
                                                        sleep 2
                                                done
                        echo "Delete temp files in S3"
                        echo "hadoop fs -mkdir -p $ft"
                        sudo -u hadoop hadoop fs -mkdir -p $ft
                        echo "aws s3 sync $folder/* $ft/"
                        aws s3 sync --delete $folder $ft
                        echo "aws s3 rm $folder1/*.parquet"
                        aws s3 rm --recursive --exclude "*" --include "*.parquet*" $folder1
                        #sleep 1m
                        echo "Copy files from core hdfs to s3"
                        sudo -u hadoop hadoop distcp -Dmapreduce.map.memory.mb=4028 -Dyarn.app.mapreduce.am.resource.mb=4028 -m 3 $hdfspathmerge* $folder1/
                        sleep 5
                        echo '$query;' > /tmp/kpi$ht.hql
                        echo "$query2;" > /tmp/kpi$ht.sql
                        echo "$query3;" > /tmp/kpi1$ht.sql
                        #beeline -u jdbc:hive2://localhost:10000/default -n hadoop -f /tmp/kpi$ht.hql
                        psql -t -v location='$folder' -f /tmp/kpi1$ht.sql > /tmp/kpi$ht.txt
                        echo "emrfs delete $ft"
                        #emrfs delete $ft
                                                n=0
                                                until [ "\$n" -ge 50 ]
                                                do
                                                        timeout 10  emrfs delete $ft && break
                                                        n=\$((n+1))
                                                        echo "Failed \$n time, Retrying in 2 seconds"
                                                        sleep 2
                                                done
                        echo "emrfs delete $folder"
                        #emrfs delete $folder
                                                n=0
                                                until [ "\$n" -ge 50 ]
                                                do
                                                        timeout 10  emrfs delete $folder && break
                                                        n=\$((n+1))
                                                        echo "Failed \$n time, Retrying in 2 seconds"
                                                        sleep 2
                                                done
EOF
           redt="$(ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $1 "head -1 /tmp/kpi"$ht".txt" | tr -d ' ')"
           rt=hive_schema_stg.$redt
           echo "redshifttable=$rt"
           ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $1 << EOF
                        #!/bin/bash
                        export PGDATABASE=$PGDATABASE;
                        export PGHOST=$PGHOST;
                        export PGPORT=$PGPORT;
                        export PGUSER=$PGUSER;
                        export PGPASSWORD=$PGPASSWORD;
                        echo "emrfs delete $s3p"
                        #emrfs delete $s3p
                                                n=0
                                                until [ "\$n" -ge 50 ]
                                                do
                                                        timeout 10  emrfs delete $s3p && break
                                                        n=\$((n+1))
                                                        echo "Failed \$n time, Retrying in 2 seconds"
                                                        sleep 2
                                                done
                        echo "emrfs delete $folder1"
                        #emrfs delete $folder1
                                                n=0
                                                until [ "\$n" -ge 50 ]
                                                do
                                                        timeout 10  emrfs delete $folder1 && break
                                                        n=\$((n+1))
                                                        echo "Failed \$n time, Retrying in 2 seconds"
                                                        sleep 2
                                                done
                        psql -v tblName='$rt' -v location='$ft' -f /tmp/kpi$ht.sql
                        echo "tables renamed"
                        #sleep 1m
                        echo "aws s3 rm $s3p/*.parquet"
                        aws s3 rm --recursive --exclude "*" --include "*.parquet*" $s3p
                        echo "hadoop fs -mkdir -p $s3p"
                        sudo -u hadoop hadoop fs -mkdir -p $s3p
                        echo "aws s3 sync $folder1 $s3p"
                        aws s3 sync --delete $folder1 $s3p
                        echo '$query1;' > /tmp/kpi$ht.hql
                        echo "data deleted from core and copied from temp"
                        #beeline -u jdbc:hive2://localhost:10000/default -n hadoop -f /tmp/kpi$ht.hql
                        psql -v tblName='$rt' -v location='$folder' -f /tmp/kpi$ht.sql
                        echo "emrfs delete $s3p"
                        #emrfs delete $s3p
                                                n=0
                                                until [ "\$n" -ge 50 ]
                                                do
                                                        timeout 10  emrfs delete $s3p && break
                                                        n=\$((n+1))
                                                        echo "Failed \$n time, Retrying in 2 seconds"
                                                        sleep 2
                                                done
                        echo "emrfs delete $folder1"
                        #emrfs delete $folder1
                                                n=0
                                                until [ "\$n" -ge 50 ]
                                                do
                                                        timeout 10  emrfs delete $folder1 && break
                                                        n=\$((n+1))
                                                        echo "Failed \$n time, Retrying in 2 seconds"
                                                        sleep 2
                                                done
EOF

factrev=$(echo $s3p | rev | cut -d '/' -f1,2 | rev | grep 'revenue_fact\|sail_cabin_detail_fact\|addon_fact\|group_fact\|cabin_fact\|protected_commission_detail_fact\|commission_fact\|summary_fact\|cx_fact')
if [ -z "$factrev" ]
then
 echo "Not a revenue fact table"
else
 echo "Its revenue fact table calling redshift refresh script"
 #sh /data/apps/talend/shared/scripts/revenue_fact_refresh.sh $1 >> /tmp/flag.txt
 tstamp=$(echo $var | cut -d '=' -f2)
 query5="ALTER TABLE $ht ADD IF NOT EXISTS PARTITION (snapshot_date='$tstamp')"
 query6="ALTER TABLE :tblName ADD IF NOT EXISTS PARTITION (snapshot_date=:'partition') location :'location'"
 ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $1 << EOF
 #!/bin/bash
 export PGDATABASE=$PGDATABASE;
 export PGHOST=$PGHOST;
 export PGPORT=$PGPORT;
 export PGUSER=$PGUSER;
 export PGPASSWORD=$PGPASSWORD;
 echo "$query5;" > /tmp/kpif$ht.hql
 beeline -u jdbc:hive2://localhost:10000/default -n hadoop -f /tmp/kpif$ht.hql
 echo "$query6;" > /tmp/kpif$ht.sql
 psql -t -v tblName='$rt' -v partition='$tstamp' -v location='$s3p' -f /tmp/kpif$ht.sql
EOF

echo "Triggering SparkCode that Invokes Tableau Refresh"
rt1="$( echo "$rt"|cut -d "." -f2)"
rtf=seaware.$rt1

nohup sh /data/apps/talend/shared/scripts/tableau_api_trigger.sh $rtf $getenv >/dev/null 2>&1 &

fi
fi

echo "Triggering Script that checks the table count for $rt and sends alerts"
nohup sh /data/apps/talend/shared/scripts/redshift_count_alert.sh $rt $ht $getenv >/dev/null 2>&1 &
#sh -x  /data/apps/talend/shared/scripts/redshift_count_alert.sh $rt $ht $getenv

now="$(date)"
echo "Script ended at" "$now"
else echo "Incorrect Input"
fi
