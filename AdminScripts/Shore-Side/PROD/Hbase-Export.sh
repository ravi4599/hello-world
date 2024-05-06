SCRIPTDIR=/mnt/scripts/hbase-export
HDFSDIR=/tmp/hbase-backup
LOCALDIR=/mnt/scripts/hbase-export/temp
mkdir -p $LOCALDIR
LOGDIR=/mnt/scripts/hbase-export/logs
S3DIR=s3://vv-nbx-backups/Hbase
purge_date="$(date +%Y%m%d -d "2 day ago")"

#Logic to export data to S3
logger=$LOGDIR/log-`date +"%Y%m%d"`
echo "$(date)" >> $logger
echo "Starting Export" >> $logger
for table in `cat $SCRIPTDIR/table-list`
do
hbase org.apache.hadoop.hbase.mapreduce.Export $table $HDFSDIR/$table-`date +"%Y%m%d"` >> $logger 2>&1
done
echo "downloading files to local" >> $logger
mkdir $LOCALDIR/`date +"%Y%m%d"`
hadoop fs -get $HDFSDIR/* $LOCALDIR/`date +"%Y%m%d"`/ >> $logger 2>&1
echo "cleaning files in hdfs" >> $logger
hadoop fs -rm -r -f -skipTrash $HDFSDIR/* >> $logger 2>&1
echo "Uploading files to S3" >> $logger
aws s3 cp $LOCALDIR/ $S3DIR --recursive  >> $logger 2>&1
echo "Cleaning Local Dir" >> $logger
rm -rf $LOCALDIR/*  >> $logger 2>&1

#logic to purge old backup from S3
#echo "Purging old files from S3" >> $logger
#get_list="$(aws s3 ls $S3DIR/ | rev | cut -d' ' -f1 | cut -d'/' -f2 | rev | grep `date +%Y`)" >> $logger 2>&1
#for file in $get_list
#do
#if [[ ${#file} == 8 && ${file} -lt ${purge_date} ]]
#then
#aws s3 rm s3://vv-uat-emr-cluster/tmp/hbase-backup/$file --recursive >> $logger 2>&1
#fi
#done


echo "SUCCESS" >> $logger