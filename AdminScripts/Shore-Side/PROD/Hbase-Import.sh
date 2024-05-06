#!/bin/bash
script="$0"
SCRIPTDIR="$(dirname $script)"
HDFSDIR=/tmp/hbase-import
hadoop fs -mkdir -p $HDFSDIR
LOGDIR=$SCRIPTDIR/logs
#############Configure per requirement####################
S3DIR=s3://vv-nbx-backups/Hbase
dte=20201016
##########################################################
#Logic to export data to S3
logger=$LOGDIR/log-`date +"%d%m%Y"`
echo "$(date)" >> $logger
echo "Starting Import" >> $logger
for table in `cat $SCRIPTDIR/table-list`
do
echo "Downloading files to HDFS" >> $logger
s3-dist-cp --src $S3DIR/$dte/$table-$dte --dest $HDFSDIR/$table-$dte/ >> $logger 2>&1
echo "Truncating Table" >> $logger
echo -e "truncate '$table'" | hbase shell >> $logger 2>&1
echo "Importing Table" >> $logger
hbase org.apache.hadoop.hbase.mapreduce.Import $table $HDFSDIR/$table-$dte >> $logger 2>&1
echo "Cleaning Directories" >> $logger
hadoop fs -rm -r -f $HDFSDIR/$table-$dte >> $logger 2>&1
done
echo "SUCCESS" >> $logger