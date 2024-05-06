#Usage sailorsurveys3cp.sh ec2-user@10.xx.xx.xx /source/hdfs/path /destination/s3/path /backup/s3/path
Master=$1
Source=$2
Dest=$3
bkup=$4
fnames=$(echo $2 | rev | cut -d '/' -f1 | rev)
fnamed=$(echo $3 | rev | cut -d '/' -f1 | rev)

ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $Master << EOF
hadoop fs -mkdir -p $Dest
hadoop fs -rm -r -f -skipTrash $Dest/*.txt
hadoop fs -cp $Source/*.txt $Dest/
hadoop fs -cp $Source/*.txt $bkup/
hadoop fs -rm -r -f -skipTrash $Source/*.txt
emrfs delete $Dest
EOF

exit 0