echo userandserver=$1
ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $1 << EOF
#ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/ec2-key.pem hadoop@10.3.100.232 << EOF
#if to check hdfs postcruise response folder exist and then remove folder
hdfs dfs -test -e $2/PostCruise
if [[ \$? -eq 0 ]]; then
  echo "PostCruise Response folder exists in HDFS location"
  hadoop fs -rm -r -f -skipTrash $2/PostCruise
else
  echo "PostCruise Response folder does not exists in HDFS location"
fi
#if to check hdfs postcruise question folder exist and then remove folder
hdfs dfs -test -e $2/QD
if [[ \$? -eq 0 ]]; then
  echo "PostCruise Question folder exist in HDFS location"
  hadoop fs -rm -r -f -skipTrash $2/QD
else
  echo "PostCruise Question folder does not exists in HDFS location"
fi
#if to check hdfs postcruise question folder exist and then remove folder
hdfs dfs -test -e $2/Remi
if [[ \$? -eq 0 ]]; then
  echo "PostCruise Remi folder exist in HDFS location"
  hadoop fs -rm -r -f -skipTrash $2/Remi
else
  echo "PostCruise Remi folder does not exists in HDFS location"
fi

aws s3 ls $3
if [[ \$? -eq 0 ]]; then
  echo "PostCruise files exists in s3"
  aws s3 mv $3 $4 --include "*.txt" --recursive
else
  echo "PostCruise files not present"
fi


EOF

