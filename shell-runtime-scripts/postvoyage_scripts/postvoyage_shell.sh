echo userandserver=$1
ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $1 << EOF
#if to check hdfs postcruise response files exist and then do a delete
hdfs dfs -test -e $3/PostCruise
if [[ \$? -eq 0 ]]; then
  echo "PostCruise Response folder exists in HDFS location"
  hdfs dfs -rm -skipTrash $3/PostCruise/*
else
  echo "PostCruise Response folder does not exists in HDFS location"
fi
#if to check hdfs postcruise question files exist and then do a delete
hdfs dfs -test -e $3/QD
if [[ \$? -eq 0 ]]; then
  echo "QD folder exist in HDFS location"
  hdfs dfs -rm -skipTrash $3/QD/*
else
  echo "QD folder does not exists in HDFS location"
fi
#if to check hdfs postcruise remi files exist and then do a delete
hdfs dfs -test -e $3/Remi
if [[ \$? -eq 0 ]]; then
  echo "Remi folder exist in HDFS location"
  hdfs dfs -rm -skipTrash $3/Remi/*
else
  echo "Remi folder does not exists in HDFS location"
fi
#aws s3 ls $2/ | grep -i responses
aws s3 ls $2/ | grep -i PostSailorSurveys|grep -i VirginPostCruise|grep -v QD
if [[ \$? -eq 0 ]]; then
  echo "PostCruise Response files exist in S3 location"
  hdfs dfs -mkdir -p $3/PostCruise 
  #hadoop fs -cp $2/*responses* $3/PostCruise/  
  hadoop fs -cp $2/PostSailorSurveys*VirginPostCruise* $3/PostCruise/  
else
  echo "PostCruise Response files does not exists in S3 location"
fi
#aws s3 ls $2/ | grep remi
aws s3 ls $2/ | grep -i PostSailorSurveys|grep -i VirginReminiscence|grep -v QD
if [[ \$? -eq 0 ]]; then
  echo "Reminiscene files exist in S3 location"
  hdfs dfs -mkdir -p $3/Remi
  hadoop fs -cp $2/PostSailorSurveys*VirginReminiscence* $3/Remi/
else
  echo "Reminiscene files does not exists in S3 location"
fi
hdfs dfs -ls $2/ | grep QD
if [[ \$? -eq 0 ]]; then
  echo "PostCruise Response qd files exist in S3 location"
  hdfs dfs -mkdir -p $3/QD
  hadoop fs -cp $2/*QD* $3/QD/
else
  echo "PostCruise Response qd files does not exists in S3 location"
fi

EOF
