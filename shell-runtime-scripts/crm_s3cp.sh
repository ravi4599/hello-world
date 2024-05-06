UserServer=$1
Src=$2
Dest=$3
echo "$1,$2,$3"
if [ ${#2} -ge 5 ] && [ ${#3} -ge 5 ]
then
ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $1 << EOF
hdfs dfs -test -e $2/*.parquet
if [[ \$? -eq 0 ]]; then
echo "Files exists in source"
echo "Syncing  $2:$3"
hadoop fs -mkdir -p $3
aws s3 sync --delete $2 $3
#emrfs delete $2
#emrfs delete $3
n=0
until [ "\$n" -ge 50 ]
do
   timeout 10  emrfs delete $2 && break
   n=\$((n+1))
   echo "Failed \$n time, Retrying in 2 seconds"
   sleep 2
done
n=0
until [ "\$n" -ge 50 ]
do
   timeout 10  emrfs delete $3 && break
   n=\$((n+1))
   echo "Failed \$n time, Retrying in 2 seconds"
   sleep 2
done
else
  echo "Files does not exixts in source staging location check if the spark job executed succesfully"
fi
EOF
else echo "Incorrect Input"
fi