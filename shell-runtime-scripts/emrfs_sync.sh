#list=$(ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem ${1} aws s3 ls  ${2} | grep PRE | awk '{print $2}')
#for path in $list
#do
#s3path=$(echo $2$path | sed 's:/*$::')
s3path=$2
echo "Syncing Path $s3path"
ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem ${1} << EOF
n=0
until [ "\$n" -ge 50 ]
do
   timeout 10 emrfs delete $s3path && break
   n=\$((n+1))
   echo "Failed \$n time, Retrying in 2 seconds"
   sleep 2
done
#emrfs import $s3path
EOF
#done
echo ssh ${1} "emrfs sync " ${2} Sync Completed
