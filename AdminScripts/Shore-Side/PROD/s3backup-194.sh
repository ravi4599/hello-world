Folderlist=$1
bucket=vv-nbx-backups
env=PROD
NOW=$(date +"%d%b%y")
ssh -o StrictHostKeyChecking=no -i /data/key/key-ec2.key hadoop@10.15.2.225 "aws s3api put-object --bucket $bucket --key $env/$NOW/"
for line in `cat $Folderlist`
do
folder="${line#*/*/*/}"
ssh -o StrictHostKeyChecking=no -i /data/key/key-ec2.key hadoop@10.15.2.225 "aws s3 cp $line s3://$bucket/$env/$NOW/$folder --recursive"
done