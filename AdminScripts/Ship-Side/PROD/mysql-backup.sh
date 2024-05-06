# Backup storage directory
backupfolder=/mnt/Mysql-bkup
# Notification email address
recipient_email=zill.silveira@capgemini.com
# MySQL user
user=root
# MySQL password
password=xxxxxx
# Number of days to store the backup
keep_day=15
sqlfile=$backupfolder/all-database-$(date +%d-%m-%Y_%H-%M-%S).sql
zipfile=$backupfolder/all-database-$(date +%d-%m-%Y_%H-%M-%S).zip
# Create a backup
sudo mysqldump -u $user -p$password --all-databases > $sqlfile
if [ $? == 0 ]; then
  echo 'Sql dump created'
else
  echo 'mysqldump return non-zero code' | mailx -s 'No backup was created!' $recipient_email
  exit
fi
# Compress backup
zip $zipfile $sqlfile
if [ $? == 0 ]; then
  echo 'The backup was successfully compressed'
else
  echo 'Error compressing backup' | mailx -s 'Backup was not created!' $recipient_email
  exit
fi
rm $sqlfile
echo $zipfile | mailx -s 'Backup was successfully created' $recipient_email
# Delete old backups
find $backupfolder -type f -mtime +$keep_day -delete

scp $zipfile P-SCL-HAD-ADB1:$backupfolder/
ssh P-SCL-HAD-ADB1 'find /mnt/Mysql-bkup/ -type f -mtime +15 -delete'