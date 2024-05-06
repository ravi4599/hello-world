#sudo find /var/log/hadoop/hdfs/ -type f -name 'hdfs-audit.log.*' -mtime +3 -exec rm -f {} \;
#sudo find /var/log/hadoop/hdfs/ -type f -name 'SecurityAuth.audit.*' -mtime +3 -exec rm -f {} \;
sudo find /var/log/hadoop/hdfs/ -type f -name 'gc.log-*' -mtime +15 -exec rm -f {} \;
sudo find /var/log/hadoop/hdfs/audit/solr/spool/archive/ -type f -name 'spool_hdfs*log' -mtime +2 -exec rm -f {} \;
sudo find /var/log/hadoop/hdfs/audit/hdfs/spool/archive/ -type f -name 'spool_hdfs*log' -mtime +2 -exec rm -f {} \;
sudo find /var/log/hbase/audit/solr/spool/archive/ -type f -name 'spool_hbaseMaster_*.log' -mtime +2 -exec rm -f {} \;
sudo find /var/log/ranger/admin/ -type f -name 'xa_portal.log.*' -mtime +90 -exec rm -f {} \;
sudo find /var/log/ranger/admin/ -type f -name 'access_log.*.log' -mtime +90 -exec rm -f {} \;