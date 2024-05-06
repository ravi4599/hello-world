sudo find /data/apps/talend/shared/test/logs/ -type f -name '*.log' -mtime +30 -exec rm -f {} \;
sudo find /data/apps/talend/shared/test/err/ -type f -name '*.err' -mtime +30 -exec rm -f {} \;
sudo find /data/apps/talend/shared/spf/logs/ -type f -name '*.log' -mtime +30 -exec rm -f {} \;
sudo find /data/apps/talend/shared/spf/err/ -type f -name '*.err' -mtime +30 -exec rm -f {} \;
sudo find /data/apps/talend/shared/scripts/sprak-streaming-job-logs/ -type f -name '*.log' -mtime +30 -exec rm -f {} \;