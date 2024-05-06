echo $date
for command in `cat /mnt/scripts/hbase-count/tables.txt`
do
echo "$command"
echo -e "count $command" | sudo -u hbase hbase shell -n | tail -1
done