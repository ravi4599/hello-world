##############################################################################################
#    Script usage : sh <scriptname>                                                          #
#    This shell script notifies streaming job failure by email     		             #
##############################################################################################
#!/bin/sh
if [ $# -lt 1 ]
then
    echo $#" arguments mismatch; require 1"
    echo "Usage: script <environment ex : dev/qa/training/uat/prod>"
    exit
fi
# Check the status of Multiple Consumer jobs

# Log FIles variables
dir=$PWD
parentdir_1="$(dirname "$dir")"
parentdir="$(dirname "$parentdir_1")"
#echo " Parent "$parentdir
log_path=$parentdir/sprak-streaming-job-logs
log_file="Streaming_EmailNotification_`date -u +'%Y_%m_%d_%H_%M'`.log"
echo "Log file : "$log_path/$log_file

# Read the config file and create variables
unset IFS
jobNames=`cat notifier_config.conf | grep "jobname;"  | awk -F ";" '{ print $2 }'`
rm_url=`cat notifier_config.conf | grep "resource_manager_url;" | awk -F ";" '{print $2}'`
reservationEmailNotifier=`cat notifier_config.conf | grep "reservationemailnotifyfile;" | awk -F ";" '{print $2}'`
clientmergeEmailNotifier=`cat notifier_config.conf | grep "clientmerge;" | awk -F ";" '{print $2}'`
acxiomrefresh=`cat notifier_config.conf | grep "acxiomrefresh;" | awk -F ";" '{print $2}'`
toEmail=`cat notifier_config.conf | grep "to;" | awk -F ";" '{print $2}'`
fromEmail=`cat notifier_config.conf | grep "from;" | awk -F ";" '{print $2}'`

# Print for Logs 
echo "Job name : "$jobNames > $log_path/$log_file
echo "rm url : "$rm_url  >> $log_path/$log_file
export IFS=","

for job in $jobNames
do
  echo "------------- Start -----------------" >> $log_path/$log_file
  echo "Job name --> "$job >> $log_path/$log_file
  jobwithoutquotes_T=`echo ${job} | sed 's/"//g'`
  jobwithoutquotes="."${jobwithoutquotes_T}
  echo "job without para : "${jobwithoutquotes} >> $log_path/$log_file
  jobResponse=`curl -s ${rm_url} | grep -o ${job}`
  
  echo "Job Response --> "$jobResponse >> $log_path/$log_file
  if [ -z "${jobResponse}" ] || [ "${jobResponse}" = " " ] 
  then
     echo "job response is empty. Job is not running.." >> $log_path/$log_file
     if [ ! -f ${jobwithoutquotes} ]
     then
         echo "email notifier doesn't exsist" >> $log_path/$log_file
         #sh ./send_email.sh ${fromEmail} "${toEmail}" ${jobwithoutquotes_T}" job failed" $log_path/$log_file
         {
   	 echo "Subject: "${jobwithoutquotes_T}" job failed - $1"
    	 echo "From : "$fromEmail
    	 echo "MIME-Version: 1.0"
    	 echo "Content-Type: text/html"
    	 echo "Content-Disposition: inline"
    	 echo "To: "$toEmail
	 } | /usr/sbin/sendmail -t
         echo "Email sent" >> $log_path/$log_file 
         touch ${jobwithoutquotes}
     else 
         echo "email notifier exisit" >> $log_path/$log_file
     fi
  elif [ ! -z "${jobResponse}" ] || [ "${jobResponse}" != " " ]
  then 
     echo "Job "${job}" is in RUNNING state" >> $log_path/$log_file
     rm -f ${jobwithoutquotes}
  fi
  echo "-------------- End -------------------" >>  $log_path/$log_file
done

