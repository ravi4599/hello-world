#########################################################################################################
# Script Usage : 									   		#
#                Script to send a email						        		#
#		 updates									 	#
#		 Ex : Script <job name> (job name = newreservation/circlereservation/clientmerge..      #
#########################################################################################################
#!/bin/sh

if [ $# -lt 4 ]
then
    echo $#" arguments mismatch; require 4 parameters"
    echo "Usage: sh <script name> <from> <to> <subject> <logfile>"
    exit
fi

logfile=$4

echo "sending a email" >> ${logfile}
cat <<EOF | /usr/sbin/sendmail/sendmail -t
From: $1
To: $2
Subject: $3
.
EOF
echo "email sent" >> ${logfile}
