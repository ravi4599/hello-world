#FUNCTION TO SEND EMAILS
sendemail()
{
#echo $1, $2
cat <<EOF | /usr/sbin/sendmail -t
From:vv-app-alert@virginvoyages.com
To:virginnbxservicedesk.in@capgemini.com
Subject:$1
$2
EOF
}


l_TELNET=`echo "quit" | telnet 10.101.220.144 22 | grep "Escape character is"`
if [ "$?" -ne 0 ]; then
  echo "a"
  #sendemail "Ship PROD Postgres server (10.101.220.144) is unreachable" "Ship PROD Postgres server 10.101.220.144 is unreachable"
else

l_TELNET=`echo "quit" | telnet 10.101.220.144 5432 | grep "Escape character is"`
if [ "$?" -ne 0 ]; then
  echo "b"
  #sendemail "Ship PROD Postgres DB (10.101.220.144) is down" "Ship PROD Postgres DB 10.101.220.144 is down"
fi
fi