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

#Checking Ship Side
l_TELNET=`echo "quit" | telnet 10.101.220.213 8083 | grep "Escape character is"`
if [ "$?" -ne 0 ]; then
  sendemail "Debezium on Ship PROD is down" "Debezium on Ship PROD is down"
else
output=$(curl -s "http://10.101.220.213:8083/connectors" | jq '.[]' | grep -E 'nbx|vv' | grep -v prod_ship_vv_mxp_tax_aggregator_24Sep2021 | xargs -I{connector_name} curl -s "http://10.101.220.213:8083/connectors/"{connector_name}"/status" | jq -c -M '[.name,.connector.state,.tasks[].state]|join(":|:")' | column -s : -t| sed 's/\"//g'| sort)
if [[ $output =~ "FAILED" ]]; then
    sendemail "One or more Debezium connectors have Failed on Ship PROD" "$output"
fi
fi

#Checking Shore Side
l_TELNET=`echo "quit" | telnet 10.15.2.118 8083 | grep "Escape character is"`
if [ "$?" -ne 0 ]; then
  sendemail "Debezium on Shore PROD is down" "Debezium on Shore PROD is down"
else
output=$(curl -s "http://10.15.2.118:8083/connectors" | jq '.[]' | grep -E 'nbx' | xargs -I{connector_name} curl -s "http://10.15.2.118:8083/connectors/"{connector_name}"/status" | jq -c -M '[.name,.connector.state,.tasks[].state]|join(":|:")' | column -s : -t| sed 's/\"//g'| sort)
if [[ $output =~ "FAILED" ]]; then
    sendemail "One or more Debezium connectors have Failed on Shore PROD" "$output"
fi
fi