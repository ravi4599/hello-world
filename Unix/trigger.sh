d=$(date -d '+1min' '+%F %T')
echo $d
now=$(TZ=":US/Eastern" date '+%s')
midnight=$(TZ=":US/Eastern" date -d "00:00:00 tomorrow" '+%s')
diff=$((midnight - now-70))
echo $diff
sh /data/talend/Talend-6.4.1/tac/apache-tomcat/webapps/org.talend.administrator/WEB-INF/classes/MetaServletCaller.sh --tac-url=http://ip-10-3-100-215:8080/org.talend.administrator --json-params='{"actionName":"updateTrigger","authPass":"rvoleti","authUser":"ravi-teja.voleti@capgemini.com","id":'"$1"',"label":"'"$2"'","repeatInterval":'"$diff"',"startTime":"'"$d"'"}'