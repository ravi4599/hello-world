filename="/data/apps/talend/shared/scripts/stop_script_names.txt"


while IFS='' read -r line || [[ -n "$line" ]]; do
        linenospace="$(echo -e "${line}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
        PID=`ps -ef | grep "$linenospace" | grep -v grep | awk '{print $2}'`
        echo "Script name: $linenospace"
        echo "Process id: $PID"
        if [[ "" != "$PID" ]]; then
                echo "Killing $PID"
                kill -9 $PID
        else
                echo "No process called $linenospace running"
        fi
        printf "\n"
done < "$filename"
