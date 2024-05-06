echo "$(date)" "Script Started"
Paramfile=/data/apps/talend/shared/parameterfiles/seaware_parameterfile.txt
PGDATABASE=$(cat $Paramfile | grep PGDATABASE | cut -d "|" -f2)
PGHOST=$(cat $Paramfile | grep PGHOST | cut -d "|" -f2)
PGPORT=$(cat $Paramfile | grep PGPORT | cut -d "|" -f2)
PGUSER=$(cat $Paramfile | grep PGUSER | cut -d "|" -f2)
PGPASSWORD=$(cat $Paramfile | grep PGPASSWORD | cut -d "|" -f2)
scp -i /data/key/key.pem /data/apps/talend/shared/scripts/revenue_fact.sql $1:/tmp/
ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i /data/key/key.pem $1 << EOF
#!/bin/bash
export PGDATABASE=$PGDATABASE;
export PGHOST=$PGHOST;
export PGPORT=$PGPORT;
export PGUSER=$PGUSER;
export PGPASSWORD=$PGPASSWORD;
echo $PGPASSWORD
psql -f /tmp/revenue_fact.sql
EOF
echo "$(date)" "Script Ended"
