#!/bin/bash

jobname=`echo "$1"|sed -e 's/~--name\(.*\)~--class/\1/'|cut -d "~" -f 2`




spark_submit=`echo $1 | sed -e "s/~/ /g"`
echo "------------------- Starting Job $jobname ----------"
bash -c "$spark_submit"

rc=$?;

if [ $rc -eq 0 ]
then
  echo "The script $jobname  with status $rc succeeded on `date`"
  exit 0
else
  echo "The script $jobname with status $rc failed on `date`" >&2
  exit 1
fi
