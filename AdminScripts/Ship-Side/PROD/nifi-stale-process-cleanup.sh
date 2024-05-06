#!/bin/bash

for i in $(pgrep -u 1008 -f org.apache.spark.deploy.SparkSubmit)
do
    TIME=$(ps --no-headers -o etimes $i)
    if [ "$TIME" -ge 14400 ] ; then
        #ps $i
        kill -9 $i
    fi
done

for i in $(pgrep -u 1008 -f trigger_spark.sh)
do
    TIME=$(ps --no-headers -o etimes $i)
    if [ "$TIME" -ge 14400 ] ; then
        #ps $i
        kill -9 $i
    fi
done