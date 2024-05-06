########################################################################################################
# Script Usage :                                                                                        #
#                Script invokes Spark Streaming jobs for Reservation and SFDC Table                     #
#                updates                                                                                #
#                Ex : Script <job name> (job name = newreservation/circlereservation/clientmerge..      #
#########################################################################################################
#!/bin/sh
if [ $# -lt 2 ]
then
    echo $#" arguments mismatch; require 2"
    echo "Usage: script <Streaming job name to invoke ex: reservation> <environment> "
    exit
fi

# Log FIles variables
dir=$PWD
parentdir_1="$(dirname "$dir")"
parentdir="$(dirname "$parentdir_1")"
#echo " Parent "$parentdir
log_path=$parentdir/sprak-streaming-job-logs
log_file="Streaming_$1_`date -u +'%Y_%m_%d_%H_%M'`.log"
echo "Log file : "$log_path/$log_file

# Module specific configs
if [ $2 = "prod" ] || [ $2 = "integration" ]
then
  sparkLocation="/opt/spark/bin/spark-submit"
elif [[ $2 = "dev" ]] || [[ $2 = "qa" ]] || [[ $2 = "training" ]] || [[ $2 = "uat" ]] || [[ $2 = "stag" ]]
then
  sparkLocation="/opt/spark-2.3.0-bin-hadoop2.7/bin/spark-submit"
fi
reservationClass="com.virginvoyages.sparkstreaming.KafkaSeawareReservation"
sfTableUpdateClass="com.virginvoyages.sparkstreaming.KafkaSalesForceTableUpdateConsumer"
nbxMergeClass="com.virginvoyages.sparkstreaming.KafkaClientMergeSeaware"
nbxSeawareMergeClass="com.virginvoyages.sparkstreaming.KafkaClientMergeSFDC"
nbxFeedbackStreamClass="com.virginvoyages.sparkstreaming.KafkaCalendarFeedbackConsumer"
nbxCMSIngestionStreaClass="com.virginvoyages.sparkstreaming.CMSActivityConsumer"
nbxShorexIngestionStreamClass="com.virginvoyages.sparkstreaming.SeawareReservationActivityParser"
nbxSeawareTableChnageStreamClass="com.virginvoyages.sparkstreaming.PackageTypeTableChangeEvents"
nbxPersonCreatedXREF="com.virginvoyages.sparkstreaming.KafkaPersonCreatedJsonConsumer"
nbxCRMAcxiomDemographics="com.virginvoyages.sparkstreaming.KafkaCRMAcxiomDataRefresh"
nbxCRMAcessKey="com.virginvoyages.sparkstreaming.SeawareReservationAccessKey"
nbxShoreDining="com.virginvoyages.sparkstreaming.SeawareReservationDinningActivity"
nbxBainSegment="com.virginvoyages.sparkstreaming.SewareBainSegmentation"
nbxFitnessParser="com.virginvoyages.sparkstreaming.KafkaFitnessParser"
nbxPersonAccXref="com.virginvoyages.sparkstreaming.KafkaPersonCreatedProducerConsumer"
nbxPersonUpdate="com.virginvoyages.sparkstreaming.UpdateMasterId"

#Spark Command Variables
sparkClassVar="--class "
sparkProVar="--properties-file "
sparkFilesVar="--files "
sparkJarVar="--jars "
sparkMaster=" --master yarn"
sparkDeployMode=" --deploy-mode cluster"
if [ $2 = "prod" ]
then
   sparkDriverMemory=" --driver-memory 8g --num-executors 3 "
elif [[ $2 = "dev" ]] || [[ $2 = "qa" ]] || [[ $2 = "training" ]] || [[ $2 = "uat" ]] || [[ $2 = "stag" ]] || [[ $2 = "integration" ]]
then
   sparkDriverMemory=" --driver-memory 8g --num-executors 1 "
fi
sparkExecMem=" --executor-memory 4g"
sparkcores=" --executor-cores 2"
sparkSpace=" "
sparkcircleJobNm=" --name CircleReservation "
sparkclientJobNm=" --name clientmerge"
sparktableChangeJobNm=" --name tablechange"

# Log4j Configs
# Log 4j Configs for seaware reservation
seaware_reservation_driver_log4j="spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j_conf_seaware.reservation_driver.properties"
seaware_reservation_execution_log4j="spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j_conf_seaware.reservation_executor.properties"
# Log 4j Configs for client merge
client_merge_driver_log4j="spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j_conf_sfdc_client.merge_driver.properties"
client_merge_executor_log4j="spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j_conf_sfdc_client.merge_executor.properties"
# Log 4j Configs for table change
table_change_driver_log4j="spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j_conf_sfdc_tablechange_driver.properties"
table_change_executor_log4j="spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j_conf_sfdc_tablechange_executor.properties"
# Log 4j Configs for sfdc reservation
sfdc_reservation_driver_log4j="spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j_conf_sfdc_reservation_driver.properties"
sfdc_reservation_executor_log4j="spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j_conf_sfdc_reservation_executor.properties"
# Log 4j Configs for circle reservation
circle_reservation_driver_log4j="spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j_conf_sfdc_circle.reservation_driver.properties"
circle_reservation_executor_log4j="spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j_conf_sfdc_circle.reservation_executor.properties"

# Log 4j Configs for calendar feedback
cal_feedback_driver_log4j="spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j_conf_calendar.feedback_driver.properties"
cal_feedback_executor_log4j="spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j_conf_calendar.feedback_executor.properties"

# Log4J configs for NBX client merge
nbx_client_merge_driver_log4j="spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j_conf_nbx_client.merge_driver.properties"
nbx_client_merge__executor_log4j="spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j_conf_nbx_client.merge_executor.properties"

# Log4J configs for NBX CMS ingestion
nbx_cms_ingestion_driver_log4j="spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j_conf_nbx_cms.ingestion_driver.properties"
nbx_cms_ingestion_executor_log4j="spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j_conf_nbx_cms.ingestion_executor.properties"

# Log4J configs for NBX Shorex Activites Ingestion
nbx_shorex_ingestion_driver_log4j="spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j_conf_nbx_shorex.ingestion_driver.properties"
nbx_shorex_ingestion_executor_log4j="spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j_conf_nbx_shorex.ingestion_executor.properties"

# Log4J configs for NBX Shorex Activites Ingestion
nbx_seaware_tablechange_ingestion_driver_log4j="spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j_conf_nbx_seaware.table.ingestion_driver.properties"
nbx_seaware_tablechange_ingestion_executor_log4j="spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j_conf_nbx_seaware.table.ingestion_executor.properties"

# Log4J configs for NBX Shorex Activites Ingestion
nbx_person_xref_driver_log4j="spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j_conf_nbx_person_XREF_driver.properties"
nbx_person_xref_executor_log4j="spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j_conf_nbx_person_XREF_executor.properties"

# Log4J configs for NBX Shorex Activites Ingestion
nbx_crm_demographics_driver_log4j="spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j_conf_nbx_crm_demographics_driver.properties"
nbx_crm_demographics_executor_log4j="spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j_conf_crm_demographics_executor.properties"

# Log4J configs for NBX Shorex Activites Ingestion
nbx_shore_dining_driver_log4j="spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j_conf_nbx_shore_dining_driver.properties"
nbx_shore_dining_executor_log4j="spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j_conf_shore_dining_executor.properties"

# Log4J configs for NBX Shorex Activites Ingestion
nbx_shore_bain_segmentation_driver_log4j="spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j_conf_nbx_bain_driver.properties"
nbx_shore_bain_segmentation_executor_log4j="spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j_conf_bain_executor.properties"

# Log4J configs for NBX Shorex Activites Ingestion
nbx_shore_fitness_parser_log4j="spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j_conf_nbx_shore_fitness_driver.properties"
nbx_shore_fitness_parser_executor_log4j="spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j_conf_shore_fitness_executor.properties"


# Check the log present or not. If not create one
if [ ! -d "$log_path" ]
then
    echo "Logs folder not found...create.."
    mkdir -p $log_path
fi

echo "RealTime Script start" `date` > $log_path/$log_file

# Jar/Config Variables
echo "Parent dir "$parentdir >> $log_path/$log_file
realtimeJarLoc=`ls $parentdir/*cap-bigdata-real*/*-depend*.jar`
realtimeResConfigLoc=`ls $parentdir/*cap-bigdata-real*/config/vvcon*.conf`
realtimeCRConfigLoc=`ls $parentdir/*cap-bigdata-real*/config/circle*.conf`
realtimeClientConfigLoc=`ls $parentdir/*cap-bigdata-real*/config/client*.conf`
realtimeTableConfigLoc=`ls $parentdir/*cap-bigdata-real*/config/table*.conf`
realtimeSFResConfigLoc=`ls $parentdir/*cap-bigdata-real*/config/reserva*.conf`
jksKeyStoreLoc=`ls $parentdir_1/config/*keystore.jks`
jksTrustStoreLoc=`ls $parentdir_1/config/*truststore.jks`
xsdLocation=`ls -d $parentdir/*cap-bigdata-real*/xsd*`
echo "Jar location :"$realtimeJarLoc >> $log_path/$log_file
echo "Config Location :"$realtimeResConfigLoc >> $log_path/$log_file

# log4J files
# log4j files for seaware reservation
seaware_res_log4j_driver_file=`ls $parentdir_1/config/log4j/*seaware.reservation_driver*`
seaware_res_log4j_exec_file=`ls $parentdir_1/config/log4j/*seaware.reservation_executor*`

# log4j files for client merge
client_merge_log4j_driver_file=`ls $parentdir_1/config/log4j/*client.merge_driver*`
client_merge_log4j_exec_file=`ls $parentdir_1/config/log4j/*client.merge_executor*`

# Log4j files for table change
table_change_log4j_driver_file=`ls $parentdir_1/config/log4j/*tablechange_driver*`
table_change_log4j_exec_file=`ls $parentdir_1/config/log4j/*tablechange_executor*`

#log 4j for circle reservation
circle_res_log4j_driver_file=`ls $parentdir_1/config/log4j/*circle.reservation_driver*`
circle_res_log4j_exec_file=`ls $parentdir_1/config/log4j/*circle.reservation_executor*`

#log4j files for SFDC reservation
sfdc_res_log4j_driver_file=`ls $parentdir_1/config/log4j/*sfdc_reservation_driver*`
sfdc_res_log4j_exec_file=`ls $parentdir_1/config/log4j/*sfdc_reservation_executor*`

# log4J files for NBX client merge
nbx_client_merge_log4j_driver_file=`ls $parentdir_1/config/log4j/*nbx_client.merge_driver*`
nbx_client_merge_log4j_exec_file=`ls $parentdir_1/config/log4j/*nbx_client.merge_executor*`

# log4J files for NBX Feedback
nbx_cal_feedback_log4j_driver_file=`ls $parentdir_1/config/log4j/*calendar.feedback_driver*`
nbx_cal_feedback_log4j_exec_file=`ls $parentdir_1/config/log4j/*calendar.feedback_executor*`

# log4J files for NBX CMS Ingestion
nbx_cms_ingestion_log4j_driver_file=`ls $parentdir_1/config/log4j/*cms.ingestion_driver*`
nbx_cms_ingestion_log4j_exec_file=`ls $parentdir_1/config/log4j/*cms.ingestion_executor*`

# log4J files for NBX Shorex Ingestion
nbx_shorex_ingestion_log4j_driver_file=`ls $parentdir_1/config/log4j/*shorex.ingestion_driver*`
nbx_shorex_ingestion_log4j_exec_file=`ls $parentdir_1/config/log4j/*shorex.ingestion_executor*`

# log4J files for NBX Shorex Ingestion
nbx_seaware_tablechange_log4j_driver_file=`ls $parentdir_1/config/log4j/*seaware.table.ingestion_driver*`
nbx_seaware_tablechange_log4j_exec_file=`ls $parentdir_1/config/log4j/*seaware.table.ingestion_executor*`

# log4J files for NBX Shorex Ingestion
nbx_person_xref_log4j_driver_file=`ls $parentdir_1/config/log4j/*nbx_person_XREF_driver*`
nbx_person_xref_log4j_exec_file=`ls $parentdir_1/config/log4j/*nbx_person_XREF_executor*`

# log4J files for NBX Shorex Ingestion
nbx_crm_demographics_log4j_driver_file=`ls $parentdir_1/config/log4j/*crm_demographics_driver*`
nbx_cmr_demographics_log4j_exec_file=`ls $parentdir_1/config/log4j/*crm_demographics_executor*`

# log4J files for NBX Shorex Ingestion
nbx_shore_dining_log4j_driver_file=`ls $parentdir_1/config/log4j/*shore_dining_driver*`
nbx_shore_dining_log4j_exec_file=`ls $parentdir_1/config/log4j/*shore_dining_executor*`

# log4J files for NBX Shorex Ingestion
nbx_shore_bain_log4j_driver_file=`ls $parentdir_1/config/log4j/*bain_driver*`
nbx_shore_bain_log4j_exec_file=`ls $parentdir_1/config/log4j/*bain_executor*`

# log4J files for NBX Shorex Ingestion
nbx_shore_fitness_log4j_driver_file=`ls $parentdir_1/config/log4j/*fitness_driver*`
nbx_shore_fitness_log4j_exec_file=`ls $parentdir_1/config/log4j/*fitness_executor*`


#echo "seaware_res_log4j_driver_file : "$seaware_res_log4j_driver_file
#echo "seaware_res_log4j_exec_file :  "$seaware_res_log4j_exec_filei

#Shorex Ingestion properties
shorex_extra_param="--conf spark.reservation.activity=true"
access_key_extra_param="--conf spark.reservation.accesskey=true"
kafkaCacheConfig=" --conf spark.streaming.kafka.consumer.cache.enabled=false "
shore_dining_extra_param=" --conf spark.reservation.dinningactivity=true"
shore_bain_extra_param=" --conf spark.reservation.bainsegmentation=true"

#CRM Files config
if [ $2 = "dev" ]
then
  crmGlobalFileConfig="/data/apps/talend/shared/scripts/cap-bigdata-acxiom-client/config/global.properties"
  cmrAcxiomJars=" --jars /data/apps/talend/shared/scripts/cap-bigdata-acxiom-client/dsapi-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
elif [[ $2 = "qa" ]] || [[ $2 = "training" ]] || [[ $2 = "uat" ]] || [[ $2 = "prod" ]] || [[ $2 = "integration" ]]
then
   crmGlobalFileConfig='/data/apps/talend/shared/scripts/cap-bigdata-acxiom-client-'"${2}"'/config/global.properties'
   cmrAcxiomJars=' --jars /data/apps/talend/shared/scripts/cap-bigdata-acxiom-client-'"${2}"'/dsapi-0.0.1-SNAPSHOT-jar-with-dependencies.jar'
elif [[ $2 = "stag" ]]
then
  crmGlobalFileConfig="/data/apps/talend/shared/scripts/cap-bigdata-acxiom-client-staging/config/global.properties"
  cmrAcxiomJars=" --jars /data/apps/talend/shared/scripts/cap-bigdata-acxiom-client-staging/dsapi-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
fi

if [ ! -z "$realtimeJarLoc" -a "$realtimeJarLoc" != " " ] && [ ! -z "$realtimeResConfigLoc" -a "$realtimeResConfigLoc" != " " ]
then
        echo "Jar and config location are good" >> $log_path/$log_file

        if [ "$1" = "newreservation" ]
        then

            echo "Executing the spark job with command...." >> $log_path/$log_file
            echo "sh "$sparkLocation ${kafkaCacheConfig} "--conf" ${seaware_reservation_driver_log4j} "--conf" ${seaware_reservation_execution_log4j} $sparkClassVar $sparkSpace $reservationClass $sparkSpace $sparkProVar $sparkSpace $realtimeResConfigLoc $sparkSpace $sparkFilesVar $sparkSpace $jksKeyStoreLoc","$jksTrustStoreLoc","$xsdLocation/*.xsd $sparkSpace $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file
         sh $sparkLocation ${kafkaCacheConfig} "--conf" ${seaware_reservation_driver_log4j} "--conf" ${seaware_reservation_execution_log4j} $sparkClassVar $sparkSpace $reservationClass $sparkSpace $sparkProVar $sparkSpace $realtimeResConfigLoc $sparkSpace $sparkFilesVar $sparkSpace ${seaware_res_log4j_driver_file}","${seaware_res_log4j_exec_file},$jksKeyStoreLoc","$jksTrustStoreLoc","$xsdLocation/*.xsd $sparkSpace $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file 2>> $log_path/$log_file &

        elif [ "$1" = "circlereservation" ]
        then
            echo "Executing the spark job with command...." >> $log_path/$log_file
            echo "sh "$sparkLocation "--files" $circle_res_log4j_driver_file","$circle_res_log4j_exec_file "--conf" ${circle_reservation_driver_log4j} "--conf" ${circle_reservation_executor_log4j} $sparkClassVar $sparkSpace $sfTableUpdateClass ${sparkcircleJobNm} $sparkProVar $sparkSpace $realtimeCRConfigLoc $sparkSpace $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file
           sh $sparkLocation "--files" $circle_res_log4j_driver_file","$circle_res_log4j_exec_file "--conf" ${circle_reservation_driver_log4j} "--conf" ${circle_reservation_executor_log4j} $sparkClassVar $sparkSpace $sfTableUpdateClass $sparkcircleJobNm $sparkProVar $sparkSpace $realtimeCRConfigLoc $sparkSpace $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file 2>> $log_path/$log_file &

        elif [ "$1" = "clientmerge" ]
        then
            echo "Executing the spark job with command...." >> $log_path/$log_file
            echo "sh "$sparkLocation "--files" $client_merge_log4j_driver_file","$client_merge_log4j_exec_file "--conf" ${client_merge_driver_log4j} "--conf" ${client_merge_executor_log4j} $sparkClassVar $sparkSpace $sfTableUpdateClass ${sparkclientJobNm} $sparkProVar $sparkSpace $realtimeClientConfigLoc $sparkSpace $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file
           sh $sparkLocation "--files" $client_merge_log4j_driver_file","$client_merge_log4j_exec_file "--conf" ${client_merge_driver_log4j} "--conf" ${client_merge_executor_log4j} $sparkClassVar $sparkSpace $sfTableUpdateClass ${sparkclientJobNm} $sparkProVar $sparkSpace $realtimeClientConfigLoc $sparkSpace $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file 2>> $log_path/$log_file &

        elif [ "$1" =  "tablechange" ]
        then
            echo "Executing the spark job with command...." >> $log_path/$log_file
            echo "sh "$sparkLocation "--files" $table_change_log4j_driver_file","$table_change_log4j_exec_file "--conf" ${table_change_driver_log4j} "--conf" ${table_change_executor_log4j} $sparkClassVar $sparkSpace $sfTableUpdateClass $sparktableChangeJobNm $sparkProVar $sparkSpace $realtimeTableConfigLoc $sparkSpace $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file
          sh $sparkLocation "--files" $table_change_log4j_driver_file","$table_change_log4j_exec_file "--conf" ${table_change_driver_log4j} "--conf" ${table_change_executor_log4j} $sparkClassVar $sparkSpace $sfTableUpdateClass ${sparktableChangeJobNm} $sparkProVar $sparkSpace $realtimeTableConfigLoc $sparkSpace $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file 2>> $log_path/$log_file &

        elif [ "$1" =  "sfdcreservation" ]
        then
            echo "Executing the spark job with command...." >> $log_path/$log_file
            echo "sh "$sparkLocation "--files" $sfdc_res_log4j_driver_file","$sfdc_res_log4j_exec_file "--conf" ${sfdc_reservation_driver_log4j} "--conf" ${sfdc_reservation_executor_log4j} $sparkClassVar $sparkSpace $sfTableUpdateClass $sparkSFResJobNm $sparkProVar $sparkSpace $realtimeSFResConfigLoc $sparkSpace $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file
           sh $sparkLocation "--files" $sfdc_res_log4j_driver_file","$sfdc_res_log4j_exec_file "--conf" ${sfdc_reservation_driver_log4j} "--conf" ${sfdc_reservation_executor_log4j} $sparkClassVar $sparkSpace $sfTableUpdateClass ${sparkSFResJobNm} $sparkProVar $sparkSpace $realtimeSFResConfigLoc $sparkSpace $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file 2>> $log_path/$log_file &

        elif [ "$1" =  "nbxseawareclientmerge" ]
        then
             echo "Executing the spark job with command...." >> $log_path/$log_file
             echo "sh "$sparkLocation "--conf" ${nbx_client_merge_driver_log4j} "--conf" ${nbx_client_merge__executor_log4j} $sparkClassVar $sparkSpace ${nbxMergeClass} $sparkSpace $sparkProVar $sparkSpace $realtimeResConfigLoc $sparkSpace $sparkFilesVar $sparkSpace ${nbx_client_merge_log4j_driver_file}","${nbx_client_merge_log4j_exec_file}  $sparkSpace $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file
         sh $sparkLocation "--conf" ${nbx_client_merge_driver_log4j} "--conf" ${nbx_client_merge__executor_log4j} $sparkClassVar $sparkSpace ${nbxMergeClass} $sparkSpace $sparkProVar $sparkSpace $realtimeResConfigLoc $sparkSpace $sparkFilesVar $sparkSpace ${nbx_client_merge_log4j_driver_file}","${nbx_client_merge_log4j_exec_file}  $sparkSpace $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc  >> $log_path/$log_file 2>> $log_path/$log_file &

       elif [ "$1" =  "nbxsfdcclientmerge" ]
       then
           echo "Executing the spark job with command...." >> $log_path/$log_file
           echo "sh "$sparkLocation "--conf" ${nbx_client_merge_driver_log4j} "--conf" ${nbx_client_merge__executor_log4j} $sparkClassVar $sparkSpace ${nbxSeawareMergeClass} $sparkSpace $sparkProVar $sparkSpace $realtimeResConfigLoc $sparkSpace $sparkFilesVar $sparkSpace ${nbx_client_merge_log4j_driver_file}","${nbx_client_merge_log4j_exec_file}  $sparkSpace $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file
           sh $sparkLocation "--conf" ${nbx_client_merge_driver_log4j} "--conf" ${nbx_client_merge__executor_log4j} $sparkClassVar $sparkSpace ${nbxSeawareMergeClass} $sparkSpace $sparkProVar $sparkSpace $realtimeResConfigLoc $sparkSpace $sparkFilesVar $sparkSpace ${nbx_client_merge_log4j_driver_file}","${nbx_client_merge_log4j_exec_file}  $sparkSpace $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file 2>> $log_path/$log_file &

       elif [ "$1" =  "nbxcalendarfeedback" ]
       then
           echo "Executing the spark job with command...." >> $log_path/$log_file
           echo "sh "$sparkLocation "--conf" ${cal_feedback_driver_log4j} "--conf" ${cal_feedback_executor_log4j} $sparkClassVar $sparkSpace ${nbxFeedbackStreamClass} $sparkSpace $sparkProVar $sparkSpace $realtimeResConfigLoc $sparkSpace $sparkFilesVar $sparkSpace ${nbx_cal_feedback_log4j_driver_file}","${nbx_cal_feedback_log4j_exec_file}  $sparkSpace $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file
          sh $sparkLocation "--conf" ${cal_feedback_driver_log4j} "--conf" ${cal_feedback_executor_log4j} $sparkClassVar $sparkSpace ${nbxFeedbackStreamClass} $sparkSpace $sparkProVar $sparkSpace $realtimeResConfigLoc $sparkSpace $sparkFilesVar $sparkSpace ${nbx_cal_feedback_log4j_driver_file}","${nbx_cal_feedback_log4j_exec_file} $sparkSpace $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file 2>> $log_path/$log_file &

        elif [ "$1" = "nbxcmsingestion" ]
        then
            echo "Executing the spark job with command...." >> $log_path/$log_file
            echo "sh "$sparkLocation "--conf" ${nbx_cms_ingestion_driver_log4j} "--conf" ${nbx_cms_ingestion_executor_log4j} $sparkClassVar ${nbxCMSIngestionStreaClass} $sparkProVar $realtimeResConfigLoc $sparkFilesVar ${nbx_cms_ingestion_log4j_driver_file}","${nbx_cms_ingestion_log4j_exec_file} $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file
            sh $sparkLocation "--conf" ${nbx_cms_ingestion_driver_log4j} "--conf" ${nbx_cms_ingestion_executor_log4j} $sparkClassVar ${nbxCMSIngestionStreaClass} $sparkProVar $realtimeResConfigLoc $sparkFilesVar ${nbx_cms_ingestion_log4j_driver_file}","${nbx_cms_ingestion_log4j_exec_file} $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file 2>> $log_path/$log_file &

        elif [ "$1" = "nbxshorexingestion" ]
        then
            echo "sh "$sparkLocation "--conf" ${nbx_shorex_ingestion_driver_log4j} "--conf" ${nbx_shorex_ingestion_executor_log4j} $sparkClassVar ${nbxShorexIngestionStreamClass} $sparkProVar $realtimeResConfigLoc ${shorex_extra_param} $sparkFilesVar ${nbx_shorex_ingestion_log4j_driver_file}","${nbx_shorex_ingestion_log4j_exec_file}","$xsdLocation/*.xsd $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file
            sh $sparkLocation "--conf" ${nbx_shorex_ingestion_driver_log4j} "--conf" ${nbx_shorex_ingestion_executor_log4j} $sparkClassVar ${nbxShorexIngestionStreamClass} $sparkProVar $realtimeResConfigLoc ${shorex_extra_param} $sparkFilesVar ${nbx_shorex_ingestion_log4j_driver_file}","${nbx_shorex_ingestion_log4j_exec_file}","$xsdLocation/*.xsd $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file 2>> $log_path/$log_file &

        elif [ "$1" = "nbxseawaretablechanges" ]
        then
            echo "sh "$sparkLocation "--conf" ${nbx_seaware_tablechange_log4j_driver_file} "--conf" ${nbx_seaware_tablechange_log4j_exec_file} $sparkClassVar ${nbxSeawareTableChnageStreamClass} $sparkProVar $realtimeResConfigLoc $sparkFilesVar ${nbx_seaware_tablechange_ingestion_driver_log4j}","${nbx_seaware_tablechange_ingestion_executor_log4j} $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file
           sh $sparkLocation "--conf" ${nbx_seaware_tablechange_ingestion_driver_log4j} "--conf" ${nbx_seaware_tablechange_ingestion_executor_log4j} $sparkClassVar ${nbxSeawareTableChnageStreamClass} $sparkProVar $realtimeResConfigLoc  $sparkFilesVar ${nbx_seaware_tablechange_log4j_driver_file}","${nbx_seaware_tablechange_log4j_exec_file} $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file 2>> $log_path/$log_file &

        elif [ "$1" = "nbxpersonxref" ]
        then
            echo "sh "$sparkLocation "--conf" ${nbx_person_xref_driver_log4j} "--conf" ${nbx_person_xref_executor_log4j} $sparkClassVar ${nbxPersonCreatedXREF} $sparkProVar $realtimeResConfigLoc $sparkFilesVar ${nbx_person_xref_log4j_driver_file}","${nbx_person_xref_log4j_exec_file} $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file
            sh $sparkLocation "--conf" ${nbx_person_xref_driver_log4j} "--conf" ${nbx_person_xref_executor_log4j} $sparkClassVar ${nbxPersonCreatedXREF} $sparkProVar $realtimeResConfigLoc  $sparkFilesVar ${nbx_person_xref_log4j_driver_file}","${nbx_person_xref_log4j_exec_file} $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file 2>> $log_path/$log_file &

        elif [ "$1" = "nbxcrmdemographics" ]
        then
            echo "sh "$sparkLocation ${cmrAcxiomJars} "--conf" ${nbx_crm_demographics_driver_log4j} "--conf" ${nbx_crm_demographics_executor_log4j} $sparkClassVar ${nbxCRMAcxiomDemographics} $sparkProVar $realtimeResConfigLoc $sparkFilesVar ${nbx_crm_demographics_log4j_driver_file}","${nbx_cmr_demographics_log4j_exec_file}","${crmGlobalFileConfig} $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file
            sh $sparkLocation ${cmrAcxiomJars}  "--conf" ${nbx_crm_demographics_driver_log4j} "--conf" ${nbx_crm_demographics_executor_log4j} $sparkClassVar ${nbxCRMAcxiomDemographics} $sparkProVar $realtimeResConfigLoc  $sparkFilesVar ${nbx_crm_demographics_log4j_driver_file}","${nbx_cmr_demographics_log4j_exec_file}","${crmGlobalFileConfig} $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file 2>> $log_path/$log_file &

        elif [ "$1" = "nbxaccesskey" ]
        then
            echo "sh "$sparkLocation "--conf" ${nbx_shorex_ingestion_driver_log4j} "--conf" ${nbx_shorex_ingestion_executor_log4j} $sparkClassVar ${nbxCRMAcessKey} $sparkProVar $realtimeResConfigLoc ${access_key_extra_param} $sparkFilesVar ${nbx_shorex_ingestion_log4j_driver_file}","${nbx_shorex_ingestion_log4j_exec_file}","$xsdLocation/*.xsd $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file
            sh $sparkLocation "--conf" ${nbx_shorex_ingestion_driver_log4j} "--conf" ${nbx_shorex_ingestion_executor_log4j} $sparkClassVar ${nbxCRMAcessKey} $sparkProVar $realtimeResConfigLoc ${access_key_extra_param} $sparkFilesVar ${nbx_shorex_ingestion_log4j_driver_file}","${nbx_shorex_ingestion_log4j_exec_file}","$xsdLocation/*.xsd $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file 2>> $log_path/$log_file &

        elif [ "$1" = "nbxshorexdining" ]
        then
            echo "sh "$sparkLocation "--conf" ${nbx_shore_dining_driver_log4j} "--conf" ${nbx_shore_dining_executor_log4j} $sparkClassVar ${nbxShoreDining} $sparkProVar $realtimeResConfigLoc ${shore_dining_extra_param} $sparkFilesVar ${nbx_shore_dining_log4j_driver_file}","${nbx_shore_dining_log4j_exec_file} $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file
            sh $sparkLocation "--conf" ${nbx_shore_dining_driver_log4j} "--conf" ${nbx_shore_dining_executor_log4j} $sparkClassVar ${nbxShoreDining} $sparkProVar $realtimeResConfigLoc ${shore_dining_extra_param} $sparkFilesVar ${nbx_shore_dining_log4j_driver_file}","${nbx_shore_dining_log4j_exec_file} $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file 2>> $log_path/$log_file &
        
        elif [ "$1" = "nbxbainsegment" ]
        then
            echo "sh "$sparkLocation "--conf" ${nbx_shore_bain_segmentation_driver_log4j} "--conf" ${nbx_shore_bain_segmentation_executor_log4j} $sparkClassVar ${nbxBainSegment} $sparkProVar $realtimeResConfigLoc ${shore_bain_extra_param} $sparkFilesVar ${nbx_shore_bain_log4j_driver_file}","${nbx_shore_bain_log4j_exec_file} $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file
            sh $sparkLocation "--conf" ${nbx_shore_bain_segmentation_driver_log4j} "--conf" ${nbx_shore_bain_segmentation_executor_log4j} $sparkClassVar ${nbxBainSegment} $sparkProVar $realtimeResConfigLoc ${shore_bain_extra_param} $sparkFilesVar ${nbx_shore_bain_log4j_driver_file}","${nbx_shore_bain_log4j_exec_file} $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file 2>> $log_path/$log_file &
         
        elif [ "$1" = "nbxfitnessparser" ]
        then
            echo "sh "$sparkLocation "--conf" ${nbx_shore_fitness_parser_log4j} "--conf" ${nbx_shore_fitness_parser_executor_log4j} $sparkClassVar ${nbxFitnessParser} $sparkProVar $realtimeResConfigLoc  $sparkFilesVar ${nbx_shore_fitness_log4j_driver_file}","${nbx_shore_fitness_log4j_exec_file} $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file
            sh $sparkLocation "--conf" ${nbx_shore_fitness_parser_log4j} "--conf" ${nbx_shore_fitness_parser_executor_log4j} $sparkClassVar ${nbxFitnessParser} $sparkProVar $realtimeResConfigLoc  $sparkFilesVar ${nbx_shore_fitness_log4j_driver_file}","${nbx_shore_fitness_log4j_exec_file} $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file 2>> $log_path/$log_file &
        
        elif [ "$1" = "nbxpersonaccref" ]
        then
            echo "sh "$sparkLocation "--conf" ${nbx_person_xref_driver_log4j} "--conf" ${nbx_person_xref_executor_log4j} $sparkClassVar ${nbxPersonAccXref} $sparkProVar $realtimeResConfigLoc $sparkFilesVar ${nbx_person_xref_log4j_driver_file}","${nbx_person_xref_log4j_exec_file} $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file
           sh $sparkLocation "--conf" ${nbx_person_xref_driver_log4j} "--conf" ${nbx_person_xref_executor_log4j} $sparkClassVar ${nbxPersonAccXref} $sparkProVar $realtimeResConfigLoc  $sparkFilesVar ${nbx_person_xref_log4j_driver_file}","${nbx_person_xref_log4j_exec_file} $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file 2>> $log_path/$log_file &
        elif [ "$1" = "nbxpersonmasterupdate" ]
        then
            echo "sh "$sparkLocation ${nbxmasterupdate_name} "--conf" ${nbx_person_xref_driver_log4j} "--conf" ${nbx_person_xref_executor_log4j} $sparkClassVar ${nbxPersonUpdate} $sparkProVar $realtimeResConfigLoc $sparkFilesVar ${nbx_person_xref_log4j_driver_file}","${nbx_person_xref_log4j_exec_file} $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file
           sh $sparkLocation ${nbxmasterupdate_name} "--conf" ${nbx_person_xref_driver_log4j} "--conf" ${nbx_person_xref_executor_log4j} $sparkClassVar ${nbxPersonUpdate} $sparkProVar $realtimeResConfigLoc  $sparkFilesVar ${nbx_person_xref_log4j_driver_file}","${nbx_person_xref_log4j_exec_file} $sparkMaster $sparkDeployMode $sparkDriverMemory $sparkExecMem $sparkcores $realtimeJarLoc >> $log_path/$log_file 2>> $log_path/$log_file &
        fi
fi
