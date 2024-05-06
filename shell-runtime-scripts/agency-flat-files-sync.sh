parameterfile=/data/apps/talend/shared/parameterfiles/agency-flat-files-sync.txt

type=$1

if [[ $type = "agency-flat-files" ]]
then
SourceBucket=$(cat $parameterfile | grep agency-flat-files-SourceBucket | cut -d "|" -f2)
DestinationBucket=$(cat $parameterfile | grep agency-flat-files-DestinationBucket | cut -d "|" -f2)
TempLocation=$(cat $parameterfile | grep agency-flat-files-TempLocation | cut -d "|" -f2)
SourceProfile=$(cat $parameterfile | grep SourceProfile | cut -d "|" -f2)
DestinationProfile=$(cat $parameterfile | grep DestinationProfile | cut -d "|" -f2)

mkdir -p $TempLocation

rm $TempLocation/*.csv

aws s3 sync $SourceBucket $TempLocation --exclude "*" --include "VV_pricing_USD.csv" --include "VV_pricing_CAD.csv" --include "VV_pricing_AUD.csv"  --include "VV_pricing_GBP.csv" --include "*sailings.csv" --include "*cabin_categories.csv" --include "*cabins.csv" --include "*ship.csv" --profile $SourceProfile

aws s3 sync $TempLocation $DestinationBucket --exclude "*" --include "VV_pricing_USD.csv" --include "VV_pricing_CAD.csv" --include "VV_pricing_AUD.csv"  --include "VV_pricing_GBP.csv" --include "*sailings.csv" --include "*cabin_categories.csv" --include "*cabins.csv" --include "*ship.csv" --profile $DestinationProfile

elif [[ $type = "net-rate" ]]
then
SourceBucket=$(cat $parameterfile | grep net-rate-SourceBucket | cut -d "|" -f2)
DestinationBucket=$(cat $parameterfile | grep net-rate-DestinationBucket | cut -d "|" -f2)
TempLocation=$(cat $parameterfile | grep net-rate-TempLocation | cut -d "|" -f2)
SourceProfile=$(cat $parameterfile | grep SourceProfile | cut -d "|" -f2)
DestinationProfile=$(cat $parameterfile | grep DestinationProfile | cut -d "|" -f2)

mkdir -p $TempLocation

rm $TempLocation/*

aws s3 sync $SourceBucket $TempLocation --exclude "*" --include "VV_net-rate-agency-flat-files_GBP" --include "VV_net-rate-agency-flat-files_AUD" --include "VV_net-rate-agency-flat-files_USD" --profile $SourceProfile

aws s3 sync $TempLocation $DestinationBucket --exclude "*" --include "VV_net-rate-agency-flat-files_GBP" --include "VV_net-rate-agency-flat-files_AUD" --include "VV_net-rate-agency-flat-files_USD" --profile $DestinationProfile

else
echo "Incorrect Parameter"
fi