CREATE EXTERNAL TABLE vv_db.hvtb_nbx_core_gnip_brandtweets
(
displayName string,
favoritesCount int,
followersCount int,
friendsCount int,
id string,
languages string,
listedCount int,
actor_location_displayName string,
preferredUsername string,
statusesCount int,
twitterTimeZone string,
body string,
generator_displayName string,
generator_link string,
country string,
countryCode string,
profilelocations_locality string,
region string,
profilelocations_subregion string,
profilelocation_displayName string,
profilelocation_coordinates string,
epoch bigint,
brand_name string
)
STORED AS PARQUET 
LOCATION 's3://vv-qa-emr-cluster/data/core/social-media/twitter/gnip/tweetsdata/';
