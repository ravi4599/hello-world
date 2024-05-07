voyageid=$1 
env=$2 
inputFile=$3 
echo $inputFile
sep="\001" 
gawk -F, '{$1=$1; printf("%s", $0 RT)}' RS='"[^"]*"' OFS='\001' $3 > temp.csv
awk -v voyageid=$1 -v env=$2 -v dq=\" -v sp="" -F'\001' '$1==voyageid && $2==env
{ 
rmQuotes=gsub(dq,"",$7) 
print $6,$7 > $5
}' temp.csv
rm temp.csv
spark.postgresql.url jdbc:postgresql://10.102.209.154:5432/
spark.postgresql.user nifiusr
spark.postgresql.password finiPRO@123
 
spark.ship.code VAL

