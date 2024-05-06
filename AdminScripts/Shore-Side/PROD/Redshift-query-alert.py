import psycopg2,datetime,smtplib,csv,ssl,io,sys
from psycopg2.extras import RealDictCursor
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
env = str(sys.argv[1])

con=psycopg2.connect(dbname= 'prodnbxpub', host= '10.15.1.16', port= '5439', user= 'admin', password= 'xxxxxxxxxx')
print("The Sqlite connection is open")
cur = con.cursor(cursor_factory=RealDictCursor)
#cur.execute("select rtrim(user_name) as user,starttime,duration,SUBSTRING(query,1,30) as query from STV_RECENTS limit 10")
cur.execute("select rtrim(user_name) as user,pid,starttime as starttime_utc,ROUND(cast(duration as decimal) / 60000000) as duration_minutes ,SUBSTRING(query,1,30) as query from STV_RECENTS where status = 'Running' and duration >= '1800000000'")
result = cur.fetchall()
cur.close()
print("The Sqlite connection is closed")
con.close()
#for row in result:
#    print(row)

str_io = io.StringIO()
df = pd.DataFrame(result, columns=['user', 'pid', 'starttime_utc', 'duration_minutes', 'query'])
#df = df.to_csv(index=False)
#print(df)
df.to_html(buf=str_io,index=False)
table_html = str_io.getvalue()
#print(table_html)


message = MIMEMultipart("alternative")
message["Subject"] = "Redshift Long Running Queries in "+env+" environment"
message["From"] = 'vv-app-alert-prod@virginvoyages.com'
message["To"] = 'virginnbxservicedesk.in@capgemini.com;zeba.shaikh@capgemini.com;derek.noce@capgemini.com'

text = """\
Subject: Redshift Long Running Queries"""

html = """\
<html>
  <body>
    <p>{table_html}</p>
  </body>
</html>
""".format(table_html=table_html)

part1 = MIMEText(text, "plain")
part2 = MIMEText(html, "html")
message.attach(part1)
message.attach(part2)
if result:
    print(datetime.datetime.now())
    print("Sending Email")
    fromaddr = 'vv-app-alert-prod@virginvoyages.com'
    toaddrs  = 'virginnbxservicedesk.in@capgemini.com;zeba.shaikh@capgemini.com;derek.noce@capgemini.com'
    username = 'vv-app-alert@virginvoyages.com'
    password = 'VVoy@ges2020'
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.starttls()
    server.login(username,password)
    server.sendmail(fromaddr, toaddrs, message.as_string())
    server.quit()

else:
    print(datetime.datetime.now())
    print("No queries found")