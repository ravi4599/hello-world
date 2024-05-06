whitelist = ['com.virginvoyages.sparkstreaming.KafkaCalendarFeedbackConsumer', 'com.virginvoyages.sparkstreaming.PackageTypeTableChangeEvents', 'com.virginvoyages.sparkstreaming.KafkaClientMergeSFDC', 'com.virginvoyages.sparkstreaming.KafkaClientMergeSeaware', 'com.virginvoyages.sparkstreaming.SeawareReservationActivityParser', 'com.virginvoyages.sparkstreaming.KafkaPersonCreatedJsonConsumer', 'com.virginvoyages.sparkstreaming.KafkaCRMAcxiomDataRefresh', 'com.virginvoyages.sparkstreaming.SeawareReservationAccessKey', 'com.virginvoyages.sparkstreaming.KafkaSeawareReservation', 'com.virginvoyages.sparkstreaming.CMSActivityConsumer', 'com.virginvoyages.sparkstreaming.SeawareReservationDinningActivity', 'com.virginvoyages.sparkstreaming.SewareBainSegmentation', 'com.virginvoyages.sparkstreaming.KafkaFitnessParser', 'IngestionEnablerFrameworkArs', 'IngestionEnablerFrameworkiti', 'IngestionEnablerGoVenue', 'IngestionEnablerFrameworkInternalAccount', 'IngestionEnablerFrameworkapollo', 'IngestionKonamiPtsum', 'IngestionKonamiRtg', 'IngestionKonamiDevice', 'IngestionKonamiCarding', 'IngestionEnablerFrameworkcruise', 'IngestionEnablerFrameworkfolio', 'IngestionEnablerFrameworkAccount', 'IngestionEnablerFrameworkguest', 'IngestionEnablerFrameworksales', 'IngestionEnablerFrameworkitems', 'Feedbackloop', 'IngestionRequests' ,'IngestionRequestCategory' ,'IngestionVenues' ,'IngestionRequestType' ,'IngestionEnablerFrameworkAttribu' ,'IngestionEnablerFrameworkTableClear','com.virginvoyages.sparkstreaming.UpdateMasterId','com.virginvoyages.sparkstreaming.KafkaPersonCreatedProducerConsumer','IngestionFoodBevMenuItemm','IngestionFoodMenuItemPrice','IngestionCasinoGamesite','IngestionFoodBevMenuItemType','IngestionfoodVenu','IngestionfoodnbevorderOrigin','Ingestionfoodnbevordertype','Ingestioncasino','Ingestionfooodnbevorder','Ingestionfoodnbevordercancel','IngestionOrderDetails','IngestionLocaConVenueTags','Ingestionshorehousekeeper','IngestionOrderdetailcancellation','IngestionSwToVxpWearables']

import sys
import json
import urllib
import urllib.request
import numpy as np
import pandas as pd
import io
import datetime,smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import socket
import time

env = str(sys.argv[1])

host_name = socket.gethostname()
IPAddr = socket.gethostbyname(host_name)

url = "http://"+IPAddr+":8088/ws/v1/cluster/apps?states=RUNNING"
req = urllib.request.Request(url)
response = urllib.request.urlopen(req)
data = response.read()
values = json.loads(data)


def extract_values(obj, key):
    """Pull all values of specified key from nested JSON."""
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    results = extract(obj, arr, key)
    return results

def epoch2human(epoch):
    return time.strftime('%Y-%m-%d %H:%M:%S',
        time.localtime(int(epoch)/1000.0))


list1 = extract_values(values, 'id')
list2 = extract_values(values, 'name')
list3 = extract_values(values, 'startedTime')
list4 = extract_values(values, 'elapsedTime')
list5 = [x / 3600000 for x in list4]
list6 = [epoch2human(x) for x in list3]
#print(list1,list2,list3,list4)
df = pd.DataFrame()
df["ID"] = np.array(list1)
df["Name"] = np.array(list2)
df["StartTime"] = np.array(list6)
df["elapsedHours"] = np.array(list5)
shortjobdf = df[df.elapsedHours > 2]
shortjobdf = shortjobdf.query("Name not in @whitelist")
longjobdf = df[df.elapsedHours > 24]
longjobdf = longjobdf.query("Name in @whitelist")
df = pd.concat([shortjobdf, longjobdf])
print(df)
numOfRows = df.shape[0]
str_io = io.StringIO()
df.to_html(buf=str_io,index=False)
table_html = str_io.getvalue()
#print(table_html)
if numOfRows > 0:
        message = MIMEMultipart("alternative")
        message["Subject"] = "Yarn Long Running Jobs in "+env+" environment"
        message["From"] = 'vv-app-alert@virginvoyages.com'
        message["To"] = 'virginnbxservicedesk.in@capgemini.com'

        text = """\
        Subject: Yarn Long Running Jobs in "+env+" environment
        Body: Following is the lsit of long running jobs kindly check."""
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
        print(datetime.datetime.now())
        print("Sending Email")
        fromaddr = 'vv-app-alert@virginvoyages.com'
        toaddrs  = 'virginnbxservicedesk.in@capgemini.com'
        username = 'vv-app-alert@virginvoyages.com'
        password = 'VVoy@ges2020'
        server = smtplib.SMTP('smtp.gmail.com:587')
        server.starttls()
        server.login(username,password)
        server.sendmail(fromaddr, toaddrs, message.as_string())
        server.quit()

else:
        print(datetime.datetime.now())
        print("No jobs found")