import pymssql
import psycopg2
import configparser
from config import config
import json
import collections
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def lambda_handler(event, context):

    try:
        conn_mssql = pymssql.connect(**config('sqlserver'))
        print('HUB Connection established successfully')

        conn_pg = psycopg2.connect(**config('postgresql'))
        print('PT Connection established successfully')

        cur_mssql = conn_mssql.cursor()

        cur_pg = conn_pg.cursor()

        cur_pg.execute('Insert into org_tran.etl_job_log(moment,job,message)values(%s,%s,%s)',
                       (datetime.now(), 'cml_nex_gen_fcly_master_ld', 'HUB Connection established successfully'))

        cur_pg.execute('Insert into org_tran.etl_job_log(moment,job,message)values(%s,%s,%s)',
                       (datetime.now(), 'cml_nex_gen_fcly_master_ld', 'PT Connection established successfully'))

        cur_pg.execute('Insert into org_tran.etl_job_log(moment,job,message)values(%s,%s,%s)',
                       (datetime.now(), 'cml_nex_gen_fcly_master_ld', 'cml_nex_gen_fcly_master_ld job started'))

        print('cml_nex_gen_fcly_master_ld job started')

        cur_mssql.execute('exec dbo.spOrginationsGetFacilityDetails')

        rows = cur_mssql.fetchall()

        # print(type(rows))

        objects_list = []
        for row in rows:
            d = collections.OrderedDict()
            d['primaryBorrowerCustomerId'] = str(row[4] or '')
            d['facilityStatusCode'] = str(row[6] or '')

            cur_pg.execute("SELECT facility_id FROM org_tran.facility_master WHERE facility_id = %s", (row[0],))
            if cur_pg.fetchone() is None:
                cur_pg.execute("""INSERT INTO org_tran.facility_master(facility_id,sequence_number,facility_extended_fields, created_user_eid,created_timestamp,modified_user_eid,modified_timestamp,etl_modified_timestamp,etl_inserted_timestamp )
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
                    row[0], row[1], json.dumps(d), row[9], row[10], row[11], row[12], datetime.now(), datetime.now()))
            else:
                cur_pg.execute(""" UPDATE org_tran.facility_master
                SET sequence_number=%s,
                facility_extended_fields=%s,
                created_user_eid=%s,
                created_timestamp=%s,
                modified_user_eid=%s,
                modified_timestamp=%s,
                etl_modified_timestamp=%s
                WHERE facility_id = %s;""", (
                    row[1], json.dumps(d), row[9], row[10], 'ETL ADMIN UPDATE', row[12], datetime.now(), row[0]))

        cur_pg.execute('Insert into org_tran.etl_job_log(moment,job,message)values(%s,%s,%s)',
                       (datetime.now(), 'cml_nex_gen_fcly_master_ld', 'Job Completed Successfully'))

        print('cml_nex_gen_fcly_master_ld job completed successfully')


    except Exception as e:
        print(e)
        try:
            host = config('mail')['host']
            fromaddr = config('mail')['fromaddr']
            toaddr = config('mail')['toaddr']
            sub = config('mail')['sub']
            msg = MIMEMultipart()
            msg['From'] = fromaddr
            msg['To'] = toaddr
            msg['Subject'] = sub
            body = str(e)
            msg.attach(MIMEText(body, 'plain'))
            server = smtplib.SMTP(host)
            text = msg.as_string()
            server.sendmail(fromaddr, toaddr, text)
            server.quit()
        except Exception as f:
            print(f)
            cur_pg.execute('Insert into org_tran.etl_job_log(moment,job,message)values(%s,%s,%s)',
                       (datetime.now(), 'cml_nex_gen_fcly_master_ld', f))

    conn_pg.commit()

    conn_pg.close()

    conn_mssql.close()


