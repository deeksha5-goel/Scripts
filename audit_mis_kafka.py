import requests
import sys
import logging
import json
import os
from datetime import date, timedelta, datetime
from kafka import KafkaProducer
import mysql.connector

logging.basicConfig(level=logging.INFO)


if __name__ == "__main__":
	print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),"-------------Started-------------")
	KAFKA_HOST = ['10.222.200.141:9092', '10.222.200.142:9092', '10.222.200.143:9092']
	producer = KafkaProducer(
        bootstrap_servers= KAFKA_HOST,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(3, 9), 
        retries=5,
        linger_ms=10,
        batch_size=16384
    )
	rdbms_host = '10.165.0.13'
	rdbms_user = 'vmax_readonly'
	rdbms_cred = '9L50fusOrf#8b'
	rdbms_db = 'demand_app'

	# Connect to MySQL
	conn = mysql.connector.connect(
	    host=rdbms_host,         
	    user=rdbms_user,
	    password=rdbms_cred,
	    database=rdbms_db
	)

	today = date.today()
	yesterday = today - timedelta(days=1)

	# Create a cursor object
	cursor = conn.cursor()
	print("select id,event,auditable_id,old_values,new_values,url,created_at,updated_at,user_id \
	                        from audits where ((created_at >= '"+str(yesterday)+" 00:00:00' and created_at < '"+str(today)+" 00:00:00') OR \
	                         (updated_at >= '"+str(yesterday)+" 00:00:00' and updated_at < '"+str(today)+" 00:00:00')) and \
	                         auditable_type = 'App\\\\Models\\\\CampaignModel'  and new_values != '{\"recent\":{}}' and \
	                         new_values != '{\"last_modified_at\":{},\"last_modified_by_user_at\":{}}' and user_id != 'NULL';")

	cursor.execute("select id,event,auditable_id,old_values,new_values,url,created_at,updated_at,user_id \
	                        from audits where ((created_at >= '"+str(yesterday)+" 00:00:00' and created_at < '"+str(today)+" 00:00:00') OR \
	                         (updated_at >= '"+str(yesterday)+" 00:00:00' and updated_at < '"+str(today)+" 00:00:00')) and \
	                         auditable_type = 'App\\\\Models\\\\CampaignModel'  and new_values != '{\"recent\":{}}' and \
	                         new_values != '{\"last_modified_at\":{},\"last_modified_by_user_at\":{}}' and user_id != 'NULL';")

	# Fetch all rows
	rows = cursor.fetchall()
	audit_log = []
	topic = 'ng-audit-report' # Topic on which we want to produce messages

for row in rows:
    record = {"campaign": str(row[2]), "event": str(row[1]), "old value": str(row[3]), "new value": str(row[4]), "updated on": str(row[7]), "updated by": str(row[8])}
    print('message to push in kafka: ', record)
    try:
                producer.send(topic, value= record)
                producer.flush(timeout=10) 
                print("message succesfully produced in topic ",topic)
    except Exception as e:
                print(f" ❌ Error while producing message: {e}")
                raise

producer.flush()
producer.close()

	
	
		


    
