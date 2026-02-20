import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
import json
import csv
from pathlib import Path
from kafka import KafkaProducer

print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),"-------------Started-------------")

import json

# KAFKA_HOST = 'localhost:9092'. # Need to change this for Production Environment
KAFKA_HOST = ['10.222.200.141:9092', '10.222.200.142:9092', '10.222.200.143:9092']

producer = KafkaProducer(
    bootstrap_servers= KAFKA_HOST,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(3, 9), 
    retries=5,
    linger_ms=10,
    batch_size=16384
)






# Druid endpoint
url = "http://10.165.24.8:8888/druid/v2/sql" # For Production Environment

# Authentication
username = "admin"
password = "A1GRSL1Uadmin"
 

inventory_metadata = {
    "OTT STRIP":"'87bb88f4','e93e32e8','5d66bd98','156dadd9','160a9f3b','b51998a1','411f58a2','b24dc8f0','9bc959bc','5e8f9f0f','269d1afb','4a6b8a53','42561e21','5a859e81'",
    "MONEY HALF CARD":"'f6ebe5fd','b48bfdab'",
    "MANAGE GROWTH BANNER":"'23563bc0','d4436bad','4f8c8dea','dc760ab7','21def1a4','487ae007','5a2b7efa','a972a495','a647f9fd','428a4844','1c300cd6','cfaa505b'",
    "Prepaid Service Landing Banner":"'ebc988a4','ec0278ea','bc372f2b','bd64f8d1','991c1d0e','3437fb8b','47610088','94befbd4','ebcda6d8','aecafefb','81dfbde1','230b326d'",
    "Scratch Card":"'00c93433','a71645b0'",
    "Data Loan Scratch Card":"'f2970a4d','79ccd943'",
    "Manage Scratch Card":"'c7aac47b','26570824'",
    "FS Top Rail":"'62f12e29','16e90cbd','b944b0c0','05e946f8','c7a6d5b0','a745b6a5','3904dc58','f1ef8e51'",
    "Top Rail":"'bb92672d','1da29207','bbb71fcd','7b47ccdc','eb8e4e16','5a5f3453','0fca128e','dde95f5a'",
    "Financial Services Bottom Rail":"'7735fbf3','61a49bda','e8ed88e9','3c2faaaa','f2ab0e1f','f730a97e','f76832de','9faa57d8'",
    "App Bottom Rail":"'a5b059f8','aa5fd8ff','f7b4a020','98946904','ae8885c8','4ceea56b','a8d648b1','c77db573','d858d426','e1851fdc','0f9056b9','f4df6d02','b94b478f','36db7e46', \
        'bf2a64e3','648e870d','378f7ec0','de5fe26f','7cfaa7f4','53f68d4d','d37fc73e','8ad045ec','0d14844d','6b244373','d81ed45d','01d8b781','c3ed7829','3eb1f311','2449ee47','86010732'",
    "Recharge Shortcut Banner":"'f88469d9','0558a1da','90dc79c9','22ff2261','16a879e0','40b99908'",
    "Manage Proposition":"'c4ee16c7','d32eafb4','22ebce3e','a37d378c'",
    "Manage Money Card":"'533ccf4f','91d39b12'",
    "Service Landing Banner":"'e3b9cb42','6126c258','5eb792e8','cc8b9683','88d7b6b1','2fb90633'",
    "Service Landing Offer":"'21f73db4','335f73ee','07be42bf','19dbbe14','c1d940ae','2bcf730f'",
    "Prepaid Service Landing Banner":"'230b326d','81dfbde1','aecafefb','ebcda6d8','94befbd4','47610088','3437fb8b','991c1d0e','bd64f8d1','bc372f2b','ec0278ea','ebc988a4'",
    "Postpaid Top Banner":"'58ac023f','637870ab','12896cd4','689581dc','93f03899','880ad349','V86176471'",
    "Postpaid Bottom Banner":"'dd1a047d','a51dd0fd','8db773eb','48c34cca','1f1e35cb','572fab02'",
    "Prepaid Detail Offer Card":"'b9062d0d','a32fa3ce','b1af9808','c6a8a89e','237d525a','1e807957','1ddacf29','3d2b9c9b'",
    "BB Service Top Banner":"'f11054e7','2e186328','9fe9d099','d8e542ba','81abbd87','fcf1a079'",
    "BB Bottom Banner (dsl_service_banner)":"'6f74c144','a1299b91','f554e820','8091bc3d','2f4b8f6a','18256d1e'",
    "DTH Top Banner":"'b4c4e0c2','c4aa5d68','56f459c7','6a51a397','61af4976','9f072dbb'",
    "DTH Bottom Banner":"'991b44a7','409cd110','889e74d5','4d59d888','6819055d','320ef467'",
    "Bills Landing Offer Card":"'7306d6d3','a66f217b','d16811ed','63a763e2','1f32b96e','fc3d8172','0e71bb08','4131d235'",
    "Bills Offer Banner":"'b41a30df','c1c046f3','23574e26','019e0561','9e8de5b2','13dd0e50'",
    "Manage Offer Card":"'a6af3de2','0b5748d2','2225ac6c','7ef71759','a9768c28','246e961c','4b407082','3c474014','acf85d85','2b87f967','dbff6d13','5c80c9f1'",
    "Common Recharge Home":"'a1576aa8','9bc12867','f677b475','442af065','0f4baa33','7092b488','a3809593','4dc2e174'",
    "Prepaid Offer Recharge Screen":"'8b080012','02e62839','eb23acee','9dfb0a25','719e6100','f44e1be7','b7ba2f8b','a6fc9aa6'",
    "DTH Offer Recharge Screen":"'a7189ba7','6cc34096','2e7071d6','135c02d4','a636e489','7c1f771c','297a33de','a12d7202'",
    "Featuring Fresh":"'e06e3201','405af1e9','003bc238','7d101b9b','a94955f7','82a0e747','b9b44197','1486bc6f','496f352d','40d888e2','75e630d2','805411a4'",
    "Curated for you":"'f8e35b72','db1f535d','e204699a','ceda4da3','a298c073','869181a8','a6ce9d97','3ea51aec','ffb0d879','246911e7','52410df0','314d1738'",
    "FS_Explore_Money":"'66fd8c8b','68d64fc8','07260a1d','be7ac48a','2e444c8f','c868e58b'",
    "FS Pay Page":"'7c9471df','e7b53a02'",
    "FS Loan Page":"'f83d93bd','9fd39b6d'",
    "P2P Scratch Card":"'202ae67c','ff919627'",
    "Curated for you rewards":"'6209fef4','354f8ee6','c1027542','83b6a1db','58ab5766','a742bd11'",
    "Bottom_more_qab_on_recharge":"'23d2f65c','bb73a7df','fb8787c1','8353c7a9','93ef3a2b','aa4dbf42','8eda945a','db445f60'",
    "CRJ":"'a1576aa8','f677b475','0f4baa33','a3809593','9bc12867','442af065','7092b488','4dc2e174'",
    "LCO_Offer_Recharge":"'8bc5a641','8d217925'",
    "Top_services_qab_on_recharge":"'798dc21d','4aa1ba3b','b2566f88','a000d471','daecbef8','f91a939f','ea635a6c','e6bcedc3'",
    "Engagement_nugget":"'ac01592e','248c0c9f','ea454d67','a5be96e7','af909b9a','6dbd8f3c'",
    "Manage_quickactions":"'4aa2bbde','02d598ce','eb4c14b5','769a6faf','83a90654','65d0857b','ff92da58','2ed194c3','13952664','de2c0308','82022bdb','3e0be33f','2b9aae11','dd2a5d67','4f011933','c3db05a9','230743de','ce0ebdc5','df94cdc8','c2008e93','da2b2578','50c978b4','c29f644d','09750db5','e29c0f98','f714b663','785ccae8','e5e8dd99','5b1640fa','7da80a15'"

}
# Current date and time
today = datetime.now()

# Yesterday
two_weeks_ago = today - timedelta(days=1)
output = []

for inventory_name, inventory_adspot in inventory_metadata.items():
    # SQL query
    #query = """
    #SELECT FLOOR(__time TO DAY) as data_day,campaign_id, sum("requests") as request, sum("impressions") as impression, sum("clicks") as click
    #FROM a207953
    #WHERE __time >= '"""+two_weeks_ago.strftime("%Y-%m-%d")+"""' AND __time < '"""+today.strftime("%Y-%m-%d")+"""'
    #  AND adspot_id IN ("""+inventory_adspot+""")
    #GROUP BY FLOOR(__time TO DAY),campaign_id
    #"""

    query = """
    SELECT FLOOR(__time TO DAY) as data_day, sum("requests") as request, sum("ads") as ad, sum("impressions") as impression, sum("clicks") as click
    FROM a207953
    WHERE __time >= '"""+two_weeks_ago.strftime("%Y-%m-%d")+"""' AND __time < '"""+today.strftime("%Y-%m-%d")+"""'
      AND adspot_id IN ("""+inventory_adspot+""")
    GROUP BY FLOOR(__time TO DAY)
    """

    print(query)

    payload = {
        "query": query
    }

    # Send POST request
    response = requests.post(
        url,
        json=payload,
        auth=HTTPBasicAuth(username, password),
        headers={"Content-Type": "application/json"}
    )

    print(response.status_code)

    topic = 'ng-mis-report' # Topic on which we want to produce messages

    # Print results
    if response.status_code == 200:
        print("✅ Query executed successfully!")
        druid_response = response.json()
        print(druid_response)
        for record in druid_response:
            record["inventory"] = inventory_name
            record["fillrate"] = (record["ad"]/record["request"])*100
            record["data_day"] = record["data_day"].replace("Z", "+00:00")

            print('message to push in kafka: ', record)
            try:
                producer.send(topic, value= record)
                producer.flush(timeout=10) 
                print("message succesfully produced in topic ",topic)
            except Exception as e:
                print(f" ❌ Error while producing message: {e}")
                raise


    else:
        print(f"❌ Error {response.status_code}: {response.text}")

producer.flush()
producer.close()


print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),"-------------Completed-------------")
