import copy
import requests 
import json 
from datetime import datetime, timedelta 
import pandas as pd 
from dateutil.relativedelta import relativedelta
from pytz import timezone


###     CHANGE YOUR VARIABLES HERE      ###
api_key = 'YOUR_API_KEY'
api_secret = 'YOUR_API_SECRET'
api_url = 'https://api.eu.crosschexcloud.com/' #Change EU to your region
tz = timezone('Europe/Madrid') #Change  to your region
start_date = datetime(2020, 5, 21, 0, 0, 0) #AAAA-MM-dd-HH-mm-ss
start_date = tz.localize(start_date)
end_date = datetime.now(tz)
######################################################################

now = datetime.now(tz)
timestampAPI = now.isoformat()

payload = json.dumps({
    "header": {
        "nameSpace": "authorize.token",
        "nameAction": "token",
        "version": "1.0",
        "requestId": "fd676dd2-db6e-48ae-b3b8-57ca2c428fb9",
        "timestamp": timestampAPI
    },
    "payload": {
        "api_key": api_key,
        "api_secret": api_secret
    }
})
headers = {
    'Content-Type': 'application/json'
}
response = requests.request("POST", api_url, headers=headers, data=payload)

response_json = json.loads(response.text)
token = response_json['payload']['token']
expiration_time = response_json['payload']['expires']

all_data = []
metadata_list = []


while start_date < end_date:
    
    begin_time = start_date.isoformat()
    end_time = (start_date + relativedelta(days=1)).isoformat()
    
    page = 1
    while True:
        payload = json.dumps({
            "header": {
                "nameSpace": "attendance.record",
                "nameAction": "getrecord",
                "version": "1.0",
                "requestId": "f1becc28-ad01-b5b3-7cef-392eb1526f39",
                "timestamp": timestampAPI
            },
            "authorize": {
                "type": "token",
                "token": token
            },
            "payload": {
                "begin_time": begin_time,
                "end_time": end_time,
                "workno": "",
                "order": "asc",
                "page": page,
                "per_page": 100,
                "department": "",
                "job_title": ""
            }
        })
        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", api_url, headers=headers, data=payload)
        data = response.json()
        all_data.extend(data['payload']['list'])
        
        metadata = copy.deepcopy(data)
        del metadata['payload']['list']
        metadata_list.append(metadata)
        
        if len(data['payload']['list']) < 100:
            break
        page += 1
    
    start_date += relativedelta(days=1)
    print(F"Day : {start_date} . . .")

df = pd.json_normalize(all_data)

timsestampvar=timestampAPI.replace(':','-')
df.to_csv(f"TablaOriginal{timsestampvar}.csv")
with open(f"Metadatos{timsestampvar}.txt","a") as withenv:
    for metadata in metadata_list:
        json.dump(metadata, withenv)

## Data transformation
df['Name'] = (df['employee.first_name'] + ' ' + df['employee.last_name']).str.title()
df = df.rename(columns={'employee.workno': 'Employee No.'})
df['checktime'] = pd.to_datetime(df['checktime'])
#Corrects Timezone bug on API (set up to your timezone, currently setted for Spain)
df['checktime'] = df['checktime'].apply(lambda x: x + timedelta(hours=2) if tz.localize(datetime(x.year, 3, 31)) < x < tz.localize(datetime(x.year, 10, 29)) else x + timedelta(hours=1))
df['Date'] = df['checktime'].dt.date
df['Time'] = df['checktime'].dt.time
df = df.rename(columns={'employee.department': 'Department'})
df = df.rename(columns={'device.name': 'Device'})
df = df.rename(columns={'employee.job_title': 'Email'})
df = df.loc[:, ['Name', 'Employee No.', 'Department', 'Date', 'Time', 'Device','uuid','Email', 'checktype']]
df.to_csv(f"Facial_recognition__{timsestampvar}__.csv", index=False)
print("Finished!")