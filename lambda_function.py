import boto3
import pandas as pd
import json
from io import StringIO

s3_client=boto3.client("s3")
sns_client=boto3.client("sns")
sns_arn="arn:aws:sns:eu-north-1:842675997150:de-assign-day3"

def lambda_handler(event,context):
    print(event)
    try:
        bucket_name=event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_key=event["Records"][0]["s3"]["object"]["key"]
        print("bucket_name : ",bucket_name)
        print("file_key : ",s3_file_key)
        
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_file_key)
        data = object["Body"].read()
        json_dict=data.decode('utf-8').split("\r\n")
        df=pd.DataFrame(columns=["id","status","amount","date"])
        for line in json_dict:
             py_dict=json.load(line)
             if py_dict['status']=='delivered':
                  df.loc[py_dict['id']]=py_dict
        print("Filtered DataFrame:\n", df.head())
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        target_bucket = "doordash-target-zone-de"
        target_key = "filtered_data.csv"
        s3_client.put_object(Bucket=target_bucket, Key=target_key,Body=csv_buffer.getvalue())
        message="data loaded successfully to bucket"
        response=sns_client.publish(Subject="Sucesss data loaded to taget bucket",TargetArn=sns_arn,Message=message,MessageStructure="text")
    except:
          print("Data load failed")
          message="data loaded failed to bucket"
          response=sns_client.publish(Subject="Failed data loaded",TargetArn=sns_arn,Message=message,MessageStructure="text")