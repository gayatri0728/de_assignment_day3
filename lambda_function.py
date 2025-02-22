import boto3
import pandas as pd
import json
import StringIO

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
        data = response["Body"].read().decode("utf-8")
        s3_df = pd.read_json(StringIO(data))
        print("Original DataFrame:\n", s3_df.head())
        # Filter Data: Select rows where status is 'delivered'
        result_df = s3_df[s3_df["status"] == "delivered"]
        print("Filtered DataFrame:\n", result_df.head())
        csv_buffer = StringIO()
        result_df.to_csv(csv_buffer, index=False)
        target_bucket = "doordash-target-zone-de"
        target_key = "filtered_data.csv"
        result_df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=target_bucket, Key=target_key,Body=csv_buffer.getvalue())

        message="data loaded successfully to bucket"
        response=sns_client.publish(Subject="Sucesss data loaded to taget bucket",TargetArn=sns_arn,Message=message,MessageStructure="text")
    except:
          print("Data load failed")
          message="data loaded failed to bucket"
          response=sns_client.publish(Subject="Failed data loaded",TargetArn=sns_arn,Message=message,MessageStructure="text")