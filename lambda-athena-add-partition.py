'''
This script will be a Lambda function to be scheduled using CloudWatch Events on hourly basis to add hourly partition based on current time
'''


import json
import boto3
import datetime

yr, mnth, d, hr = datetime.datetime.now().year, datetime.datetime.now().month, datetime.datetime.now().day, datetime.datetime.now().hour
s3path = 's3://bucket/input/' + "{:04d}/{:02d}/{:02d}/{:02d}".format(datetime.datetime.now().year,datetime.datetime.now().month,datetime.datetime.now().day,datetime.datetime.now().hour)

query = str("ALTER TABLE table-name " +" ADD PARTITION (year=" + str(yr) + ",month=" + str('%02d' %mnth) + ",day=" + str('%02d' %d) +  ",hour = " + str('%02d' %hr) + ")" + " location '%s'" %s3path)

client = boto3.client('athena')



def lambda_handler(event, context):
    
    response = client.start_query_execution(
    QueryString= query,

    QueryExecutionContext={
        'Database': 'db-name'
    },

    ResultConfiguration={
        'OutputLocation': 's3://output-location/',
    }
    )
    
    return(response["ResponseMetadata"]['HTTPStatusCode'])
