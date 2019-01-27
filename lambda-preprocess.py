'''
This python script will be a Lamda function which is to be run along with Kinesis Firehose so that it will be triggered on Kinesis records
-- preprocess
-- pass it on to kinesis 
-- kinesis will then write it in S3
'''


from __future__ import print_function

import base64
import json

print('Loading function')


def lambda_handler(event, context):
    output = []

    for record in event['records']:
        print(record['recordId'])
        payload = base64.b64decode(record['data'])
        
        #trim spaces
        payload = payload.strip()
        
        #separating last columns with other columns
        last = ((payload.split('['))[-1])[0:-1]
        initial = ((payload.split('['))[0])[0:-2]
        
        #remove single quotes from last column
        x1 = last.replace("'", "")
        
        #creating a list of hashtags (split done based on (, )
        x1_list = x1.split(", ")
        
        #removing spaces from hastags:
        for i, s in enumerate(x1_list):
            x1_list[i]  = s.strip()
        
        #creating all other columns
        a1 = initial.split(", ")
        
        #if location has format " xyz, abc, .."
        part = a1[-4:]
        name = a1[0]
        tril = a1[1:len(a1)-4]
        
        location = ''
        
        #location column
        for x in tril:
            if location == '':
                location = location + x
            else:
                location = location +', '+ x
        
        #verify while writing that n(columns) == 6 (excluding last column)
        
        try:
            x = {"screen_name": a1[0], "location": location.strip(), "language": a1[-4], "posts": int(a1[-3].strip()),"followers": int(a1[-2].strip()), "friends": int(a1[-1].strip()), "hashtags": x1_list}
                
            y = json.dumps(x,ensure_ascii=False) + "\n"
            output_record = {
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': base64.b64encode(y)
            }
            output.append(output_record)
        except Exception:
            print('invalid',payload)
        
        
                

    print('Successfully processed {} records.'.format(len(event['records'])))

    return {'records': output}
