#Author : Saurabh Kulkarni


'''
This Glue job is triggered on hourly basis to perform an Spark job to generate following reports on 24 hrs of data from the time the job is triggggered
[Q1] Top 10 popular users (those with the most number of followers) who have tweeted in the last 24 hours
[Q2] Top 10 countries of tweet origin and count in the last 24 hours
[Q3] Top 10 popular languages and count used in the last 24 hours
[Q4] Top 10 trending individual hashtags and count used grouped by the top 10 countries in the last 24 hours

'''
#this is a sample file which soves 1st que, similar approach will be implemented for other questions


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
from awsglue.job import Job
import datetime

year, month, day, hour= datetime.datetime.now().year, datetime.datetime.now().month, datetime.datetime.now().day, datetime.datetime.now().hour
push_filter = 'year == ' + str(year) + ' and month == ' + str(month) + ' and day == ' + str(day) + ' and hour == ' + '10'

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
#-------Step 1 : create Dynamic frame with using 'push_down_predicate'
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "gluee", table_name = "record1_mkc", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
main_DyF = glueContext.create_dynamic_frame.from_catalog(database = "f", table_name = "tweet_firehose_pre",push_down_predicate = push_filter, 
    transformation_ctx = "datasource")

print ("Count:  ", main_DyF.count())
main_DyF.printSchema()
#main_DyF.show()

#---------Step 2: Converting it to normal DF
df = main_DyF.toDF()
#df.show()

'''
#Q1] Top 10 popular users (those with the most number of followers) who have tweeted in the last 24 hours
'''
#creating dataframe from main
q1 = df.select('screen_name','followers')

#drop rows with null values
q1 = q1.na.drop()

#created a output list 
temp = q1.orderBy(desc("followers")).distinct().limit(10).collect()

#Now convert list to dataframe so to write back to S3
q1_ans = spark.createDataFrame(temp,['screen_name','followers']) 



#writing to s3
#q1_ans.coalesce(1).write.format('json').save('s3n://polly-bucket-cgnv/out-fire/year1/month1/day1/hour26/')

q1_ans.limit(10).show()
q1_ans = q1_ans.repartition(1)



#action

#write back
q1_ans_tmp = DynamicFrame.fromDF(q1_ans, glueContext,"nested")
print('ppp*************')
q1_ans_tmp.show()
output_dir = "s3://polly-bucket-cgnv/out-fire/year1/month1/day1/hour26/"
# Write it out in JSON
glueContext.write_dynamic_frame.from_options(frame = q1_ans_tmp, connection_type = "s3", connection_options = {"path": output_dir}, format = "json")
print('pp*************')




job.commit()
