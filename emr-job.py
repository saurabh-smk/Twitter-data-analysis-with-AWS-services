'''
To sample test this script:
-- SSH on EMR master node
-- spark-submit emr-job.py
-- check the given S3 paths for output
'''



#Script EMR job

#import statements
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)



# Main Data-frame
df = spark.read.json("s3n://bucket-name/path/to/data/*/*/")
#df.show(500,False)

'''
Schema format
root
 |-- followers: long (nullable = true)
 |-- friends: long (nullable = true)
 |-- hashtags: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- language: string (nullable = true)
 |-- location: string (nullable = true)
 |-- posts: long (nullable = true)
 |-- screen_name: string (nullable = true)
'''

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
q1_ans.coalesce(1).write.format('json').save('s3n://bucket-name/path/to/data/q1.json')


# Q2] Top 10 popular languages and count used in the last 24 hours

#Creating Dataframe
q2 = df.select("language")
q2 = q2.na.drop()

#creating list
temp2 = q2.groupBy("language").count().orderBy(desc("count")).limit(10).collect()

q2_ans = spark.createDataFrame(temp2,['language'])

#writing to s3 (in 1 file)
q2_ans.coalesce(1).write.format('json').save('s3n://bucket-name/path/to/data/day/hour/q2.json')


# Q3] Top 10 countries of tweet origin and count in the last 24 hours

#creating dataframe
q3 = df.select("location")
q3 = q3.na.drop()

#temp list of output
temp3 = q3.groupBy("location").count().orderBy(desc("count")).limit(10).collect()

#create dataframe from temp
q3_ans = spark.createDataFrame(temp3,['location'])

#writing to s3 (in 1 file)
q3_ans.coalesce(1).write.format('json').save('s3n://bucket-name/path/to/data/day/hour/q3.json')


#Q4: Top 10 trending individual hashtags and count used grouped by the top 10 locations in the last 24 hours

q4 = df.select("location","hashtags")
#q4.show(500,False)

#explode hashtags so that each individual hastag is 1 row
after = q4.select("location",explode(q4.hashtags).alias("hashtags"))

#filtering out rows where hashtags were none
after = after.filter( after.hashtags != 'None')


temp4 = after.groupBy(after.location,"hashtags").count().orderBy(desc("count")).limit(10).collect()

q4_ans = spark.createDataFrame(temp4,['location','hashtags','count'])
#last question write
q4_ans.coalesce(1).write.format('json').save('s3n://bucket-name/path/to/data/day/hour/q4.json')
