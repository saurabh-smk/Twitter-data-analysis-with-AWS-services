# Twitter-data-analysis-with-AWS-services
An automated system for Twitter data Analysis using AWS Services. In this architecture, a python script pulls data continuously using Twitter API, a Lambda function along with Kinesis Firehose is then used for pre-processing data and then writing it to S3 in JSON format. An EMR activity (Spark Job) is scheduled on hourly basis to create reports using Spark-SQL which are then written in S3.


Overall Architecture:
1) Python script running continuouly pulls data from Twitter api, from the API only few attributes are extracted.  So a single extracted records will consists of these fileds: [screen_name, location, language, posts, followers, friends, [hashtags]]. Note, last column [hashtags] is extracted in list format.

2) AWS Kinesis Firehose is then used to store this data in S3. In default condition this data will be stored in text format and will have several erroneous formating like additional space etc. Hence for pre-processing AWS Lambda is used alongwith Kinesis Firehose. This Lambda function is a Python script which in simple terms will iterate on each record in kinesis and then after cleaning data store data in JSON format.
Advantages:
-- Reading JSON data will be much easier in Spark, additionally [hashtags] column will be directly taken as Array data type when we read this in Spark DataFrame from S3.

3) AWS Glue job written in Pyspark will be scheduled on hourly basis to generate reports on 24 hrs data in S3. 
Advantages of Glue:
-- Can directly take data in partitions from S3. As our data is stored in [s3://bucket-name/2019/01/23/12] i.e partitioned based on [year, month, day, hour] this is handy in extracting only 24 hours data from S3 rather than processing on whole data 


4) To update the Glue Crawler continuouly with new partitions in data a Lambda function is triggered on hourly basis so that Glue metadata is updated for new partition.
