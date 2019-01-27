# Twitter-data-analysis-with-AWS-services
An automated system for Twitter data Analysis using AWS Services. In this architecture, a python script pulls data continuously using Twitter API, a Lambda function along with Kinesis Firehose is then used for pre-processing data and then writing it to S3 in JSON format. An EMR activity (Spark Job) is scheduled on hourly basis to create reports using Spark-SQL which are then written in S3.
