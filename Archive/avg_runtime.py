"""Computing average run time of Spark ETL jobs on AWS clusters"""

import boto3
s3 = boto3.client('s3')
response = s3.list_buckets()
buckets = [bucket['Name'] for bucket in response['Buckets']]
print("Bucket List: %s" % buckets)

#connect to aws ec2
#computation of key metrics
#further analysis / visualization


