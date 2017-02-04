#Refer to the following link for boto3 S3 connection
#http://boto3.readthedocs.io/en/latest/guide/migrations3.html

import boto3
import botocore
import time
from decimal import *

#Connecting to S3 through Boto3
s3 = boto3.resource('s3')
print "Connection to S3 Established!!"

# Assigning name to a var
bucket_name = 'aws-earthquakeDataRetrival'

#Accessing a Bucket
#import botocore for bucket access
bucket = s3.Bucket(bucket_name)
exists = True
try:
    s3.meta.client.head_bucket(Bucket=bucket_name)
except botocore.exceptions.ClientError as e:
    # If a client error is thrown, then check that it was a 404 error.
    # If it was a 404 error, then the bucket does not exist.
    error_code = int(e.response['Error']['Code'])
    if error_code == 404:
		exists = False
		#Creating Bucket in S3
		#s3.create_bucket(Bucket=bucket_name
		s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
		 'LocationConstraint': 'us-west-2'})
		print "Bucket "+bucket_name+" Created Successfully" 
		
if exists == True:
   print "\nBucket "+bucket_name+" Already Exists" 

print "\n\tExisting Buckets:"
for bucket in s3.buckets.all():
	print(bucket.name)

print "\n\tExecuting Queries for File Upload:"	
#Start Timer
start_time = time.clock()
print "Start Time File Upload: " + str(round(Decimal(start_time),3)) + " seconds"

#Storing Data
s3.Object(bucket_name, 'data/all_month_s3.csv').put(Body=open('/home/ubuntu/earthquakeDataRetrival/all_month_s3.csv', 'rb'))

#End Timer
end_time = time.clock()
print "End Time File Upload: " + str(round(Decimal(end_time),3)) + " seconds"

total_time = end_time - start_time
print "Total Time to Load Data into S3: "+ str(round(Decimal(total_time),3))+" seconds"

'''
#Code to delete the bucket
for bucket in s3.buckets.all():
	if bucket.name == bucket_name:
		print "\n\tBucket Name: "+bucket.name
		print "\tKey Name: "
		
		for key in bucket.objects.all():
			print"\t"+key.key
			key.delete()
			print "\nKey "+key.key+" Deleted Successfully"	
		bucket.delete()
		print "Bucket "+bucket.name+" Deleted Successfully"
'''

for bucket in s3.buckets.all():
	print "\n\tAll Buckets in S3"
	print "Bucket Name: "+bucket.name
	print "\n\tList Key Names: "
	
	for key in bucket.objects.all():
		print(key.key)
		
			