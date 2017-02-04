import boto3
import botocore
import time
import MySQLdb
import csv
import urllib
import itertools
from decimal import *

'''
#Connecting to S3 through Boto3
s3 = boto3.resource('s3')
print "Connection to S3 Established!!"
'''
# Assigning name to a var
bucket_name = 'aws-earthquakeDataRetrival'
file_name = 'all_month_s3.csv'
'''
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
s3.Object(bucket_name, 'all_month_s3.csv').put(Body=open('/home/ubuntu/assignment3/all_month_s3.csv', 'rb'))

#End Timer
end_time = time.clock()
print "End Time File Upload: " + str(round(Decimal(end_time),3)) + " seconds"

total_time = end_time - start_time
print "Total Time to Load Data into S3: "+ str(round(Decimal(total_time),3))+" seconds"
'''

#Connect to RDS with my user credentials and declare a for the DB instance
db = MySQLdb.connect("aishwarya.ccxfz4hgtou6.us-west-2.rds.amazonaws.com","root","aishwarya","earthquakeDataRetrival")
cursor = db.cursor()
print '\n\tConnected to RDS MySQLdb'

#Make the link Public: Actions-> Make Public
#This execution is through url
#urllink ="https://s3.amazonaws.com/aws-testassignment3/all_month_s3.csv"
#https://s3-us-west-2.amazonaws.com/aws-testassignment3/all_month_s3.csv

urllink ="https://s3-us-west-2.amazonaws.com/"+bucket_name+"/"+file_name
print "\n\tS3 URL LINK: "+urllink

file=urllib.urlopen(urllink)
csvfile= csv.reader(file)

#First row in the csv file:
row1 = next(csvfile)
print row1

# Taking the FileName Create Database:
table_name = "test_"+file_name[:-4]

sql = "CREATE TABLE IF NOT EXISTS " + table_name + "("
for i in range (0,len(row1)):
	sql+= row1[i] + " varchar(100),"
sql+=" ashid int unsigned AUTO_INCREMENT PRIMARY KEY)"
#print "query fired"
print "===========\n"+sql+"\n==========="
cursor.execute(sql)
print "Executed Create Statement"

sql="LOAD DATA LOCAL INFILE '" + file_name + "' INTO TABLE " + table_name + " FIELDS TERMINATED BY ','  ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES"
#LOAD DATA LOCAL INFILE 'all_month_s3.csv' INTO TABLE test_all_month_s3 FIELDS TERMINATED BY ','  ENCLOSED BY '\"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES
# "LOAD DATA INFILE '/path/to/my/file' INTO TABLE sometable FIELDS TERMINATED BY ';' ENCLOSED BY '\"' ESCAPED BY '\\\\'"
print "===========\n"+sql+"\n==========="

start_time = time.clock()
cursor.execute(sql)
db.commit()
end_time = time.clock()

'''
#Create a table to upload the all_month.csv file to RDS
#sql = "CREATE TABLE all_month_s3(time_occured varchar(50), latitude varchar(50), longitude varchar(50), depth varchar(50), magnitude varchar(50), magtype varchar(50), nst varchar(50), gap varchar(50), dmin varchar(50), rms varchar(50), net varchar(50), id varchar(50) PRIMARY KEY, updated varchar(50), place varchar(50), type_occured varchar(50), horizontalError varchar(50), depthError varchar(50),magError varchar(50), magNst varchar(50), status varchar(50), locationSource varchar(50), magSource varchar(50));"
#cursor.execute(sql)

#File Upload Starts Here
start_time = time.clock()
for row in itertools.islice(csvfile, 1, None):
	sql= "INSERT INTO all_month_s3 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
	cursor.execute(sql,row)
	db.commit()
end_time = time.clock()
'''
print "\tLoad File Start Time: \t"+ str(round(Decimal(start_time),3)) + " seconds"
print "\tLoad File End Time: \t"+ str(round(Decimal(end_time),3)) + " seconds"
print "\tTotal File Load Time: \t"+ str(round(Decimal(end_time-start_time),3)) + " seconds"

sql = "SHOW TABLES;"
cursor.execute(sql)
result = cursor.fetchall()
#Display Tables
print "\n\tTable Name: "
for row in result:
	print row
	
db.close()
