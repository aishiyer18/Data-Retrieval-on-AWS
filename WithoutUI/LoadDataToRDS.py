import boto3
import MySQLdb
import csv
import time
import urllib
import itertools
from decimal import *

# Connect RDS
db = MySQLdb.connect("aishwarya.ccxfz4hgtou6.us-west-2.rds.amazonaws.com","root","aishwarya","earthquakeDataRetrival")
cursor = db.cursor()
print 'Connected to RDS MySQLdb'

'''
#This execution is through local link
link = "/home/ubuntu/assignment3/all_month_s3.csv"
file = urllib.urlopen(link)
csvfile = csv.reader(file)
'''

#Make the link Public: Actions-> Make Public
#This execution is through url
urllink ="https://s3.amazonaws.com/aws-assignment3/data/all_month_s3.csv"
file=urllib.urlopen(urllink)
csvfile= csv.reader(file)

'''
for row in csvfile:
	print row
'''	

#File Upload Starts Here
start_time = time.clock()
for row in itertools.islice(csvfile, 1, None):
	sql= "INSERT INTO all_month_s3 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
	cursor.execute(sql,row)
	db.commit()
end_time = time.clock()
print "Load File Start Time: \t"+ str(round(Decimal(start_time),3)) + " seconds"
print "Load File End Time: \t"+ str(round(Decimal(end_time),3)) + " seconds"
print "Total File Load Time: \t"+ str(round(Decimal(end_time-start_time),3)) + " seconds"

db.close()
