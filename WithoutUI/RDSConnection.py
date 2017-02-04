import boto3
import MySQLdb

#Connect to RDS with my user credentials and declare a for the DB instance
db = MySQLdb.connect("aishwarya.ccxfz4hgtou6.us-west-2.rds.amazonaws.com","root","aishwarya","earthquakeDataRetrival")
cursor = db.cursor()
	
#Create a table to upload the all_month.csv file to RDS
#sql = "CREATE TABLE all_month_s3(time_occured varchar(50), latitude varchar(50), longitude varchar(50), depth varchar(50), magnitude varchar(50), magtype varchar(50), nst varchar(50), gap varchar(50), dmin varchar(50), rms varchar(50), net varchar(50), id varchar(50) PRIMARY KEY, updated varchar(50), place varchar(50), type_occured varchar(50), horizontalError varchar(50), depthError varchar(50),magError varchar(50), magNst varchar(50), status varchar(50), locationSource varchar(50), magSource varchar(50));"
#cursor.execute(sql)

sql = "SHOW TABLES;"
cursor.execute(sql)
result = cursor.fetchall()
#Display Tables
print "\n\tTable Name: "
for row in result:
	print row
	
db.close()