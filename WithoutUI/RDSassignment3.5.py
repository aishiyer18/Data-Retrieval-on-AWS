import time, MySQLdb, os, math, random
from decimal import *

db = MySQLdb.connect("aishwarya.ccxfz4hgtou6.us-west-2.rds.amazonaws.com","root","aishwarya","earthquakeDataRetrival")
cursor = db.cursor()
table_name = "all_month_s3"

#for 1000,5000,20000 random queries
arrlooprange  = ['1000','5000','20000']

for lrange in arrlooprange:
	#Assign range(1-times)
	looprange = int(lrange)
	print "\n\tLoop Execution Times: "+str(looprange)
	
	total_time=time.clock()	
	sql_time = nosql_time = Decimal(0.0)
	
	for count in range(1,looprange+1):
		randmag = round(random.uniform(0,6),2)
		clause = " magnitude = '"+str(randmag)+"'"
		
		print "Magnitude: " +str(randmag)
		
		sql = "SELECT * FROM "+table_name+" WHERE "+clause
		start_time = time.clock()
		cursor.execute(sql)
		sql_time += Decimal(time.clock()-start_time)

		result = cursor.fetchall()
		num_rows = int(cursor.rowcount)

		print "Row Count: " +str(num_rows)

		# for row in result:
			# print row	

	total_time = Decimal(time.clock()-total_time)
	print "\tSQL Query Execution Time: "+str(round(sql_time,3))
	print "\tTotal Function Execution Time: "+str(round(total_time,3))
	