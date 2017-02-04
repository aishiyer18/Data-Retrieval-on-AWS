import time, MySQLdb, os, math, random
##  sudo apt-get install python-memcache
import memcache  
from decimal import *

# Setting up connections
db = MySQLdb.connect("aishwarya.ccxfz4hgtou6.us-west-2.rds.amazonaws.com","root","aishwarya","earthquakeDataRetrival")
cursor = db.cursor()

mc = memcache.Client(['aishwarya.9q0qsg.cfg.usw2.cache.amazonaws.com:11211'], debug=0)

table_name = "all_month_s3"
#for 1000,5000,20000 random queries
arrlooprange  = ['1000','2000','5000']

for lrange in arrlooprange:
	looprange = int(lrange)
	print "\n\tLoop Execution Times: "+str(looprange)
	
	total_time=time.clock()	
	sql_time = nosql_time = Decimal(0.0)

	for count in range(1,looprange+1):
		print count
		randmag = round(random.uniform(0,6),2)
		clause = " magnitude = '"+str(randmag)+"'"
		
		print "Magnitude: " +str(randmag)
		
		# Code to access memcache:
		mcobj = mc.get(str(randmag))
		if not mcobj:
			print "\n\tLoaded Data for "+str(randmag)+" from RDS MySQLdb"
			
			#Execute MySQLdb Queries:
			sql = "SELECT * FROM "+table_name+" WHERE "+clause 
			start_time = time.clock()
			cursor.execute(sql)
			result = cursor.fetchall()
			num_rows = int(cursor.rowcount)
			
			# for row in result:
				# print row
			
			# SQL Time
			sql_time += Decimal(time.clock()-start_time)
			print "Row Count: " +str(num_rows)
			
			if num_rows>0:
				mc.set(str(randmag),result)
				print "Updated Memcached with MySQL Data: "+str(randmag)
		else:
			start_time = time.clock()
			print "\n\tLoaded Data for "+str(randmag)+" from Memcached"
			rowcount = 0
			
			for row in mcobj:
				rowcount += 1
				# print (row[1])
			nosql_time += Decimal(time.clock()-start_time)
			print "Row Count: " +str(rowcount)
	
	total_time = Decimal(time.clock()-total_time)
	print "\n\tSQL Query Execution Time: "+str(sql_time)
	print "\tNoSQL Query Execution Time: "+str(nosql_time)
	
	# print "\tSQL Query Execution Time: "+str(round(sql_time,3))
	# print "\tNoSQL Query Execution Time: "+str(round(nosql_time,3))
	# print "\tTotal Function Execution Time: "+str(round(total_time,3))
	print "\n\tEnd Execution\n"
