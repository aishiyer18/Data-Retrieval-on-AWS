import time, MySQLdb, os, math, random
##  sudo apt-get install python-memcache
import memcache  
from decimal import *

#Command to Flush Memcached is as below:
#telnet aishwarya.9q0qsg.cfg.usw2.cache.amazonaws.com 11211
#flush_all
#quit

# Setting up connections
db = MySQLdb.connect("aishwarya.ccxfz4hgtou6.us-west-2.rds.amazonaws.com","root","aishwarya","earthquakeDataRetrival")
cursor = db.cursor()

mc = memcache.Client(['aishwarya.9q0qsg.cfg.usw2.cache.amazonaws.com:11211'], debug=0)

table_name = "all_month_s3"

#for 1000,5000,20000 random queries which return results betweeen 200-800
# arrlooprange  = ['1000','5000','20000']
arrlooprange  = ['4']

for lrange in arrlooprange:
	#Assign range(1-times)
	looprange = int(lrange)
	print "\n\tLoop Execution Times: "+lrange
	print "\n\tLoop Execution Times: "+str(looprange)
	
	total_time=time.clock()	
	sql_time =  nosql_time =  Decimal(0.0)
	
	for count in range(1,looprange+1):
		print count
		arrnet  = ['hv','pr','us','uw']
		randnet = random.choice(arrnet)
		clause = " net = '"+str(randnet)+"'"
		
		print "Net: " +str(randnet)
		
		# Code to access memcache:
		mcobj = mc.get(randnet)
		if not mcobj:
			#Execute MySQLdb Queries:
			sql = "SELECT * FROM "+table_name+" WHERE "+clause 
			start_time = time.clock()
			cursor.execute(sql)
			result = cursor.fetchall()
			num_rows = int(cursor.rowcount)
			print "\n\tLoaded Data for net '"+str(randnet)+"' from RDS MySQLdb"
			# for row in result:
				# print row
			
			# SQL Time
			sql_time += Decimal(time.clock()-start_time)
			print "Row Count: " +str(num_rows)
			# IF result set is null then does not get inserted into memcache
			if num_rows > 0:
				mc.set(randnet,result)
				print "\tUpdated Memcached with MySQL Data: "+str(randnet)+"\n"
		else:
			start_time = time.clock()
			print "\n\tLoaded Data for net '"+str(randnet)+"' from Memcached"
			rowcount = 0
			for row in mcobj:
				rowcount += 1
				# print (row[1])
			print "Row Count: " +str(rowcount)+"\n"
			nosql_time += Decimal(time.clock()-start_time)
		print "\n\tEnd Range Loop Execution\n"
	
	
	total_time = Decimal(time.clock()-total_time)
	print "\tSQL Query Execution Time: "+str(sql_time)
	print "\tNoSQL Query Execution Time: "+str(nosql_time)
	
	# print "\tSQL Query Execution Time: "+str(round(sql_time,3))
	# print "\tNoSQL Query Execution Time: "+str(round(nosql_time,3))
	# print "\tTotal Function Execution Time: "+str(round(total_time,3))
	print "\n\tEnd Execution\n"