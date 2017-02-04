import time, MySQLdb, os, math, random
from decimal import *

db = MySQLdb.connect("aishwarya.ccxfz4hgtou6.us-west-2.rds.amazonaws.com","root","aishwarya","earthquakeDataRetrival")
cursor = db.cursor()
table_name = "all_month_s3"

#for 1000,5000,20000 random queries which return results betweeen 200-800
arrlooprange  = ['1000','50000','20000']

for lrange in arrlooprange:
	#Assign range(1-times)
	looprange = int(lrange)
	print "\n\tLoop Execution Times: "+lrange
	print "\n\tLoop Execution Times: "+str(looprange)
	total_time=time.clock()	
	sql_time = nosql_time = Decimal(0.0)

	for count in range(1,looprange+1):
		arrnet  = ['hv','pr','us','uw']
		randnet = random.choice(arrnet)
		clause = " net = '"+str(randnet)+"'"
		
		print "Net: " +str(randnet)
		
		sql = "SELECT * FROM "+table_name+" WHERE "+clause 
		start_time = time.clock()
		cursor.execute(sql)
		sql_time += Decimal(time.clock()-start_time)

		result = cursor.fetchall()
		num_rows = int(cursor.rowcount)

		print "Row Count: " +str(num_rows)

	total_time = Decimal(time.clock()-total_time)
	print "\tSQL Query Execution Time: "+str(round(sql_time,3))
	print "\tTotal Function Execution Time: "+str(round(total_time,3))

'''
total_time = time.clock()

for count in range(1,50):
	foo  = [ 'hv','pr','us','uw']
	randmag = random.choice(foo)
	clause = " net = '"+str(randmag)+"'"

	sql = "SELECT * FROM all_month_ec2 WHERE "+clause 
	start_time = time.clock()
	cursor.execute(sql)
	sql_time5 += Decimal(time.clock()-start_time)

	result = cursor.fetchall()
	num_rows = int(cursor.rowcount)

	print "Magnitude: " +str(randmag)
	print "Row Count: " +str(num_rows)

total_time = Decimal(time.clock()-total_time)
print "SQL Query Execution Time: "+str(round(sql_time5,3))
print "Total Function Execution Time: "+str(round(total_time,3))
s5000_200=str(sql_time5)



total_time = time.clock()

for count in range(1,200):
	foo  = [ 'hv','pr','us','uw']
	randmag = random.choice(foo)
	clause = " net = '"+str(randmag)+"'"

	sql = "SELECT * FROM all_month_ec2 WHERE "+clause 
	start_time = time.clock()
	cursor.execute(sql)
	sql_time6 += Decimal(time.clock()-start_time)

	result = cursor.fetchall()
	num_rows = int(cursor.rowcount)

	print "Magnitude: " +str(randmag)
	print "Row Count: " +str(num_rows)

total_time = Decimal(time.clock()-total_time)
print "SQL Query Execution Time: "+str(round(sql_time6,3))
print "Total Function Execution Time: "+str(round(total_time,3))
s20000_200=str(sql_time6)
#list = '</table><center>Time(s)<br>SQL: '+str(sql_time)+'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; No-SQL: '+str(nosql_time)+'&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Total Time: '+str(time.clock()-total_time)+'</center>'
cursor.close()


print "Time to run 1000 random queries"+s1000
print "Time to run 5000 random queries"+s5000
print "Time to run 20000 random queries"+s20000
print "Time to run 1000 random queries"+s1000_200
print "Time to run 5000 random queries"+s5000_200
print "Time to run 20000 random queries"+s20000_200

'''



