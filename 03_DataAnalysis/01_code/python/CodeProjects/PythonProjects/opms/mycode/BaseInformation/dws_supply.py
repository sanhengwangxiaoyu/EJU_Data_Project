# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Aug 27 16:44:47 2021
"""
import configparser
import os
import sys
import pymysql
import getopt

##设置配置信息##
pymysql.install_as_MySQLdb()
cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
date_quarter = '2021Q3'    # 季度

def connect_mysql(conn):
    #判断链接是否正常
 conn.ping(True)
#建立操作游标
 cursor=conn.cursor()
#设置数据输入输出编码格式
 cursor.execute('set names utf8')
 return cursor


# In[2]:
##通过输入的参数的方式获取变量值##  如果不需要使用输入参数的方式，可以不用这段
opts,args=getopt.getopt(sys.argv[1:],"t:q:",["database=","table="])
for opts,arg in opts:
  if opts=="-t" or opts=="--table":
    table_name = arg
  elif opts=="-q" or opts=="--quarter":
    date_quarter = arg


# In[1]:
#设置格式
conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
# 建立链接游标
cur=connect_mysql(conn)
#====================================季度===============================================
update_sql1 = "insert into dws_db_prd.dws_supply(city_name,county_name,city_id,period,value,local_issue_value,local_room_sum_value,cric_value,value_from_index,county_name_merge,city_county_index,period_index,update_time,dr,create_time,follow_people_num,cityid,quarter) select city_name, county_name, county_id city_id, period, supply_value value, null local_issue_value , null local_room_sum_value, null cric_value, null value_from_index , null county_name_merge, '2' city_county_index, '1' period_index, now() update_time, 0 dr, now() create_time, intention follow_people_num, city_id cityid,'"+date_quarter+"' quarter from dwb_db.dwb_issue_supply_county where period = '"+date_quarter+"' and dr = 0 union all select city_name, city_name county_name, city_id city_id, period, sum(supply_value) value, null local_issue_value , null local_room_sum_value, null cric_value, null value_from_index , null county_name_merge, '1' city_county_index, '1' period_index, now() update_time, 0 dr, now() create_time, null follow_people_num, city_id cityid,'"+date_quarter+"' quarter from dwb_db.dwb_issue_supply_county  where period = '"+date_quarter+"' and dr = 0  group by city_name,city_id,period union all select city_name, city_name county_name, city_id city_id, period, sum(supply_num) value, null local_issue_value , null local_room_sum_value, null cric_value, null value_from_index , null county_name_merge, '1' city_county_index, '1' period_index, now() update_time, 0 dr, now() create_time, null follow_people_num, city_id cityid,'"+date_quarter+"' quarter from dwb_db.dwb_issue_supply_city where dr = 0 and period = '"+date_quarter+"' and city_id in ('442000','441900') group by city_name,city_id,period"
cur.execute(update_sql1)
conn.commit() # 提交记
conn.close() # 关闭数据库链接
print('>>> quarter data load Done')



#In[]
#设置格式
conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
# 建立链接游标
cur=connect_mysql(conn)
#====================================月度===============================================
update_sql1 = "insert into dws_db_prd.dws_supply(city_name,county_name,city_id,period,value,local_issue_value,local_room_sum_value,cric_value,value_from_index,county_name_merge,city_county_index,period_index,update_time,dr,create_time,follow_people_num,cityid,quarter) select city_name, county_name county_name, county_id city_id, period,supply_value, null local_issue_value , null local_room_sum_value, null cric_value, null value_from_index , null county_name_merge, '2' city_county_index, '2' period_index, now() update_time, 0 dr, now() create_time, null follow_people_num, city_id cityid,'"+date_quarter+"' quarter from dwb_db.dwb_issue_supply_county where quarter = '"+date_quarter+"' and period != quarter and dr = 0"
cur.execute(update_sql1)
conn.commit() # 提交记
conn.close() # 关闭数据库链接
print('>>>month data load Done')



# %%
#设置格式
conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
# 建立链接游标
cur=connect_mysql(conn)
#====================================月度浏览人数===============================================
update_sql1 = "update dws_db_prd.dws_supply a , (select intention,period,county_id,city_id from dwb_db.dwb_newest_county_customer_num where period is not null and dr = 0 and city_id != county_id and period != quarter and quarter = '"+date_quarter+"') b set a.follow_people_num = b.intention where a.city_id = b.county_id and a.cityid = b.city_id and a.period = b.period and a.city_county_index = 2 and a.period_index = 2 and a.dr = 0 and a.quarter = '"+date_quarter+"' "
cur.execute(update_sql1)
conn.commit() # 提交记
conn.close() # 关闭数据库链接
print('>>>month people data load Done')




# %%
#设置格式
conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
# 建立链接游标
cur=connect_mysql(conn)
update_sql1 = "update dws_db_prd.dws_supply set value = '-' where value = '' or value = '0'"
cur.execute(update_sql1)
conn.commit() # 提交记
conn.close() # 关闭数据库链接
print('>>>value update Done')



