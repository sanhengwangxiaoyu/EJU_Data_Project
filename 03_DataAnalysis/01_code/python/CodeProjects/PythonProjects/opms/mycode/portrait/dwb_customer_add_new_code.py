# In[]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 26 16:44:47 2021

@author: admin1
"""
import configparser
import os
import sys,io
import pymysql
import pandas as pd
import numpy as np
from collections import Counter
import time
from sqlalchemy import create_engine
import getopt

##读取配置文件##
pymysql.install_as_MySQLdb()
cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
table_name = 'dwb_customer_add_new_code'
database = 'dwb_db'
date_quater = '2021Q2'

# In[2]:
##通过输入的参数的方式获取变量值##  如果不需要使用输入参数的方式，可以不用这段
opts,args=getopt.getopt(sys.argv[1:],"t:q:d:c:",["city_id","database=","table=","quarter="])
for opts,arg in opts:
  if opts=="-t" or opts=="--table": # 获取输入参数 -t或者--table 后的值
    table_name = arg
  elif opts=="-q" or opts=="--quarter":  # 获取输入参数 -1或者--quarter 后的值
    date_quater = arg
  elif opts=="-d" or opts=="--database":  # 获取输入参数 -1或者--quarter 后的值
    database = arg
  elif opts=="-c" or opts=="--city_id":  # 获取输入参数 -1或者--quarter 后的值
    city_id = arg
print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())  + '  starting: '+date_quater)


# In[3]:
##重置时间格式
start_time = str(pd.to_datetime(date_quater))[0:10]   #截取成yyyy-MM-dd
stop_time =  str(pd.to_datetime(date_quater) + pd.offsets.QuarterEnd(0))[0:10]      #截取成yyyy-MM-dd

# -*- coding: utf-8 -*-
class MysqlClient:
    def __init__(self, db_host,database,user,password):
        """
        create connection to hive server
        """
        self.conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
    def query(self, sql):
        """
        query
        """
        cur = self.conn.cursor()
        cur.execute(sql)
        res = cur.fetchall()
        columnDes = cur.description #获取连接对象的描述信息
        columnNames = [columnDes[i][0] for i in range(len(columnDes))]
        data = pd.DataFrame([list(i) for i in res],columns=columnNames)
        cur.close()
        return data
    def close(self):
        self.conn.close()

def to_dws(result,table):
    engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database+'?charset=utf8')
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)

con = MysqlClient(db_host,database,user,password)

print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())  + '  reading data to : a_dwb_customer_browse_log')


# In[]
#增存量标识#
#获取周维度#
ori=con.query("SELECT id ods_id,'a_dwb_customer_browse_log' ods_table_name,imei,visit_month,concat(substr(visit_month,1,4),'Q',QUARTER(visit_date)) visit_quarter,city_id,visit_date FROM  dwb_db.a_dwb_customer_browse_log where visit_date >= '"+start_time+"' and visit_date <='"+stop_time+"'")
date_time=con.query("select cal_date visit_date,period visit_week from dws_db_prd.dim_period_date dpd where  cal_date >= '"+start_time+"' and cal_date <='"+stop_time+"'")
ori[['visit_date']] = ori['visit_date'].astype(str)
ori=pd.merge(ori,date_time,how='left',on=['visit_date'])


# In[2]:
# 去重
# ori = ori.groupby(['ods_table_name','imei','visit_week','visit_month','visit_quarter','city_id'])['ods_id'].max().reset_index()
# # ori=pd.merge(ori,add_new_code,how='left',on=['imei']) 
# # # 按照周进行排序，按照imei进行分组。然后将月份整体偏移一位。筛选出客户第一次出现的时间
# # ori['add_new_code1']=ori.sort_values(by=['imei','visit_week']).groupby(['imei'])['visit_week'].shift(1)
# ori['add_new_code']=ori.sort_values(by=['imei','visit_week']).groupby(['imei'])['visit_week'].shift(1)
# # 判断最后一周的值为是否nan（not a number）的时候就是增量数据 ：本周/月之前未浏览本楼盘为增量客户，统一周期为半年
# ori.at[ori['add_new_code'].isna(),'add_new_code']="0"
# # ori.at[ori['add_new_code'].isna(),'add_new_code'] = "0"
# # 判断不为增量的都是存量数据   ：本周/月之前有浏览本楼盘则为存量客户，统计周期为半年
# ori.at[ori['add_new_code']!="0",'add_new_code']="1"
# # ori['add_new_code'] = ori['add_new_code'].apply(lambda x: ori['add_new_code1'] if x == "0" else x)
# # test=ori[ori['add_new_code'].isna()]
# # test.at[ori['add_new_code'].isna(),'add_new_code'] = None
# # test['add_new_code'] = test['add_new_code'].apply(lambda x: test['add_new_code1'] if x is None else x)
# ori['dr'] = 0
# ori['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
# ori['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 


# # ===========================================================================
# # ===========================================================================
# # ===========================================================================
# # ===========================================================================
# # ===========================================================================
# # In[3]:
# #结果信息数据集
# ori = ori[['ods_id','ods_table_name','imei','visit_week','visit_month','visit_quarter','add_new_code','dr','create_time','update_time','city_id']]
print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())  + '  reading data to : dwb_customer_add_new_code')


# In[3]:
#获取历史用户
add_new_code=con.query("SELECT ods_table_name,imei,visit_week,visit_month,visit_quarter,city_id,ods_id,'visit_date' FROM  dwb_db.dwb_customer_add_new_code where add_new_code = '0' and dr = '0'")
print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())  + '  merge data')
#去重
ori = ori.groupby(['ods_table_name','imei','visit_week','visit_month','visit_quarter','city_id'])['ods_id','visit_date'].min().reset_index()
ori = ori.append(add_new_code,ignore_index=True)
#按照星期偏移
ori['add_new_code']=ori.sort_values(by=['imei','visit_week']).groupby(['imei'])['visit_week'].shift(1)
#赋值
ori.at[ori['add_new_code'].isna(),'add_new_code']="0"
ori.at[ori['add_new_code']!='0','add_new_code']="1"
#去除历史数据
ori = ori[ori['visit_quarter'] >= date_quater]
ori['dr'] = 0
ori['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
ori['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
ori = ori[['ods_id','ods_table_name','imei','visit_week','visit_month','visit_quarter','add_new_code','dr','create_time','update_time','city_id','visit_date']]


# In[27]:
to_dws(ori,table_name)
print('>>> Done')


# In[1000]:
# df.to_csv('C:\\Users\\86133\\Desktop\\df.csv')

