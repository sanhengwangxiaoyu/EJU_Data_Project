# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Seq 09 15:44:47 2021
  用户浏览楼盘均价面积表
"""
import configparser
import os
import sys,io
from numpy.lib.function_base import append
import pymysql
import pandas as pd
import numpy as np
from collections import Counter
import re
from sqlalchemy import create_engine
import getopt
import time

##读取配置文件##
pymysql.install_as_MySQLdb()
cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

##设置变量初始值##
user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
date_quarter = '2020Q2'   # 季度
table_name = 'dwb_cust_2_newest_price_are' # 要插入的表名称
database = 'dwb_db'


# In[3]:
##mysql连接配置##
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

##mysql写入执行##
def to_dws(result,table):
    engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database+'?charset=utf8')
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)
##room_sum清洗逻辑
def sum_str(s):
    new_str = ""  		#创建一个空字符串
    for ch in s:
	    if ch.isdigit():		#字符串中的方法，可以直接判断ch是否是数字
		    new_str += ch
	    else:
		    new_str += " "
    sub_list = new_str.split()   #对新的字符串切片
    num_list = list(map(int, sub_list)) 	#map方法，使列表中的元素按照指定方式转变
    res  = sum(num_list)
    # print(res)
    return res
con = MysqlClient(db_host,database,user,password)


# In[2]:
##通过输入的参数的方式获取变量值##  如果不需要使用输入参数的方式，可以不用这段
opts,args=getopt.getopt(sys.argv[1:],"t:q:d:",["database=","table=","quarter="])
for opts,arg in opts:
  if opts=="-t" or opts=="--table": # 获取输入参数 -t或者--table 后的值
    table_name = arg
  elif opts=="-q" or opts=="--quarter":  # 获取输入参数 -1或者--quarter 后的值
    date_quarter = arg
  elif opts=="-d" or opts=="--database":  # 获取输入参数 -1或者--quarter 后的值
    database = arg


# In[3]:
##重置时间格式
start_date = str(pd.to_datetime(date_quarter))[0:10]   #截取成yyyy-MM-dd
end_date =  str(pd.to_datetime(date_quarter) + pd.offsets.QuarterEnd(0))[0:10]      #截取成yyyy-MM-dd

cust=con.query("SELECT housing_id,plat_name pal_name,concat(substr(visit_time,1,4),'Q',QUARTER(visit_time)) `period`,avg_price browse_avg_price_sum FROM dwb_db.cust_browse_log_201801_202106 where housing_id is not null and avg_price is not null and avg_price != 'null' and avg_price != 'NULL'  and substr(visit_time,1,10)>='"+start_date+"' and substr(visit_time,1,10)<='"+end_date+"' union all SELECT housing_id,plat_name pal_name,concat(substr(idate,1,4),'Q',QUARTER(concat(substr(idate,1,4),'-',substr(idate,5,2),'-',substr(idate,8,2)))) `period`,avg_price browse_avg_price_sum FROM dwb_db.cust_browse_log_201801_202106 where housing_id is not null and avg_price is not null and avg_price != 'null' and avg_price != 'NULL'  and concat(substr(idate,1,4),'-',substr(idate,5,2),'-',substr(idate,8,2))>='"+start_date+"' and concat(substr(idate,1,4),'-',substr(idate,5,2),'-',substr(idate,8,2))<='"+end_date+"' and avg_price != '2147483647' and avg_price != ''")



# In[]
housing=con.query("SELECT max(id) housing_id,uuid newest_id,newest_name from dwb_db.dim_housing where uuid is not null group by uuid,newest_name")

newest=con.query("SELECT newest_id,unit_price direct_avg_price from dws_db_prd.dws_newest_info  where newest_id is not null group by newest_id,unit_price")

housing = pd.merge(housing,newest,how='inner',on=['newest_id'])


# In[]
cust['browse_avg_price_sum'] = cust['browse_avg_price_sum'].map(lambda x:x. split('.')[0])
cust['browse_avg_price_sum'] = cust['browse_avg_price_sum'].map(lambda x:x. split('-')[0])
cust['browse_avg_price_sum'] = cust['browse_avg_price_sum'].map(lambda x:x. split('；')[0])
cust['browse_avg_price_sum'] = cust['browse_avg_price_sum'].map(lambda x:x. split('，')[0])
cust['browse_avg_price_sum'] = cust['browse_avg_price_sum'].apply(lambda x:re.sub("\D", "", x))
cust['browse_avg_price_sum'] = cust['browse_avg_price_sum'].apply(lambda x: x[:5] if x > '1000000' else x )
cust['browse_avg_price_count'] = '1'
cust.at[cust['browse_avg_price_sum']=='','browse_avg_price_sum'] = 0
cust[['browse_avg_price_sum']] = cust['browse_avg_price_sum'].astype(int)
cust.groupby(['browse_avg_price_sum'])['pal_name'].count().reset_index()


# In[]:
df_count = cust.groupby(['housing_id','pal_name','period'])['browse_avg_price_count'].count().reset_index()
df_sum = cust.groupby(['housing_id','pal_name','period'])['browse_avg_price_sum'].sum().reset_index()
df = pd.merge(df_count,df_sum,how='inner',on=['housing_id','pal_name','period'])
df['browse_avg_price'] = df['browse_avg_price_sum']/df['browse_avg_price_count'] 
df = pd.merge(df,housing,how='left',on=['housing_id'])
df['avg_price_rate'] = df['browse_avg_price']/df['direct_avg_price'] 


#In[]
df['browse_count'] = '57778237'
df['dr'] = '0'
df['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
df['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
df=df[['newest_id','newest_name','direct_avg_price','browse_avg_price','avg_price_rate','browse_avg_price_sum','browse_avg_price_count','browse_count','period','dr','create_time','update_time','pal_name']]
df=df[~df['newest_id'].isna()]

# housing[housing['newest_id'] == '544a0be563a644f5df767cbce1171a23']
# cust[cust['housing_id'] == 749404]
# test = cust[['browse_avg_price_sum']].drop_duplicates(inplace=False)

# cust[['browse_avg_price_sum']].drop_duplicates(inplace=False).to_csv('C:\\Users\\86133\\Desktop\\test.csv')


#In[]
to_dws(df,table_name)
# result
print('>> Done!') #完毕

