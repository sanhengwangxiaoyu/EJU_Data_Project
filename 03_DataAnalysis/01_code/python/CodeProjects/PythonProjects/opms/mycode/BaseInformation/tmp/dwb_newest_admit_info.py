# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Jul 12 17:44:47 2021
     
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
cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

##设置变量初始值##
user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
date_quarter = '2021Q1'   # 季度
table_name = 'dwb_newest_admit_info' # 要插入的表名称
database = 'dwb_db'


# In[2]:
##通过输入的参数的方式获取变量值##  如果不需要使用输入参数的方式，可以不用这段
opts,args=getopt.getopt(sys.argv[1:],"t:q:d:c:",["city_id","database=","table=","quarter="])
for opts,arg in opts:
  if opts=="-t" or opts=="--table": # 获取输入参数 -t或者--table 后的值
    table_name = arg
  elif opts=="-q" or opts=="--quarter":  # 获取输入参数 -1或者--quarter 后的值
    date_quarter = arg
  elif opts=="-d" or opts=="--database":  # 获取输入参数 -1或者--quarter 后的值
    database = arg
  elif opts=="-c" or opts=="--city_id":  # 获取输入参数 -1或者--quarter 后的值
    city_id = arg


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


##正式代码##
"""
1> 获取数据信息：dws_newest_info，dws_newest_period_admit
    通过admit筛选准入楼盘信息，通过dws_newest_info获取楼盘具体信息
"""
con = MysqlClient(db_host,database,user,password)


# In[4]:
# dws_newest_info   新房楼盘
#     楼盘id	newest_id
#     楼盘名称	newest_name
#     楼盘别名	alias_name
#     城市id	city_id
#     城市名称	city_name
#     区县id	county_id
#     区县名称	county_name
#     销售状态	sales_state
#     均价	unit_price
newest_id=con.query('''select newest_id,newest_name,alias_name,city_id,county_id,sales_state,unit_price from dws_db.dws_newest_info where dr = 0 ''')
# newest_id=con.query('''select newest_id,newest_name,alias_name,city_id,city_name,county_id,county_name,sales_state,unit_price from dws_db.dws_newest_info where dr = 0 city_id = "'''+city_id+'''"''')
# dws_newest_period_admit  楼盘周期表
#     周期	period
#     楼盘id	newest_id
admit = con.query('''select distinct newest_id,period from dws_db.dws_newest_period_admit where period = "'''+date_quarter+'''"  and dr = 0''')
# dim_geography  地域维度表
#     城市id	city_id
#     城市名称	city_name
#     区县id	region_id
#     区县名称	region_name
geography = con.query('''select city_id,city_name,region_id,region_name from dws_db.dim_geography where city_id is not null and city_name is not null and region_id is not null and region_name is not null''')
geography.columns = ['city_id','city_name','county_id','county_name']
#转换数据类型
geography[['city_id']] = geography[['city_id']].astype('int').astype('str')
geography[['county_id']] = geography[['county_id']].astype('int').astype('str')
newest_id.at[newest_id['unit_price'].isna(),'unit_price']=0
newest_id[['unit_price']] = newest_id[['unit_price']].astype('int').astype('str')


# In[3]:
#城市id和名字
city_geography = geography[['city_id','city_name']].drop_duplicates(inplace=False)
#区县id和名字
county_geography = geography[['county_id','county_name']].drop_duplicates(inplace=False)
#获取指定季度准入的楼盘id,和楼盘的城市id、区县id
df = pd.merge(admit, newest_id, how='left', on=['newest_id'])
#获取到城市和区县的名字
df = pd.merge(df, city_geography, how='left', on=['city_id'])
df = pd.merge(df, county_geography, how='left', on=['county_id'])
result = df[['newest_id','newest_name','alias_name','city_id','city_name','county_id','county_name','sales_state','unit_price','period']]
result['dr'] = 0
result['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
result['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
result.at[result['city_id'] == '442000','city_name'] = '中山市'
result.at[result['city_id'] == '441900','city_name'] = '东莞市'
result.at[result['city_id'] == '442000','county_id'] = result['county_name']
result.at[result['city_id'] == '441900','county_id'] = result['county_name']


# In[4]:
# 加载到新表 dws_newest_investment_pop_rownumber_quarter
result.drop_duplicates(inplace=True)
to_dws(result,table_name)
# grouped.to_csv('C:\\Users\\86133\\Desktop\\dws_newest_investment_pop_top30_quarter.csv')
# result
print('>> Done!')

