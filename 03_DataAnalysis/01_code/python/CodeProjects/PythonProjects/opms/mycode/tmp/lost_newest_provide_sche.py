# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Seq 10 17:44:47 2021
  -- 竞品动态信息缺失和开盘时间的关系，以及动态缺失的比例
--        没有动态的楼盘
--        有动态的楼盘
--             所有季度都有的
--             4个季度有的
--             
--        开盘时间
"""
import configparser
from datetime import datetime
import os
import sys
from typing import Tuple
import pymysql
import pandas as pd
import time
from sqlalchemy import create_engine
import getopt
import re
import numpy as np

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
#创建数据库连接
con = MysqlClient(db_host,database,user,password)


# In[]:
ods_sche = con.query("select newest_id,date,period,provide_title from dws_db_prd.dws_newest_provide_sche")

newest = con.query("select newest_id,newest_name,recent_opening_time, from dws_db_prd.dws_newest_info where newest_id is not null and recent_opening_time is not null group by newest_id,newest_name,recent_opening_time")

df1 = pd.merge(newest,ods_sche,how='left',on=['newest_id'])


#In[]
#有动态的楼盘
have_sche = df1[~df1['date'].isna()]
have_sche['recent_opening_month'] = have_sche['recent_opening_time'].apply(lambda x:x[:7]).tolist()
have_sche = have_sche.groupby(['recent_opening_month'])['newest_id'].count().reset_index()

#有动态楼盘占有率
all_sche = df1
all_sche['recent_opening_month'] = all_sche['recent_opening_time'].apply(lambda x:x[:7]).tolist()
all_sche = all_sche.groupby(['recent_opening_month'])['newest_id'].count().reset_index()
df_sche_rate = pd.merge(all_sche,have_sche,how='left',on=['recent_opening_month'])
df_sche_rate = df_sche_rate[df_sche_rate['recent_opening_month']>='2018-01']
df_sche_rate = df_sche_rate[df_sche_rate['recent_opening_month']<='2021-07']
df_sche_rate['rate'] = df_sche_rate['newest_id_y']/df_sche_rate['newest_id_x']*100
df_sche_rate.at[df_sche_rate['rate'].isna(),'rate'] = 0
df_sche_rate[['rate']] = df_sche_rate['rate'].astype(float).astype(int)


# In[11]:
open_date_sche = df1[~df1['date'].isna()]
open_date_sche['recent_opening_month'] = open_date_sche['recent_opening_time'].apply(lambda x:x[:7]).tolist()
open_date_sche.date = pd.to_datetime(open_date_sche.date)
open_date_sche.recent_opening_time = pd.to_datetime(open_date_sche.recent_opening_time)
# 先做一个空白列表
open_date_sche['name'] = ''
# where是条件 符合条件的 在name列传入value2
open_date_sche.loc[open_date_sche['recent_opening_time'] >=open_date_sche['date'], 'name'] = 'value2'

open_date_sche1 = open_date_sche[open_date_sche['name']=='value2']
open_date_sche2 = open_date_sche[open_date_sche['name']== '']


# In[11]:
df_sche_rate.to_csv('C:\\Users\\86133\\Desktop\\df_sche_rate.csv')

