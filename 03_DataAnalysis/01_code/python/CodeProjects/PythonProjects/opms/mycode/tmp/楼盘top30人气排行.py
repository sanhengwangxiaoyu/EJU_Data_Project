#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 26 16:44:47 2021
@author: admin1

"""
import configparser
import os
import sys
import pymysql
import pandas as pd
import numpy as np
from collections import Counter
import re
from sqlalchemy import create_engine


cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
# database = 'temp_db'
date_quarter = '2021Q1'
start_date = '20210101'  #  获取取数的开始年月日
stop_date = '20210401'    #  获取取数结束的年月日
 
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
    engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database)
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)

con = MysqlClient(db_host,database,user,password)


# In[4]:

#意向客户总量#
con = MysqlClient(db_host,database,user,password)
# dwb_customer_browse_log  客户浏览楼盘日志表（每日增量） 
#                                                      newest_id       楼盘id
#                                                      count(imei)     客户数  
#                                                      visit_date      浏览日期
ori=con.query('''select newest_id,count(imei) imei from dwb_db.dwb_customer_browse_log where visit_date>='''+start_date+''' and visit_date<'''+stop_date+''' group by newest_id''')


# In[5]:

#意向客户总量#
# dwb_newest_info   新房楼盘
#                                                      newest_id       楼盘id
#                                                      city_id         城市id
#                                                      county_id       区县id
newest_id=con.query('''select distinct newest_id,city_id,county_id from dwb_db.dwb_newest_info''')
#意向客户总量#
# dws_newest_period_admit  楼盘周期表
#                                                      newest_id       楼盘id
admit = con.query('''select distinct newest_id from dws_db.dws_newest_period_admit_1 where period = "'''+date_quarter+'''"''')

admit

# In[6]:

# 获取指定季度的楼盘id,和城市id，区县id
grouped2 = pd.merge(newest_id, admit, how='left', on=['newest_id'])
# 通过楼盘id，获取浏览的客户数量
ori = pd.merge(grouped2, ori, how='inner', on=['newest_id'])
# In[100]:
ori
# In[7]:

# 拿取指定列
ori1 = ori[['city_id','newest_id','imei']]
# 对每个城市中的客户数进行排序
grouped = ori1.sort_values(['city_id','imei'],ascending=[1,0],inplace=True)
# 取出每个城市客户数是前30的数据
grouped = ori1.groupby(['city_id']).head(1000)

grouped
# In[8]:

#项目人气度（楼盘占比）：所有浏览过该楼盘的imei总量（去重）/城市imei总量（去重）
# 每个城市的客户总数
grouped1 = ori1.groupby(by = ['city_id'], as_index=False)['imei'].sum()
# 将城市的客户总数和城市客户数前30的关联在一起
grouped2 = pd.merge(grouped, grouped1, how='left', on=['city_id'])
# 新增字段rate
grouped2['rate'] =grouped2['imei_x']/grouped2['imei_y']


# In[9]:

#城市楼盘占比排名：
# 每个城市的楼盘占比排序
grouped2.sort_values(['city_id','rate'],ascending=[1,0],inplace=True)
# 新加字段sort_id :对每个城市楼盘占比的排名
grouped2['sort_id'] = grouped2.groupby(['city_id'])['imei_x'].rank(ascending=False,method='dense')


# In[10]:

# 修改列名
grouped2.columns=['city_id','newest_id','imei_newest','imei_city','rate','sort_id']


# In[11]:

# 截取指定列
ori2 = ori[['county_id','newest_id','imei']]
# 对区域的客户数进行排序
ori2.sort_values(['county_id','imei'],ascending=[1,0],inplace=True)
# 前30
grouped0 = ori2.groupby(['county_id']).head(30)
# 计算每个区域的客户总数
grouped01 = ori2.groupby(by = ['county_id'], as_index=False)['imei'].sum()
# 通过区域id，获取到区域的客户总数
grouped02 = pd.merge(grouped0, grouped01, how='left', on=['county_id'])
#区县楼盘占比
# 新增字段rate  所有浏览过该楼盘的imei总量（去重）/区域imei总量（去重）
grouped02['rate'] =grouped02['imei_x']/grouped02['imei_y']
# 按照区域对区县楼盘占比进行排序
grouped02.sort_values(['county_id','rate'],ascending=[1,0],inplace=True)
# 获取区县楼盘排序的排名
grouped02['sort_id'] = grouped02.groupby(['county_id'])['imei_x'].rank(ascending=False,method='dense')


# In[12]:

# 去除空值
grouped02.dropna(axis = 0,inplace=True)
# 修改列名
grouped02.columns=['city_id','newest_id','imei_newest','imei_city','rate','sort_id']


# In[13]:

# 城市楼盘占比，区域楼盘占比合并在一起，union all
grouped = pd.concat([grouped2,grouped02])
# 新加一列，值为当前季度
grouped['period'] = date_quarter



# In[14]:

# 调用to_dws方法，加载数据到dws_newest_popularity_top30_quarter表中
to_dws(grouped,'dws_newest_popularity_top30_quarter')