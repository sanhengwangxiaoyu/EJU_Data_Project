# In[1]:
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
from numpy.core.einsumfunc import _compute_size_by_dict
from numpy.lib.ufunclike import isneginf
from pandas.core.indexes.base import ensure_index
import pymysql
import pandas as pd
import numpy as np
from collections import Counter
import re
from sqlalchemy import create_engine
import datetime
from dateutil.relativedelta import relativedelta
import getopt


cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
start_time = '2021-04-01'
stop_time = '2021-07-31'
city_name = '北京'

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


# In[2]:
city_name = '北京'
# 取城市预售证数据
deal=con.query("select city_name ,gd_city ,floor_name ,issue_code , issue_date, issue_date_clean ,case when room_sum is null then 0 when room_sum = '' then 0 when room_sum = 'None' then 0 else substr(replace(convert(room_sum using ascii),'?',0),1,5) end room_sum,building_code ,room_code ,case when issue_area = 'None' then 0 when issue_area = '' then 0 else substr(issue_area,1,8) end issue_area ,case when area = '' then 0 else area end area ,insert_time  from temp_db.tmp_city_newest_deal where issue_date_clean between '"+start_time+"' and '"+stop_time+"' and city_name = '"+city_name+"'")


# In[3]:
deal['room_sum'] = deal['room_sum'].apply(lambda x:re.sub("\D","",x))
# 按照room_sum统计一份套数
#楼栋粒度供应套数
df1 = deal.groupby(['gd_city','issue_code','issue_date_clean','building_code'])['room_sum'].max().reset_index()
df1[['room_sum']] = df1[['room_sum']].astype('int')
#获取年月
df1['year_date'] = df1['issue_date_clean'].map(lambda x:x. split('-')[0])
df1['month_date'] = df1['issue_date_clean'].map(lambda x:x. split('-')[1])
#计算数量
df1_year = df1.groupby(['year_date'],as_index=False)['room_sum'].sum().reindex()
df1_month = df1.groupby(['year_date','month_date'],as_index=False)['room_sum'].sum().reindex()
#数据合并
df1_year['month_date'] = df1_year['year_date']
result1 = df1_month.append(df1_year,ignore_index=True)


# 按照room_sum统计一份套数 （深圳 佛山 嘉兴 南宁）
#预售证粒度供应套数
df1_1 = deal.groupby(['gd_city','issue_code','issue_date_clean'])['room_sum'].max().reset_index()
df1_1[['room_sum']] = df1_1[['room_sum']].astype('int')
#获取年月
df1_1['year_date'] = df1_1['issue_date_clean'].map(lambda x:x. split('-')[0])
df1_1['month_date'] = df1_1['issue_date_clean'].map(lambda x:x. split('-')[1])
#计算数量
df1_1_year = df1_1.groupby(['year_date'],as_index=False)['room_sum'].sum().reindex()
df1_1_month = df1_1.groupby(['year_date','month_date'],as_index=False)['room_sum'].sum().reindex()
#数据合并
df1_1_year['month_date'] = df1_1_year['year_date']
result1_1 = df1_1_month.append(df1_1_year,ignore_index=True)


# In[3]:
# 按照issue_area统计一份套数 （无锡 长春）
#预售证粒度供应套数
deal['issue_area'] = deal['issue_area'].apply(lambda x:re.sub("\D","",x))
df1_2 = deal.groupby(['gd_city','issue_code','issue_date_clean'])['issue_area'].max().reset_index()
df1_2[['room_sum']] = (df1_2[['issue_area']].astype('float')/5000).astype('int')
#获取年月
df1_2['year_date'] = df1_2['issue_date_clean'].map(lambda x:x. split('-')[0])
df1_2['month_date'] = df1_2['issue_date_clean'].map(lambda x:x. split('-')[1])
#计算数量
df1_2_year = df1_2.groupby(['year_date'],as_index=False)['room_sum'].sum().reindex()
df1_2_month = df1_2.groupby(['year_date','month_date'],as_index=False)['room_sum'].sum().reindex()
#数据合并
df1_2_year['month_date'] = df1_2_year['year_date']
result_2 = df1_2_month.append(df1_2_year,ignore_index=True)


# In[3]:
# 按照issue_area统计一份套数 （成都）
#预售证粒度供应套数
deal['issue_area'] = deal['issue_area'].apply(lambda x:re.sub("\D","",x))
df1_2 = deal.groupby(['gd_city','issue_code','issue_date_clean'])['issue_area'].max().reset_index()
df1_2[['room_sum']] = (df1_2[['issue_area']].astype('float')/255).astype('int')
#获取年月
df1_2['year_date'] = df1_2['issue_date_clean'].map(lambda x:x. split('-')[0])
df1_2['month_date'] = df1_2['issue_date_clean'].map(lambda x:x. split('-')[1])
#计算数量
df1_2_year = df1_2.groupby(['year_date'],as_index=False)['room_sum'].sum().reindex()
df1_2_month = df1_2.groupby(['year_date','month_date'],as_index=False)['room_sum'].sum().reindex()
#数据合并
df1_2_year['month_date'] = df1_2_year['year_date']
result_2 = df1_2_month.append(df1_2_year,ignore_index=True)


# In[3]:
# 按照issue_area统计一份套数 （贵阳）
#预售证粒度供应套数
deal['issue_area'] = deal['issue_area'].apply(lambda x:re.sub("\D","",x))
df1_2 = deal.groupby(['gd_city','issue_code','issue_date_clean'])['issue_area'].max().reset_index()
df1_2[['room_sum']] = (df1_2[['issue_area']].astype('float')/11000).astype('int')
#获取年月
df1_2['year_date'] = df1_2['issue_date_clean'].map(lambda x:x. split('-')[0])
df1_2['month_date'] = df1_2['issue_date_clean'].map(lambda x:x. split('-')[1])
#计算数量
df1_2_year = df1_2.groupby(['year_date'],as_index=False)['room_sum'].sum().reindex()
df1_2_month = df1_2.groupby(['year_date','month_date'],as_index=False)['room_sum'].sum().reindex()
#数据合并
df1_2_year['month_date'] = df1_2_year['year_date']
result_2 = df1_2_month.append(df1_2_year,ignore_index=True)


# In[3]:
# 按照issue_area统计一份套数 （惠州）
#预售证粒度供应套数
deal['issue_area'] = deal['issue_area'].apply(lambda x:re.sub("\D","",x))
df1_2 = deal.groupby(['gd_city','issue_code','issue_date_clean'])['issue_area'].max().reset_index()
df1_2[['room_sum']] = (df1_2[['issue_area']].astype('float')/2500).astype('int')
#获取年月
df1_2['year_date'] = df1_2['issue_date_clean'].map(lambda x:x. split('-')[0])
df1_2['month_date'] = df1_2['issue_date_clean'].map(lambda x:x. split('-')[1])
#计算数量
df1_2_year = df1_2.groupby(['year_date'],as_index=False)['room_sum'].sum().reindex()
df1_2_month = df1_2.groupby(['year_date','month_date'],as_index=False)['room_sum'].sum().reindex()
#数据合并
df1_2_year['month_date'] = df1_2_year['year_date']
result_2 = df1_2_month.append(df1_2_year,ignore_index=True)


# In[3]:
# 按照issue_area统计一份套数 （武汉）
#预售证粒度供应套数
deal['issue_area'] = deal['issue_area'].apply(lambda x:re.sub("\D","",x))
df1_2 = deal.groupby(['gd_city','issue_code','issue_date_clean'])['issue_area'].max().reset_index()
df1_2[['room_sum']] = (df1_2[['issue_area']].astype('float')/11000).astype('int')
#获取年月
df1_2['year_date'] = df1_2['issue_date_clean'].map(lambda x:x. split('-')[0])
df1_2['month_date'] = df1_2['issue_date_clean'].map(lambda x:x. split('-')[1])
#计算数量
df1_2_year = df1_2.groupby(['year_date'],as_index=False)['room_sum'].sum().reindex()
df1_2_month = df1_2.groupby(['year_date','month_date'],as_index=False)['room_sum'].sum().reindex()
#数据合并
df1_2_year['month_date'] = df1_2_year['year_date']
result_2 = df1_2_month.append(df1_2_year,ignore_index=True)



# In[3]:
# 按照issue_area统计一份套数 （济南）
#预售证粒度供应套数
deal['issue_area'] = deal['issue_area'].apply(lambda x:re.sub("\D","",x))
df1_2 = deal.groupby(['gd_city','issue_code','issue_date_clean'])['issue_area'].max().reset_index()
df1_2[['room_sum']] = (df1_2[['issue_area']].astype('float')/20000).astype('int')
#获取年月
df1_2['year_date'] = df1_2['issue_date_clean'].map(lambda x:x. split('-')[0])
df1_2['month_date'] = df1_2['issue_date_clean'].map(lambda x:x. split('-')[1])
#计算数量
df1_2_year = df1_2.groupby(['year_date'],as_index=False)['room_sum'].sum().reindex()
df1_2_month = df1_2.groupby(['year_date','month_date'],as_index=False)['room_sum'].sum().reindex()
#数据合并
df1_2_year['month_date'] = df1_2_year['year_date']
result_2 = df1_2_month.append(df1_2_year,ignore_index=True)


# In[3]:
# 按照issue_area统计一份套数 （九江）
#预售证粒度供应套数
deal['issue_area'] = deal['issue_area'].apply(lambda x:re.sub("\D","",x))
df1_2 = deal.groupby(['gd_city','issue_code','issue_date_clean'])['issue_area'].max().reset_index()
df1_2[['room_sum']] = (df1_2[['issue_area']].astype('float')/85000).astype('int')
#获取年月
df1_2['year_date'] = df1_2['issue_date_clean'].map(lambda x:x. split('-')[0])
df1_2['month_date'] = df1_2['issue_date_clean'].map(lambda x:x. split('-')[1])
#计算数量
df1_2_year = df1_2.groupby(['year_date'],as_index=False)['room_sum'].sum().reindex()
df1_2_month = df1_2.groupby(['year_date','month_date'],as_index=False)['room_sum'].sum().reindex()
#数据合并
df1_2_year['month_date'] = df1_2_year['year_date']
result_2 = df1_2_month.append(df1_2_year,ignore_index=True)


# In[4]:
# 按照room_code统计一份套数
df2 = deal.groupby(['gd_city','issue_code','issue_date_clean','building_code'])['room_code'].max().reset_index()
df2 = df2.groupby(['issue_code','issue_date_clean'])['room_code'].count().reset_index()
#获取年月
df2['year_date'] = df2['issue_date_clean'].map(lambda x:x. split('-')[0])
df2['month_date'] = df2['issue_date_clean'].map(lambda x:x. split('-')[1])
#计算数量
df2_year = df2.groupby(['year_date'],as_index=False)['room_code'].sum().reindex()
df2_month = df2.groupby(['year_date','month_date'],as_index=False)['room_code'].sum().reindex()
#数据合并
df2_year['month_date'] = df2_year['year_date']
result2 = df2_month.append(df2_year,ignore_index=True)




# In[5]:
#
 

# In[6]:
#


# In[7]:
#

# In[8]:
# 



# In[9]:
# result=df6.append(df8,ignore_index=True)
# to_dws(result,'dws_supply')
# df02.to_csv('C:\\Users\\86133\\Desktop\\df02.csv')

