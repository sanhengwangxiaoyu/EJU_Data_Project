#!/usr/bin/env python
# coding: utf-8

# In[2]:


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


# In[3]:


def to_dws(result,table):
    engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database)
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)


# In[4]:


#意向客户总量#
con = MysqlClient(db_host,database,user,password)


# In[4]:


#意向客户总量#
con = MysqlClient(db_host,database,user,password)
ori=con.query('''select newest_id,count(imei) imei 
                 from dwb_db.dwb_customer_browse_log 
                 where visit_date>=20201001 and visit_date<20210101 
                 group by newest_id''')


# In[5]:


#意向客户总量#
newest_id=con.query('''select distinct newest_id,city_id,county_id from dwb_db.dwb_newest_info''')
admit = con.query('''select distinct newest_id from dws_db.dws_newest_period_admit where period = '2020Q4' ''')


# In[6]:


grouped2 = pd.merge(admit, newest_id, how='left', on=['newest_id'])
ori = pd.merge(grouped2, ori, how='inner', on=['newest_id'])
# ori
# grouped_tmp= ori[ori['newest_id']=='c0f52d5ad6450153aa3ffd5868ede2bb']
# grouped_tmp


    
# In[7]:


ori1 = ori[['city_id','newest_id','imei']]
ori1.sort_values(['city_id','imei'],ascending=[1,0],inplace=True)
grouped = ori1.groupby(['city_id']).head(30)


# In[8]:


grouped1 = ori1.groupby(by = ['city_id'], as_index=False)['imei'].sum()
grouped2 = pd.merge(grouped, grouped1, how='left', on=['city_id'])
grouped2['rate'] =grouped2['imei_x']/grouped2['imei_y']


# In[9]:


grouped2.sort_values(['city_id','rate'],ascending=[1,0],inplace=True)
grouped2['sort_id'] = grouped2.groupby(['city_id'])['imei_x'].rank(ascending=False,method='dense')


# In[10]:



grouped2.columns=['city_id','newest_id','imei_newest','imei_city','rate','sort_id']
grouped2


# In[11]:


ori2 = ori[['county_id','newest_id','imei']]
ori2.sort_values(['county_id','imei'],ascending=[1,0],inplace=True)
grouped0 = ori2.groupby(['county_id']).head(30)
grouped01 = ori2.groupby(by = ['county_id'], as_index=False)['imei'].sum()
grouped02 = pd.merge(grouped0, grouped01, how='left', on=['county_id'])


grouped02['rate'] =grouped02['imei_x']/grouped02['imei_y']
grouped02.sort_values(['county_id','rate'],ascending=[1,0],inplace=True)
grouped02['sort_id'] = grouped02.groupby(['county_id'])['imei_x'].rank(ascending=False,method='dense')


# In[12]:


grouped02.dropna(axis = 0,inplace=True)
grouped02.columns=['city_id','newest_id','imei_newest','imei_city','rate','sort_id']
grouped02


# In[13]:


grouped = pd.concat([grouped2,grouped02])
grouped['period'] = '2021Q1'
grouped


# In[14]:


# to_dws(grouped,'dws_newest_popularity_top30_quarter')


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[15]:


ori


# In[5]:


#投资意向客户总量#

con = MysqlClient(db_host,database,user,password)
ori=con.query('''select newest_id,imei from dwb_db.dwb_customer_browse_log where visit_date>=20201001 and visit_date<20210101''')
#意向客户总量#
newest_id=con.query('''select distinct newest_id,city_id,county_id from dwb_db.dwb_newest_info''')
admit = con.query('''select distinct newest_id from dws_db.dws_newest_period_admit where period = '2020Q4' ''')
grouped2 = pd.merge(admit, newest_id, how='left', on=['newest_id'])
ori = pd.merge(grouped2, ori, how='inner', on=['newest_id'])
ori


#ori=con.query('''select city_id,county_id,newest_id,imei from dwb_db.dwb_customer_browse_log where visit_date>=20210101 and visit_date<20210401''')
tag = con.query('''
 select distinct imei,'投资型' type  from dwb_db.dwb_customer_imei_tag where is_college_stu = '否' -- and period = '2021Q1'
and marriage = '已婚' and education = '高' and have_child = '有'
''')
df_invest = pd.merge(ori,tag,how='left',on='imei')

df_invest_y = df_invest[df_invest['type']=='投资型']

df_invest_1 = df_invest_y.groupby(['city_id','newest_id'])['imei'].count().reset_index()
df_invest_1


# In[6]:


ori1 = df_invest_1[['city_id','newest_id','imei']]
ori1.sort_values(['city_id','imei'],ascending=[1,0],inplace=True)
grouped = ori1.groupby(['city_id']).head(30)


# In[7]:


grouped1 = ori1.groupby(by = ['city_id'], as_index=False)['imei'].sum()
grouped2 = pd.merge(grouped, grouped1, how='left', on=['city_id'])
grouped2['rate'] =grouped2['imei_x']/grouped2['imei_y']


# In[8]:


grouped2.sort_values(['city_id','rate'],ascending=[1,0],inplace=True)
grouped2['sort_id'] = grouped2.groupby(['city_id'])['imei_x'].rank(ascending=False,method='dense')
grouped2.columns=['city_id','newest_id','imei_newest','imei_city','rate','sort_id']
grouped2


# In[9]:


df_invest_2 = df_invest_y.groupby(['county_id','newest_id'])['imei'].count().reset_index()
df_invest_2

ori2 = df_invest_2[['county_id','newest_id','imei']]
ori2.sort_values(['county_id','imei'],ascending=[1,0],inplace=True)
grouped0 = ori2.groupby(['county_id']).head(30)
grouped01 = ori2.groupby(by = ['county_id'], as_index=False)['imei'].sum()
grouped02 = pd.merge(grouped0, grouped01, how='left', on=['county_id'])


grouped02['rate'] =grouped02['imei_x']/grouped02['imei_y']
grouped02.sort_values(['county_id','rate'],ascending=[1,0],inplace=True)
grouped02['sort_id'] = grouped02.groupby(['county_id'])['imei_x'].rank(ascending=False,method='dense')


# In[10]:


grouped02.dropna(axis = 0,inplace=True)
grouped02.columns=['city_id','newest_id','imei_newest','imei_city','rate','sort_id']
grouped02


# In[11]:


grouped = pd.concat([grouped2,grouped02])
grouped['period'] = '2020Q4'
grouped


# In[ ]:





# In[ ]:





# In[ ]:





# In[12]:


# to_dws(grouped,'dws_newest_investment_pop_top30_quarter')


# In[ ]:




