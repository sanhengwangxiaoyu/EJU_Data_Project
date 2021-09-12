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
from pandas.io.pytables import AppendableMultiFrameTable
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
date_quarter = '2021Q2'
database = 'temp_db'
##重置时间格式
start_date = str(pd.to_datetime(date_quarter))[0:10].replace('-','')  #截取成yyyy-MM-dd
end_date =  str(pd.to_datetime(date_quarter) + pd.offsets.QuarterEnd(0))[0:10].replace('-','')      #截取成yyyy-MM-dd

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

info_base = con.query("select id dim_city_id ,city_id,city_name from odsdb.dim_city ")


# In[2]:

dim = con.query("select dim_city_id,id housing_id,uuid newest_id,newest_name from odsdb.dim_housing dh where uuid is not null group by uuid,newest_name,id ")
merge_uuid = pd.merge(info_base,dim,how='inner',on=['dim_city_id'])


# In[3]:
cust = con.query("select customer imei,housing_id from odsdb.79_cust_browse_log_201801_202106 where idate between '"+start_date+"' and '"+end_date+"' and housing_id is not null group by customer,housing_id")


# In[4]:
cust[['housing_id']] = cust[['housing_id']].astype('int')
df = pd.merge(merge_uuid,cust,how='left',on=['housing_id'])
df_cust = df.groupby(['city_id','city_name','newest_id','newest_name'])['imei'].count().reset_index()
df_cust['period'] = date_quarter
df_cust['dr'] = '0'
df_cust = df_cust[['city_id','city_name','newest_id','newest_name','period','dr','imei']]
to_dws(df_cust,'tmp_city_newest_customer_imei')
print('>>>>Done to_dws df_cust')


# In[5]:
cust_n_uuid = con.query("select customer imei ,clean_floor_name newest_name ,gd_city city_name from odsdb.79_cust_browse_log_201801_202106 where gd_city is not null and idate between '"+start_date+"' and '"+end_date+"' and housing_id is null group by customer,clean_floor_name ,gd_city")


# In[5]:
city = info_base[['city_id','city_name']]
city.drop_duplicates(inplace=True)
df01 = pd.merge(cust_n_uuid,city,how='left',on=['city_name'])
df01 = df01.groupby(['city_id','city_name','newest_name'])['imei'].count().reset_index()
df01['dr'] = 0
df01['period'] = date_quarter
df01 = df01[['city_id','city_name','newest_name','period','dr','imei']]
to_dws(df01,'tmp_city_newest_customer_imei_noid')
print('>>>>Done to_dws df01')





# In[10000]:
df_cust.to_excel('C:\\Users\\86133\\Desktop\\df3.xlsx')


