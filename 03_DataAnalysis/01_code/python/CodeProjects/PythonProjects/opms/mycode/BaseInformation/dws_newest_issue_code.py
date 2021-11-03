
# In[0]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Aug 05 16:44:47 2021
@author: admin1
dws_newest_issue_code
"""
import configparser
import os
import sys
from typing import Reversible
from pandas.core import groupby
from pandas.core.frame import DataFrame
import pymysql
import pandas as pd
import numpy as np
from collections import Counter
import re
from sqlalchemy import create_engine
import datetime
from dateutil.relativedelta import relativedelta


cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
# database = 'dwb_db'  # 获取dbname对应的值

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

# In[1]:


deal_ls3=con.query("select floor_name,gd_city,newest_name from temp_db.tmp_city_newest_deal_ls3")
deal_ls3.drop_duplicates(inplace=True)

newest_id = con.query("select a0.newest_id,a0.newest_name,a0.county_id,a0.address,a1.city_name from dws_db.dws_newest_info a0 left join dws_db.dim_geography a1 on a1.city_id=a0.city_id and a1.grade= 3")


admit = con.query("select city_id,period,newest_id,dr from dws_db.dws_newest_period_admit")

merg1 = pd.merge(admit, newest_id, how='left', on=['newest_id'])
merg2 = pd.merge(merg1, deal_ls3, how='left', on=['newest_name'])


# In[2]:

newest_deal=con.query("select gd_city, floor_name, max(business), issue_code, \
                       max(issue_date_clean) issue_date, open_date, issue_area, \
                       sale_state, building_code, room_sum, area, simulation_price, \
                       sale_telephone, sale_address, 'tmp_city_newest_deal' from_code \
                       from temp_db.tmp_city_newest_deal where issue_date_clean != '' \
                      group by gd_city, floor_name, issue_code, open_date, issue_area, \
                    sale_state, building_code, room_sum, area, simulation_price, \
                        sale_telephone, sale_address")
newest_deal.rename(columns={'gd_city':'city_name'},inplace=True)
# In[3]:

result_tmp = merg2[['city_id','period','newest_id','dr','newest_name','county_id','address','city_name','floor_name']]
result = pd.merge(result_tmp, newest_deal, how='left', on=['floor_name','city_name'])
result['update_time'] = '20210705'
result_end = result[['city_id','period','newest_id','dr','city_name','floor_name','newest_name','address','max(business)','issue_code','issue_date','open_date','issue_area','sale_state','building_code','room_sum','area','simulation_price','sale_telephone','sale_address','update_time','from_code','county_id']]
result_end.columns=['city_id', 'period', 'newest_id', 'dr', 'gd_city', 'floor_name', 'newest_name', 'address', 'business', 'issue_code', 'issue_date', 'open_date', 'issue_area', 'sale_state', 'building_code', 'room_sum', 'area', 'simulation_price', 'sale_telephone', 'sale_address', 'update_time', 'from_code', 'county_id']


# In[4]:
# result.to_excel('C:\\Users\\86133\\Desktop\\result.xlsx')
result_end.drop_duplicates(inplace=True)
to_dws(result_end,'dws_newest_issue_code')

# In[11]:
print('>> Done!') #完毕
