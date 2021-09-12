
# %%



#%% 


import requests
import json

import sys
import pandas as pd
import numpy as np
import os
from pandas.core.frame import DataFrame
import pymysql
import configparser
from sqlalchemy import create_engine
import re

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
con = MysqlClient(db_host,database,user,password)

def to_dws(result,table):
    engine = create_engine("mysql+pymysql://yangzhen:6V5_0rviExpxBzHj@172.28.36.77:3306/dws_db_prd?charset=utf8")
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)


newest = con.query("SELECT DISTINCT newest_name,city_name FROM dws_db_prd.dws_function_newest WHERE county_id IS NULL ")

city_ls = list(newest.city_name.drop_duplicates())

city_detail = pd.DataFrame()
lost_city_detail = pd.DataFrame()

for a in city_ls:
    # print(a)
    newest = con.query("SELECT DISTINCT newest_name,city_name FROM dws_db_prd.dws_function_newest WHERE county_id IS NULL and city_name ='"+a+"' ")
    newest_ls = list(newest.newest_name)
    for i in newest_ls:
        try:
            # print(i)
            url = 'https://restapi.amap.com/v3/geocode/geo'   # 输入API问号前固定不变的部分
            params = { 'key': 'd9aa6e41d00153b3b466e408aea92b39',
                    'address': i,
                    'city': a }                # 将两个参数放入字典
            res = requests.get(url, params)
            res.text
            jd = json.loads(res.text)      # 将json数据转化为Python字典格式
            # print(jd)
            params = params['address']
            city = jd['geocodes'][0]['city']
            district = jd['geocodes'][0]['district']
            coords = jd['geocodes'][0]['location']
            formatted_address = jd['geocodes'][0]['formatted_address']

            # address1 = params['address'] +'-' + jd['geocodes'][0]['city']+'-' + jd['geocodes'][0]['district']+'-'+jd['geocodes'][0]['location']+'-'+ jd['geocodes'][0]['formatted_address']

            city_detail1 = pd.DataFrame([params])
            city_detail1['city'] = pd.DataFrame([city])
            city_detail1['county'] = pd.DataFrame([district])
            city_detail1['lat_lng'] = pd.DataFrame([coords])
            city_detail1['address'] = pd.DataFrame([formatted_address])
            city_detail = city_detail.append(city_detail1)
            # print(address)
        except: 
            print(i+'-'+a)
            lost_city_detail1 = pd.DataFrame([i+'-'+a])
            lost_city_detail = lost_city_detail.append(lost_city_detail1)

city_detail.to_csv('city_detail.csv',mode='a',header=True,index=False)
lost_city_detail.to_csv('lost_city_detail.csv',mode='a',header=True,index=False)


#%%




#%% 




import requests
import json


url = 'https://restapi.amap.com/v3/geocode/geo'   # 输入API问号前固定不变的部分
params = { 'key': '61d7e8cf769016c6904b3cea7b719e3d',
           'address': '南通璟园 宏伟路、红海路交汇处',
           'city': '南通市' }                # 将两个参数放入字典
res = requests.get(url, params)
res.text
jd = json.loads(res.text)      # 将json数据转化为Python字典格式
jd

params['address']
city = jd['geocodes'][0]['city']
district = jd['geocodes'][0]['district']
coords = jd['geocodes'][0]['location']
address = params['address'] +'-' + jd['geocodes'][0]['city']+'-' + jd['geocodes'][0]['district']+'-'+jd['geocodes'][0]['location']

# %%
import requests
import json

url = 'https://restapi.amap.com/v3/geocode/regeo?parameters'   # 输入API问号前固定不变的部分
params = { 'key': '61d7e8cf769016c6904b3cea7b719e3d',
           'location': '121.172063,31.898745'   }                # 将两个参数放入字典
res = requests.get(url, params)
res.text
jd = json.loads(res.text)      # 将json数据转化为Python字典格式
city = jd['regeocode']['addressComponent']['city']
location = jd['regeocode']['addressComponent']['streetNumber']['location']
city



# %%

import requests
import json

url = 'https://restapi.amap.com/v3/geocode/geo'   # 输入API问号前固定不变的部分
params = { 'key': '61d7e8cf769016c6904b3cea7b719e3d',
           'address': '永定河孔雀城悦未来' }                # 将两个参数放入字典
res = requests.get(url, params)
res.text
jd = json.loads(res.text)      # 将json数据转化为Python字典格式
coords = jd['geocodes'][0]['location']
coords

