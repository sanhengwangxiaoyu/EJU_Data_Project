
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
from difflib import SequenceMatcher#导入库

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


#%%


newest = con.query("SELECT DISTINCT a.newest_name,b.city_name,a.address FROM dws_newest_info a JOIN dws_db_prd.dim_geography b ON b.grade=3 AND b.city_id=a.city_id WHERE a.newest_id IN (SELECT DISTINCT newest_id FROM temp_db.newest_city_fialed_info WHERE dr=0) ")

newest_ls = newest.newest_name.drop_duplicates()
newest_ls = list(newest_ls)
city_detail = pd.DataFrame()

for n in newest_ls:
    print(n)
    url = 'https://api.map.baidu.com/place/v2/suggestion'   # 输入API问号前固定不变的部分
    params = {  'query': n,
                'region':'苏州',
                'ak': 'v06Ob0IHtxFjW6lNN6Xu0dfXLczGCol2',
                # 'city_limit':True,
                'output':'json'
                }                # 将两个参数放入字典
    res = requests.get(url, params)
    res.text
    jd = json.loads(res.text)      # 将json数据转化为Python字典格式
    
    for i in range(len(jd['result'])):
        try:
            print(i)
            coords = jd['result'][i]
            name = coords['name']
            lat = coords['location']['lat']
            lng = coords['location']['lng']
            province = coords['province']
            city = coords['city']
            district = coords['district']
            address = coords['address']
            tag = coords['tag']

            city_detail1 = pd.DataFrame([n])
            # city_detail1['city_y'] = pd.DataFrame([name])
            city_detail1['name'] = pd.DataFrame([name])
            city_detail1['lat'] = pd.DataFrame([lat])
            city_detail1['lng'] = pd.DataFrame([lng])
            city_detail1['province'] = pd.DataFrame([province])
            city_detail1['city'] = pd.DataFrame([city])
            city_detail1['district'] = pd.DataFrame([district])
            city_detail1['address'] = pd.DataFrame([address])
            city_detail1['tag'] = pd.DataFrame([tag])
            city_detail = city_detail.append(city_detail1)
        except: print('')

city_detail_re = city_detail.merge(newest,how='left',left_on=[0],right_on=['newest_name'])
city_detail_re['address_x'] = city_detail_re['address_x'].apply(lambda x: x.split('-')[-1])


def similarity(a,b):
    return SequenceMatcher(None,a,b).ratio()#引用ratio方法，返回序列相似性的度量

city_detail_re['xx'] = city_detail_re.apply(lambda x:similarity(x.address_x,x.address_y),axis=1)
city_detail_re = city_detail_re[city_detail_re['tag']=='住宅区']
# city_detail_re1 = city_detail_re[city_detail_re['xx']>=0.5]
city_detail_re.sort_values(['newest_name','address_y','xx'],ascending=False,inplace=True)
city_detail_re1 = city_detail_re.groupby(['newest_name','address_y']).head(1).reset_index(drop=True)

city_detail_re1.drop(0,axis=1,inplace=True)


to_dws(city_detail,'city_detail_baidu')


#%%

# url = 'https://api.map.baidu.com/place/v2/suggestion'   # 输入API问号前固定不变的部分
# params = {  'query': '一米阳光',
#             'region':'苏州',
#             'ak': 'Kau5v5aIsndOOlEmVpvA7oGlxLk9lOmB',
#             # 'city_limit':True,
#             'output':'json'
#             }                # 将两个参数放入字典
# res = requests.get(url, params)
# res.text
# jd = json.loads(res.text) 
# jd  