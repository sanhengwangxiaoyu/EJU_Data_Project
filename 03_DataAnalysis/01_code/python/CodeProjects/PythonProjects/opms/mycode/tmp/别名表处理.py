# 要添加一个新单元，输入 '# %%'
# 要添加一个新的标记单元，输入 '# %% [markdown]'
# test
# %%

import sys,time
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
database = 'dws_db_prd'


# user = 'user_dw1'
# password = 'j5O_ermkAc4oqrKr'
# database = 'prd_dws_db'


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

# %
# %%
df = con.query("SELECT tt1.newest_id, tt1.newest_name, tt2.alias alias_name FROM ( SELECT newest_id,newest_name FROM dws_db_prd.dws_newest_info GROUP BY newest_id,newest_name HAVING newest_id NOT IN (SELECT newest_id FROM dws_db_prd.dws_newest_alias GROUP BY newest_id)) tt1 LEFT JOIN ( SELECT t1.uuid, t2.alias FROM ( SELECT uuid, id housing_id FROM dwb_db.dim_housing WHERE uuid IS NOT NULL GROUP BY uuid, id) t1 LEFT JOIN ( SELECT alias , housing_id FROM dwb_db.dim_housing_alias GROUP BY alias , housing_id) t2 ON t1.housing_id = t2.housing_id)tt2 ON tt1.newest_id = tt2.uuid where tt2.alias is not null ")

def similarity(a,b):
    return SequenceMatcher(None,a,b).ratio()#引用ratio方法，返回序列相似性的度量

df['xx'] = df.apply(lambda x:similarity(x.alias_name,x.newest_name),axis=1)
df1 = df[df['xx']>=0.5]

df2 = con.query("SELECT DISTINCT a.newest_id,a.city_id,b.city_name  FROM dws_db_prd.dws_newest_info a LEFT JOIN dws_db_prd.dim_geography b ON b.grade=3 AND b.city_id=a.city_id WHERE b.city_name IS NOT NULL ")

df3 = df1.merge(df2,how='left',on=['newest_id'])
df3.drop('xx',axis=1,inplace=True)

df3=df3[['alias_name','city_id','city_name','newest_id','newest_name']]
df3['dr']=0
df3['create_date'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
df3['create_user'] = np.nan
df3['update_date'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
df3['update_user'] = np.nan

to_dws(df3,'dws_newest_alias')



