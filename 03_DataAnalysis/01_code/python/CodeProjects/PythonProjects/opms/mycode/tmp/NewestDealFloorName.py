#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Aug 5 9:44:47 2021
@author: mw
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
import datetime
from dateutil.relativedelta import relativedelta
import time

cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
# database = cf.get("Mysql", "database")  # 获取dbname对应的值
database = 'temp_db'
city_name = '广州'


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

def connect_mysql(conn):
    #建立操作游标
    cursor=conn.cursor()
    #设置数据输入输出编码格式
    cursor.execute('set names utf8')
    return cursor

startTimeM = int(round(time.time() * 1000))

con = MysqlClient(db_host,database,user,password)

conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
cur=connect_mysql(conn)

cur.execute("update temp_db.tmp_city_newest_deal set floor_name_clean = floor_name  where city_name = '"+city_name+"'")
conn.commit()

# In[0]:


deal=con.query("select gd_city,clean_floor_name,floor_name from temp_db.tmp_city_newest_deal where city_name = '"+city_name+"'")

main=con.query("select gd_city, alias, newest_name from odsdb.ori_newest_info_main where remark is null and city_name = '"+city_name+"'")


# In[1]:
#去重#
deal.drop_duplicates(inplace=True)
main.drop_duplicates(inplace=True)


# In[2]:
#广州#
deal.to_excel("C:\\Users\\86133\\Desktop\\deal.xlsx")




# In[100]:
conn.close() # 关闭数据库链接
print('>> Done!') #完毕
