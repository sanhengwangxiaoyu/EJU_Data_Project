import sys
from typing import Tuple
import pandas as pd
import numpy as np
import time
import datetime
import configparser
import os
from pandas.core import groupby
import pymysql
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
        cur.close()
        return res,columnNames
    def close(self):
        self.conn.close()

con = MysqlClient(db_host,database,user,password)

pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
engine = create_engine('mysql+mysqldb://'+'mysql'+':'+'egSQ7HhxajHZjvdX'+'@'+'172.28.36.77'+':'+'3306'+'/'+'dws_db')# 初始化引擎


res,columnNames = con.query('''
SELECT newest_id,park_num FROM dwb_db.dwb_newest_info WHERE dr=0 
''')
df = pd.DataFrame([list(i) for i in res],columns=columnNames)
df = df.astype('str')
def sum_str(s):
    new_str = ""  		#创建一个空字符串
    for ch in s:
	    if ch.isdigit():		#字符串中的方法，可以直接判断ch是否是数字
		    new_str += ch
	    else:
		    new_str += " "
    sub_list = new_str.split()   #对新的字符串切片
    num_list = list(map(int, sub_list)) 	#map方法，使列表中的元素按照指定方式转变
    res  = sum(num_list)
    # print(res)
    return res

df['a'] = df['park_num'].apply(lambda x:sum_str(x))

df[df['newest_id']=='9acbed1abf9c11eb86162cea7f6c2bde']
