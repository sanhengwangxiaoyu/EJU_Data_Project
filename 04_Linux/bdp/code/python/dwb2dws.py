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
import datetime
from dateutil.relativedelta import relativedelta
import getopt

pymysql.install_as_MySQLdb()

cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
input_table_name = ''
output_table_name = ''
# input_table_name = 'temp_db.bak_20210618_dws_newest_provide_sche'
# output_table_name = 'bak_20210618_dws_newest_provide_sche_111111111'
# database = 'temp_db'

opts,args=getopt.getopt(sys.argv[1:],"i:o:s:e:d:",["input=","output=","startdate=","enddate=","database="])
for opts,arg in opts:
  if opts=="-i" or opts=="--input":
    input_table_name = arg
  elif opts=="-o" or opts=="--output":
    output_table_name = arg
  elif opts=="-d" or opts=="--database":
    database = arg


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



print('>> Start!') #完毕
result=con.query('''SELECT * FROM '''+input_table_name)
print(result)
to_dws(result,output_table_name)
print('>> Done!') #完毕
