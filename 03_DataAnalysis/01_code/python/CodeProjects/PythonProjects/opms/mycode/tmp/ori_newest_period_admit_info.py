# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on sep 6 17:44:47 2021

"""
import configparser
import os
import sys,io
from numpy.lib.function_base import append
import pymysql
import pandas as pd
import numpy as np
from collections import Counter
import re
from sqlalchemy import create_engine
import getopt
import time

##读取配置文件##
pymysql.install_as_MySQLdb()
cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

##设置变量初始值##
user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
database='odsdb'
##mysql连接配置##
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

##mysql写入执行##
def to_dws(result,table):
    engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database+'?charset=utf8')
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)
##room_sum清洗逻辑
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

##正式代码##
"""
1> 获取数据信息：dws_newest_info，dws_newest_period_admit
    通过admit筛选准入楼盘信息，通过dws_newest_info获取楼盘具体信息
"""
con = MysqlClient(db_host,database,user,password)


# In[4]:
issue_offer=con.query("select newest_id ,id from dws_db_prd.dws_tag_purchase_poi")


# In[]:
df = issue_offer.groupby(['newest_id'])['id'].count().reset_index()

df_1 = df[df['id'] == 1]

df_1_on = df[df['id'] != 1]


# In[4]:
newest_id = con.query("select null ods_id,'dws_db.dws_newest_info' ,city_id,newest_id,newest_name,address,concat(lng,',',lat) lnglat from dws_db_prd.dws_newest_info where newest_id is not null")
admit = con.query("select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id")
newest_id = pd.merge(newest_id,admit,how='inner',on=['newest_id'])


#In[]
df_in = newest_id[~newest_id.isin(df)['newest_id']]
df_in['id'] = 0
df_result_0 = df_in[['ods_id','dws_db.dws_newest_info','city_id','newest_id','newest_name','address','lnglat','id']]
df_result_0.columns=['ods_id','ods_table_name','city_id','newest_id','newest_name','address','lnglat','poi_num']
df_result_0['dr'] = 0
df_result_0['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
df_result_0['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 


#In[]
df_result_1 = pd.merge(df_1,newest_id,how='inner',on=['newest_id'])
df_result_1 = df_result_1[['ods_id','dws_db.dws_newest_info','city_id','newest_id','newest_name','address','lnglat','id']]
df_result_1.columns=['ods_id','ods_table_name','city_id','newest_id','newest_name','address','lnglat','poi_num']
df_result_1['dr'] = 0
df_result_1['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
df_result_1['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 


#In[]
df_result_1_on = pd.merge(df_1_on,newest_id,how='inner',on=['newest_id'])
df_result_1_on = df_result_1_on[['ods_id','dws_db.dws_newest_info','city_id','newest_id','newest_name','address','lnglat','id']]
df_result_1_on.columns=['ods_id','ods_table_name','city_id','newest_id','newest_name','address','lnglat','poi_num']
df_result_1_on['dr'] = 0
df_result_1_on['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
df_result_1_on['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 


# In[10]:
# 加载到新表 dwb_newest_issue_offer
# result.drop_duplicates(inplace=True)
to_dws(df_result_1_on,'ori_newest_period_admit_info')
# df_fz.to_csv('C:\\Users\\86133\\Desktop\\df_fz.csv')
# result
print('>>>>>>>Done')

