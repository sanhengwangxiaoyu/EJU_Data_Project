# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Seq 08 15:44:47 2021
  预售证楼盘和现有楼盘对照

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


# In[3]:
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
con = MysqlClient(db_host,database,user,password)


# In[4]:
newest_deal=con.query("select floor_name newest_name,address,business developer from odsdb.city_newest_deal where city_name = '三亚' and issue_date_clean >= '2018-01-01' group by floor_name,address,business")

newest_name=con.query("select newest_id,newest_name,address,alias,developer from dwb_db.a_dwb_newest_info where city_id = '460200' group by newest_id,newest_name,address,alias,developer ")


# In[]:
###    直接根据楼盘名和公司关联上的
df_1 = pd.merge(newest_deal,newest_name,how='inner',on=['newest_name','developer'])


# In[]:
###    直接根据楼盘名关联上的
df_2_deal = newest_deal[~newest_deal['newest_name'].isin(df_1['newest_name'])]
df_2_name = newest_name[~newest_name['newest_id'].isin(df_1['newest_name'])]
df_2 = pd.merge(df_2_deal,df_2_name,how='inner',on=['newest_name'])


###    直接根据公司名关联上的
df_3_deal = df_2_deal[~df_2_deal['newest_name'].isin(df_2['newest_name'])]
df_3_name = df_2_name[~df_2_name['newest_id'].isin(df_2['newest_name'])]
df_3 = pd.merge(df_3_deal,df_3_name,how='inner',on=['developer'])


###    直接根据公司名关联上的
df_4_deal = df_3_deal[~df_3_deal['newest_name'].isin(df_3['newest_name_x'])]
df_4_name = df_3_name[~df_3_name['newest_id'].isin(df_3['newest_name_y'])]



# In[10]:
# 加载到新表 dwb_newest_issue_offer
# result.drop_duplicates(inplace=True)
# to_dws(result,table_name)
df_4_name.to_csv('C:\\Users\\86133\\Desktop\\df_4_name.csv')
# result
print('>>>>>>>Done')

#In[]
from difflib import SequenceMatcher#导入库
def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()#引用ratio方法，返回序列相似性的度量
print(similarity('万科湖心岛', '万科湖畔度假公园'))
print(similarity('同心家园', '万科湖畔度假公园'))
print(similarity('三亚·清平乐', '同心家园九期一期项目,清平乐-西郡,清平乐·西郡,清平乐•西郡,清平乐西郡'))
print(similarity('三亚伟奇温泉度假公寓', '三亚伟奇温泉度假公寓,三亚天骄-海棠湾,三亚天骄·海棠湾,三亚天骄•海棠湾'))
print(similarity('三亚伟奇温泉度假公寓', '三亚伟奇温泉度假公寓,三亚天骄-海棠湾,三亚天骄·海棠湾,三亚天骄•海棠湾'))

