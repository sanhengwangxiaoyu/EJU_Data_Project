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
from pandas.core import groupby
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
# database = cf.get("Mysql", "database")  # 获取dbname对应的值
database = 'temp_db'
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


# In[0]:

# 楼盘动态表 # dws_newest_provide_sche
# 第一步需要先从楼盘表 ori_newest_info_base 将provide_sche字段拆分到临时表 tmp_newest_provide_sche 中
# 从临时表中将数据的标题，动态，时间拆分出来


#意向客户总量#
con = MysqlClient(db_host,database,user,password)
# ori_newest_info_base  新房-贝壳未去重结果集 
#                                                      uuid             楼盘id
#                                                      platform         平台  
#                                                      provide_sche     加推时间/楼盘时刻/楼盘动态d 
ori=con.query('''SELECT uuid,city_id,platform,provide_sche FROM odsdb.ori_newest_info_base''')


# In[1]:

# 一行变多行
ori_only_row =  ori.drop('provide_sche',axis=1).join(ori['provide_sche'].str.split('\^',expand=True).stack().reset_index(level=1,drop=True).rename('provide_sche')).reset_index(drop=True)
# ori_only_row = ori.loc[~ori['provide_sche'].str.contains("\^")]
# 去掉空，重复
ori_only_row = ori_only_row.loc[ori_only_row['provide_sche'].str.contains("[\u4e00-\u9fa5]")]
ori_only_row.drop_duplicates()
# 一行当中不包含[|
ori_only_row_1 = ori_only_row.loc[~ori_only_row['provide_sche'].str.contains("\[\|")]
# 一行当中包含[|
ori_only_row_2 = ori_only_row.loc[ori_only_row['provide_sche'].str.contains("\[\|")]
# 1列
ori_only_row_1['provide_sche_num'] = ori_only_row_1['provide_sche'].map(lambda x:len(x.split('|')))
ori_only_row_1['provide_sche1'] = ori_only_row_1['provide_sche'].map(lambda x:x.split('|'))
ori_only_row_1_1c = ori_only_row_1[ori_only_row_1['provide_sche_num'] == 1]
# 2列
# ori_only_row_1_2c = ori_only_row_1[ori_only_row_1['provide_sche_num'] == 2]
# 3列 # 一行当中正常的
ori_only_row_1_3c = ori_only_row_1[ori_only_row_1['provide_sche_num'] == 3]
# 4列
ori_only_row_1_4c = ori_only_row_1[ori_only_row_1['provide_sche_num'] == 4]
# 5列
ori_only_row_1_5c = ori_only_row_1[ori_only_row_1['provide_sche_num'] == 5]



#进一步清洗
# In[2]:
# 一行中不包含[\的拆分结果
#切分结果1个
sche_result_only_row_1_1c = ori_only_row_1_1c[['uuid','provide_sche']]
# sche_result_only_row_1_1c['date']  = ori_only_row_1_1c['provide_sche'].map(lambda x:x. split('|')[0])
# # sche_result_only_row_1_1c['date'] = sche_result_only_row_1_1c['date'].str.replace('年', '-01-01')
# sche_result_only_row_1_1c['provide_title'] = ''
# sche_result_only_row_1_1c['provide_sche1'] = ''
# sche_result_only_row_1_1c = sche_result_only_row_1_1c[['uuid','date','provide_title','provide_sche1']]
sche_result_only_row_1_1c

# #切分结果3个
# sche_result_only_row_1_3c = ori_only_row_1_3c[['uuid','provide_sche']]
# sche_result_only_row_1_3c['provide_title'] = ori_only_row_1_3c['provide_sche'].map(lambda x:x. split('|')[0])
# sche_result_only_row_1_3c['provide_sche1'] = ori_only_row_1_3c['provide_sche'].map(lambda x:x. split('|')[1])
# sche_result_only_row_1_3c['date'] = ori_only_row_1_3c['provide_sche'].map(lambda x:x. split('|')[2])
# sche_result_only_row_1_3c_2 = sche_result_only_row_1_3c[['uuid','date','provide_title','provide_sche1']]
# sche_result_only_row_1_3c_2


# #切分结果4个
# sche_result_only_row_1_4c = ori_only_row_1_4c[['uuid','provide_sche']]
# sche_result_only_row_1_4c['tmp1'] = ori_only_row_1_4c['provide_sche'].map(lambda x:x. split('|')[0])
# sche_result_only_row_1_4c['tmp2'] = ori_only_row_1_4c['provide_sche'].map(lambda x:x. split('|')[1])
# sche_result_only_row_1_4c['tmp3'] = ori_only_row_1_4c['provide_sche'].map(lambda x:x. split('|')[2])
# sche_result_only_row_1_4c['tmp4'] = ori_only_row_1_4c['provide_sche'].map(lambda x:x. split('|')[3])
# #动态标题处理
# # tmp2为时间的数据和替他数据分开
# sche_result_only_row_1_4c_2 = sche_result_only_row_1_4c.loc[sche_result_only_row_1_4c['tmp2'].str.contains("[\u4e00-\u9fa5]")]
# sche_result_only_row_1_4c_3 = sche_result_only_row_1_4c.loc[~sche_result_only_row_1_4c['tmp2'].str.contains("[\u4e00-\u9fa5]")]
# # 标题拼接
# sche_result_only_row_1_4c_2['provide_title'] = sche_result_only_row_1_4c_2['tmp1']+sche_result_only_row_1_4c_2['tmp2']
# sche_result_only_row_1_4c_2['provide_sche1'] = sche_result_only_row_1_4c_2['tmp3']
# sche_result_only_row_1_4c_2['date'] = sche_result_only_row_1_4c_2['tmp4']
# sche_result_only_row_1_4c_3['provide_title'] = sche_result_only_row_1_4c_3['tmp1']+sche_result_only_row_1_4c_3['tmp4']
# sche_result_only_row_1_4c_3['provide_sche1'] = sche_result_only_row_1_4c_3['tmp3']
# sche_result_only_row_1_4c_3['date'] = sche_result_only_row_1_4c_3['tmp2']
# #时间处理
# sche_result_only_row_1_4c_2['date'] = sche_result_only_row_1_4c_2['date'].str.replace('年', '-')
# sche_result_only_row_1_4c_2['date'] = sche_result_only_row_1_4c_2['date'].str.replace('月', '-')
# sche_result_only_row_1_4c_2['date'] = sche_result_only_row_1_4c_2['date'].str.replace('日', '')
# sche_result_only_row_1_4c_2['date'] = sche_result_only_row_1_4c_2['date'].str.replace('\/', '-')
# sche_result_only_row_1_4c_3['date1'] = sche_result_only_row_1_4c_3['date'].map(lambda x:x. split('/')[0])
# sche_result_only_row_1_4c_3['date_1'] = sche_result_only_row_1_4c_3['date'].map(lambda x:x. split('/')[1])
# sche_result_only_row_1_4c_3['date2'] = sche_result_only_row_1_4c_3['date_1'].apply(lambda x:x[:2]).tolist()
# sche_result_only_row_1_4c_3['date3'] = sche_result_only_row_1_4c_3['date_1'].apply(lambda x:x[-4:]).tolist()
# sche_result_only_row_1_4c_3['date'] = sche_result_only_row_1_4c_3['date3']+'-'+sche_result_only_row_1_4c_3['date1']+'-'+sche_result_only_row_1_4c_3['date2']
# # 规范字段，合并数据
# sche_result_only_row_1_4c_4 = sche_result_only_row_1_4c_3[['uuid','date','provide_title','provide_sche1']]
# sche_result_only_row_1_4c_5 = sche_result_only_row_1_4c_2[['uuid','date','provide_title','provide_sche1']]
# sche_result_only_row_1_4c_6 =pd.concat([sche_result_only_row_1_4c_5,sche_result_only_row_1_4c_4])
# sche_result_only_row_1_4c_6




# # 切分结果5个
# sche_result_only_row_1_5c = ori_only_row_1_5c[['uuid','provide_sche']]
# sche_result_only_row_1_5c['provide_title'] = ori_only_row_1_5c['provide_sche'].map(lambda x:x. split('|')[0])+ori_only_row_1_5c['provide_sche'].map(lambda x:x. split('|')[1])+ori_only_row_1_5c['provide_sche'].map(lambda x:x. split('|')[2])
# sche_result_only_row_1_5c['provide_sche1'] = ori_only_row_1_5c['provide_sche'].map(lambda x:x. split('|')[3])
# sche_result_only_row_1_5c['date'] = ori_only_row_1_5c['provide_sche'].map(lambda x:x. split('|')[4])
# sche_result_only_row_1_5c = sche_result_only_row_1_5c[['uuid','date','provide_title','provide_sche1']]
# #时间处理
# # 一行当中时间是yyyy年mm月dd日的
# sche_result_only_row_1_5c['date'] = sche_result_only_row_1_5c['date'].str.replace('年', '-')
# sche_result_only_row_1_5c['date'] = sche_result_only_row_1_5c['date'].str.replace('月', '-')
# sche_result_only_row_1_5c['date'] = sche_result_only_row_1_5c['date'].str.replace('日', '')
# sche_result_only_row_1_5c['date'] = sche_result_only_row_1_5c['date'].str.replace('\/', '-')
# # 取结果，合并
# sche_result_only_row_1_5c = sche_result_only_row_1_5c[['uuid','date','provide_title','provide_sche1']]
# sche_result_only_row_1_5c


# sche_result_only_row_2 = =pd.concat([sche_result_only_row_1_5c,sche_result_only_row_1_4c_6,sche_result_only_row_1_3c_2,])

# # In[3]:

# # 一行中包含[\的拆分结果
# sche_result_only_row_2 = ori_only_row_2[['uuid','provide_sche']]
# sche_result_only_row_2['provide_sche1'] =  sche_result_only_row_2['provide_sche'].map(lambda x:x. split('...')[0])
# sche_result_only_row_2['provide_sche2'] =  sche_result_only_row_2['provide_sche1'].map(lambda x:x. split('[|')[0])
# sche_result_only_row_2['provide_sche3'] =  sche_result_only_row_2['provide_sche2'].map(lambda x:x. split('（房天下讯')[0])
# sche_result_only_row_2['provide_title'] = sche_result_only_row_2['provide_sche3'].map(lambda x:x. split('|')[0])
# sche_result_only_row_2['provide_sche4'] = sche_result_only_row_2['provide_sche3'].map(lambda x:x. split('|')[1])


# sche_result_only_row_2 = sche_result_only_row_2[['uuid','provide_title','provide_sche4']]
# sche_result_only_row_2




# # In[4]:



# # 去重











# ori1 = ori.loc[ori['provide_sche'].str.contains("\^")]
sche_result_only_row_1_1c.to_csv('C:\\Users\\86133\\Desktop\\sche_result_only_row_1_1c(v2).csv')






