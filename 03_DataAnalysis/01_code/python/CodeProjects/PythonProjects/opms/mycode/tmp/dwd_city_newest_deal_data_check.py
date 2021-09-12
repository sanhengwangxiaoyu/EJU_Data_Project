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


cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
# database = cf.get("Mysql", "database")  # 获取dbname对应的值
database = 'temp_db'
city_name = '保定'


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


con = MysqlClient(db_host,database,user,password)

# In[0]:
df1=con.query("select * from temp_db.city_newest_deal_data_check where city_name = '"+city_name+"'")


# In[1]:
df_room_tmp = df1[df1['gd_city'] == city_name+'市']
df_room = df_room_tmp[df_room_tmp['issue_code'] == '']

df2 = df1[df1['gd_city'] != city_name+'市']
df_issu = df2[df2['issue_code'] != '']
# df_issu_1 = df_issu[df_issu['floor_name'] != '']
# df_issu_2 = df_issu[df_issu['floor_name'] == '']

# df_comment = df2[df2['issue_code'] == '']

# In[11]:
# split = df_comment[['url','floor_name']]
# split = split.replace ( '备注：' , '' , regex = True )
# split = split.replace ( '备注:' , '' , regex = True )
# split = split.replace ( '备注；' , '' , regex = True )
# split = split.replace ( '备注;' , '' , regex = True )
# split = split.replace ( '见备注：' , '' , regex = True )
# split = split.replace ( '：' , '' , regex = True )
# split_r = df_issu_2[['url','city_name','gd_city','floor_name_new','clean_floor_name','floor_name_clean','address','business','issue_code','issue_date','issue_date_clean','open_date','issue_area','sale_state','building_code','room_sum','area','simulation_price','sale_telephone','sale_address','room_code','room_sale_area','room_sale_state','create_time']]
# df_issu_3 = pd.merge(split_r,split,how='left',on='url')
# df_issu_4 = df_issu_3[['url','city_name','gd_city','floor_name','floor_name_new','clean_floor_name','floor_name_clean','address','business','issue_code','issue_date','issue_date_clean','open_date','issue_area','sale_state','building_code','room_sum','area','simulation_price','sale_telephone','sale_address','room_code','room_sale_area','room_sale_state','create_time']]
# df_issu_s = pd.concat([df_issu_1,df_issu_4])
# In[12]:
# conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
# # 建立链接游标
# cur=connect_mysql(conn)
# update_sql = "delete from temp_db.city_newest_deal_data_check where city_name = '沈阳' and isnull(gd_city)"
# cur.execute(update_sql)
# conn.commit() # 提交记
# conn.close() # 关闭数据库链接
# print('>> Done!') #完毕
# to_dws(df_issu_s,'city_newest_deal_data_check')



# In[12]:
df_room_2 = df_room[[]]




# In[2]:
result = df_room
df_floor_name = result[['floor_name']]
df_floor_name.drop_duplicates(inplace=True)

df_address = result[['address']]
df_address.drop_duplicates(inplace=True)

df_business = result[['business']]
df_business.drop_duplicates(inplace=True)


df_issue_code = result[['issue_code']]
df_issue_code.drop_duplicates(inplace=True)


df_issue_date = result[['issue_date']]
df_issue_date.drop_duplicates(inplace=True)


df_open_date = result[['open_date']]
df_open_date.drop_duplicates(inplace=True)


df_issue_area = result[['issue_area']]
df_issue_area.drop_duplicates(inplace=True)


df_sale_state = result[['sale_state']]
df_sale_state.drop_duplicates(inplace=True)




# In[3]:
result_tmp = result[result['issue_date_clean'] > '2020']



df_building_code = result_tmp[['building_code']]
df_building_code.drop_duplicates(inplace=True)



df_room_sum = result_tmp[['room_sum']]
df_room_sum.drop_duplicates(inplace=True)


df_area = result_tmp[['area']]
df_area.drop_duplicates(inplace=True)


df_simulation_price = result_tmp[['simulation_price']]
df_simulation_price.drop_duplicates(inplace=True)


df_sale_telephone = result_tmp[['sale_telephone']]
df_sale_telephone.drop_duplicates(inplace=True)


df_sale_address = result_tmp[['sale_address']]
df_sale_address.drop_duplicates(inplace=True)


df_room_code = result_tmp[['room_code']]
df_room_code.drop_duplicates(inplace=True)


df_room_sale_area = result_tmp[['room_sale_area']]
df_room_sale_area.drop_duplicates(inplace=True)


df_room_sale_state = result_tmp[['room_sale_state']]
df_room_sale_state.drop_duplicates(inplace=True)




# In[9]:
result_end = df_floor_name
# to_csv
result_end.to_excel('C:\\Users\\86133\\Desktop\\other_n_h_3.xlsx')
