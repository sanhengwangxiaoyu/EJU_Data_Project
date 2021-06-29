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
import getopt

cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
date_quarter = '2020Q4'
start_date = '20201001'  #  获取取数的开始年月日
stop_date = '20210101'    #  获取取数结束的年月日
table_name = 'dws_newest_investment_pop_top30_quarter'


opts,args=getopt.getopt(sys.argv[1:],"t:",["table="])
for opts,arg in opts:
  if opts=="-t" or opts=="--table":
    table_name = arg

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
    engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database)
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)


#投资意向客户总量#

con = MysqlClient(db_host,database,user,password)
# dwb_customer_browse_log  客户浏览楼盘日志表（每日增量） 
#                                                      newest_id       楼盘id
#                                                      imei            客户号 
#                                                      visit_date      浏览日期
ori=con.query('''select newest_id,imei from dwb_db.dwb_customer_browse_log where visit_date>='''+start_date+''' and visit_date<'''+stop_date)
#意向客户总量#
# dws_newest_info   新房楼盘
#                                                      newest_id       楼盘id
#                                                      city_id         城市id
#                                                      county_id       区县id
newest_id=con.query('''select distinct newest_id,city_id,county_id from dws_db.dws_newest_info''')
# dws_newest_period_admit  楼盘周期表
#                                                      newest_id       楼盘id
admit = con.query('''select distinct newest_id from dws_db.dws_newest_period_admit where period = "'''+date_quarter+'''"  and dr = 0''')


# In[1]:

# 获取指定季度的楼盘id,和城市id，区县id
grouped2 = pd.merge(admit, newest_id, how='left', on=['newest_id'])
# 通过楼盘id，获取浏览的客户数量
ori = pd.merge(grouped2, ori, how='inner', on=['newest_id'])


# In[2]:

#ori=con.query('''select city_id,county_id,newest_id,imei from dwb_db.dwb_customer_browse_log where visit_date>=20210101 and visit_date<20210401''')
# tag = con.query('''
#  select distinct imei,'投资型' type  from dwb_db.dwb_customer_imei_tag where is_college_stu = '否' -- and period = '2021Q1'
# and marriage = '已婚' and education = '高' and have_child = '有'
# ''')
# dwb_customer_imei_tag   客户单体标签
#                                                      imei       客户号
#                                                      type       投资类型
#                                                      is_college_stu    在校大学生
#                                                      period      季度
#                                                      marriage    婚姻状态
#                                                      education   教育水平
#                                                      have_child  有孩子
tag = con.query('''
 select distinct imei,'投资型' type  from dwb_db.dwb_customer_imei_tag where is_college_stu = '否' -- and period = "'''+date_quarter+'''"
and marriage = '已婚' and education = '高' and have_child = '有'
''')

# In[3]:

# 投资型客户与楼盘合并
df_invest = pd.merge(ori,tag,how='left',on='imei')
# 筛选出投资型客户
df_invest_y = df_invest[df_invest['type']=='投资型']

# In[3333]:
# 楼盘投资型客户数量
df_invest_1 = df_invest_y.groupby(['city_id','newest_id'])['imei'].count().reset_index()


# In[4]:

# 摘取字段
ori1 = df_invest_1[['city_id','newest_id','imei']]
# 城市的楼盘客户数量排序
ori1.sort_values(['city_id','imei'],ascending=[1,0],inplace=True)
# 去每个城市的前30用户数量的楼盘
# 修改：2021-06-09 Damon 取消只取前30.
grouped = ori1.groupby(['city_id']).head(3000000)


# In[5]:

# 城市投资客户总浏览量
grouped1 = ori1.groupby(by = ['city_id'], as_index=False)['imei'].sum()
# 合并到最终表中
grouped2 = pd.merge(grouped, grouped1, how='left', on=['city_id'])
# 城市楼盘投资用户占比    : 楼盘的投资类型的客户总量/城市的投资类型的客户总量
grouped2['rate'] =grouped2['imei_x']/grouped2['imei_y']

# 占比排序
grouped2.sort_values(['city_id','rate'],ascending=[1,0],inplace=True)
# 新增字段sort_id  占比排名
grouped2['sort_id'] = grouped2.groupby(['city_id'])['imei_x'].rank(ascending=False,method='dense')
# 修改列名
grouped2.columns=['city_id','newest_id','imei_newest','imei_city','rate','sort_id']

# In[7]:
df_0 = df_invest_y.groupby(['city_id','county_id','newest_id'])['imei'].count().reset_index()

df_1 = df_0[df_0['city_id'] != df_0['county_id']]

# In[7]:
##区域的投资型客户占比
#  获取每个区域的楼盘浏览客户数量
df_invest_2 = df_1.groupby(['county_id','newest_id'])['imei'].count().reset_index()
#  摘取指定的列
ori2 = df_invest_2[['county_id','newest_id','imei']]
#  对区域的客户数进行排序，拿取每个楼盘前30的客户数
ori2.sort_values(['county_id','imei'],ascending=[1,0],inplace=True)
grouped0 = ori2.groupby(['county_id']).head(3000000)


# In[8]:

#  计算每个区域的投资型客户浏览总数
grouped01 = ori2.groupby(by = ['county_id'], as_index=False)['imei'].sum()
#  将区域楼盘客户数和区域客户数合并到一个数据集
grouped02 = pd.merge(grouped0, grouped01, how='left', on=['county_id'])
#  新增字段tate，计楼盘在区域的占比
grouped02['rate'] =grouped02['imei_x']/grouped02['imei_y']
#  排序
grouped02.sort_values(['county_id','rate'],ascending=[1,0],inplace=True)
# 新字段sort_id  排名
grouped02['sort_id'] = grouped02.groupby(['county_id'])['imei_x'].rank(ascending=False,method='dense')


# In[10]:

# 去空值
grouped02.dropna(axis = 0,inplace=True)
# 修改列名
grouped02.columns=['city_id','newest_id','imei_newest','imei_city','rate','sort_id']


# In[11]:

# 合并城市和区域
grouped = pd.concat([grouped2,grouped02])
# 新字段period季度
grouped['period'] = date_quarter


# In[12]:
# 加载到新表dws_newest_investment_pop_top30_quarter
grouped.drop_duplicates(inplace=True)
to_dws(grouped,table_name)
# grouped.to_csv('C:\\Users\\86133\\Desktop\\dws_newest_investment_pop_top30_quarter.csv')
grouped

