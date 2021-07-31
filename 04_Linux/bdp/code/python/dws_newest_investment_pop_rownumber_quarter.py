#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Jul 12 17:44:47 2021

项目榜单 - 比例改为指数数值,区间为0-5、固定展示2位小数，不能为空，不足2位补0
     (目标楼盘人气数-城市min)/(城市max-城市min)*5
     
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

pymysql.install_as_MySQLdb()
##读取配置文件##
cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

##设置变量初始值##
user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
date_quarter = '2021Q1'   # 季度
start_date = '20210101'  # 季度开始时间
stop_date = '20210401'    # 季度结束时间
table_name = 'dws_newest_investment_pop_rownumber_quarter' # 要插入的表名称

##通过输入的参数的方式获取变量值##  如果不需要使用输入参数的方式，可以不用这段
opts,args=getopt.getopt(sys.argv[1:],"t:q:s:e:d:",["database","table=","quarter=","startdate=","enddate="])
for opts,arg in opts:
  if opts=="-t" or opts=="--table": # 获取输入参数 -t或者--table 后的值
    table_name = arg
  elif opts=="-q" or opts=="--quarter":  # 获取输入参数 -1或者--quarter 后的值
    date_quarter = arg
  elif opts=="-s" or opts=="--startdate":  # 同上
    start_date = arg
  elif opts=="-e" or opts=="--enddate":   # 同上
    stop_date = arg
  elif opts=="-d" or opts=="database":
    database = arg

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
    engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database)
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)



##正式代码##
"""
1> 获取数据信息：dwb_customer_browse_log，dws_newest_info，dws_newest_period_admit,dwb_customer_imei_tag
    通过admit筛选准入楼盘信息，通过dws_newest_info获取楼盘具体信息，通过browse获取楼盘人气,通过dwb_customer_imei_tag获取投资性的客户
2> 对browse去重
   获取指定季度的楼盘id,和城市id，区县id
   通过楼盘id，获取浏览的客户
   投资型的客户打上标签
   计算投资型客户数量
3> 城市楼盘维度
       1.拿取指定列
       2.对每个城市中的客户数进行排序
       3.项目人气度（楼盘占比）：所有浏览过该楼盘的imei总量（去重）/城市imei总量（去重）
          每个城市的客户总数
          将城市的客户总数和城市楼盘客户数的关联在一起
          新增字段rate
       4.项目投资型客户人气度（楼盘指数占比）： (目标楼盘投资型客户人气数-城市min)/(城市max-城市min)*5
          每个城市的最大投资型客户数
          每个城市的最小投资型客户数
          将城市的最小投资型客户数和最大投资型客户数和城市楼盘客户数的关联在一起
          新增字段index_rate
       5.城市楼盘指数占比排名：
          每个城市的楼盘指数占比排序
          新加字段sort_id :对城市楼盘指数占比排名
       8.修改列名
4> 区域楼盘维度:先清除掉脏数据
       1.截取指定列
       2.对区域的客户数进行排序
       3.项目人气度（楼盘占比）： 所有浏览过该楼盘的imei总量（去重）/区域imei总量（去重）
          计算每个区域的客户总数
          通过区域id，获取到区域的客户总数
          区县楼盘占比
          新增字段rate 
       4.项目人气度（楼盘指数占比）： (目标楼盘人气数-区县min)/(区县max-区县min)*5
          每个区县的最大客户数
          每个区县的最小客户数
          将区县的最小客户数和最大客户数和区县楼盘客户数的关联在一起
          新增字段index_rate
       5.区县楼盘指数占比排名：
          每个区县的楼盘指数占比排序
          新加字段sort_id :对区县楼盘指数占比排名
       6.去除空值
       7.修改列名
5> 合并写入
       城市楼盘占比，区域楼盘占比合并在一起，union all
       新加一列，值为当前季度
       调用to_dws方法，加载数据到dws_newest_popularity_top30_quarter表中
"""

con = MysqlClient(db_host,database,user,password)   # 创建mysql链接
# In[1]:
#投资意向客户总量#
# dwb_customer_browse_log  客户浏览楼盘日志表（每日增量） 
#                                                      newest_id       楼盘id
#                                                      imei     客户数  
#                                                      visit_date      浏览日期
o=con.query('''select newest_id,imei from dwb_db.dwb_customer_browse_log where visit_date>='''+start_date+''' and visit_date<'''+stop_date)
#去重
o.drop_duplicates(inplace=True)


# In[2]:
#意向客户总量#
# dws_newest_info   新房楼盘
#                                                      newest_id       楼盘id
#                                                      city_id         城市id
#                                                      county_id       区县id
newest_id=con.query('''select distinct newest_id,city_id,county_id from dws_db.dws_newest_info where dr=0''')
# dws_newest_period_admit  楼盘周期表
#                                                      newest_id       楼盘id
admit = con.query('''select distinct newest_id from dws_db.dws_newest_period_admit where period = "'''+date_quarter+'''"  and dr = 0''')


# In[3]:
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


# In[4]:
# 获取指定季度的楼盘id,和城市id，区县id
grouped2 = pd.merge(admit, newest_id, how='left', on=['newest_id'])
# 通过楼盘id，获取浏览的客户数量
ori = pd.merge(grouped2, o, how='inner', on=['newest_id'])


# In[5]:
# 投资型客户与楼盘合并
df_invest = pd.merge(ori,tag,how='left',on='imei')
# 筛选出投资型客户
df_invest_y = df_invest[df_invest['type']=='投资型']
# 楼盘投资型客户数量
df_invest_1 = df_invest_y.groupby(['city_id','newest_id'])['imei'].count().reset_index()
 

# In[6]:
# 摘取字段
ori1 = df_invest_1[['city_id','newest_id','imei']]
# 城市的楼盘客户数量排序
ori1.sort_values(['city_id','imei'],ascending=[1,0],inplace=True)


# In[7]:
# 城市投资客户总浏览量
grouped1 = ori1.groupby(by = ['city_id'], as_index=False)['imei'].sum()
# 合并到最终表中
grouped2 = pd.merge(ori1, grouped1, how='left', on=['city_id'])
# 城市楼盘投资用户占比    : 楼盘的投资类型的客户总量/城市的投资类型的客户总量
grouped2['rate'] =grouped2['imei_x']/grouped2['imei_y']
grouped2.columns = ['city_id','newest_id','imei_newest','imei_city','rate']


# In[8]:
#项目投资型客户人气度（楼盘指数占比）： (目标楼盘投资型客户人气数-城市min)/(城市max-城市min)*5
# 每个城市的最大投资型客户数
ic_max = ori1.groupby(by = ['city_id'], as_index=False)['imei'].max()
# 每个城市的最小投资型客户数
ic_min = ori1.groupby(by = ['city_id'], as_index=False)['imei'].min()
# 将城市的最小投资型客户数和最大投资型客户数和城市楼盘客户数的关联在一起
grouped2 = pd.merge(grouped2, ic_max, how='left', on=['city_id'])
grouped2 = pd.merge(grouped2, ic_min, how='left', on=['city_id'])
# 新增字段index_rate
grouped2['index_rate'] =round((grouped2['imei_newest']-grouped2['imei_y'])/(grouped2['imei_x']-grouped2['imei_y'])*4.5+0.5,2)


# In[9]:
#城市楼盘指数占比排名：
# 每个城市的楼盘指数占比排序
grouped2.sort_values(['city_id','index_rate'],ascending=[1,0],inplace=True)
# 新加字段sort_id :对城市楼盘客户数排名
grouped2['sort_id'] = grouped2.groupby(['city_id'])['imei_newest'].rank(ascending=False,method='dense')
# 修改列名
grouped2.dropna(axis = 0,inplace=True)
# 修改列名
grouped2.columns=['city_id','newest_id','imei_newest','imei_city','rate','imei_c_max','imei_c_min','index_rate','sort_id']


# In[10]:
#去除脏数据#
# df_0 = df_invest_y[['city_id','county_id','newest_id','imei']]
# df_1 = df_0[df_0['city_id'] != df_0['county_id']]


# In[11]:
##区域的投资型客户占比
#  获取每个区域的楼盘浏览客户数量
df_invest_2 = df_invest_y.groupby(['county_id','newest_id'])['imei'].count().reset_index()
#  摘取指定的列
ori2 = df_invest_2[['county_id','newest_id','imei']]
#  对区域的客户数进行排序
ori2.sort_values(['county_id','imei'],ascending=[1,0],inplace=True)


# In[12]:
#项目人气度（楼盘占比）： 所有浏览过该楼盘的imei总量（去重）/区域imei总量（去重）
# 计算每个区域的客户总数
grouped01 = ori2.groupby(by = ['county_id'], as_index=False)['imei'].sum()
#  通过区域id，获取到区域的客户总数
grouped02 = pd.merge(ori2, grouped01, how='left', on=['county_id'])
#  新增字段tate，计楼盘在区域的占比
grouped02['rate'] =grouped02['imei_x']/grouped02['imei_y']
#修改列名
grouped02.columns = ['county_id','newest_id','imei_newest','imei_city','rate']


# In[13]:
#项目人气度（楼盘指数占比）： (目标楼盘人气数-区县min)/(区县max-区县min)*5
# 每个区县的最大客户数
ico_max = ori2.groupby(by = ['county_id'], as_index=False)['imei'].max()
# 每个区县的最小客户数
ico_min = ori2.groupby(by = ['county_id'], as_index=False)['imei'].min()
# 将区县的最小客户数和最大客户数和区县楼盘客户数的关联在一起
grouped02 = pd.merge(grouped02, ico_max, how='left', on=['county_id'])
grouped02 = pd.merge(grouped02, ico_min, how='left', on=['county_id'])
# 新增字段index_rate
grouped02['index_rate'] =round((grouped02['imei_newest']-grouped02['imei_y'])/(grouped02['imei_x']-grouped02['imei_y'])*4.5+0.5,2)


# In[14]:
#  排序
grouped02.sort_values(['county_id','index_rate'],ascending=[1,0],inplace=True)
# 新加字段sort_id :对区县楼盘指数占比排名
grouped02['sort_id'] = grouped02.groupby(['county_id'])['imei_newest'].rank(ascending=False,method='dense')


# In[15]:
# 去除空值
grouped02.dropna(axis = 0,inplace=True)
# 修改列名
grouped02.columns=['city_id','newest_id','imei_newest','imei_city','rate','imei_c_max','imei_c_min','index_rate','sort_id']


# In[16]:
# 合并城市和区域
grouped = pd.concat([grouped2,grouped02])
# 新字段period季度
grouped['period'] = date_quarter


# In[17]:
# 加载到新表 dws_newest_investment_pop_rownumber_quarter
grouped.drop_duplicates(inplace=True)
to_dws(grouped,table_name)
# grouped.to_csv('C:\\Users\\86133\\Desktop\\dws_newest_investment_pop_top30_quarter.csv')
grouped
print('>> Done!')


