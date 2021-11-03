# In[]
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Jul 12 12:44:47 2021

      
 
项目榜单 - 比例改为指数数值,区间为0-5、固定展示2位小数，不能为空，不足2位补0
     (目标楼盘人气数-城市min)/(城市max-城市min)*5
人气热度 - 增加人气热度，与本市均值对比情况,人气热度评分参考需求1
     (楼盘人气指数-城市人气指数均值)/城市人气指数均值

Change on Aug 26 15:35 2021
删除区域热度占比，统一使用城市区域占比
改正有楼盘被过滤

Change on Sep 01 14:41 2021
将从原始log日志表中获取的饿数据通过中间表来获取

Changed on Seq 09 17:22:00 2021
计算区县的数据的时候过滤掉东莞和中山



"""
import configparser
import os
import sys
from numpy.lib.arraysetops import isin
from numpy.lib.function_base import place
import pymysql
import pandas as pd
import numpy as np
from collections import Counter
import re
from sqlalchemy import create_engine
import getopt
import time

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
# database = 'temp_db'
date_quarter = '2021Q3'  # 季度
table_name = 'dws_newest_popularity_rownumber_quarter' # 要插入的表名称


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
1> 获取数据信息：dwb_customer_browse_log，dws_newest_info，dws_newest_period_admit
    通过admit筛选准入楼盘信息，通过dws_newest_info获取楼盘具体信息，通过browse获取楼盘人气
2> 对browse去重
   获取指定季度的楼盘id,和城市id，区县id
   通过楼盘id，获取浏览的客户数量
3> 城市楼盘维度
       1.拿取指定列
       2.对每个城市中的客户数进行排序
       3.项目人气度（楼盘占比）：所有浏览过该楼盘的imei总量（去重）/城市imei总量（去重）
          每个城市的客户总数
          将城市的客户总数和城市楼盘客户数的关联在一起
          新增字段rate
       4.项目人气度（楼盘指数占比）： (目标楼盘人气数-城市min)/(城市max-城市min)*5
          每个城市的最大客户数
          每个城市的最小客户数
          将城市的最小客户数和最大客户数和城市楼盘客户数的关联在一起
          新增字段index_rate
       5.城市楼盘指数占比排名：
          每个城市的楼盘指数占比排序
          新加字段sort_id :对城市楼盘指数占比排名
       6.修改列名
       7.人气热度与本市均值对比情况：(楼盘人气指数-城市人气指数均值)/城市人气指数均值
          每个城市的人气指数平均值
          新增字段index_rate_change
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


# In[]
##通过输入的参数的方式获取变量值##  如果不需要使用输入参数的方式，可以不用这段
opts,args=getopt.getopt(sys.argv[1:],"t:q:s:e:d:",["database=","table=","quarter=","startdate=","enddate="])
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


# In[]
##重置时间格式
start_date = str(pd.to_datetime(date_quarter))[0:10]   #截取成yyyy-MM-dd
stop_date =  str(pd.to_datetime(date_quarter) + pd.offsets.QuarterEnd(0))[0:10]      #截取成yyyy-MM-dd

#意向客户#
# dwb_customer_browse_log  客户浏览楼盘日志表（每日增量） 
#                                                      newest_id       楼盘id
#                                                      imei     客户数  
#                                                      visit_date      浏览日期
# o=con.query("select newest_id,imei from dwb_db.dwb_customer_browse_log where visit_date>='"+start_date+"' and visit_date<='"+stop_date+"'")
#去重
# o=o.groupby(['newest_id']).agg({'imei':pd.Series.nunique}).reset_index()
o=con.query("select newest_id,intention imei from dwb_db.dwb_newest_customer_info where period=quarter and period='"+date_quarter+"'")


# In[2]:
#准入楼盘信息#
# dws_newest_info   新房楼盘
#                                                      newest_id       楼盘id
#                                                      city_id         城市id
#                                                      county_id       区县id
admit=con.query("select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 and period='"+date_quarter+"'")

newest_id=con.query("select newest_id,city_id,county_id from dws_db_prd.dws_newest_info where newest_id is not null and city_id in ('110000','120000','130100','130200','130600','210100','220100','310000','320100','320200','320300','320400','320500','320600','321000','330100','330200','330300','330400','330500','330600','331100','340100','350100','350200','360100','360400','360700','370100','370200','370300','370600','370800','410100','420100','430100','440100','440300','440400','440500','440600','441200','441300','441900','442000','450100','460100','460200','500000','510100','520100','530100','610100','610300','610400') and county_id is not null and county_id != '' group by newest_id,city_id,county_id")


# In[3]:
# 获取指定季度的楼盘id,和城市id，区县id
newest_admit = pd.merge(admit,newest_id, how='left', on=['newest_id'])
# 通过楼盘id，获取浏览的客户数量
ori = pd.merge(newest_admit, o, how='inner', on=['newest_id'])
ori = ori[~ori['city_id'].isna()]


# In[4]:
# 拿取指定列
ori1 = ori[['city_id','newest_id','imei']]
# 对每个城市中的客户数进行排序
ori1.sort_values(['city_id','imei'],ascending=[1,0],inplace=True)


# In[5]:
#项目人气度（楼盘占比）：所有浏览过该楼盘的imei总量（去重）/城市imei总量（去重）
# 每个城市的客户总数
grouped1 = ori1.groupby(by = ['city_id'], as_index=False)['imei'].sum()
# 将城市的客户总数和城市客户数的关联在一起
grouped2 = pd.merge(ori1, grouped1, how='left', on=['city_id'])
# 新增字段rate
grouped2['rate'] =grouped2['imei_x']/grouped2['imei_y']
grouped2.columns = ['city_id','newest_id','imei_newest','imei_city','rate']


# In[6]:
#项目人气度（楼盘指数占比）： (目标楼盘人气数-城市min)/(城市max-城市min)*5
# 每个城市的最大客户数
ic_max = ori1.groupby(by = ['city_id'], as_index=False)['imei'].max()
# 每个城市的最小客户数
ic_min = ori1.groupby(by = ['city_id'], as_index=False)['imei'].min()
# 将城市的最小客户数和最大客户数和区县楼盘客户数的关联在一起
grouped2 = pd.merge(grouped2, ic_max, how='left', on=['city_id'])
grouped2 = pd.merge(grouped2, ic_min, how='left', on=['city_id'])
# 新增字段index_rate,保留两位小数
grouped2['index_rate'] =round((grouped2['imei_newest']-grouped2['imei_y'])/(grouped2['imei_x']-grouped2['imei_y'])*4.5+0.5,2)
grouped2['index_rate'] = grouped2['index_rate'].fillna(5)


# In[7]:
#城市楼盘指数占比排名：
# 每个城市的楼盘指数占比排序
grouped2.sort_values(['city_id','index_rate'],ascending=[1,0],inplace=True)
# 新加字段sort_id :对城市楼盘客户数排名
grouped2['sort_id'] = grouped2.groupby(['city_id'])['imei_newest'].rank(ascending=False,method='dense')

# #城市楼盘占比排名：
# # 每个城市的楼盘占比排序
# grouped2.sort_values(['city_id','rate'],ascending=[1,0],inplace=True)
# # 新加字段sort_id :对每个城市楼盘占比的排名
# grouped2['sort_id'] = grouped2.groupby(['city_id'])['imei_x'].rank(ascending=False,method='dense')


# In[8]:
#人气热度与本市均值对比情况：(楼盘人气指数-城市人气指数均值)/城市人气指数均值
# 每个城市的人气指数平均值
ic_avg = grouped2.groupby(by = ['city_id'], as_index=False)['index_rate'].mean()
ic_avg.columns = ['city_id','imei_c_avg']
# 将城市的指数占比的平均值和城市楼盘客户数的关联在一起
grouped2 = pd.merge(grouped2, ic_avg, how='left', on=['city_id'])
# 新增字段index_rate_change
grouped2['index_rate_change'] = round((grouped2['index_rate']-grouped2['imei_c_avg'])/grouped2['imei_c_avg'],6)
grouped2['imei_c_avg']  = round(grouped2['imei_c_avg'],6)


# In[9]:
# grouped2.dropna(axis = 0,inplace=True)
# 修改列名
grouped2.columns=['city_id','newest_id','imei_newest','imei_city','rate','imei_c_max','imei_c_min','index_rate','sort_id','imei_c_avg','index_rate_change']

# In[10]:
#去除问题数据
# df_0 = ori[['city_id','county_id','newest_id','imei']]
# df_1 = df_0[df_0['city_id'] != df_0['county_id']]


# In[11]:
# 截取指定列
# ori2 = df_1[['county_id','newest_id','imei']]
ori2 = ori[['county_id','newest_id','imei']]
# 对区域的客户数进行排序
ori2.sort_values(['county_id','imei'],ascending=[1,0],inplace=True)


# In[13]:
# 所有浏览过该楼盘的imei总量（去重）/区域imei总量（去重）
# 计算每个区域的客户总数
grouped01 = ori2.groupby(by = ['county_id'], as_index=False)['imei'].sum()
# 通过区域id，获取到区域的客户总数
grouped02 = pd.merge(ori2, grouped01, how='left', on=['county_id'])
#区县楼盘占比
grouped02['rate'] =grouped02['imei_x']/grouped02['imei_y']
#修改列名
grouped02.columns = ['county_id','newest_id','imei_newest','imei_city','rate']


# In[14]:
#项目人气度（楼盘指数占比）： (目标楼盘人气数-区县min)/(区县max-区县min)*5
# 每个区县的最大客户数
ico_max = ori2.groupby(by = ['county_id'], as_index=False)['imei'].max()
# 每个区县的最小客户数
ico_min = ori2.groupby(by = ['county_id'], as_index=False)['imei'].min()
# 将区县的最小客户数和最大客户数和区县楼盘客户数的关联在一起
grouped02 = pd.merge(grouped02, ico_max, how='left', on=['county_id'])
grouped02 = pd.merge(grouped02, ico_min, how='left', on=['county_id'])
# grouped02['index_rate'] =round((grouped02['imei_newest']-grouped02['imei_y'])/(grouped02['imei_x']-grouped02['imei_y'])*4.5+0.5,2)


# In[15]:
#区县楼盘指数占比排名：
# 每个区县的楼盘指数占比排序
grouped02.sort_values(['county_id','imei_newest'],ascending=[1,0],inplace=True)
# 新加字段sort_id :对区县楼盘指数占比排名
grouped02['sort_id'] = grouped02.groupby(['county_id'])['imei_newest'].rank(ascending=False,method='dense')


# In[16]:
# # 按照区域对区县楼盘占比进行排序
# grouped02.sort_values(['county_id','rate'],ascending=[1,0],inplace=True)
# # 获取区县楼盘排序的排名
# grouped02['sort_id'] = grouped02.groupby(['county_id'])['imei_newest'].rank(ascending=False,method='dense')
# # 将区域热度占比使用城市热度占比
#获取城市热度占比
df_city = grouped2[['newest_id','index_rate']]
# 新增字段index_rate,保留两位小数
grouped02 = pd.merge(grouped02,df_city,how='left',on=['newest_id'])
grouped02 = grouped02[['county_id','newest_id','imei_newest','imei_city','rate','imei_x','imei_y','index_rate','sort_id']]


# In[17]:
# 去除空值
# grouped02.dropna(axis = 0,inplace=True)
grouped02['imei_c_avg'] = -999
grouped02['index_rate_change'] = -999
# 修改列名
grouped02.columns=['city_id','newest_id','imei_newest','imei_city','rate','imei_c_max','imei_c_min','index_rate','sort_id','imei_c_avg','index_rate_change']
grouped02 = grouped02[~grouped02['city_id'].isin(['441900','442000'])]


# In[18]:
# 城市楼盘占比，区域楼盘占比合并在一起，union all
grouped = pd.concat([grouped2,grouped02])
# 新加一列，值为当前季度
grouped['period'] = date_quarter


# In[19]:
# 去重
grouped.drop_duplicates(inplace=True)
grouped = grouped[['city_id','newest_id','imei_newest','imei_city','rate','imei_c_max','imei_c_min','index_rate','sort_id','period','imei_c_avg','index_rate_change']]
grouped['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
grouped['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
grouped['dr'] = 0

# In[]:
# test = grouped[grouped['newest_id'] == 'f6d276744ddcb55c3dbec0c70eff718c']
# 调用to_dws方法，加载数据到dws_newest_popularity_top30_quarter表中
to_dws(grouped,table_name)
print('>> Done!') #完毕


# # In[20]:
# #更新空值
# conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
# def connect_mysql(conn):
# #判断链接是否正常
#  conn.ping(True)
# #建立操作游标
#  cursor=conn.cursor()
# #设置数据输入输出编码格式
#  cursor.execute('set names utf8')
#  return cursor
# # 建立链接游标
# cur=connect_mysql(conn)
# update_sql1 = "update dws_db."+table_name+" a ,dws_db."+table_name+" b set a.index_rate = b.index_rate where a.newest_id = b.newest_id and a.imei_c_avg = '-999' and b.imei_c_avg != '-999'; "
# cur.execute(update_sql1)
# conn.commit() # 提交记
# conn.close() # 关闭数据库链接


