#!/usr/bin/env python
# coding: utf-8

# In[1]:


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


cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
date_quarter = '2021Q1'    #  获取季度（统计周期）
start_date = '20210101'   #  获取取数的开始年月日
stop_date = '20210401'   #  获取取数结束的年月日
start_date_DF = datetime.datetime.strptime(start_date, "%Y%m%d")  #转换为yyyy-MM-dd HH:mm:ss 的时间格式
end_date_DF = datetime.datetime.strptime(stop_date, "%Y%m%d")     #转换为yyyy-MM-dd HH:mm:ss 的时间格式
pre_start_date = str(start_date_DF - relativedelta(months=+3))[0:10]   #截取成yyyy-MM-dd
pre_end_date =  str(end_date_DF - relativedelta(months=+3))[0:10]      #截取成yyyy-MM-dd


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


# In[2]:

def to_dws(result,table):
    engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database)
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)

# In[18]:

#意向客户总量#
con = MysqlClient(db_host,database,user,password)
# dwb_customer_browse_log  客户浏览楼盘日志表（每日增量） 
#                                                      imei           客户号
#                                                      visit_date     浏览日期  
#                                                      newest_id      楼盘id
#                                                      pv             浏览次数  
ori=con.query('''SELECT imei,visit_date date,newest_id,pv FROM dwb_db.dwb_customer_browse_log where visit_date>='''+start_date+''' and visit_date<'''+stop_date)
# dwb_newest_info  新房楼盘
#                                                      newest_id       楼盘id
#                                                      city_id         城市id  
#                                                      county_id       区县id  
newest_id=con.query('''select distinct newest_id,city_id city,county_id from dwb_db.dwb_newest_info''')
# dws_newest_period_admit  准入楼盘表
#                                                      newest_id       楼盘id
admit = con.query('''select distinct newest_id from dws_db.dws_newest_period_admit where period = "'''+date_quarter+'''"''')


# 获取楼盘的城市和区县，以及浏览基本情况
grouped2 = pd.merge(admit, newest_id, how='left', on=['newest_id'])
ori = pd.merge(grouped2, ori, how='inner', on=['newest_id'])
# 修改列名
ori.rename(columns={'newest_id':'newest'},inplace=True)
# 字符串转换成时间格式
# ori['date']=pd.to_datetime(ori['date'],format='%Y-%m-%d').dt.date
# 根据城市id和楼盘id分组，关注客户总量
cus_sum=ori.groupby(['city','newest']).agg({'imei':pd.Series.nunique}).reset_index()
# 修改列名
cus_sum.columns=['city_name','newest_name','cou_imei']
# 对城市名字分组，求取关注客户的平均值
cus_sum_city=cus_sum.groupby('city_name')['cou_imei'].mean().reset_index()
# 修改列名
cus_sum_city.columns=['city_name','city_avg']
# 均值对比：保留2位小数
cus_sum_city['city_avg']=round(cus_sum_city['city_avg'],2)
# 合并关注客户总量和关注客户均量比
cus_sum_res=pd.merge(cus_sum,cus_sum_city,how="left",on=['city_name'])
#  新增列tatio ，目标楼盘与城市平均比较  ==》 关注客户总量/关注客户的均量
cus_sum_res['ratio']=round(cus_sum_res['cou_imei']/cus_sum_res['city_avg']-1,4)
#  新增列，指定周期值
cus_sum_res['period']= date_quarter
#  截取指定列
cus_sum_res=cus_sum_res[['city_name','newest_name','period','cou_imei','city_avg','ratio']]
#  重命名列名
cus_sum_res.columns=['city_id','newest_id','period','cou_imei','city_avg','ratio']


# In[19]:

#  插入数据到dws_customer_sum表中
to_dws(cus_sum_res,'dws_customer_sum')#画像首页意向用户数总量
