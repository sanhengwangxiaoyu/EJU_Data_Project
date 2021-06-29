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

cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
# database = 'temp_db'
date_quarter = '2020Q4'    #  获取季度（统计周期）
start_date = '20201001'   #  获取取数的开始年月日
stop_date = '20210101'   #  获取取数结束的年月日
table_name = 'dws_customer_month'

opts,args=getopt.getopt(sys.argv[1:],"t:",["table="])

for opts,arg in opts:
  if opts=="-t" or opts=="--table":
    table_name = arg


start_date_DF = datetime.datetime.strptime(start_date, "%Y%m%d")
end_date_DF = datetime.datetime.strptime(stop_date, "%Y%m%d")
pre_start_date = str(start_date_DF)[0:10]
# pre_start_date = str(start_date_DF - relativedelta(months=+5))[0:10]
# pre_end_date =  str(end_date_DF - relativedelta(months=+5))[0:10]
month_date = (start_date_DF - relativedelta(days=+1)).strftime("%Y%m")   # 截取成yyyy-week




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

# In[1]:
#当月意向客户#
# dwb_customer_browse_log  客户浏览楼盘日志表（每日增量） 
#                                                      imei           客户号
#                                                      visit_date     浏览日期  
#                                                      newest_id      楼盘id
ori=con.query('''SELECT imei,visit_date date,newest_id FROM dwb_db.dwb_customer_browse_log where visit_date>='''+start_date+''' and visit_date<'''+stop_date)

# In[2]:
#当前季度准入楼盘#
#dws_newest_info  新房楼盘
#                                                      newest_id       楼盘id
#                                                      city_id         城市id  
#                                                      county_id       区县id  
newest_id=con.query('''select distinct newest_id,city_id city,county_id from dws_db.dws_newest_info''')
# dws_newest_period_admit  准入楼盘表
#                                                      newest_id       楼盘id
admit = con.query('''select distinct newest_id from dws_db.dws_newest_period_admit where period = "'''+date_quarter+'''"  and dr = 0''')


# In[3]:
# 获取楼盘的城市和区县，以及浏览基本情况#
grouped2 = pd.merge(admit, newest_id, how='left', on=['newest_id'])
ori = pd.merge(grouped2, ori, how='inner', on=['newest_id'])
# 修改列名
ori.rename(columns={'newest_id':'newest'},inplace=True)

# In[4]:
#前5月意向客户#
#imei标签-增存
#cust_browse_log_202004_202103  客户浏览日志表季度拼接
#                                                      customer        imei客户号
#                                                      idate           数据接收日期
las=con.query("SELECT distinct customer imei,concat(substr(idate,1,4),'-',substr(idate,5,2),'-',substr(idate,7,2)) FROM odsdb.cust_browse_log_202004_202103 where date_format(idate,'%Y-%m-%d') <'"+pre_start_date+"' ")
# las.columns=['imei','date','city','newest','pv']
las.columns=['imei','date']
las['city'] = ''
las['newest'] = ''
# 转换时间格式
las['date']=pd.to_datetime(las['date'],format='%Y-%m-%d').dt.date
# 转换时间格式
ori['date']=pd.to_datetime(ori['date'],format='%Y-%m-%d').dt.date


# In[6]:
# 添加以前时间的数据
cus_mon=ori.append(las,ignore_index=True)
#意向客户趋势-按月
# 将时间转换为月
cus_mon['month']=cus_mon['date'].apply(lambda x:x.strftime('%Y%m'))
# 去重
cus_mon=cus_mon[['city','newest','month','imei']].drop_duplicates()
# 按照月份进行排序，按照imei进行分组。然后将月份整体偏移一位。筛选出客户第一次出现的时间
cus_mon['last_month']=cus_mon.sort_values(by=['imei','month']).groupby(['imei'])['month'].shift(1)

# In[8]:
# 判断增存量：  存量客户：本周/月之前有浏览本楼盘则为存量客户，统计周期为半年
#              增量客户：本周/月之前未浏览本楼盘为增量客户，统计周期为半年
cus_mon.at[cus_mon['last_month'].isna(),'last_month']="增量"
cus_mon.at[cus_mon['last_month']!="增量",'last_month']="存量"
# 从当前季度前一月的数据
cus_mon=cus_mon[cus_mon['month']>=month_date]
# 统计总人数
cus_mon_res=cus_mon.groupby(['city','newest','month','last_month'])['imei'].count().reset_index()
# 新增字段，赋值这季度
cus_mon_res['period']=date_quarter
# 修改列名
cus_mon_res.columns=['city_id','newest_id','month','exist','imei_num','period']
# 这季度数据
cus_mon_res = cus_mon_res[cus_mon_res['month']>month_date]

# In[9]:
# cus_mon_res
# 加载数据到dws_customer_month
to_dws(cus_mon_res,table_name)