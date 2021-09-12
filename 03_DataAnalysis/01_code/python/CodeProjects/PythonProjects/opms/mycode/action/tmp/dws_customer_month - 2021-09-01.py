#In[1]:
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
date_quarter = '2020Q4'    #  获取季度（统计周期）
table_name = 'dws_customer_month'


#In[2]:
opts,args=getopt.getopt(sys.argv[1:],"t:q:s:e:",["table=","quarter=","startdate=","enddate="])
for opts,arg in opts:
  if opts=="-t" or opts=="--table":
    table_name = arg
  elif opts=="-q" or opts=="--quarter":
    date_quarter = arg
  elif opts=="-s" or opts=="--startdate":
    start_date = arg
  elif opts=="-e" or opts=="--enddate":
    stop_date = arg


#In[3]:
start_date = str(pd.to_datetime(date_quarter))[0:10]   #截取成yyyy-MM-dd
end_date =  str(pd.to_datetime(date_quarter) + pd.offsets.QuarterEnd(0))[0:10]      #截取成yyyy-MM-dd

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


# In[4]:
#当月意向客户#
# dwb_customer_browse_log  客户浏览楼盘日志表（每日增量） 
#                                                      imei           客户号
#                                                      visit_date     浏览日期  
#                                                      newest_id      楼盘id
ori=con.query("SELECT imei,newest_id,visit_month FROM dwb_db.dwb_customer_browse_log where visit_date>='"+start_date+"' and visit_date<='"+end_date+"' group by imei,newest_id,visit_month")


# In[5]:
#当前季度准入楼盘#
#dws_newest_info  新房楼盘
#                                                      newest_id       楼盘id
#                                                      city_id         城市id  
#                                                      county_id       区县id  
newest_id=con.query('''select distinct newest_id,city_id city,county_id from dws_db.dws_newest_info where dr=0''')
# dws_newest_period_admit  准入楼盘表
#                                                      newest_id       楼盘id
admit = con.query('''select distinct newest_id from dws_db.dws_newest_period_admit where period = "'''+date_quarter+'''"  and dr = 0''')
# dws_imei_browse_tag  客户浏览标签结果表
#                                   imei            类似客户号的东西
#                                   concern        关注
#                                   intention      意向
#                                   urgent         迫切
#                                   cre            增存
ime = con.query('''select imei,concern,intention,urgent,cre from dws_db.dws_imei_browse_tag where period = "'''+date_quarter+'''"''')


# In[6]:
# 获取楼盘的城市和区县，以及浏览基本情况#
df = pd.merge(admit, newest_id, how='left', on=['newest_id'])
df = pd.merge(df, ori, how='inner', on=['newest_id'])
# 修改列名
df = pd.merge(df,ime ,how='left' ,on=['imei'])
df = df[['city','newest_id','visit_month','imei','cre']]


# In[7]:
df1_in_tmp =  df[df['cre'] == '增长']
df1_in_tmp = df1_in_tmp[['city','newest_id','visit_month','imei']].drop_duplicates()
df1_increase = df1_in_tmp.groupby(['city','visit_month','newest_id'])['imei'].count().reset_index()
df1_increase['exist'] = "增量"
df1_re_tmp =  df[df['cre'] == '活跃']
df1_re_tmp = df1_re_tmp[['city','newest_id','visit_month','imei']].drop_duplicates()
df1_retained = df1_re_tmp.groupby(['city','newest_id','visit_month'])['imei'].count().reset_index()
df1_retained['exist'] = "存量"


# In[8]:
cus_mon_res = pd.concat([df1_increase,df1_retained])
cus_mon_res['period'] = date_quarter
cus_mon_res = cus_mon_res[['city','newest_id','visit_month','exist','imei','period']]
cus_mon_res.columns = ['city_id','newest_id','month','exist','imei_num','period']


# In[9]:
# cus_mon_res
# 加载数据到dws_customer_month
to_dws(cus_mon_res,table_name)


print('>> Done!') #完毕