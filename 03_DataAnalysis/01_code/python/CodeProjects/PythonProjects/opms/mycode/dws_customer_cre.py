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

start_date_DF = datetime.datetime.strptime(start_date, "%Y%m%d")
end_date_DF = datetime.datetime.strptime(stop_date, "%Y%m%d")
pre_start_date = str(start_date_DF - relativedelta(months=+3))[0:10]
pre_end_date =  str(end_date_DF - relativedelta(months=+3))[0:10]

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
#意向客户总量#
con = MysqlClient(db_host,database,user,password)


# dwb_customer_browse_log  客户浏览楼盘日志表（每日增量） 
#                                                      imei           客户号
#                                                      visit_date     浏览日期  
#                                                      newest_id      楼盘id
#                                                      pv             浏览次数  
ori=con.query('''SELECT imei,visit_date date,newest_id,pv FROM dwb_db.dwb_customer_browse_log where visit_date>='''+start_date+''' and visit_date<'''+stop_date)
#dwb_newest_info  新房楼盘
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
# 转换时间格式
ori['date']=pd.to_datetime(ori['date'],format='%Y-%m-%d').dt.date

#imei标签-增存
las=con.query("SELECT distinct customer imei,concat(substr(idate,1,4),'-',substr(idate,5,2),'-',substr(idate,7,2)),city_name,floor_name,pv FROM odsdb.cust_browse_log_202004_202103 where date_format(idate,'%Y-%m-%d') >= '"+pre_start_date+"'  and date_format(idate,'%Y-%m-%d') <'"+pre_end_date+"' ")
las.columns=['imei','date','city','newest','pv']
# 转换时间格式
las['date']=pd.to_datetime(las['date'],format='%Y-%m-%d').dt.date


# In[21]:

#增存留占比#
# 截取列，去重
cus_list=ori[['city','newest','imei']].drop_duplicates()
# 截取列，去重
cus_last=las[['imei']].drop_duplicates()
# 新增字段exist 值为存量
cus_last['exist']="存量"
# 按照imei，浏览用户合存量用户拼接
cus_list=pd.merge(cus_list,cus_last,how='left',on=['imei'])
# 将新字段exist为空的，都填充为增量
cus_list.at[cus_list['exist'].isna(),'exist']="增量"
# 计算每个城市的项目增存量浏览了多少人
cus_cre_res=cus_list.groupby(['city','newest','exist']).count().reset_index()
# 新增季度字段并且赋值
cus_cre_res['period']=date_quarter
# 修改列名，加载数据到dws_customer_cre
cus_cre_res.columns=['city_id','newest_id','exist','imei_num','period']
cus_cre_res
to_dws(cus_cre_res,'dws_customer_cre')