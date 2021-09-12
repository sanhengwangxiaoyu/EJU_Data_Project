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
date_quarter = '2021Q2'    #  获取季度（统计周期）
start_date = '20210401'   #  获取取数的开始年月日
stop_date = '20210701'   #  获取取数结束的年月日

table_name = 'dws_customer_week'


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




start_date_DF = datetime.datetime.strptime(start_date, "%Y%m%d")  #转换为yyyy-MM-dd HH:mm:ss 的时间格式
end_date_DF = datetime.datetime.strptime(stop_date, "%Y%m%d")     #转换为yyyy-MM-dd HH:mm:ss 的时间格式
pre_start_date = str(start_date_DF)[0:10]   #截取成yyyy-MM-dd
# pre_start_date = str(start_date_DF - relativedelta(months=+3))[0:10]   #截取成yyyy-MM-dd
# pre_end_date =  str(end_date_DF - relativedelta(months=+3))[0:10]      #截取成yyyy-MM-dd
week_date = (start_date_DF - relativedelta(days=+1)).strftime("%Y-%W")   # 截取成yyyy-week
year_id = date_quarter.split('Q')[0]
quarter_id = date_quarter.split('Q')[1]

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

#获取周维度#
date=con.query("SELECT cal_date,Revised_year,Revised_quarter,Revised_month,Revised_week,concat(Revised_year,'-',Revised_week) week_id FROM  dws_db.dim_period_date where Revised_year='" + year_id + "' and Revised_quarter='" + quarter_id + "' ")

week_list = list(date['week_id'].drop_duplicates())
week_list[0]
# week = pd.DataFrame()
# week


# In[1]:
#当月意向客户#
# dwb_customer_browse_log  客户浏览楼盘日志表（每日增量） 
#                                                      imei           客户号
#                                                      visit_date     浏览日期  
#                                                      newest_id      楼盘id
# ori=con.query('''SELECT imei,visit_date date,newest_id FROM dwb_db.dwb_customer_browse_log where visit_date>='''+start_date+''' and visit_date<'''+stop_date)

# In[2]:
#当前季度准入楼盘#
#dws_newest_info  新房楼盘
#                                                      newest_id       楼盘id
#                                                      city_id         城市id  
#                                                      county_id       区县id  
newest_id=con.query('''select distinct newest_id,city_id city,county_id from dws_db.dws_newest_info where dr = 0''')
# dws_newest_period_admit  准入楼盘表
#                                                      newest_id       楼盘id
admit = con.query('''select distinct newest_id from dws_db.dws_newest_period_admit where period = "'''+date_quarter+'''"  and dr = 0''')


# In[3]:
# 获取楼盘的城市和区县，以及浏览基本情况#
grouped2 = pd.merge(admit, newest_id, how='left', on=['newest_id'])
week = pd.DataFrame()
for a in week_list:
    print(a)
    con = MysqlClient(db_host,database,user,password)
    data = con.query("SELECT distinct newest_id,imei,current_week FROM  dwb_db.dwb_customer_browse_log where current_week = '"+a+"' ")
    week=week.append(data,ignore_index=True)
    print(week)

ori=pd.merge(week, grouped2, how='inner', on=['newest_id'])
# ori = pd.merge(grouped2, ori, how='inner', on=['newest_id'])
# 修改列名
ori.rename(columns={'newest_id':'newest'},inplace=True)
ori

# cus_week_nn_1 = week[week['current_week'] == '2021-1']
# cus_week_nn_1


# In[4]:
#前5月意向客户#
#imei标签-增存
#cust_browse_log_202004_202103  客户浏览日志表季度拼接
#                                                      customer        imei客户号
#                                                      idate           数据接收日期
# las=con.query("SELECT distinct customer imei,concat(substr(idate,1,4),'-',substr(idate,5,2),'-',substr(idate,7,2)) FROM odsdb.cust_browse_log_202004_202103 where date_format(idate,'%Y-%m-%d') <'"+pre_start_date+"' ")

las=con.query("SELECT distinct customer imei,concat(substr(idate,1,4),'-',substr(idate,5,2),'-',substr(idate,7,2)) FROM odsdb.cust_browse_log_202004_202103 where date_format(idate,'%Y-%m-%d') between '"+pre_start_date+"' and  ")

# las.columns=['imei','date','city','newest','pv']
las.columns=['imei','date']
las['city'] = ''
las['newest'] = ''
# 转换时间格式
las['date']=pd.to_datetime(las['date'],format='%Y-%m-%d').dt.date
las['current_week']=las['date'].apply(lambda x:x.strftime('%Y-%W'))

las=las[['imei','city','newest','current_week']]

# 转换时间格式
# ori['date']=pd.to_datetime(ori['date'],format='%Y-%m-%d').dt.date
#意向客户趋势-按周
# 拼接数据
cus_combi_ori=ori.append(las,ignore_index=True)
cus_week=cus_combi_ori
# 获取周
# 拿取这季度前一周的数据
# cus_week=cus_week[cus_week['week']>=week_date]
# 重命名 + 去重
cus_week=cus_week[['city','newest','current_week','imei']].drop_duplicates()
# # 按照周进行排序，按照imei进行分组。然后将月份整体偏移一位。筛选出客户第一次出现的时间
cus_week['last_week']=cus_week.sort_values(by=['imei','current_week']).groupby(['imei'])['current_week'].shift(1)

# 判断最后一周的值为是否nan（not a number）的时候就是增量数据 ：本周/月之前未浏览本楼盘为增量客户，统一周期为半年
cus_week.at[cus_week['last_week'].isna(),'last_week']="增量"
# 判断不为增量的都是存量数据   ：本周/月之前有浏览本楼盘则为存量客户，统计周期为半年
cus_week.at[cus_week['last_week']!="增量",'last_week']="存量"
cus_week
# 拿取这季度数据
cus_week_nn = cus_week[cus_week['current_week'] >= week_list[0]]
cus_week_nn

cus_week_nn_test = cus_week_nn[cus_week_nn['newest'] == '94817e7f7ccac282df942c77d6ec27aa']

# 统计这季度的总人数
cus_week_res=cus_week_nn.groupby(['city','newest','current_week','last_week'])['imei'].count().reset_index()
# 新增字段，赋值这季度
cus_week_res['period']=date_quarter
# 修改列名
cus_week_res.columns=['city_id','newest_id','week','exist','imei_num','period']

cus_week_res


# cus_week_res_1 = cus_week_res[cus_week_res['week'] == '2020-53']
# cus_week_res_1

# In[27]:

#  插入数据到dws_customer_week
to_dws(cus_week_res,table_name)

# In[1000]:
cus_week_nn_test.to_csv('C:\\Users\\86133\\Desktop\\cus_week_nn_test.csv')


# In[111]:
#更新星期


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
# update_sql = "UPDATE dws_db."+table_name+" SET  week = concat(substring_index(week,'-',1),'-',substring_index(week,'-',-1)+1)"
# cur.execute(update_sql)
# conn.commit() # 提交记
# conn.close() # 关闭数据库链接
# print('>> Done!') #完毕