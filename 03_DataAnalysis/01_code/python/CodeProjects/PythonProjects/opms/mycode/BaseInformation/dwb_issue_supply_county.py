# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Jul 12 17:44:47 2021
  城市 区县 供应套数

"""
import configparser
import os
import sys,io
import pymysql
import pandas as pd
import numpy as np
from collections import Counter
import time
import datetime
from sqlalchemy import create_engine
import getopt

##读取配置文件##
pymysql.install_as_MySQLdb()
cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

##设置变量初始值##
user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
date_quarter = '2018Q1'   # 季度
table_name = 'dwb_issue_supply_county' # 要插入的表名称
database = 'dwb_db'


# In[2]:
##通过输入的参数的方式获取变量值##  如果不需要使用输入参数的方式，可以不用这段
opts,args=getopt.getopt(sys.argv[1:],"t:q:d:c:",["city_id","database=","table=","quarter="])
for opts,arg in opts:
  if opts=="-t" or opts=="--table": # 获取输入参数 -t或者--table 后的值
    table_name = arg
  elif opts=="-q" or opts=="--quarter":  # 获取输入参数 -1或者--quarter 后的值
    date_quarter = arg
  elif opts=="-d" or opts=="--database":  # 获取输入参数 -1或者--quarter 后的值
    database = arg
  elif opts=="-c" or opts=="--city_id":  # 获取输入参数 -1或者--quarter 后的值
    city_id = arg


# In[3]:
##重置时间格式
start_date = str(pd.to_datetime(date_quarter))[0:10]   #截取成yyyy-MM-dd
end_date =  str(pd.to_datetime(date_quarter) + pd.offsets.QuarterEnd(0))[0:10]      #截取成yyyy-MM-dd

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
    engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database+'?charset=utf8')
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)

##正式代码##
"""
  临时脚本--->根据人数比例分配供应套数
"""
con = MysqlClient(db_host,database,user,password)


# In[4]:
# dwb_issue_supply_city  城市总供应套数
#     城市id	city_id
#     城市名称	city_name
#     当前周期	period
#     供应套数	supply_num
supply_city=con.query("select city_id,city_name,period,supply_num,cric_supply_num,num_index,dr from dwb_db.dwb_issue_supply_city where dr=0  and period='"+date_quarter+"'")
# dwb_newest_county_customer_num  区县楼盘各楼盘客流统计表
#     城市id	city_id
#     城市名称	city_name
#     区县id	county_id
#     区县名称	county_name
#     意向数量	intention
#     当前周期	period
#     当前季度时间	quarter
customer_num_co=con.query("select city_id ,city_name ,county_id ,county_name ,intention,period,quarter from dwb_db.dwb_newest_county_customer_num where dr=0 and city_id not in ('442000','441900','331100') and quarter='"+date_quarter+"'")
# dwb_newest_city_customer_num  城市楼盘各楼盘客流统计表
#     城市id	city_id
#     城市名称	city_name
#     意向数量	intention
#     当前周期	period
customer_num_ci=con.query("select city_id ,city_name ,intention intention_city,period from dwb_db.dwb_newest_city_customer_num where dr=0 and city_id not in ('442000','441900','331100') and period='"+date_quarter+"'")


# In[5]:
####季度数据
customer_co_q = customer_num_co[customer_num_co['period'] == customer_num_co['quarter']]
###区域获取城市总数量
customer_q = pd.merge(customer_co_q,customer_num_ci,how='inner',on=['city_id','city_name','period'])
##计算区域所占城市比
customer_q['rate'] = customer_q['intention'].astype('int')/customer_q['intention_city'].astype('int')
customer_q[customer_q['intention_city'].isna()]
##计算区域供应套数
customer_q = pd.merge(customer_q,supply_city,how='left',on=['city_id','city_name','period'])
##计算区域供应套数
customer_q.at[customer_q['supply_num'] == '-' , 'supply_num'] = 0
customer_q['supply_value'] = (customer_q['supply_num'].astype('int')*customer_q['rate']).astype('int')
##筛选数据
customer_q.at[customer_q['supply_num'] == 0 , 'dr'] = '1'
##更新时间
customer_q['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
customer_q['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
##重新排列字段
result_county_q = customer_q[['city_id','city_name','county_id','county_name','intention','period','quarter','intention_city','rate','supply_num','supply_value','cric_supply_num','num_index','dr','create_time','update_time']]


# In[6]:
####月度数据
# customer_num_m = customer_num_co[customer_num_co['period'] != customer_num_co['quarter']]
result = result_county_q


# In[9]:
# 加载到新表 dws_newest_investment_pop_rownumber_quarter
result.drop_duplicates(inplace=True)
##加载数据到表 dwb_issue_supply_county
to_dws(result,table_name)
# grouped.to_csv('C:\\Users\\86133\\Desktop\\dws_newest_investment_pop_top30_quarter.csv')
# result
print('>>>>>>>Done')

