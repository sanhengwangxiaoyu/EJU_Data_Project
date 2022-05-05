# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Jul 12 17:44:47 2021
  dwb_issue_supply_city
  楼盘 意向人数 定向人数 迫切人数 增量人数  存量人数

"""
import configparser
import os
import sys,io
from numpy.lib.function_base import append
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
date_quarter = '2021Q4'   # 季度
table_name = 'dwb_issue_supply_city' # 要插入的表名称
database = 'dwb_db'
# index = 1

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
1> 获取数据信息：dws_newest_info，dws_newest_period_admit
    通过admit筛选准入楼盘信息，通过dws_newest_info获取楼盘具体信息
"""
con = MysqlClient(db_host,database,user,password)


# In[2]:
##通过输入的参数的方式获取变量值##  如果不需要使用输入参数的方式，可以不用这段
opts,args=getopt.getopt(sys.argv[1:],"t:q:d:c:i:",["index","city_id","database=","table=","quarter="])
for opts,arg in opts:
  if opts=="-t" or opts=="--table": # 获取输入参数 -t或者--table 后的值
    table_name = arg
  elif opts=="-q" or opts=="--quarter":  # 获取输入参数 -1或者--quarter 后的值
    date_quarter = arg
  elif opts=="-d" or opts=="--database":  # 获取输入参数 -1或者--quarter 后的值
    database = arg
  elif opts=="-c" or opts=="--city_id":  # 获取输入参数 -1或者--quarter 后的值
    city_id = arg
  elif opts=="-i" or opts=="--index":  # 获取输入参数 -1或者--quarter 后的值
    index = arg


# In[3]:
##重置时间格式
start_date = str(pd.to_datetime(date_quarter))[0:10]   #截取成yyyy-MM-dd
end_date =  str(pd.to_datetime(date_quarter) + pd.offsets.QuarterEnd(0))[0:10]      #截取成yyyy-MM-dd

# if index == 1 :
  # dwb_newest_issue_offer  楼盘预售证供应表
  #          所在城市 city
  #          发证季度	issue_quarter
  #          许可证套数	issue_room
  #          有效标识	dr
deal=con.query("select city ,issue_room,issue_quarter from dwb_db.dwb_newest_issue_offer where dr!=1 and newest_id is not null and issue_quarter='"+date_quarter+"'")
# dwb_dim_geography_55city  楼盘预售证供应表
#          城市id	city_id
#          城市名称	city_name
#          有效标识	dr
city=con.query("select city_id ,city_name from dwb_db.dwb_dim_geography_55city where dr=0 group by city_id ,city_name")
#计算供应数量
df = deal.groupby(['city','issue_quarter'])['issue_room'].sum().reset_index()
df.columns = ['city_name','period','supply_num']
#获取城市id
result = pd.merge(city,df,how='left',on='city_name') 
#空值赋值
result['period'] = date_quarter
result.at[result['supply_num'].isna(),'supply_num'] = '-'
#填充列
result['dr'] = 0
result['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
result['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
result['cric_supply_num'] = '-' 
result['num_index'] = '-'
# elif index == 2 :  
  # supply_city = con.query("select city_id,city_name,period,supply_num from dwb_db.dwb_issue_supply_city where dr = 0 and period = '"+date_quarter+"'")

  # supply_county = con.query("select city_id,city_name,quarter period,sum(supply_value) supply_num,dr,create_time,update_time,cric_supply_num,num_index from dwb_db.dwb_issue_supply_county where dr = 0 and quarter = period and quarter = '"+date_quarter+"' group by  city_id,city_name,quarter,dr,create_time,update_time,cric_supply_num,num_index")

  # ##替换空值
  # df_merge = pd.merge(supply_city,supply_county,how='left',on=['city_id','city_name','period'])
  # result_nan = df_merge[df_merge['supply_num_y'].isna()]
  # result_nan = result_nan[['city_id','city_name','period','supply_num_x','dr','create_time','update_time','cric_supply_num','num_index']]
  # result_nan.columns = ['city_id','city_name','period','supply_num','dr','create_time','update_time','cric_supply_num','num_index']
  # result = df_merge[~df_merge['supply_num_y'].isna()]
  # result = result[['city_id','city_name','period','supply_num_y','dr','create_time','update_time','cric_supply_num','num_index']]
  # result.columns = ['city_id','city_name','period','supply_num','dr','create_time','update_time','cric_supply_num','num_index']
  # ##重新合并
  # result = result.append(result_nan,ignore_index=True)
  # result['dr'] = '0'
  # result['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
  # result['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
  # result.at[result['cric_supply_num'].isna(),'cric_supply_num'] = '-' 
  # result.at[result['num_index'].isna(),'num_index'] = '-'
# result = result[result['city']== '嘉兴市']


# In[8]:
#更新旧值
conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
def connect_mysql(conn):
#判断链接是否正常
 conn.ping(True)
#建立操作游标
 cursor=conn.cursor()
#设置数据输入输出编码格式
 cursor.execute('set names utf8')
 return cursor
# 建立链接游标
cur=connect_mysql(conn)
update_sql1 = "UPDATE "+database+"."+table_name+" SET dr = 1 WHERE period = '"+date_quarter+"'"
cur.execute(update_sql1)
conn.commit() # 提交记
conn.close() # 关闭数据库链接


# In[9]:
# 加载到新表 dws_newest_investment_pop_rownumber_quarter
result.drop_duplicates(inplace=True)
to_dws(result,table_name)
# test.to_csv('C:\\Users\\86133\\Desktop\\test.csv')
# result
print('>>>>>>>Done')

