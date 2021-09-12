# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Jul 12 17:44:47 2021
  待售数量 在售数量 项目总量 均价

"""
import configparser
import os
import sys,io
import pymysql
import pandas as pd
import numpy as np
from collections import Counter
import time
from sqlalchemy import create_engine
import getopt

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
table_name = 'dwb_newest_county_state_num' # 要插入的表名称
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


# In[4]:
#意向客户总量#
# dwb_newest_city_state_num   城市楼盘各销售状态和均价统计表
#      楼盘id newest_id
#      城市id	city_id
#      城市名称	city_name
#      区域id	county_id
#      区域名称	county_name
#      销售状态	sales_state
#      均价	unit_price
#      周期	period
#      有效标识	dr

newest_county=con.query("select newest_id,city_id,city_name,county_id,county_name,sales_state,unit_price from dwb_db.dwb_newest_admit_info where city_id not in ('442000','441900') and dr = 0 and period='"+date_quarter+"'")


# In[3]:
########################===============季度=============#######################
#获取均价
newest_county[['unit_price']] = newest_county[['unit_price']].astype('int')
df1 = newest_county.groupby(['city_id','city_name','county_id','county_name'])['unit_price'].mean().astype('int').reset_index()
#计算各状态项目数量
df2 = newest_county.groupby(['sales_state','city_id','city_name','county_id','county_name'])['newest_id'].count().reset_index()
# 统计各个状态的楼盘数量
df2_for_sale= df2[df2['sales_state']=='待售']
df2_for_sale.rename(columns={'newest_id':'for_sale'},inplace=True)
# 在售
df2_on_sale= df2[df2['sales_state']=='在售']
df2_on_sale.rename(columns={'newest_id':'on_sale'},inplace=True)
# 合并拼接
df3 = pd.merge(df1, df2_for_sale, how='left', on=['city_id','city_name','county_id','county_name'])
df3 = pd.merge(df3, df2_on_sale, how='left', on=['city_id','city_name','county_id','county_name'])
# 空为0
df3.at[df3['for_sale'].isna(),'for_sale']=0
df3.at[df3['on_sale'].isna(),'on_sale']=0
# 项目总量
df3["total_count"] = df3['for_sale']+df3['on_sale']
# 转换数据类型
df3[['for_sale', 'on_sale','total_count']] = df3[['for_sale', 'on_sale','total_count']].astype('int').astype('str')
# 整理结果
result = df3[['city_id','city_name','county_id','county_name','for_sale','on_sale','total_count','unit_price']]
result['period'] = date_quarter
result['dr'] = 0
result['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
result['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 


# In[4]:
# 加载到新表 dws_newest_investment_pop_rownumber_quarter
result.drop_duplicates(inplace=True)
to_dws(result,table_name)
# grouped.to_csv('C:\\Users\\86133\\Desktop\\dws_newest_investment_pop_top30_quarter.csv')
# result


# In[5]:
#更新空值
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
update_sql = "UPDATE "+database+"."+table_name+" SET unit_price = NULL WHERE unit_price = 0"
cur.execute(update_sql)
conn.commit() # 提交记
conn.close() # 关闭数据库链接
print('>> Done!') #完毕

