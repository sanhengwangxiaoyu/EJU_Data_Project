# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Jul 12 17:44:47 2021
  关注楼盘数量  意向选房数量  迫切买房数量  当季新增  当季留存

"""
import configparser
import os
import sys
import pymysql
import pandas as pd
import numpy as np
from collections import Counter
import time
from sqlalchemy import create_engine
import getopt

##设置配置信息##
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
table_name = 'dwb_newest_city_customer_num' # 要插入的表名称
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
# dwb_newest_admit_info   准入楼盘信息表
#     城市id	city_id
#     城市名称	city_name
#     有效标识	dr
#     周期	period
#admit_info=con.query("select newest_id,city_id from dwb_db.a_dws_newest_period_admit where dr = 0 and period='"+date_quarter+"'")
#city_name=con.query("select city_id,city_name from dwb_db.dwb_dim_geography_55city where dr = 0 group by city_id,city_name")
#admit_info=pd.merge(admit_info,city_name,how='left',on=['city_id'])

admit_info=con.query("select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 and period='"+date_quarter+"'")


newest_id=con.query("select newest_id,city_id from dws_db_prd.dws_newest_info where newest_id is not null and city_id in ('110000','120000','130100','130200','130600','210100','220100','310000','320100','320200','320300','320400','320500','320600','321000','330100','330200','330300','330400','330500','330600','331100','340100','350100','350200','360100','360400','360700','370100','370200','370300','370600','370800','410100','420100','430100','440100','440300','440400','440500','440600','441200','441300','441900','442000','450100','460100','460200','500000','510100','520100','530100','610100','610300','610400') and county_id is not null and county_id != '' group by newest_id,city_id")

city_name=con.query("select city_id,city_name from dwb_db.dwb_dim_geography_55city where dr = 0 group by city_id,city_name")

newest_id=pd.merge(newest_id,city_name,how='left',on=['city_id'])

admit_info = pd.merge(admit_info,newest_id, how='left', on=['newest_id'])



# In[5]:
# dwb_newest_customer_info   楼盘浏览客户信息表
#      楼盘id	newest_id
#      周期	period
#      意向人数	intention
#      定向人数	orien
#      迫切人数	urgent
#      增量人数	increase
#      存量人数	retained
#      有效标识	dr
customer_info=con.query("select newest_id,intention,orien,urgent,increase,retained from dwb_db.dwb_newest_customer_info where dr = 0 and period='"+date_quarter+"'")


# In[6]:
#表合并
df = pd.merge(admit_info,customer_info,how='left',on=['newest_id'])
########################===============季度=============#######################
#计算城市总数
result = df.groupby(['city_id','city_name'])['intention','orien','urgent','increase','retained'].sum().reset_index()
result['period'] = date_quarter
result['dr'] = 0
result['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
result['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 


# In[4]:
# 加载到新表 dwb_newest_city_customer_num
result.drop_duplicates(inplace=True)
to_dws(result,table_name)
# grouped.to_csv('C:\\Users\\86133\\Desktop\\dws_newest_investment_pop_top30_quarter.csv')
# result
print('>> Done!') #完毕


