#%%
import sys
from typing import Tuple
import pandas as pd
import numpy as np
import time
import configparser
import os
from pandas.core import groupby
import pymysql



cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值


# period_value = '2020Q4'
# start_date = '2020-10-01'
# end_date = '2020-12-31'

period_value = '2021Q1'
start_date = '2021-01-01'
end_date = '2021-03-31'


table_name = 'dws_tag_purchase_prefer2'


opts,args=getopt.getopt(sys.argv[1:],"t:q:s:e:",["table=","quarter=","startdate=","enddate="])
for opts,arg in opts:
  if opts=="-t" or opts=="--table":
    table_name = arg
  elif opts=="-q" or opts=="--quarter":
    period_value = arg
  elif opts=="-s" or opts=="--startdate":
    start_date = arg
  elif opts=="-e" or opts=="--enddate":
    end_date = arg




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
        cur.close()
        return res,columnNames
    def close(self):
        self.conn.close()

# con = MysqlClient(db_host,database,user,password)

pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database+'?charset=utf8')
con = MysqlClient(db_host,database,user,password)


# %%
# # # # # # # # # # # # # # # # # # # # # # # # APP一级偏好 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# IMEI对应的季度与1级2级标签
res,columnNames = con.query(" SELECT a0.imei,a0.period,a0.app_prefer,a1.type1 FROM dwb_db.dwb_customer_imei_tag a0 LEFT JOIN dwb_db.app_type a1 ON a1.type2=a0.app_prefer WHERE a0.app_prefer IS NOT NULL AND a0.period ='"+period_value+"' ")
df = pd.DataFrame([list(i) for i in res],columns=columnNames)

# 浏览日志全局消重,筛选admin中楼盘
res,columnNames = con.query(" SELECT distinct imei,newest_id,concat(year(visit_date),'Q',quarter(visit_date)) period FROM dwb_db.dwb_customer_browse_log where visit_date >= '"+start_date+"' and visit_date<='"+end_date+"' ")
log = pd.DataFrame([list(i) for i in res],columns=columnNames)

# admin表过滤
res,columnNames = con.query(" select newest_id from dws_db.dws_newest_period_admit WHERE dr=0 and period = '"+period_value+"' ")
admin = pd.DataFrame([list(i) for i in res],columns=columnNames)

log = log.merge(admin,how='inner',on='newest_id')

# 日志拼接1级2级标签
tag = log.merge(df,how='inner',on=['imei','period'])

# 统计每个楼盘1级标签数量（top10）
tag1 = tag.groupby(['newest_id','period','type1'])['imei'].count().reset_index()
tag1.sort_values(['newest_id','period','imei'],ascending=False,inplace=True)

# top10每个标签对应的楼盘与人数
tag1top10 = tag1.groupby(['newest_id','period']).head(10).reset_index(drop=True)
# 目标楼盘所有有标签的IMEI总数
tagonly = tag.groupby(['newest_id','period']).agg({'imei':pd.Series.nunique}).reset_index()
# 本次统计周期内所有单个标签的类型的总IMEI数
tag1top10all = tag1top10.groupby('type1')['imei'].sum().reset_index()
# 本次统计周期内所有有标签的总人数
tagonlyall = tag['imei'].drop_duplicates().reset_index(drop=True)
tagonlyall1 = tagonlyall.count()

# 拼接
merge = tag1top10.merge(tagonly,how='inner',on=['newest_id','period']).merge(tag1top10all,how='inner',on='type1')
merge['all'] = tagonlyall1
# 添加city_id
res,columnNames = con.query(" SELECT newest_id,city_id FROM dws_db.dws_newest_info ")
city = pd.DataFrame([list(i) for i in res],columns=columnNames)
merge = merge.merge(city,how='inner',on=['newest_id'])
merge.rename(columns={'type1':'tag_value','imei_x':'value1','imei_y':'value2','imei':'value3','all':'value4'},inplace=True)
merge['value5'] = (merge['value1']/merge['value2'])/(merge['value3']/merge['value4'])
merge['tag_name'] = 'APP一级偏好'
merge = merge[['city_id','newest_id','tag_value','value1','value2','value3','value4','value5','tag_name','period']]

#%%
# # # # # # # # # # # # # # # # # # # # # # # # APP二级偏好 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

tag2 = tag.groupby(['newest_id','period','app_prefer','type1'])['imei'].count().reset_index()
prefer2 = tag2.merge(merge[['newest_id','period','tag_value','city_id']],how='inner',left_on=['newest_id','period','type1'],right_on=['newest_id','period','tag_value'])
prefer2 =prefer2[['newest_id','period','app_prefer','type1','imei','city_id']]
prefer2.sort_values(['newest_id','period','imei'],ascending=False,inplace=True)
prefer2 = prefer2.groupby(['newest_id','period']).head(10).reset_index(drop=True)
prefer2_sum = prefer2.groupby(['newest_id','period'])['imei'].sum().reset_index()
prefer2_merge = prefer2.merge(prefer2_sum,how='inner',on=['newest_id','period'])
prefer2_merge['value4'] = prefer2_merge['imei_x']/prefer2_merge['imei_y']
prefer2_merge['tag_name'] = 'APP偏好类型二级分类占比TOP10'
prefer2_merge.rename(columns={'app_prefer':'tag_value','type1':'value1','imei_x':'value2','imei_y':'value3'},inplace=True)
prefer2_re = prefer2_merge[['city_id','newest_id','tag_value','value1','value2','value3','value4','tag_name','period']]

prefer2_re.to_sql(table_name,engine,index=False,if_exists='append')


#%%



