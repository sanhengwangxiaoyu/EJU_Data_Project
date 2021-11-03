# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Seq 07 17:44:47 2021
  配套信息关联楼盘和计算直线距离

"""
import configparser
import os
import pymysql
import pandas as pd
from sqlalchemy import create_engine
from geopy.distance import geodesic
import time

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


#In[]
#加载数据
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
update_sql = "insert into dws_db_prd.dws_tag_purchase_poi(city_id,newest_id,tag_value,tag_detail,pure_distance,lng,lat,tag_value2) select t2.city_id,newest_id,tag_value,tag_detail,pure_distance,SUBSTRING_INDEX(poi_lnglat,',',1) lng,SUBSTRING_INDEX(poi_lnglat,',',-1) lat,poi_type tag_value2 from (select city,newest_id,poi_index tag_value,poi_name tag_detail,pure_distance,poi_lnglat,poi_type from dwb_db.dwb_tag_purchase_poi_info where dr = 0) t1 inner join (select city_id,city_name from dwb_db.dwb_dim_geography_55city group by city_id,city_name) t2 on t1.city = t2.city_name group by city_id,newest_id,poi_index,poi_name,pure_distance,poi_lnglat,poi_type"
cur.execute(update_sql)
conn.commit() # 提交记
conn.close() # 关闭数据库链接
print('>>> insert into Done')


#In[]
#删除历史数据
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
update_sql = "update dwb_db.dwb_tag_purchase_poi_info set dr = 1"
cur.execute(update_sql)
conn.commit() # 提交记
conn.close() # 关闭数据库链接
print('>>> set dr=1 Done')

