# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Jul 12 17:44:47 2021
  清洗动态信息
"""
import configparser
import os
import sys
import pymysql
import pandas as pd
import time
from sqlalchemy import create_engine
import getopt
import re

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
table_name = 'dwb_newest_provide_sche' # 要插入的表名称
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
#创建数据库连接
con = MysqlClient(db_host,database,user,password)


# In[4]:
# ori_newest_provide_sche 楼盘动态原始信息表
# 数据网址	url || uuid
# 楼盘名	newest_name
# tag标签	sche_tag || 周期
# title标题	provide_title
# date发布时间	provide_date
# content内容	provide_sche
# 时间清洗结果	date_clean
# dim_housing主键 housing_id 
sche=con.query("select * from odsdb.ori_newest_provide_sche where date_clean between '"+start_date+"' and '"+end_date+"'")


# In[5]:
# dws_newest_info   新房楼盘
#     楼盘id	newest_id
#     楼盘名称	newest_name
#     楼盘别名	alias_name
# newest_id=con.query("select url,main_id,newest_name from odsdb.ori_newest_info_base where main_id is not null union all select url,uuid,newest_name from odsdb.ori_newest_info_base where main_id is null and remark is null group by url,uuid,newest_name having count(1) = 1")
# newest_id.drop_duplicates(inplace=True)


# In[6]:
#对正文进行清洗
# park_r['park_rate2'] = park_r['park_rate1'].apply(lambda x:re.sub("\.*[^\u4E00-\u9FA5][\u4E00-\u9FA5]{1,3}.*[^\u4E00-\u9FA5]", "", x))
# ori['recent_opening_time'] = ori['recent_opening_time'].str.replace('[\u4e00-\u9fa5]', '') 
#替换关键字内容
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*搜狐[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*房天下[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*吉屋[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*贝壳[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*全文[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*图[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*效果图[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*经纪人[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*售楼[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*贝壳[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*&nbsp[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*电话[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*致电[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*电[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*致[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*微信[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*交流群[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*进群[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*点击[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*点[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*击[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*下载[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*详询[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*询：[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*询:[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*更多[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*详情[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*热线[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*更轻松[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*专线[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*VIP[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*转[^， , . 。！ !]*[， , . 。！ !]", "", x))



sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*搜狐[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*房天下[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*吉屋[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*贝壳[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*全文[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*图[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*效果图[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*经纪人[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*售楼[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*贝壳[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*&nbsp[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*电话[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*致电[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*电[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*致[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*微信[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*交流群[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*进群[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*点击[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*点[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*击[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*下载[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*详询[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*询：[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*询:[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*更多[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*详情[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*热线[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*更轻松[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*专线[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*VIP[\s\S]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*转[\s\S]", "", x))



sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*搜狐[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*房天下[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*吉屋[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*贝壳[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*全文[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*图[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*效果图[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*经纪人[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*售楼[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*贝壳[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*&nbsp[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*电话[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*致电[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*电[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*致[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*微信[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*交流群[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*进群[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*点击[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*点[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*击[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*下载[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*详询[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*询：[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*询:[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*更多[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*详情[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*热线[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*更轻松[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*专线[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*VIP[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[\s\S]*转[^， , . 。！ !]*[， , . 。！ !]", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*效果图", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("\d{3,3}-\d{1,8}|\(?0\d{1,8}[)-]?\d{1,8}|\(?转\d{1,8}[)-]*\d{1,8}", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("展开全文", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("收起全文", "", x))


#替换标点符号
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("[， , . 。！ !][^， , . 。！ !]*\.\.\.", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("\!", "！", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("\,", "，", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("\:", "：", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("\.", "。", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("\;", "；", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("\?", "？", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("！！", "！", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("！，", "！", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("！：", "！", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("！。", "！", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("！？；", "！", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("！；", "！", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("，，", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("，！", "。", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("，：", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("，。", "。", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("，？", "。", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("，；", "。", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("：，", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("：！", "。", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("：：", "", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("：。", "。", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("：？", "。", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("：；", "。", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("。，", "。", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("。！", "。", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("。：", "。", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("。。", "。", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("。？", "。", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("。；", "。", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("？，", "？", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("？！", "？", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("？：", "？", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("？。", "？", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("？？", "？", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("？；", "？", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("；，", "。", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("；！", "。", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("；：", "。", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("；。", "。", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("；？", "。", x))
sche['provide_sche'] = sche['provide_sche'].apply(lambda x:re.sub("；；", "。", x))


#清洗动态标题
sche['provide_title'] = sche['provide_title'].apply(lambda x:re.sub("售楼[\s\S]*", "", x))




# In[7]:
#区分数据来源
#数据来源为梁文广Q2季度爬取的动态
df = sche[sche['create_time'] == '2021-08-17 14:22:36.0']
#数据为刚开始爬取的最原始数据
df02 = sche[sche['create_time'] == '2021-08-29 22:13:47.0']


# In[8]:
df02 = df02[['url','newest_name','date_clean','sche_tag','provide_title','provide_sche']]
df02.columns = ['url','newest_name','date_clean','period','provide_title','provide_sche']


# In[9]:
result = df.groupby(['provide_sche'])['newest_name','sche_tag','provide_title','date_clean','url'].max().reset_index()
result.at[result['provide_sche'].isna(),'provide_sche'] = result['provide_title']
result.at[result['provide_sche'] == '','provide_sche'] = result['provide_title']
result['provide_title'] = result.apply(lambda x:x['provide_title']+""+x['sche_tag'],axis=1)
result['period'] = date_quarter



# In[10]:
##数据合并和加载
#摘取指定列
result = result[['url','newest_name','date_clean','period','provide_title','provide_sche']]
#数据合并
result = result.append(df02,ignore_index=True)
#针对正文进行去重
result = result.groupby(['provide_sche'])['url','newest_name','date_clean','period','provide_title'].max().reset_index()
#补充空的动态正文
result['provide_sche'] = result['provide_sche'].apply(lambda x: result['provide_title'] if x is None else x)
result['provide_sche'] = result['provide_sche'].apply(lambda x: result['provide_title'] if x == '。。' else x)
#补充空的动态标题
result['provide_title'] = result['provide_title'].apply(lambda x: result['provide_sche'] if x is None else x)
#获取动态正文和标题的数据长度
result['provide_sche1_num'] = result['provide_sche'].map(lambda x:len(x))
result['provide_title_num'] = result['provide_title'].map(lambda x:len(x))
#按照长度筛选动态正文和标题
sche_result_2 = result[result['provide_sche1_num'] == 2 ]
sche_result_7_out = result[result['provide_sche1_num'] > 7 ]
sche_result_7_out = result[result['provide_title_num'] < 25 ]
result = pd.concat([sche_result_2,sche_result_7_out])
result = result[['url','newest_name','date_clean','period','provide_title','provide_sche']]
#添加更新标识
result['dr'] = 0
result['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
result['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
#修改列名
result.columns=['newest_id','newest_name','date','period','provide_title','provide_sche','dr','create_time','update_time']

# test = result[result['newest_id'] == 'f99f6b36b785ca475e20df23d4202302']


#数据加载到mysql表里去
to_dws(result,table_name)


# In[11]:
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
update_sql1 = "delete from dwb_db.dwb_newest_provide_sche where length(provide_sche)<2"
cur.execute(update_sql1)
update_sql2 = "update dwb_db.dwb_newest_provide_sche set provide_sche = replace(right(provide_sche,1),'：','') where right(provide_sche,1) = '：' and period = '"+date_quarter+"'"
cur.execute(update_sql2)
conn.commit() # 提交记
conn.close() # 关闭数据库链接


# In[11]:
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
update_sql1 = "update dwb_db.dwb_newest_provide_sche set provide_sche = replace(right(provide_sche,1),'，','') where right(provide_sche,1) = '，' and period = '"+date_quarter+"'"
cur.execute(update_sql1)
update_sql2 = "update dwb_db.dwb_newest_provide_sche set provide_sche = replace(right(provide_sche,1),'；','') where right(provide_sche,1) = '；' and period = '"+date_quarter+"'"
cur.execute(update_sql2)
conn.commit() # 提交记
conn.close() # 关闭数据库链接


# In[11]:
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
update_sql1 = "update dwb_db.dwb_newest_provide_sche set provide_sche = replace(right(provide_sche,1),'-','') where right(provide_sche,1) = '-' and period = '"+date_quarter+"'"
cur.execute(update_sql1)
update_sql2 = "delete from dwb_db.dwb_newest_provide_sche where length(provide_title)<2"
cur.execute(update_sql2)
conn.commit() # 提交记
conn.close() # 关闭数据库链接
print('>>> Done')


# In[11]:
# sche.to_csv('C:\\Users\\86133\\Desktop\\sche.csv')


