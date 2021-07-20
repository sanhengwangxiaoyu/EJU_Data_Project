# 要添加一个新单元，输入 '# %%'
# 要添加一个新的标记单元，输入 '# %% [markdown]'

# %%
import sys
from typing import Tuple
import pandas as pd
import numpy as np
import time
import configparser
import os
from pandas.core import groupby
import pymysql
import re


cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值


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

con = MysqlClient(db_host,database,user,password)

pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
engine = create_engine('mysql+mysqldb://'+'mysql'+':'+'egSQ7HhxajHZjvdX'+'@'+'172.28.36.77'+':'+'3306'+'/'+'dws_db')# 初始化引擎


# %%

# 预售证统计需求
# -- 1、统计当前我们有多少个城市的预证信息，多少城市预证能精确到房间，哪些城市是到预证
# 预售证：issue_code    房间：room_code

# -- 2、统计时间段——哪些城市有去年4季度以后的所有数据，哪些没有
# 发证日期：issue_date  4季度以后的所有数据：指哪些字段必须有？

# -- 3、数值准确性——按照1里的规则，分开城市统计各城市各月的供应套数，差的离谱的标注出来
# 发证日期：issue_date  供应套数是重复的-distinct  


# 初步清洗日期，把标准日期洗成统一格式
res,columnNames = con.query('''
SELECT 
gd_city,clean_floor_name,
date(CASE WHEN issue_date_clean='None' OR issue_date_clean LIKE '%自%' OR issue_date_clean IN ('','2006-0687','2006-0688','-00--00','0108-01-09')   THEN NULL ELSE issue_date_clean END)  issue_date,
-- date(CASE WHEN issue_date_clean='None' OR issue_date_clean LIKE '%自%' OR issue_date_clean IN ('','2006-0687','2006-0688','-00--00','0108-01-09')   THEN NULL ELSE issue_date_clean END) issue_date_clean,
CASE WHEN issue_code IN ('None','') THEN NULL ELSE issue_code END issue_code,
CASE WHEN room_code IN ('None','') THEN NULL ELSE room_code END room_code,
case when room_sum in ('None','','','','','') then 0 ELSE room_sum end room_sum,
substr(date(CASE WHEN issue_date_clean='None' OR issue_date_clean LIKE '%自%' OR issue_date_clean IN ('','2006-0687','2006-0688','-00--00','0108-01-09')  THEN NULL ELSE issue_date_clean END),1,7) `year_month`
FROM temp_db.tmp_city_newest_deal
where issue_date_clean not LIKE '%-00'
having issue_date>='2020-01-01'
''')
df = pd.DataFrame([list(i) for i in res],columns=columnNames)


# %%
# 统计每个城市每月预售证个数，房间数
df1 = df.groupby(['gd_city','year_month']).agg({'room_code':pd.Series.nunique}).reset_index()

# %%
df2 = df.groupby(['gd_city','year_month']).agg({'clean_floor_name':pd.Series.nunique}).reset_index()


# %%
# 统计每个城市每月供应套数
df.drop_duplicates(subset=['gd_city','clean_floor_name','year_month'],keep='first',inplace=True)
# 提取整数部分
df['room_sum1'] = df['room_sum'].apply(lambda x:re.sub("\D","",x))
# 剔除脏数据
df = df[(df['room_sum']!='841套公寓+104套商铺')&(df['room_sum']!='一期548户，二期344户')]
df['room_sum1'] = df['room_sum1'].astype('int')
df3 = df.groupby(['gd_city','year_month'])['room_sum1'].sum().reset_index()

# %%
df4 = df.groupby(['gd_city','year_month']).agg({'issue_code':pd.Series.nunique}).reset_index()


# %%
# 拼接结果表
df5 = df1.merge(df3,df2,df4,how='inner',on=['year_month','gd_city'])

# df4.to_csv('当前预售证情况.csv')

