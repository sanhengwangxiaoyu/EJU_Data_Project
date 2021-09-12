import sys
import os
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__)))) #加载自定义模块信息
from utils import mysqlclienttest

import pandas as pd
import numpy as np
import time
import configparser
from pandas.core import groupby
import pymysql
import re

pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine


cf = configparser.ConfigParser()
# path = os.path.abspath(os.curdir)
path = os.path.abspath(os.path.dirname(os.path.dirname(__file__))) #获取上级目录
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值

con = mysqlclienttest.MysqlClient(db_host,database,user,password) #获取pymysql链接
engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database)# 获取sqlalchemy链接


# 取用户都浏览过哪些楼盘
res,columnNames = con.query('''
SELECT imei,newest_id,visit_date FROM dwb_db.dwb_customer_browse_log
''')
# 取出的数据存在dataFrame中
df = pd.DataFrame([list(i) for i in res],columns=columnNames)
# 获取指定的时间和日期
df.visit_date = pd.to_datetime(df.visit_date)
# 讲日期转换为季度
df['quarter'] = pd.PeriodIndex(df.visit_date, freq='Q')
# 新建一个dataframe，将数据添加进去并去重
df1 = ['imei','newest_id','quarter']
df2 = pd.DataFrame(df,columns = df1).drop_duplicates().reset_index(drop=True)


####
# 拆分省份 & 关联id
####
# 省份
res,columnNames = con.query('''
SELECT province_id,province_name,lat,lng from dws_db.dim_geography  where is_del =0 and province_id is not null and grade=2 
''')
df5 = pd.DataFrame([list(i) for i in res],columns=columnNames).drop_duplicates().reset_index(drop=True)
# 常驻省份 resi_province
res,columnNames = con.query('''
SELECT imei,resi_province from dwb_db.dwb_customer_imei_tag WHERE resi_province is not null
''')
df3 = pd.DataFrame([list(i) for i in res],columns=columnNames)
# df3.drop('resi_province',axis=1) 删除resi_province这一列的数据  
# df3['resi_province'].str.split(.... 在resi_province in ("上海市,安徽省") 的时候，通过split切分，并保留两条数据（一行变两行）
# join 两列数据拼接在一起
df4 = df3.drop('resi_province',axis=1).join(df3['resi_province'].str.split(',',expand=True)
  .stack().reset_index(level=1,drop=True).rename("resi_province")).reset_index(drop=True)
# 添加列province ，并赋值unknown
df4['province'] = 'Unknown'
# 通过loc筛选出指定行数据，并赋值
df4.loc[df4['resi_province']=='重庆市','province'] = '重庆'
df4.loc[df4['resi_province']=='北京市','province'] = '北京'
df4.loc[df4['resi_province']=='上海市','province'] = '上海'
df4.loc[df4['resi_province']=='天津市','province'] = '天津'
df4.loc[df4['province']=='Unknown','province'] = df4['resi_province']
# 左连接通过province和province_name做关联
df6 = pd.merge(df4,df5,how='left',left_on='province',right_on='province_name')

# 获取指定列，添加area_type列(赋值area_type)
df7 = ['imei','province','province_id','lat','lng','area_type']
df8 = pd.DataFrame(df6,columns=df7)
df8['area_type']='area_province'


####
# 拆分城市 & 关联id   :  和省份差不多的逻辑
####
# 城市
res,columnNames = con.query('''
SELECT city_id,city_name,lat,lng from dws_db.dim_geography  where is_del =0 and city_id is not null and grade=3 
''')
df55 = pd.DataFrame([list(i) for i in res],columns=columnNames).drop_duplicates().reset_index(drop=True)
# 常驻城市
res,columnNames = con.query('''
SELECT imei,resi_city from dwb_db.dwb_customer_imei_tag where resi_city is not null 
''')
df33 = pd.DataFrame([list(i) for i in res],columns=columnNames)
# 针对特殊数据进行查分
# 特殊数据     字段     ----->     值
#        ('resi_city') ---->   ('上海市-上海市,广东省-惠州市')
# 结果        字段     ---->     值
#       ('resi_city') ---->   ('上海市')
#       ('resi_city') ---->   ('惠州市')
df44 = df33.drop('resi_city',axis=1).join(df33['resi_city'].str.split(',',expand=True).stack().reset_index(level=1,drop=True).rename('resi_city')).reset_index(drop=True)
df44['city']=df44['resi_city'].apply(lambda x:re.split('-',str(x))[-1])
# 左连接通过city和city_name做关联
df66 = pd.merge(df44,df55,how='left',left_on='city',right_on='city_name')
# 获取指定列，添加area_type列(赋值area_type)
df77 = ['imei','city','city_id','lat','lng','area_type']
df88 = pd.DataFrame(df66,columns=df77)
df88['area_type']='area_city'


####
# 拆分县区 & 关联id   :  和城市一个逻辑
####
# 县区
res,columnNames = con.query('''
SELECT region_id,region_name,lat,lng from dws_db.dim_geography  where is_del =0 and region_id is not null and grade=4 
''')
df51 = pd.DataFrame([list(i) for i in res],columns=columnNames).drop_duplicates().reset_index(drop=True)
# 常驻区县
res,columnNames = con.query('''
SELECT imei,resi_county from dwb_db.dwb_customer_imei_tag where resi_county is not null
''')
df31 = pd.DataFrame([list(i) for i in res],columns=columnNames)
#  --,海南省-陵水黎族自治县-陵水黎族自治县
df41 = df31.drop('resi_county',axis=1).join(df31['resi_county'].str.split(',',expand=True).stack().reset_index(level=1,drop=True).rename('resi_county')).reset_index(drop=True)
df41['county']=df41['resi_county'].apply(lambda x:re.split('-',str(x))[-1])
# 左连接通过city和city_name做关联
df61 = pd.merge(df41,df51,how='left',left_on='county',right_on='region_name')
# 获取指定列，添加area_type列(赋值area_type)
df71 = ['imei','county','region_id','lat','lng','area_type']
df81 = pd.DataFrame(df61,columns=df71)
df81['area_type']='area_county'



#####
# 拼接用户浏览楼盘记录与常驻区域
####
# 重命名各结果集字段名
df8.rename(columns={'province': 'name','province_id': 'id'}, inplace=True)
df88.rename(columns={'city': 'name','city_id': 'id'}, inplace=True)
df81.rename(columns={'county': 'name','region_id': 'id'}, inplace=True)
# 拼接省市区
df100 = df8.append(df88).reset_index(drop=True)
df101 = df100.append(df81).reset_index(drop=True)
# 根据imei关联，将楼盘id，地区id，地区名字拼接在一起 
dfre = pd.merge(df2,df101,how='inner',on='imei')
# 统计重复数据，新加一列别名为imei ，统计人数？
dfre1 = dfre.groupby(['newest_id','quarter','area_type','name','id','lat','lng'])['imei'].count().reset_index()
# 排序，降序排序并替换原来的数据集
dfre1.sort_values(['newest_id','quarter','area_type','imei'],ascending=True,inplace=True)
# 分组取前30
dfre22 = dfre1.groupby(['newest_id','quarter','area_type','imei']).head(30).reset_index(drop=True)
# 将分组后的人数相加，求出每个季度楼盘浏览的人数
dfre33 = dfre22.groupby(['newest_id','quarter','area_type'])['imei'].sum().reset_index()
# 将得到的人数合并到总表中，表中同时存在每个楼盘城市的浏览人数，以及总人数。
dfre44 = pd.merge(dfre22,dfre33,how='inner',on=['newest_id','quarter','area_type'])
# 用了浏览人数/总人数  ，得到 占比
dfre44['percentage']=round(dfre44['imei_x']/dfre44['imei_y'],4)
# 改变所有数据元素的数据类型为字符串
dfre44=dfre44.astype('str')
# 添加到结果表

