import sys
import pandas as pd
import numpy as np
import time
import configparser
import os
from pandas.core import groupby
from sqlalchemy import create_engine
import pymysql
import re
import getopt

pymysql.install_as_MySQLdb()
cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
date_quarter = '2021Q1'    #  获取季度（统计周期）
table_name = 'dws_newest_property_score'
opts,args=getopt.getopt(sys.argv[1:],"t:q:s:e:d:",["table=","quarter=","startdate=","enddate=","database"])
for opts,arg in opts:
  if opts=="-t" or opts=="--table":
    table_name = arg
  elif opts=="-q" or opts=="--quarter":
    date_quarter = arg
  elif opts=="-d" or opts=="database":
    database = arg

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

def to_dws(result,table):
    engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database+'?charset=utf8')
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)

con = MysqlClient(db_host,database,user,password)

# 物业评分
# 获取物业费，服务过的楼盘数，服务过的总平方数
# dws_newest_property_rs   楼盘与物业公司关联表   
#                               newest_id       楼盘id
#                               property_id     物业id
# dwb_newest_info          新房楼盘
#                                land_area      占地面积
#                                city_id        城市id
#                                newest_id      楼盘id
# ori_newest_info_main  新房-贝壳未去重结果集
#                                property_fee    物业费
#                                uuid           楼盘id
res,columnNames = con.query('''
SELECT a0.newest_id,a0.property_id,a1.land_area,a2.property_fee,a1.city_id
FROM dws_db.dws_newest_property_rs a0
LEFT JOIN dwb_db.dwb_newest_info a1 ON a1.newest_id=a0.newest_id
LEFT JOIN odsdb.ori_newest_info_main a2 ON a2.uuid=a0.newest_id
WHERE a2.property_fee>0 AND a1.land_area>1000
''')
wy = pd.DataFrame([list(i) for i in res],columns=columnNames)

# 转换数据类型
wy['property_fee']=wy['property_fee'].astype('str').astype('float')
wy['land_area']=wy['land_area'].astype('str').astype('float')
wy['property_id']=wy['property_id'].astype('str')
wy['city_id']=wy['city_id'].astype('str')

# 物业公司服务最多的城市·
# 根据统计一下以城市为单位的楼盘数量，newest_city_num
wy0 = wy.groupby(['property_id','city_id']).size().to_frame('newest_city_num').reset_index()
# 按照求出的楼盘数量进行排序，降序排序
wy0.sort_values('newest_city_num',ascending=False,inplace=True)
# 删除重复项，仅保留最后一次出现的重复项。
wy0.drop_duplicates(subset=['property_id'],keep='first',inplace=True)
wy0 = wy0.reset_index(drop=True)


# 物业费评分,获取每个省份物业费的最大值和最小值
wy1 = wy.groupby('property_id')['property_fee'].agg(['min','max']).reset_index()

# 服务面积评分，获取每个物业的最大面积  area_max
wy2 = wy.groupby('property_id')['land_area'].max().to_frame('area_max').reset_index()
# 服务楼盘数评分
#获取每个物业的楼盘数量  newest_number
wy3 = wy.groupby('property_id')['newest_id'].count().to_frame('newest_number').reset_index()
# 根据物业id关联数据集wy0，wy1。获得最大物业费和楼盘数量
wy4 = wy0.merge(wy1[['property_id','max']],how='inner',on='property_id')
# 获取每个物业物业费的最大值和最小值
wy5 = wy4.groupby('city_id')['max'].agg(['min','max']).reset_index()
# 获得项目数量，和物业最大物业费，城市最小和最大物业费
wy6 = pd.merge(wy4,wy5,how='inner',on='city_id')
# 修改表名
wy6.rename(columns={'max_x': 'property_fee','min': 'city_fee_min','max_y': 'city_fee_max'}, inplace=True)
# 将wy6再进行合并，添加最大城市最大面积和物业id
wy7 = wy6.merge(wy2[['property_id','area_max']],how='inner',on='property_id')
#  接下来再把面积再整一遍
#  算出每个城市的最大面积物业和最小面积物业
wy8 = wy7.groupby('city_id')['area_max'].agg(['min','max']).reset_index()
#  合并数据集，获得每个城市的最大物业面积和最小物业面积，以及物业费
wy9 = pd.merge(wy7,wy8,how='inner',on='city_id')
#  替换列名
wy9.rename(columns={'max': 'city_area_max','min': 'city_area_min'}, inplace=True)
#  再把楼盘的数量再理一遍
#  合并wy3获取楼盘数量
wy10 = pd.merge(wy9,wy3,how='inner',on='property_id')
#  根据城市获取每个城市的最大楼盘数量和最小楼盘数量
wy11 = wy10.groupby('city_id')['newest_number'].agg(['min','max']).reset_index()
#  合并数据集，讲最大楼盘数量和最小楼盘数量进行合并
wy12 = pd.merge(wy10,wy11,how='inner',on='city_id')
#  修改列名
wy12.rename(columns={'max': 'city_newest_max','min': 'city_newest_min'}, inplace=True)


#  获取评分   物业： （每个物业的最大物业费-城市的最小物业费）/（城市的最大物业费-城市的最小物业费）
wy12['property_fee_score']=(wy12['property_fee']-wy12['city_fee_min'])/(wy12['city_fee_max']-wy12['city_fee_min'])
#  获取评分   面积 （每个物业的最大面积-城市的最小面积）/（城市的最大面积-城市的最小面积）
wy12['property_area_score']=(wy12['area_max']-wy12['city_area_min'])/(wy12['city_area_max']-wy12['city_area_min'])
#  获取评分   楼盘数量 （每个物业的最大物业费-城市的最小物业费）/（城市的最大物业费-城市的最小物业费）
wy12['property_newest_score']=(wy12['newest_number']-wy12['city_newest_min'])/(wy12['city_newest_max']-wy12['city_newest_min'])
#  为空时 用0添加
wy12 = wy12.fillna(0)
# 添加列property_all_score，值是面积物业费楼盘数量的和
wy12['property_all_score']=wy12['property_fee_score']+wy12['property_area_score']+wy12['property_newest_score']

#  新建数据集
wy13 = ['property_id','city_id','property_all_score']
wy14 = pd.DataFrame(wy12,columns=wy13)
#  新加列 quarter 赋值为2020年第四季度
wy14['quarter'] = date_quarter
wy14.rename(columns={'property_all_score': 'score'}, inplace=True)
#  转换数据类型
wy14=wy14.astype('str')
#  添加到dws_newest_property_score中
to_dws(wy14,table_name)



















