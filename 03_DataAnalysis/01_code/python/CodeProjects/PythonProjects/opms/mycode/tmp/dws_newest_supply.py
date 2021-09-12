#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on jun 14 16:44:47 2021
            供应套数：城市，区域，季度
"""
import configparser
import os
import sys
from numpy.lib.function_base import append
from pandas.core import groupby
from pandas.core.groupby.groupby import GroupBy
import pymysql
import pandas as pd
import numpy as np
from collections import Counter
import re
from sqlalchemy import create_engine
import datetime
from dateutil.relativedelta import relativedelta
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
date_quarter = '2021Q1'  # 季度
start_date = '20210101'  # 季度开始时间
stop_date = '20210401'    # 季度结束时间
table_name = 'dws_newest_supply' # 要插入的表名称


##通过输入的参数的方式获取变量值##  如果不需要使用输入参数的方式，可以不用这段
opts,args=getopt.getopt(sys.argv[1:],"t:q:s:e:",["table=","quarter=","startdate=","enddate="])
for opts,arg in opts:
  if opts=="-t" or opts=="--table":  # 获取输入参数 -t或者--table 后的值
    table_name = arg
  elif opts=="-q" or opts=="--quarter":   # 获取输入参数 -1或者--quarter 后的值
    date_quarter = arg
  elif opts=="-s" or opts=="--startdate": # 同上
    start_date = arg
  elif opts=="-e" or opts=="--enddate":  # 同上
    stop_date = arg

##转换时间格式##
start_date_DF = datetime.datetime.strptime(start_date, "%Y%m%d")  #转换为yyyy-MM-dd HH:mm:ss 的时间格式
end_date_DF = datetime.datetime.strptime(stop_date, "%Y%m%d")     #转换为yyyy-MM-dd HH:mm:ss 的时间格式
pre_start_date = str(start_date_DF)[0:10]   #截取成yyyy-MM-dd
pre_end_date =  str(end_date_DF)[0:10]      #截取成yyyy-MM-dd



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
##两者比较取最大值##
def two_c_v_max(result):
    result.columns = ['id','one','two']
    result['three'] = result.one > result.two
    result.at[result['three'] =="True",'four']= result['one']
    result.at[result['three'] !="True",'four']= result['two']
    result = result[['id','four']]
    return result
##两者比较取最大值##
def four_c_v_max(result):
    result.columns = ['id01','id02','id03','one','two']
    result['three'] = result.one > result.two
    result.at[result['three'] =="True",'four']= result['one']
    result.at[result['three'] !="True",'four']= result['two']
    result = result[['id01','id02','id03','four']]
    return result

##正式代码##
"""
   1> 获取数据信息：odsdb.city_newest_deal , temp_db.tmp_city_newest_deal_ls3, dws_db.dws_newest_info, dws_db.dws_area_detail
          根据odsdb.city_newest_deal找到当前季度的交易信息, 根据temp_db.tmp_city_newest_deal_ls3获取楼盘名字,  dws_db.dws_newest_info获取城市区县,dws_area_detail获取区县名字 
   2> 去重，求和
         比较房间数量和预售套数：
             1) 不超过10万，两者比较取大的
             2) 超过10W，两者比较取小的
"""

con = MysqlClient(db_host,database,user,password)


# In[1]:
#获取交易数据#
# odsdb.city_newest_deal
deal=con.query("select floor_name,issue_code,gd_city,min(room_sum) room_sum,count(1) room_num from odsdb.city_newest_deal cnd where issue_date_clean >= '"+pre_start_date+"' and issue_date_clean < '"+pre_end_date+"' group by floor_name, issue_code, gd_city")


# In[2]:
##tmp_city_newest_deal_ls3获取楼盘名字,  dws_db.dws_newest_info获取城市区县id ,dws_area_detail获取区县名字 
# temp_db.tmp_city_newest_deal_ls3
deal_ls=con.query("select floor_name ,gd_city ,newest_name from temp_db.tmp_city_newest_deal_ls3 tcndl")

#  dws_db.dws_newest_info
newest=con.query("select newest_name ,city_id ,county_id from dws_db.dws_newest_info group by newest_name ,city_id ,county_id")

# dws_area_detail
geography=con.query("select city_id ,city_name ,region_id,region_name from dws_db.dws_area_detail ")


# In[3]:
#预处理
# 修改列名
geography.columns = ['city_id' ,'gd_city' ,'county_id','region_name']
geography.at[geography['gd_city'] == '东莞市','county_id']=441900
geography.at[geography['gd_city'] == '东莞市','region_name']='东莞市'
geography[['county_id']] = geography[['county_id']].astype('int')
geography = geography.applymap(str)


# In[4]:
#信息整合
# 城市
meg = pd.merge(geography,newest, how='left', on=['city_id','county_id'])
# 截取列
meg = meg[['newest_name','gd_city','county_id','region_name']]

# meg_test =meg.groupby(['gd_city'])['newest_name'].count().reset_index()


# In[5]:
# 计算数据条数
geo = geography[['gd_city']].drop_duplicates(inplace=False)
df0 = deal[['issue_code','gd_city','room_sum','room_num']]
df0 = pd.merge(geo,df0,how='left', on=['gd_city'])
# df0 = df0.groupby(['issue_code','gd_city','room_sum'])['room_num'].min().reset_index()
# df0_test =df0.groupby(['gd_city'])['issue_code'].count().reset_index()

# In[2]:
#城市预售套数
# 截取列
df1 = df0[['issue_code','gd_city','room_sum']]
df1 = df1.replace ( 'None' , '0' , regex = True )
df1 = df1.replace ( '套' , '' , regex = True )
df1 = df1.replace ( '户' , '' , regex = True )
df1 = df1.replace ( '（A地块）' , '' , regex = True )
df1 = df1.replace ( '\+104套商铺' , '' , regex = True )
df1 = df1.replace ( '套公寓' , '' , regex = True )
df1 = df1.replace ( '\+104商铺' , '' , regex = True )
df1 = df1.replace ( '公寓' , '' , regex = True )
df1 = df1.replace ( '约' , '' , regex = True )
df1 = df1.replace ( '（住宅）' , '' , regex = True )
df1 = df1.replace ( '' , '0' , regex = True )
df1.at[df1['room_sum'].isnull(),'room_sum']=0
df1[['room_sum']] = df1[['room_sum']].astype('int')
# df2 = df1[df1['room_sum'] < 5000]
# 分组求每个城市的供应套数
city_room_num0 = df1.groupby('gd_city')['room_sum'].sum().reset_index()
#房间数量
# 截取列
df3 = df0[['issue_code','gd_city','room_num']]
# 分组求每个城市的供应套数
city_room_num1 = df3.groupby('gd_city')['room_num'].sum().reset_index()
city_room_num1[['room_num']] = city_room_num1[['room_num']].astype('int')


# In[3]:
#取标准值
city_room_num = pd.merge(city_room_num0, city_room_num1, how='left', on=['gd_city'])
# city_room_num = two_c_v_max(city_room_num)




city_room_num.columns = ['id','one','two']
city_room_num['three'] = city_room_num.one > city_room_num.two
city_room_num.at[city_room_num['three'] =="True",'four']= city_room_num['one']
city_room_num.at[city_room_num['three'] !="True",'four']= city_room_num['two']


# In[4]:
#过滤楼盘
# 合并信息
meg_re = pd.merge(deal_ls, meg, how='right', on=['newest_name','gd_city'])
# # 通过floor_name过滤
df00 =  pd.merge(deal, meg_re, how='right', on=['floor_name','gd_city'])
df00.drop_duplicates(inplace=True)
# df_test = df[df['room_sum'] >= '0']
#区域预售套数
# 截取列
df01 = df00[['issue_code','gd_city','county_id','region_name','room_sum']]
df01 = df01.replace ( 'None' , '0' , regex = True )
df01 = df01.replace ( '套' , '' , regex = True )
df01 = df01.replace ( '户' , '' , regex = True )
df01 = df01.replace ( '（A地块）' , '' , regex = True )
df01 = df01.replace ( '约' , '' , regex = True )
df01 = df01.replace ( '' , '0' , regex = True )
df01.at[df01['room_sum'].isnull(),'room_sum']=0
df01[['room_sum']] = df01[['room_sum']].astype('int')
# df02 = df01[df01['room_sum'] < 5000]
# 分组求每个城市的供应套数
city_room_num01 = df01.groupby(['gd_city','county_id','region_name'])['room_sum'].sum().reset_index()
#区域房间数量
# 截取列
df03 = df00[['issue_code','gd_city','county_id','region_name','room_num']]
# 分组求每个城市的供应套数
city_room_num02 = df03.groupby(['gd_city','county_id','region_name'])['room_num'].sum().reset_index()
city_room_num02[['room_num']] = city_room_num02[['room_num']].astype('int')


# In[5]:
#取标准值
city_room_num = pd.merge(city_room_num01, city_room_num02, how='left', on=['gd_city','county_id','region_name'])
city_room_num = four_c_v_max(city_room_num)
#增加季度
city_room_num['p'] = date_quarter
city_room_num_result = city_room_num[['id01','id03','id02','p','four']]
city_room_num_result.columns = ['city_name','county_name','city_id','date','value']


# In[6]:
#区域预售套数
#按照月统计供应套数




# In[7]:

city_room_num_result = city_room_num_result.drop_duplicates()
# to_dws(city_room_num_result,table_name)
# ori1 = ori.loc[ori['provide_sche'].str.contains("\^")]
city_room_num.to_excel('C:\\Users\\86133\\Desktop\\city_room_num.xlsx')



print('>> Done!') #完毕

