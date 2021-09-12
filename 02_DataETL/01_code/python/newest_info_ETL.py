#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Jul 13 17:44:47 2021

项目报告-项目信息-规划信息  --占地面积、建筑面积数据做合理性判断清洗,车位配比字段显示不全记为脏数据清洗掉    ps:脏数据用默认值来代替
     
"""
import configparser
import os
import sys
from numpy.lib.function_base import append
from pandas.core import groupby
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
table_name = "dws_newest_info"
# database = "temp_db"

##通过输入的参数的方式获取变量值##  如果不需要使用输入参数的方式，可以不用这段
opts,args=getopt.getopt(sys.argv[1:],"t:q:s:e:d:",["table=","quarter=","startdate=","enddate=","database="])
for opts,arg in opts:
  if opts=="-t" or opts=="--table":
    table_name = arg
  elif opts=="-d" or opts=="--database":
    database = arg

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

##车位数清洗逻辑
def sum_str(s):
    new_str = ""  		#创建一个空字符串
    for ch in s:
	    if ch.isdigit():		#字符串中的方法，可以直接判断ch是否是数字
		    new_str += ch
	    else:
		    new_str += " "
    sub_list = new_str.split()   #对新的字符串切片
    num_list = list(map(int, sub_list)) 	#map方法，使列表中的元素按照指定方式转变
    res  = sum(num_list)
    # print(res)
    return res


##正式代码##
"""
1>  dws_newest_info
        land_area | building_area    按照ods层表更新一遍
        household_num                按照ods层表更新一遍
        volume_rate                  按照ods层表更新一遍
        park_rate                    按照ods层表更新一遍
        park_num                     按照ods层表更新一遍
    更新默认值 

2>  dwb_newest_info
        land_area | building_area    按照ods层表更新一遍
        household_num                按照ods层表更新一遍
        volume_rate                  按照ods层表更新一遍
        park_rate                    按照ods层表更新一遍
        park_num                     按照ods层表更新一遍
    更新默认值 
"""



con = MysqlClient(db_host,database,user,password)
# In[1]:
#dws_newest_info#
                # newest_id       楼盘id
                # land_area       占地面积
                # building_area   建筑面积
                # volume_rate     容积率
                # household_num   规划户数
                # park_rate       车位配比
                # park_num        车位数

dw_newest=con.query("SELECT * FROM  "+database+"."+table_name)
dw_newest.drop_duplicates(inplace=True)
#ori_newest_info_base#
                # newest_id       楼盘id
                # land_area       占地面积
                # building_area   建筑面积
                # volume_rate     容积率
                # household_num   规划户数
                # park_rate       车位配比
                # park_num        车位数

ods_newest=con.query('''SELECT uuid,land_area,building_area,volume_rate,household_num,park_rate,park_num FROM odsdb.ori_newest_info_base''')
ods_newest.drop_duplicates(inplace=True)
id = dw_newest[['newest_id']]


# In[2]:
#land_area | building_area    按照ods层表更新一遍
# 截取列
lb_area = ods_newest[['uuid','land_area','building_area']]
lb_area.columns = ['newest_id','land_area','building_area']
# 获取楼盘占地面积和建筑面积
lb_area_r = pd.merge(id,lb_area,how='left',on=['newest_id'])
# 修改数据类型（脏数据先处理）
lb_area_r = lb_area_r.replace ( ',' , '' , regex = True )
lb_area_r = lb_area_r.replace ( '暂无信息' , '' , regex = True )
lb_area_r['land_area_1']  = lb_area_r['land_area'].map(lambda x:x. split('.')[0])
lb_area_r.at[lb_area_r['land_area_1'] == '','land_area_1']=0
lb_area_r[['land_area_1']] = lb_area_r[['land_area_1']].astype('int')
# 建筑面积一样的处理
lb_area_r['building_area_1']  = lb_area_r['building_area'].map(lambda x:x. split('.')[0])
lb_area_r.at[lb_area_r['building_area_1'] == '','building_area_1']=0
lb_area_r[['building_area_1']] = lb_area_r[['building_area_1']].astype('int')
# 截取列，修改列名
area_r = lb_area_r[['newest_id','land_area_1','building_area_1']]
area_r.columns = ['newest_id','land_area','building_area']


# In[3]:
#household_num                按照ods层表更新一遍
# 截取列
hn = ods_newest[['uuid','household_num']]
hn.columns = ['newest_id','household_num']
# 关联获取ods层表数据
hn_r = pd.merge(id,hn,how='left',on=['newest_id'])
# 修改数据类型
hn_r.at[hn_r['household_num'] == '','household_num']=0
hn_r[['household_num']] = hn_r[['household_num']].astype('int')


# In[4]:
# volume_rate  按照ods层表更新一遍
# 截取列
vrate = ods_newest[['uuid','volume_rate']]
vrate.columns = ['newest_id','volume_rate']
# 关联获取ods层表数据
vrate_r = pd.merge(id,vrate,how='left',on=['newest_id'])
# 关联获取ods层表数据(先去除脏数据和转换数据类型)
vrate_r = vrate_r.replace ( '暂无信息' , '' , regex = True )
vrate_r.at[vrate_r['volume_rate'] == '','volume_rate']=0
vrate_r[['volume_rate']] =  vrate_r[['volume_rate']].astype('float')


# In[5]:
# park_rate  按照ods层表更新一遍
# 截取列
park = ods_newest[['uuid','park_rate']]
park.columns = ['newest_id','park_rate']
# 关联获取ods层表数据
park_r = pd.merge(id,park,how='left',on=['newest_id'])
# 替换脏替换字符
park_r = park_r.replace ( '：' , ':' , regex = True )
park_r = park_r.replace ( '∶' , ':' , regex = True )
park_r = park_r.replace ( '楼栋总数:' , '' , regex = True )
park_r = park_r.replace ( '车位预计总数1706个，地上249个，地下1457个，车位配比为' , '' , regex = True )
park_r = park_r.replace ( '车位总数:1470' , '' , regex = True )
park_r = park_r.replace ( '共207个停车位，车位配比为' , '' , regex = True )
park_r = park_r.replace ( '共1362个，均为地下停车位，车位配比' , '' , regex = True )
park_r = park_r.replace ( '共926个停车位，其中地上63个，地下863个，车位配比为' , '' , regex = True )
park_r = park_r.replace ( '共156个停车位，其中地上0个，地下156个，车位配比为' , '' , regex = True )
park_r = park_r.replace ( '915个（地上64、地下851），车位配比' , '' , regex = True )
park_r = park_r.replace ( '车位配比' , '' , regex = True )
park_r = park_r.replace ( '以上' , '' , regex = True )
park_r = park_r.replace ( '车位共693个，车位配比为' , '' , regex = True )
park_r = park_r.replace ( '，共3100个车位，其中地上310个,地下2790个' , '' , regex = True )
park_r = park_r.replace ( '共1375个停车位（地下车位1261），车位配比为' , '' , regex = True )
park_r = park_r.replace ( '车位比' , '' , regex = True )
park_r = park_r.replace ( '共1472个停车位，其中地上206个，地下1269个，车位配比为' , '' , regex = True )
park_r = park_r.replace ( '车位比约' , '' , regex = True )
park_r = park_r.replace ( '共1521个停车位，均为地下车位，车位配比' , '' , regex = True )
park_r = park_r.replace ( '地下停车共计732个,车位比' , '' , regex = True )
park_r = park_r.replace ( '1632个车位' , '' , regex = True )
park_r = park_r.replace ( '车位共693个，为' , '' , regex = True )
park_r = park_r.replace ( '共1375个停车位（地下车位1261），为' , '' , regex = True )
park_r = park_r.replace ( '共1472个停车位，其中地上206个，地下1269个，为' , '' , regex = True )
park_r = park_r.replace ( '400个车位' , '' , regex = True )
park_r = park_r.replace ( '车位数:892' , '' , regex = True )
park_r = park_r.replace ( '共1521个停车位，均为地下车位，' , '' , regex = True )
park_r = park_r.replace ( '地下停车共计732个,' , '' , regex = True )
park_r = park_r.replace ( '1600个车位' , '' , regex = True )
park_r = park_r.replace ( '约' , '' , regex = True )
park_r = park_r.replace ( '共计634个，其中地下:510个，地上临停124个（含会所）' , '' , regex = True )
park_r = park_r.replace ( '为' , '' , regex = True )
park_r = park_r.replace ( '住宅:2165个，' , '' , regex = True )
park_r = park_r.replace ( '住宅停车位:1300个（纯地下）。' , '' , regex = True )
park_r = park_r.replace ( '楂樺眰' , '' , regex = True )
park_r = park_r.replace ( '1.1.1' , '1.1' , regex = True )
park_r = park_r.replace ( '1.0.93' , '1' , regex = True )
park_r = park_r.replace ( '，' , '' , regex = True )
park_r = park_r.replace ( '1.0.93' , '1' , regex = True )
park_r = park_r.replace ( '，' , '' , regex = True )
park_r = park_r.replace ( '高层' , '' , regex = True )
park_r['park_rate1'] = park_r['park_rate'].apply(lambda x:re.sub("\（.*[^\u4E00-\u9FA5][\u4E00-\u9FA5]{1,3}.*[^\u4E00-\u9FA5]", "", x))
park_r['park_rate2'] = park_r['park_rate1'].apply(lambda x:re.sub("\.*[^\u4E00-\u9FA5][\u4E00-\u9FA5]{1,3}.*[^\u4E00-\u9FA5]", "", x))
park_r['park_rate3'] = park_r['park_rate2'].apply(lambda x:re.sub("\(\d{1,9}.*", "", x))
park_r['park_rate4'] = park_r['park_rate3'].apply(lambda x:re.sub("\[^\u4E00-\u9FA5]", "", x))
park_r['park_rate4'] = park_r['park_rate4'].replace ( 'undefined' , '0' , regex = True )
park_r['park_rate4']  = park_r['park_rate4'].replace ( '地下负一层人防车位:423个' , '' , regex = True )
park_r['park_rate4']  = park_r['park_rate4'].replace ( '0.802083333333333' , '0' , regex = True )
park_r['park_rate4']  = park_r['park_rate4'].replace ( '0.111111111111111' , '0' , regex = True )
park_r['park_rate4']  = park_r['park_rate4'].replace ( '地上车位数:65' , '' , regex = True )
park_r['park_rate4']  = park_r['park_rate4'].replace ( '0.125' , '0' , regex = True )
park_r['park_rate4']  = park_r['park_rate4'].replace ( ':1:1位' , '1:1.2' , regex = True )
park_r['park_rate4']  = park_r['park_rate4'].replace ( '0.802083333' , '0' , regex = True )
park_r['park_rate4']  = park_r['park_rate4'].replace ( '0.111111111' , '0' , regex = True )
park_r['park_rate4']  = park_r['park_rate4'].replace ( '住宅:1' , '' , regex = True )
park_r['park_rate4']  = park_r['park_rate4'].replace ( '地上:2个' , '' , regex = True )
park_r['park_rate4']  = park_r['park_rate4'].replace ( ':1:1' , '1:1.2' , regex = True )
park_r['park_rate4']  = park_r['park_rate4'].replace ( '停1:1.3' , '1:1.3' , regex = True )

# park_r_test = park_r[park_r['park_rate3'] == '1:undefined' ]


# 计算分隔符数量
park_r['park_rate_num'] = park_r['park_rate4'].map(lambda x:len(x.split(':')))
# 根据分隔符分开存放
park_r_1 = park_r[park_r['park_rate_num'] == 1]
park_r_2 = park_r[park_r['park_rate_num'] == 2]
park_r_3 = park_r[park_r['park_rate_num'] == 3]
#处理没有分隔符的数据（全部替换为空）
park_r_1.park_rate = ''
#处理1个分隔符的数据（且分开，去0，再拼接）
# 切分
park_r_2['park_rate_1'] = park_r_2['park_rate4'].map(lambda x:x. split(':')[0])
park_r_2['park_rate_2'] = park_r_2['park_rate4'].map(lambda x:x. split(':')[1])
# 去0
park_r_2['park_rate_1'] =  park_r_2[['park_rate_1']].astype('float')
park_r_2['park_rate_1'] =  park_r_2[['park_rate_1']].astype(str)
park_r_2.at[park_r_2['park_rate_2'] == '','park_rate_2'] = '0'
park_r_2['park_rate_2'] =  park_r_2[['park_rate_2']].astype('float')
park_r_2['park_rate_2'] =  park_r_2[['park_rate_2']].astype(str)
# 拼接
park_r_2['park_rate'] = park_r_2['park_rate_1'] + ':' + park_r_2['park_rate_2']
#处理2个分隔符的数据（且分开，去0，再拼接）
# 切分
park_r_3['park_rate_1'] = park_r_3['park_rate4'].map(lambda x:x. split(':')[0])
park_r_3['park_rate_2'] = park_r_3['park_rate4'].map(lambda x:x. split(':')[1])
park_r_3['park_rate_3'] = park_r_3['park_rate4'].map(lambda x:x. split(':')[2])
# 去0
park_r_3.at[park_r_3['park_rate_1'] == '','park_rate_1'] = '0'
park_r_3['park_rate_1'] =  park_r_3[['park_rate_1']].astype('float')
park_r_3['park_rate_1'] =  park_r_3[['park_rate_1']].astype(str)
park_r_3['park_rate_2'] =  park_r_3[['park_rate_2']].astype('float')
park_r_3['park_rate_2'] =  park_r_3[['park_rate_2']].astype(str)
park_r_3.at[park_r_3['park_rate_3'] == '','park_rate_3'] = '0'
park_r_3['park_rate_3'] =  park_r_3[['park_rate_3']].astype('float')
park_r_3['park_rate_3'] =  park_r_3[['park_rate_3']].astype(str)
# 拼接
park_r_3.at[park_r_3['park_rate_3'] == '0.0','park_rate'] = park_r_3['park_rate_1'] + ':' + park_r_3['park_rate_2']
park_r_3.at[park_r_3['park_rate_2'] == '0.0','park_rate'] = park_r_3['park_rate_1'] + ':' + park_r_3['park_rate_3']
park_r_3.at[park_r_3['park_rate_1'] == '0.0','park_rate'] = park_r_3['park_rate_2'] + ':' + park_r_3['park_rate_3']
# 截取列
park_r_1 = park_r_1[['newest_id','park_rate']]
park_r_2 = park_r_2[['newest_id','park_rate']]
park_r_3 = park_r_3[['newest_id','park_rate']]
# 拼接
pr = pd.concat([park_r_1,park_r_2,park_r_3])


# In[6]:
##车位数清洗##
# 截取列
park_s = ods_newest[['uuid','park_num']]
park_s.columns = ['newest_id','park_num_o']
# 关联获取ods层表数据
park_s_r = pd.merge(id,park_s,how='left',on=['newest_id'])
park_s_r = park_s_r.replace ( '车位配比：1:1.7。' , '' , regex = True )
park_s_r = park_s_r.replace ( '暂无信息' , '' , regex = True )
# park_s_r['park_num'] = park_s_r['park_num'].apply(lambda x:re.sub("\D","",str(x)))
park_s_r['park_num'] = park_s_r['park_num_o'].apply(lambda x:sum_str(x))
##合并##
park_s_r = park_s_r[['newest_id','park_num']]
pr = pd.merge(pr,park_s_r,how='left',on='newest_id')


# In[7]:
#### ===============数据合并=====================  #####
#清除离群数据
area_r.at[area_r['land_area'] < 7000,'land_area'] = -1
area_r.at[area_r['land_area'] > 600000,'land_area'] = -1
area_r.at[area_r['building_area'] < 20000,'building_area'] = -1
area_r.at[area_r['building_area'] > 1200000,'building_area'] = -1
pr.at[pr['park_num'] < 200, 'park_num'] = -1
pr.at[pr['park_num'] > 100000, 'park_num'] = -1
#和结果表拼接
dw_newest
result = dw_newest[['newest_id','newest_sn','newest_name','alias_name','city_id','county_id','address','lng','lat','property_type','property_fee','property_id','building_type','building_num','floor_num','right_term','green_rate','decoration','sales_state','sale_address','opening_date','recent_opening_time','recent_delivery_time','unit_price','dr','create_date','create_user','update_date','update_user','index','jpg']]
result = pd.merge(result,area_r,how='left',on=['newest_id'])
result = pd.merge(result,hn_r,how='left',on=['newest_id'])
result = pd.merge(result,vrate_r,how='left',on=['newest_id'])
result = pd.merge(result,pr,how='left',on=['newest_id'])
#清改列名
result= result[['newest_id','newest_sn','newest_name','alias_name','city_id','county_id','address','lng','lat','property_type','property_fee','property_id','building_type','land_area','building_area','building_num','floor_num','household_num','right_term','green_rate','volume_rate','park_num','park_rate','decoration','sales_state','sale_address','opening_date','recent_opening_time','recent_delivery_time','unit_price','dr','create_date','create_user','update_date','update_user','index','jpg']]


# In[8]:
#### ===============数据加载后更新值=====================  #####
print('>> 开始写入数据') 
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
# 清空表
update_sql1 = "delete from "+database+"."+table_name+" "
cur.execute(update_sql1)
conn.commit() # 提交记
conn.close() # 关闭数据库链接
# 加载数据
to_dws(result,table_name)


# In[9]:
# 去掉 - -1 等数据都替换为null
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
update_sql1 = "UPDATE dws_db."+table_name+" SET park_rate = NULL WHERE park_rate = ''"
cur.execute(update_sql1)
update_sq2 = "UPDATE dws_db."+table_name+" SET park_num = NULL WHERE park_num = '-1'"
cur.execute(update_sq2)
conn.commit() # 提交记
conn.close() # 关闭数据库链接


# In[9]:
# 去掉 - -1 等数据都替换为null
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
update_sql1 = "UPDATE dws_db."+table_name+" SET volume_rate = NULL WHERE volume_rate < 1 or volume_rate > 5"
cur.execute(update_sql1)
update_sq2 = "UPDATE dws_db."+table_name+" SET household_num = NULL WHERE household_num < 100 "
cur.execute(update_sq2)
conn.commit() # 提交记
conn.close() # 关闭数据库链接


# In[9]:
# 去掉 - -1 等数据都替换为null
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
update_sql1 = "UPDATE dws_db."+table_name+" SET land_area = NULL WHERE land_area = '-1'"
cur.execute(update_sql1)
update_sq2 = "UPDATE dws_db."+table_name+" SET building_area = NULL WHERE building_area = '-1'"
cur.execute(update_sq2)
conn.commit() # 提交记
conn.close() # 关闭数据库链接


# In[9]:
# 通话plainfo
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
update_sql1 = "update dws_db.dws_newest_info a , dws_db.dws_newest_planinfo b set b.park_num = a.park_num where a.newest_id = b.newest_id and a.newest_name = b.newest_name and a.city_id = b.city_id"
cur.execute(update_sql1)
update_sql1 = "update dws_db.dws_newest_info a , dws_db.dws_newest_planinfo b set b.park_rate = a.park_rate where a.newest_id = b.newest_id and a.newest_name = b.newest_name and a.city_id = b.city_id"
cur.execute(update_sql1)
conn.commit() # 提交记
conn.close() # 关闭数据库链接


# In[9]:
# 通话plainfo
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
update_sql1 = "update dws_db.dws_newest_info a , dws_db.dws_newest_planinfo b set b.volume_rate = a.volume_rate where a.newest_id = b.newest_id and a.newest_name = b.newest_name and a.city_id = b.city_id"
cur.execute(update_sql1)
update_sql1 = "update dws_db.dws_newest_info a , dws_db.dws_newest_planinfo b set b.household_num = a.household_num where a.newest_id = b.newest_id and a.newest_name = b.newest_name and a.city_id = b.city_id"
cur.execute(update_sql1)
conn.commit() # 提交记
conn.close() # 关闭数据库链接
print('>> Done!') #完毕

# pr.to_csv('C:\\Users\\86133\\Desktop\\pr.csv')

