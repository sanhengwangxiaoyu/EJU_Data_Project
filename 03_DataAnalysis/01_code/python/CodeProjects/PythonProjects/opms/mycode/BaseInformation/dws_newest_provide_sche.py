#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Mar 26 16:44:47 2021
Created on Jun 17 10:44:47 2021
                    添加第86行 ：   清除动态里的展开更多这几个字
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

cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
# database = 'temp_db'
date_quarter = '2021Q1'    #  获取季度（统计周期）
table_name = 'dws_newest_provide_sche'
opts,args=getopt.getopt(sys.argv[1:],"t:q:s:e:",["table=","quarter=","startdate=","enddate="])
for opts,arg in opts:
  if opts=="-t" or opts=="--table":
    table_name = arg
  elif opts=="-q" or opts=="--quarter":
    date_quarter = arg





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

def to_dws(result,table):
    engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database+'?charset=utf8')
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)


# In[0]:

# 楼盘动态表 # dws_newest_provide_sche
# 第一步需要先从楼盘表 ori_newest_info_base 将provide_sche字段拆分到临时表 tmp_newest_provide_sche 中
# 从临时表中将数据的标题，动态，时间拆分出来


#意向客户总量#
con = MysqlClient(db_host,database,user,password)
# ori_newest_info_base  新房-贝壳未去重结果集 
#                                                      uuid             楼盘id
#                                                      provide_sche     加推时间/楼盘时刻/楼盘动态d 
ori=con.query('''SELECT uuid,city_id,provide_sche FROM odsdb.ori_newest_info_base WHERE provide_sche NOT IN ('') OR provide_sche IS NULL''')

 # 去空
# In[1]:
# 去重 
ori = ori.drop_duplicates()
# 替换颜文字
ori = ori.replace ( '\(\*\^__\^\*\)' , '-不好意思-' , regex = True )
ori = ori.replace ( '\(\〃\^\ω\^\)' , '-哦吼-' , regex = True )
ori = ori.replace ( '\ヾ\(\@\^\▽\^\@\)\ノ' , '-呀吼-' , regex = True )
ori = ori.replace ( '\~\ヽ\(\^\ω\^\)\ﾉ' , '-咦吼-' , regex = True )
ori = ori.replace ( '\\(\^o\^\)/\~' , '-吼吼-' , regex = True )
ori = ori.replace ( '展开更多' , '' , regex = True )
ori = ori.replace ( '更多详情' , '' , regex = True )
ori = ori.replace ( '更多内容' , '' , regex = True )
ori = ori.replace ( '更多信息' , '' , regex = True )
ori = ori.replace ( '更多详细信息' , '' , regex = True )
ori = ori.replace ( '更多咨询售楼处' , '' , regex = True )
ori = ori.replace ( '更多讯息详询' , '' , regex = True )
ori = ori.replace ( '，。' , '' , regex = True )
ori = ori.replace ( '【关注济宁吉屋网，了解更多实时资讯】' , '' , regex = True )
ori = ori.replace ( '售楼处' , '' , regex = True )
ori = ori.replace ( '展开全文' , '' , regex = True )
ori = ori.replace ( '吉屋网' , '克而瑞' , regex = True )
ori = ori.replace ( '贝壳网' , '克而瑞' , regex = True )
ori = ori.replace ( '搜狐网' , '克而瑞' , regex = True )
ori = ori.replace ( '温州房天下网讯' , '克而瑞' , regex = True )
ori = ori.replace ( '搜狐' , '克而瑞' , regex = True )
ori = ori.replace ( '贝壳' , '克而瑞' , regex = True )
ori = ori.replace ( '房天下' , '克而瑞' , regex = True )
ori = ori.replace ( '吉屋' , '克而瑞' , regex = True )
# 特殊脏数据处理
ori_3row_1 = ori.loc[ori['provide_sche'].str.contains("欢乐大世界\^")]
ori_3row_2 = ori.loc[ori['provide_sche'].str.contains("132\^136㎡")]
ori_3row_3 = ori.loc[ori['provide_sche'].str.contains("不容缓O\^")]
# 合并
ori_3row = ori_3row_1.append([ori_3row_2,ori_3row_3],ignore_index=True)
# 分隔符替换
ori_3row = ori_3row.replace ( '\^\【' , '||_^_||【' , regex = True )
ori_3row = ori_3row.replace ( '\^\[' , '||_^_||[' , regex = True )
ori_3row = ori_3row.replace ( '\^海' , '||_^_||海' , regex = True )
# 切得，一行变多行
ori_3row =  ori_3row.drop('provide_sche',axis=1).join(ori_3row['provide_sche'].str.split('\|\|\_\^\_\|\|',expand=True).stack().reset_index(level=1,drop=True).rename('provide_sche')).reset_index(drop=True)
# 3条脏数据揪出大部队
ori = ori.loc[~ori['provide_sche'].str.contains("欢乐大世界\^")]
ori = ori.loc[~ori['provide_sche'].str.contains("不容缓O\^")]
ori = ori.loc[~ori['provide_sche'].str.contains("132\^136㎡")]
ori_3row

# In[11]:
# 一行变多行
ori_only_row =  ori.drop('provide_sche',axis=1).join(ori['provide_sche'].str.split('\^',expand=True).stack().reset_index(level=1,drop=True).rename('provide_sche')).reset_index(drop=True)
# 拼接
ori_only_row =pd.concat([ori_only_row,ori_3row])
ori_only_row
#去重复
ori_only_row.drop_duplicates()
# 不包含[|
ori_only_row_1 = ori_only_row.loc[~ori_only_row['provide_sche'].str.contains("\[\|")]
# 包含[|
ori_only_row_2 = ori_only_row.loc[ori_only_row['provide_sche'].str.contains("\[\|")]
ori_only_row_1

#进一步清洗
# In[2]:
#不包含[|
# 获取有多少个|
ori_only_row_1['provide_sche_num'] = ori_only_row_1['provide_sche'].map(lambda x:len(x.split('|')))
# ori_only_row_1['provide_sche1'] = ori_only_row_1['provide_sche'].map(lambda x:x.split('|'))
# 1列
ori_only_row_1_1c = ori_only_row_1[ori_only_row_1['provide_sche_num'] == 1]
ori_only_row_1_1c['provide_sche'] = ori_only_row_1_1c['provide_sche'].str.replace('暂无', '')
ori_only_row_1_1c['provide_sche'] = ori_only_row_1_1c['provide_sche'].str.replace('年', '')
ori_only_row_1_1c['provide_sche'] = ori_only_row_1_1c['provide_sche'].str.replace('雅居乐新乐府', '')
# 2列
ori_only_row_1_2c = ori_only_row_1[ori_only_row_1['provide_sche_num'] == 2]
ori_only_row_1_2c['provide_sche'] = ori_only_row_1_2c['provide_sche'].str.replace('润扬观澜鹭岛已取证\|【润扬', ' | ')
ori_only_row_1_2c['provide_sche'] = ori_only_row_1_2c['provide_sche'].str.replace('174-226m²#晋安湖畔院墅\|', ' | ')
# 3列 # 一行当中正常的
ori_only_row_1_3c = ori_only_row_1[ori_only_row_1['provide_sche_num'] == 3]
# # 4列
ori_only_row_1_4c = ori_only_row_1[ori_only_row_1['provide_sche_num'] == 4]
# 5列
ori_only_row_1_5c = ori_only_row_1[ori_only_row_1['provide_sche_num'] == 5]
# 6列
ori_only_row_1_6c = ori_only_row_1[ori_only_row_1['provide_sche_num'] >= 6]
# 7列
ori_only_row_1_7c = ori_only_row_1[ori_only_row_1['provide_sche_num'] == 7]
# 8列
ori_only_row_1_8c = ori_only_row_1[ori_only_row_1['provide_sche_num'] == 8]
# 9列
ori_only_row_1_9c = ori_only_row_1[ori_only_row_1['provide_sche_num'] == 9]
# 11列
ori_only_row_1_11c = ori_only_row_1[ori_only_row_1['provide_sche_num'] == 11]
# 12列
ori_only_row_1_12c = ori_only_row_1[ori_only_row_1['provide_sche_num'] == 12]
# 13列
ori_only_row_1_13c = ori_only_row_1[ori_only_row_1['provide_sche_num'] == 13]
# 14列
ori_only_row_1_14c = ori_only_row_1[ori_only_row_1['provide_sche_num'] == 14]
# 15列
ori_only_row_1_15c = ori_only_row_1[ori_only_row_1['provide_sche_num'] == 15]
# 16列
ori_only_row_1_16c = ori_only_row_1[ori_only_row_1['provide_sche_num'] == 16]

ori_only_row_1_6c

# In[12]:
# 不包含[\的拆分结果
#切分结果1个
sche_result_only_row_1_1c = ori_only_row_1_1c[['uuid','provide_sche']]
# 获取时间
sche_result_only_row_1_1c['date']  = ori_only_row_1_1c['provide_sche'].map(lambda x:x. split('|')[0])
# sche_result_only_row_1_1c['date'] = sche_result_only_row_1_1c['date'].str.replace('年', '-01-01')
# 给标题和正文添加值
sche_result_only_row_1_1c['provide_title'] = ''
sche_result_only_row_1_1c['provide_sche1'] = ''
# 截取字段
sche_result_only_row_1_1c = sche_result_only_row_1_1c[['uuid','date','provide_title','provide_sche1']]
sche_result_only_row_1_1c
#切分结果2个
sche_result_only_row_1_2c = ori_only_row_1_2c[['uuid','provide_sche']]
# 获取标题和正文的值
sche_result_only_row_1_2c['provide_title'] = ori_only_row_1_2c['provide_sche'].map(lambda x:x. split('|')[0])
sche_result_only_row_1_2c['provide_sche1'] = ori_only_row_1_2c['provide_sche'].map(lambda x:x. split('|')[1])
# 时间为空
sche_result_only_row_1_2c['date'] = ''
sche_result_only_row_1_2c = sche_result_only_row_1_2c[['uuid','date','provide_title','provide_sche1']]
sche_result_only_row_1_2c

# In[15]:
#切分结果3个
sche_result_only_row_1_3c = ori_only_row_1_3c[['uuid','provide_sche']]
# sche_result_only_row_1_3c['provide_title'] = ori_only_row_1_3c['provide_sche'].map(lambda x:x. split('|')[0])
# sche_result_only_row_1_3c['provide_sche1'] = ori_only_row_1_3c['provide_sche'].map(lambda x:x. split('|')[1])
# sche_result_only_row_1_3c['date'] = ori_only_row_1_3c['provide_sche'].map(lambda x:x. split('|')[2])
sche_result_only_row_1_3c['tmp1'] = ori_only_row_1_3c['provide_sche'].map(lambda x:x. split('|')[0])
sche_result_only_row_1_3c['tmp2'] = ori_only_row_1_3c['provide_sche'].map(lambda x:x. split('|')[1])
sche_result_only_row_1_3c['tmp3'] = ori_only_row_1_3c['provide_sche'].map(lambda x:x. split('|')[2])

# 判断tmp3带有汉字的
sche_result_only_row_1_3c_1 = sche_result_only_row_1_3c.loc[sche_result_only_row_1_3c['tmp3'].str.contains("[\u4e00-\u9fa5]")]
# 判断tmp2 时间格式 mm/ddyyyyy
sche_result_only_row_1_3c_2 = sche_result_only_row_1_3c_1.loc[sche_result_only_row_1_3c_1['tmp2'].str.contains("(\d{2})\/(\d{6})")]
sche_result_only_row_1_3c_2['provide_title'] = sche_result_only_row_1_3c_2['provide_sche'].map(lambda x:x. split('|')[0])
sche_result_only_row_1_3c_2['provide_sche1'] = sche_result_only_row_1_3c_2['provide_sche'].map(lambda x:x. split('|')[2])
sche_result_only_row_1_3c_2['date'] = ''
# 判断tmp2 时间格式不为 mm/ddyyyyy
sche_result_only_row_1_3c_3 = sche_result_only_row_1_3c_1.loc[~sche_result_only_row_1_3c_1['tmp2'].str.contains("(\d{2})\/(\d{6})")]
sche_result_only_row_1_3c_3['provide_title'] = sche_result_only_row_1_3c_3['provide_sche'].map(lambda x:x. split('|')[0])
sche_result_only_row_1_3c_3['provide_sche1'] = sche_result_only_row_1_3c_3['provide_sche'].map(lambda x:x. split('|')[1])
sche_result_only_row_1_3c_3['date'] = ''
# 正常的  # # 判断tmp3不带有汉字的
sche_result_only_row_1_3c_4 = sche_result_only_row_1_3c.loc[~sche_result_only_row_1_3c['tmp3'].str.contains("[\u4e00-\u9fa5]")]
sche_result_only_row_1_3c_4['provide_title'] = sche_result_only_row_1_3c_4['provide_sche'].map(lambda x:x. split('|')[0])
sche_result_only_row_1_3c_4['provide_sche1'] = sche_result_only_row_1_3c_4['provide_sche'].map(lambda x:x. split('|')[1])
sche_result_only_row_1_3c_4['date'] = sche_result_only_row_1_3c_4['provide_sche'].map(lambda x:x. split('|')[2])
sche_result_only_row_1_3c_4
# 去字段
sche_result_only_row_1_3c_4 = sche_result_only_row_1_3c_4[['uuid','date','provide_title','provide_sche1']]
sche_result_only_row_1_3c_3 = sche_result_only_row_1_3c_3[['uuid','date','provide_title','provide_sche1']]
sche_result_only_row_1_3c_2 = sche_result_only_row_1_3c_2[['uuid','date','provide_title','provide_sche1']]


# 拼接
sche_result_only_row_1_3c_4 = sche_result_only_row_1_3c_4.append([sche_result_only_row_1_3c_3,
                                                                sche_result_only_row_1_3c_2],
                                                                ignore_index=True)
# 展示
sche_result_only_row_1_3c_4



# In[13]:
#切分结果4个
sche_result_only_row_1_4c = ori_only_row_1_4c[['uuid','provide_sche']]
sche_result_only_row_1_4c['tmp1'] = ori_only_row_1_4c['provide_sche'].map(lambda x:x. split('|')[0])
sche_result_only_row_1_4c['tmp2'] = ori_only_row_1_4c['provide_sche'].map(lambda x:x. split('|')[1])
sche_result_only_row_1_4c['tmp3'] = ori_only_row_1_4c['provide_sche'].map(lambda x:x. split('|')[2])
sche_result_only_row_1_4c['tmp4'] = ori_only_row_1_4c['provide_sche'].map(lambda x:x. split('|')[3])
sche_result_only_row_1_4c
#动态标题处理
#tmp2为时间的数据和其他数据分开
# 时间字段长度
sche_result_only_row_1_4c['tmp2_num'] = sche_result_only_row_1_4c['tmp2'].map(lambda x:len(x))
#太短的
sche_result_only_row_1_4c_0 = sche_result_only_row_1_4c[sche_result_only_row_1_4c['tmp2_num'] < 8]
sche_result_only_row_1_4c_0['tmp4'] = ''
#长度正常的
sche_result_only_row_1_4c_1 = sche_result_only_row_1_4c[sche_result_only_row_1_4c['tmp2_num'] >= 8]
# 时间格式不是 mm/ddyyyyy的
sche_result_only_row_1_4c_2 = sche_result_only_row_1_4c_1.loc[~sche_result_only_row_1_4c_1['tmp2'].str.contains("(\d{2})\/(\d{6})")]
# 时间格式是 mm/ddyyyyy的
sche_result_only_row_1_4c_3 = sche_result_only_row_1_4c_1.loc[sche_result_only_row_1_4c_1['tmp2'].str.contains("(\d{2})\/(\d{6})")]
# 拼接在一起
sche_result_only_row_1_4c_2 = sche_result_only_row_1_4c_2.append(sche_result_only_row_1_4c_0 ,ignore_index=True)
sche_result_only_row_1_4c_2
# 标题拼接
sche_result_only_row_1_4c_2['provide_title'] = sche_result_only_row_1_4c_2['tmp1']+sche_result_only_row_1_4c_2['tmp2']
sche_result_only_row_1_4c_2['provide_sche1'] = sche_result_only_row_1_4c_2['tmp3']
sche_result_only_row_1_4c_2['date'] = sche_result_only_row_1_4c_2['tmp4']
sche_result_only_row_1_4c_3['provide_title'] = sche_result_only_row_1_4c_3['tmp1']+sche_result_only_row_1_4c_3['tmp4']
sche_result_only_row_1_4c_3['provide_sche1'] = sche_result_only_row_1_4c_3['tmp3']
sche_result_only_row_1_4c_3['date'] = sche_result_only_row_1_4c_3['tmp2']
sche_result_only_row_1_4c_2
#时间处理
sche_result_only_row_1_4c_2['date'] = sche_result_only_row_1_4c_2['date'].str.replace('年', '-')
sche_result_only_row_1_4c_2['date'] = sche_result_only_row_1_4c_2['date'].str.replace('月', '-')
sche_result_only_row_1_4c_2['date'] = sche_result_only_row_1_4c_2['date'].str.replace('日', '')
sche_result_only_row_1_4c_2['date'] = sche_result_only_row_1_4c_2['date'].str.replace('\/', '-')
sche_result_only_row_1_4c_3['date1'] = sche_result_only_row_1_4c_3['date'].map(lambda x:x. split('/')[0])
sche_result_only_row_1_4c_3['date_1'] = sche_result_only_row_1_4c_3['date'].map(lambda x:x. split('/')[1])
sche_result_only_row_1_4c_3['date2'] = sche_result_only_row_1_4c_3['date_1'].apply(lambda x:x[:2]).tolist()
# 日期为00的处理
sche_result_only_row_1_4c_3_00 = sche_result_only_row_1_4c_3.loc[sche_result_only_row_1_4c_3['date2'].str.contains("00")]
sche_result_only_row_1_4c_3 = sche_result_only_row_1_4c_3.loc[~sche_result_only_row_1_4c_3['date2'].str.contains("00")]
sche_result_only_row_1_4c_3_00['date2'] = '01'
sche_result_only_row_1_4c_3 = sche_result_only_row_1_4c_3.append(sche_result_only_row_1_4c_3_00,ignore_index=True)
sche_result_only_row_1_4c_3
sche_result_only_row_1_4c_3['date3'] = sche_result_only_row_1_4c_3['date_1'].apply(lambda x:x[-4:]).tolist()
# 日期不准确的
# 修改06-31的数据
sche_result_only_row_1_4c_3['date4'] = sche_result_only_row_1_4c_3['date1']+'-'+sche_result_only_row_1_4c_3['date2']
sche_result_only_row_1_4c_3_06_31 = sche_result_only_row_1_4c_3.loc[sche_result_only_row_1_4c_3['date4'].str.contains("06-31")]
sche_result_only_row_1_4c_3_06_31['date2'] = '30'
# 修改11-31的数据
sche_result_only_row_1_4c_3['date4'] = sche_result_only_row_1_4c_3['date1']+'-'+sche_result_only_row_1_4c_3['date2']
sche_result_only_row_1_4c_3_11_31 = sche_result_only_row_1_4c_3.loc[sche_result_only_row_1_4c_3['date4'].str.contains("11-31")]
sche_result_only_row_1_4c_3_11_31['date2'] = '30'
# 修改09-31的数据
sche_result_only_row_1_4c_3['date4'] = sche_result_only_row_1_4c_3['date1']+'-'+sche_result_only_row_1_4c_3['date2']
sche_result_only_row_1_4c_3_09_31 = sche_result_only_row_1_4c_3.loc[sche_result_only_row_1_4c_3['date4'].str.contains("09-31")]
sche_result_only_row_1_4c_3_09_31['date2'] = '30'
# 修改04-31的数据
sche_result_only_row_1_4c_3['date4'] = sche_result_only_row_1_4c_3['date1']+'-'+sche_result_only_row_1_4c_3['date2']
sche_result_only_row_1_4c_3_04_31 = sche_result_only_row_1_4c_3.loc[sche_result_only_row_1_4c_3['date4'].str.contains("04-31")]
sche_result_only_row_1_4c_3_04_31['date2'] = '30'
#修改后合并
sche_result_only_row_1_4c_3 = sche_result_only_row_1_4c_3.loc[~sche_result_only_row_1_4c_3['date4'].str.contains("06-31")]
sche_result_only_row_1_4c_3 = sche_result_only_row_1_4c_3.loc[~sche_result_only_row_1_4c_3['date4'].str.contains("11-31")]
sche_result_only_row_1_4c_3 = sche_result_only_row_1_4c_3.loc[~sche_result_only_row_1_4c_3['date4'].str.contains("09-31")]
sche_result_only_row_1_4c_3 = sche_result_only_row_1_4c_3.loc[~sche_result_only_row_1_4c_3['date4'].str.contains("04-31")]
sche_result_only_row_1_4c_3 = sche_result_only_row_1_4c_3.append([sche_result_only_row_1_4c_3_06_31,
                                                                sche_result_only_row_1_4c_3_09_31,
                                                                sche_result_only_row_1_4c_3_04_31,
                                                                sche_result_only_row_1_4c_3_11_31],
                                                                ignore_index=True)

# 拼接日期
sche_result_only_row_1_4c_3['date'] = sche_result_only_row_1_4c_3['date3']+'-'+sche_result_only_row_1_4c_3['date1']+'-'+sche_result_only_row_1_4c_3['date2']
sche_result_only_row_1_4c_3

# 规范字段，合并数据
sche_result_only_row_1_4c_4 = sche_result_only_row_1_4c_3[['uuid','date','provide_title','provide_sche1']]
sche_result_only_row_1_4c_5 = sche_result_only_row_1_4c_2[['uuid','date','provide_title','provide_sche1']]
sche_result_only_row_1_4c_6 =pd.concat([sche_result_only_row_1_4c_5,sche_result_only_row_1_4c_4])
sche_result_only_row_1_4c_6

# In[14]:

# 切分结果5个
sche_result_only_row_1_5c = ori_only_row_1_5c[['uuid','provide_sche']]
sche_result_only_row_1_5c['provide_title'] = ori_only_row_1_5c['provide_sche'].map(lambda x:x. split('|')[0])+ori_only_row_1_5c['provide_sche'].map(lambda x:x. split('|')[1])+ori_only_row_1_5c['provide_sche'].map(lambda x:x. split('|')[2])
sche_result_only_row_1_5c['provide_sche1'] = ori_only_row_1_5c['provide_sche'].map(lambda x:x. split('|')[3])
sche_result_only_row_1_5c['date'] = ori_only_row_1_5c['provide_sche'].map(lambda x:x. split('|')[4])
sche_result_only_row_1_5c = sche_result_only_row_1_5c[['uuid','date','provide_title','provide_sche1']]
#时间处理
# 一行当中时间是yyyy年mm月dd日的
sche_result_only_row_1_5c['date'] = sche_result_only_row_1_5c['date'].str.replace('年', '-')
sche_result_only_row_1_5c['date'] = sche_result_only_row_1_5c['date'].str.replace('月', '-')
sche_result_only_row_1_5c['date'] = sche_result_only_row_1_5c['date'].str.replace('日', '')
sche_result_only_row_1_5c['date'] = sche_result_only_row_1_5c['date'].str.replace('\/', '-')
# 取结果，合并
sche_result_only_row_1_5c = sche_result_only_row_1_5c[['uuid','date','provide_title','provide_sche1']]
sche_result_only_row_1 = pd.concat([sche_result_only_row_1_5c,sche_result_only_row_1_4c_6,sche_result_only_row_1_3c_4
                                  ,sche_result_only_row_1_2c,sche_result_only_row_1_1c])
sche_result_only_row_1


# In[3]:

#包含[\
# 结尾脏符号
sche_result_only_row_2 = ori_only_row_2[['uuid','provide_sche']]
# sche_result_only_row_2['provide_sche1'] =  sche_result_only_row_2['provide_sche'].map(lambda x:x. split('...')[0])
sche_result_only_row_2['provide_sche2'] =  sche_result_only_row_2['provide_sche'].map(lambda x:x. split('[|')[0])
sche_result_only_row_2['provide_sche3'] =  sche_result_only_row_2['provide_sche2'].map(lambda x:x. split('（房天下讯')[0])


#脏数据拔除
sche_result_only_row_2_drop = sche_result_only_row_2.loc[~sche_result_only_row_2['provide_sche3'].str.contains("[\u4e00-\u9fa5]")]
#取差集
#  找到待删除元素所在的位置，返回的是 true or false 序列
sche_result_only_row_2_flag = sche_result_only_row_2['provide_sche3'].isin(sche_result_only_row_2_drop['provide_sche3'])
#  由于我们要取差集，因此对上述序列取反
sche_result_only_row_diff_flag = [not f for f in sche_result_only_row_2_flag]
#  res 为我们所需要的差集
sche_result_only_row_2 = sche_result_only_row_2[sche_result_only_row_diff_flag]
#  重置index
sche_result_only_row_2.index = [i for i in range(len(sche_result_only_row_2))]



#是否有正文
sche_result_only_row_2['provide_sche_num'] = sche_result_only_row_2['provide_sche3'].map(lambda x:len(x.split('|')))
# 1列
# sche_result_only_row_2['provide_sche11'] = sche_result_only_row_2['provide_sche'].map(lambda x:x.split('|'))
# sche_result_only_row_2_1c = sche_result_only_row_2[sche_result_only_row_2['provide_sche_num'] == 1]
# 2列
sche_result_only_row_2_2c = sche_result_only_row_2[sche_result_only_row_2['provide_sche_num'] == 2]
# 3列 # 一行当中正常的
sche_result_only_row_2_3c = sche_result_only_row_2[sche_result_only_row_2['provide_sche_num'] == 3]
# 4列
# sche_result_only_row_2_4c = sche_result_only_row_2[sche_result_only_row_2['provide_sche_num'] > 4]
# 5列
sche_result_only_row_2_5c = sche_result_only_row_2[sche_result_only_row_2['provide_sche_num'] == 5]
# 6列
sche_result_only_row_2_6c = sche_result_only_row_2[sche_result_only_row_2['provide_sche_num'] == 6]
# 7列
sche_result_only_row_2_7c = sche_result_only_row_2[sche_result_only_row_2['provide_sche_num'] == 7]
# 8列
sche_result_only_row_2_8c = sche_result_only_row_2[sche_result_only_row_2['provide_sche_num'] == 8]
# 9列
sche_result_only_row_2_9c = sche_result_only_row_2[sche_result_only_row_2['provide_sche_num'] == 9]
# 11列
sche_result_only_row_2_11c = sche_result_only_row_2[sche_result_only_row_2['provide_sche_num'] == 11]




#动态标题
# sche_result_only_row_2['provide_title'] = sche_result_only_row_2['provide_sche3'].map(lambda x:x. split('|')[0])
sche_result_only_row_2_2c['provide_title'] = sche_result_only_row_2_2c['provide_sche3'].map(lambda x:x. split('|')[0])
sche_result_only_row_2_3c['provide_title'] = sche_result_only_row_2_3c['provide_sche3'].map(lambda x:x. split('|')[0])
sche_result_only_row_2_5c['provide_title'] = sche_result_only_row_2_5c['provide_sche3'].map(lambda x:x. split('|')[0])\
                                        +'|'+sche_result_only_row_2_5c['provide_sche3'].map(lambda x:x. split('|')[1])
sche_result_only_row_2_6c['provide_title'] = sche_result_only_row_2_6c['provide_sche3'].map(lambda x:x. split('|')[0])\
                                        +'|'+sche_result_only_row_2_6c['provide_sche3'].map(lambda x:x. split('|')[1])
sche_result_only_row_2_7c['provide_title'] = sche_result_only_row_2_7c['provide_sche3'].map(lambda x:x. split('|')[0])\
                                        +'|'+sche_result_only_row_2_7c['provide_sche3'].map(lambda x:x. split('|')[1])
sche_result_only_row_2_7c['provide_title'] = sche_result_only_row_2_7c['provide_sche3'].map(lambda x:x. split('|')[0])\
                                        +'|'+sche_result_only_row_2_7c['provide_sche3'].map(lambda x:x. split('|')[1])
sche_result_only_row_2_8c['provide_title'] = sche_result_only_row_2_8c['provide_sche3'].map(lambda x:x. split('|')[0])\
                                        +'|'+sche_result_only_row_2_8c['provide_sche3'].map(lambda x:x. split('|')[1])\
                                        +'|'+sche_result_only_row_2_8c['provide_sche3'].map(lambda x:x. split('|')[2])\
                                        +'|'+sche_result_only_row_2_8c['provide_sche3'].map(lambda x:x. split('|')[3])
sche_result_only_row_2_9c['provide_title'] = sche_result_only_row_2_9c['provide_sche3'].map(lambda x:x. split('|')[0])\
                                        +'|'+sche_result_only_row_2_9c['provide_sche3'].map(lambda x:x. split('|')[1])\
                                        +'|'+sche_result_only_row_2_9c['provide_sche3'].map(lambda x:x. split('|')[2])
sche_result_only_row_2_11c['provide_title'] = sche_result_only_row_2_11c['provide_sche3'].map(lambda x:x. split('|')[0])\
                                        +'|'+sche_result_only_row_2_11c['provide_sche3'].map(lambda x:x. split('|')[1])\
                                        +'|'+sche_result_only_row_2_11c['provide_sche3'].map(lambda x:x. split('|')[2])\
                                        +'|'+sche_result_only_row_2_11c['provide_sche3'].map(lambda x:x. split('|')[3])
#动态正文
# sche_result_only_row_2['provide_sche4'] = sche_result_only_row_2['provide_sche3'].map(lambda x:x. split('|')[1])
sche_result_only_row_2_2c['provide_sche4'] = sche_result_only_row_2_2c['provide_sche3'].map(lambda x:x. split('|')[1])
sche_result_only_row_2_3c['provide_sche4'] = sche_result_only_row_2_3c['provide_sche3'].map(lambda x:x. split('|')[1])\
                                        +'|'+sche_result_only_row_2_3c['provide_sche3'].map(lambda x:x. split('|')[2])
sche_result_only_row_2_5c['provide_sche4'] = sche_result_only_row_2_5c['provide_sche3'].map(lambda x:x. split('|')[2])\
                                        +'|'+sche_result_only_row_2_5c['provide_sche3'].map(lambda x:x. split('|')[3])\
                                        +'|'+sche_result_only_row_2_5c['provide_sche3'].map(lambda x:x. split('|')[4])
sche_result_only_row_2_6c['provide_sche4'] = sche_result_only_row_2_6c['provide_sche3'].map(lambda x:x. split('|')[2])\
                                        +'|'+sche_result_only_row_2_6c['provide_sche3'].map(lambda x:x. split('|')[3])\
                                        +'|'+sche_result_only_row_2_6c['provide_sche3'].map(lambda x:x. split('|')[4])\
                                        +'|'+sche_result_only_row_2_6c['provide_sche3'].map(lambda x:x. split('|')[3])\
                                        +'|'+sche_result_only_row_2_6c['provide_sche3'].map(lambda x:x. split('|')[5])
sche_result_only_row_2_7c['provide_sche4'] = sche_result_only_row_2_7c['provide_sche3'].map(lambda x:x. split('|')[2])\
                                        +'|'+sche_result_only_row_2_7c['provide_sche3'].map(lambda x:x. split('|')[3])\
                                        +'|'+sche_result_only_row_2_7c['provide_sche3'].map(lambda x:x. split('|')[4])\
                                        +'|'+sche_result_only_row_2_7c['provide_sche3'].map(lambda x:x. split('|')[3])\
                                        +'|'+sche_result_only_row_2_7c['provide_sche3'].map(lambda x:x. split('|')[5])\
                                        +'|'+sche_result_only_row_2_7c['provide_sche3'].map(lambda x:x. split('|')[6])
sche_result_only_row_2_8c['provide_sche4'] = sche_result_only_row_2_8c['provide_sche3'].map(lambda x:x. split('|')[4])\
                                        +'|'+sche_result_only_row_2_8c['provide_sche3'].map(lambda x:x. split('|')[5])\
                                        +'|'+sche_result_only_row_2_8c['provide_sche3'].map(lambda x:x. split('|')[6])\
                                        +'|'+sche_result_only_row_2_8c['provide_sche3'].map(lambda x:x. split('|')[7])
sche_result_only_row_2_9c['provide_sche4'] = sche_result_only_row_2_9c['provide_sche3'].map(lambda x:x. split('|')[3])\
                                        +'|'+sche_result_only_row_2_9c['provide_sche3'].map(lambda x:x. split('|')[4])\
                                        +'|'+sche_result_only_row_2_9c['provide_sche3'].map(lambda x:x. split('|')[5])\
                                        +'|'+sche_result_only_row_2_9c['provide_sche3'].map(lambda x:x. split('|')[6])\
                                        +'|'+sche_result_only_row_2_9c['provide_sche3'].map(lambda x:x. split('|')[7])\
                                        +'|'+sche_result_only_row_2_9c['provide_sche3'].map(lambda x:x. split('|')[8])
sche_result_only_row_2_11c['provide_sche4'] = sche_result_only_row_2_11c['provide_sche3'].map(lambda x:x. split('|')[4])\
                                        +'|'+sche_result_only_row_2_11c['provide_sche3'].map(lambda x:x. split('|')[5])\
                                        +'|'+sche_result_only_row_2_11c['provide_sche3'].map(lambda x:x. split('|')[6])\
                                        +'|'+sche_result_only_row_2_11c['provide_sche3'].map(lambda x:x. split('|')[7])\
                                        +'|'+sche_result_only_row_2_11c['provide_sche3'].map(lambda x:x. split('|')[8])\
                                        +'|'+sche_result_only_row_2_11c['provide_sche3'].map(lambda x:x. split('|')[9])\
                                        +'|'+sche_result_only_row_2_11c['provide_sche3'].map(lambda x:x. split('|')[10])
# 结果数据集
# sche_result_only_row_2 = sche_result_only_row_2[['uuid','provide_title','provide_sche4']]
sche_result_only_row_2_2c = sche_result_only_row_2_2c[['uuid','provide_title','provide_sche4']]
sche_result_only_row_2_3c = sche_result_only_row_2_3c[['uuid','provide_title','provide_sche4']]
sche_result_only_row_2_5c = sche_result_only_row_2_5c[['uuid','provide_title','provide_sche4']]
sche_result_only_row_2_6c = sche_result_only_row_2_6c[['uuid','provide_title','provide_sche4']]
sche_result_only_row_2_7c = sche_result_only_row_2_7c[['uuid','provide_title','provide_sche4']]
sche_result_only_row_2_8c = sche_result_only_row_2_8c[['uuid','provide_title','provide_sche4']]
sche_result_only_row_2_9c = sche_result_only_row_2_9c[['uuid','provide_title','provide_sche4']]
sche_result_only_row_2_11c = sche_result_only_row_2_11c[['uuid','provide_title','provide_sche4']]
# 合并
sche_result_only_row2 =pd.concat([sche_result_only_row_2_2c,sche_result_only_row_2_3c,sche_result_only_row_2_5c,
                                sche_result_only_row_2_6c,sche_result_only_row_2_7c,sche_result_only_row_2_8c,
                                sche_result_only_row_2_9c,sche_result_only_row_2_11c])

# 处理脏数据
sche_result_only_row2['provide_sche4'] = sche_result_only_row2['provide_sche4'].str.replace('，长房', '长房')
# 给时间一个值
sche_result_only_row2['date'] = ''
sche_result_only_row2 = sche_result_only_row2[['uuid','date','provide_title','provide_sche4']]
sche_result_only_row2

# In[4]:
#合并
sche_result_only_row2.columns=['uuid','date','provide_title','provide_sche1']
sche_result = pd.concat([sche_result_only_row_1,sche_result_only_row2])

#季度
# 转换格式
sche_result.date = pd.to_datetime(sche_result.date)
# 讲日期转换为季度
# sche_result['period'] = ''
sche_result['period'] = pd.PeriodIndex(sche_result.date, freq='Q').astype(str)

#去字段
sche_result = sche_result[['uuid','date','period','provide_title','provide_sche1']]
sche_result

# In[5]:
#去掉
#去掉含有许可证标题
sche_result = sche_result.loc[~sche_result['provide_title'].str.contains("许可证")]
#去掉标题太长，和正文太短的
# 计算长度
sche_result['provide_sche1_num'] = sche_result['provide_sche1'].map(lambda x:len(x))
sche_result['provide_title_num'] = sche_result['provide_title'].map(lambda x:len(x))
# 获取指定长度的数据
sche_result_2 = sche_result[sche_result['provide_sche1_num'] == 2 ]
sche_result_7_out = sche_result[sche_result['provide_sche1_num'] > 7 ]
sche_result_7_out = sche_result_7_out[sche_result_7_out['provide_title_num'] < 20 ]
#合并
sche_result = pd.concat([sche_result_2,sche_result_7_out])
sche_result


# In[6]:
# 去字段
sche_result = sche_result[['uuid','date','period','provide_title','provide_sche1']]
# 写入表中
sche_result.columns=['newest_id','date','period','provide_title','provide_sche']
sche_result = sche_result.loc[sche_result['period'].str.contains(date_quarter)]
# 去除标题的问号
sche_result['provide_title'] = sche_result['provide_title'].str.replace('?', '')
sche_result['provide_title'] = sche_result['provide_title'].str.replace('？', '')
# sche_result['date']= sche_result['date'].apply(lambda x: x.strptime(x,'%Y-%m-%d'))
# sche_result.at[sche_result['date'].isna(),'date']='null'
sche_result = sche_result.drop_duplicates()
to_dws(sche_result,table_name)
# ori1 = ori.loc[ori['provide_sche'].str.contains("\^")]
# sche_result.to('C:\\Users\\86133\\Desktop\\sche_result(v2).txt')



print('>> Done!') #完毕

