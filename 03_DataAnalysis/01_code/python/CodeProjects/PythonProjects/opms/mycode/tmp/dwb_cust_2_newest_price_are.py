# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Seq 09 15:44:47 2021
  用户浏览楼盘均价面积表
"""
import configparser
import os
import sys,io
from numpy.lib.function_base import append
from pandas.core import groupby
import pymysql
import pandas as pd
import numpy as np
from collections import Counter
import re
from sqlalchemy import create_engine
import getopt
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
date_quarter = '2020Q2'   # 季度
table_name = 'dwb_cust_2_newest_price_are' # 要插入的表名称
database = 'dwb_db'


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
##room_sum清洗逻辑
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
con = MysqlClient(db_host,database,user,password)


# In[2]:
##通过输入的参数的方式获取变量值##  如果不需要使用输入参数的方式，可以不用这段
opts,args=getopt.getopt(sys.argv[1:],"t:q:d:",["database=","table=","quarter="])
for opts,arg in opts:
  if opts=="-t" or opts=="--table": # 获取输入参数 -t或者--table 后的值
    table_name = arg
  elif opts=="-q" or opts=="--quarter":  # 获取输入参数 -1或者--quarter 后的值
    date_quarter = arg
  elif opts=="-d" or opts=="--database":  # 获取输入参数 -1或者--quarter 后的值
    database = arg


# In[3]:
##重置时间格式
start_date = str(pd.to_datetime(date_quarter))[0:10]   #截取成yyyy-MM-dd
end_date =  str(pd.to_datetime(date_quarter) + pd.offsets.QuarterEnd(0))[0:10]      #截取成yyyy-MM-dd

cust=con.query("SELECT housing_id,plat_name pal_name,concat(substr(visit_time,1,4),'Q',QUARTER(visit_time)) `period`,avg_price browse_avg_price_sum FROM dwb_db.cust_browse_log_201801_202106 where housing_id is not null and avg_price is not null and avg_price != 'null' and avg_price != 'NULL'  and substr(visit_time,1,10)>='"+start_date+"' and substr(visit_time,1,10)<='"+end_date+"' union all SELECT housing_id,plat_name pal_name,concat(substr(idate,1,4),'Q',QUARTER(concat(substr(idate,1,4),'-',substr(idate,5,2),'-',substr(idate,8,2)))) `period`,avg_price browse_avg_price_sum FROM dwb_db.cust_browse_log_201801_202106 where housing_id is not null and avg_price is not null and avg_price != 'null' and avg_price != 'NULL'  and concat(substr(idate,1,4),'-',substr(idate,5,2),'-',substr(idate,8,2))>='"+start_date+"' and concat(substr(idate,1,4),'-',substr(idate,5,2),'-',substr(idate,8,2))<='"+end_date+"' and avg_price != '2147483647' and avg_price != ''")



# In[]
housing=con.query("SELECT max(id) housing_id,uuid newest_id,newest_name from dwb_db.dim_housing where uuid is not null group by uuid,newest_name")

newest=con.query("SELECT newest_id,unit_price direct_avg_price from dws_db_prd.dws_newest_info  where newest_id is not null group by newest_id,unit_price")

housing = pd.merge(housing,newest,how='inner',on=['newest_id'])


# In[]
cust['browse_avg_price_sum'] = cust['browse_avg_price_sum'].map(lambda x:x. split('.')[0])
cust['browse_avg_price_sum'] = cust['browse_avg_price_sum'].map(lambda x:x. split('-')[0])
cust['browse_avg_price_sum'] = cust['browse_avg_price_sum'].map(lambda x:x. split('；')[0])
cust['browse_avg_price_sum'] = cust['browse_avg_price_sum'].map(lambda x:x. split('，')[0])
cust['browse_avg_price_sum'] = cust['browse_avg_price_sum'].apply(lambda x:re.sub("\D", "", x))
cust['browse_avg_price_sum'] = cust['browse_avg_price_sum'].apply(lambda x: x[:5] if x > '1000000' else x )
cust['browse_avg_price_count'] = '1'
cust.at[cust['browse_avg_price_sum']=='','browse_avg_price_sum'] = 0
cust[['browse_avg_price_sum']] = cust['browse_avg_price_sum'].astype(int)
cust.groupby(['browse_avg_price_sum'])['pal_name'].count().reset_index()


# In[]:
df_count = cust.groupby(['housing_id','pal_name','period'])['browse_avg_price_count'].count().reset_index()
df_sum = cust.groupby(['housing_id','pal_name','period'])['browse_avg_price_sum'].sum().reset_index()
df = pd.merge(df_count,df_sum,how='inner',on=['housing_id','pal_name','period'])
df['browse_avg_price'] = df['browse_avg_price_sum']/df['browse_avg_price_count'] 
df = pd.merge(df,housing,how='left',on=['housing_id'])
df['avg_price_rate'] = df['browse_avg_price']/df['direct_avg_price'] 


#In[]
df['browse_count'] = '57778237'
df['dr'] = '0'
df['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
df['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
df=df[['newest_id','newest_name','direct_avg_price','browse_avg_price','avg_price_rate','browse_avg_price_sum','browse_avg_price_count','browse_count','period','dr','create_time','update_time','pal_name']]
df=df[~df['newest_id'].isna()]

# housing[housing['newest_id'] == '544a0be563a644f5df767cbce1171a23']
# cust[cust['housing_id'] == 749404]
# test = cust[['browse_avg_price_sum']].drop_duplicates(inplace=False)

# cust[['browse_avg_price_sum']].drop_duplicates(inplace=False).to_csv('C:\\Users\\86133\\Desktop\\test.csv')


#In[]
to_dws(df,table_name)
# result
print('>> Done!') #完毕


## =============================================================
############  1>上边代码把原始数据初步聚合到表里之后
#             2>通过sql清洗出全季度的均价
#             3>然后在通过以下代码将平台清洗出来
## =============================================================

#In[]
browse_avg_price=con.query("SELECT newest_id,browse_avg_price,pal_name from dwb_db.dwb_cust_2_newest_price_are where period is null")


#In[]
###贝壳
result01 = browse_avg_price[browse_avg_price['pal_name'] == '贝壳']
df001 = browse_avg_price[browse_avg_price['pal_name'] != '贝壳']
df001 = df001[~df001['newest_id'].isin(result01['newest_id'])]
###只有一个平台
df002 = df001.groupby(['newest_id'])['pal_name'].count().reset_index()[df001.groupby(['newest_id'])['pal_name'].count().reset_index()['pal_name'] == 1]
result02 = df001[df001['newest_id'].isin(df002['newest_id'])]
###按照平台出现的次数挨个取 
# 8026
df003 = df001[~df001['newest_id'].isin(df002['newest_id'])]
result03 = df003[df003['pal_name'] == '8026']
df004 = df003[df003['pal_name'] != '8026']
# 8001
df004 = df004[~df004['newest_id'].isin(result03['newest_id'])]
result04 = df004[df004['pal_name'] == '8001']
df005 = df004[df004['pal_name'] != '8001']
# 8002
df005 = df005[~df005['newest_id'].isin(result04['newest_id'])]
result05 = df005[df005['pal_name'] == '8002']
df006 = df005[df005['pal_name'] != '8002']
# 取均价最大的
df006 = df006[~df006['newest_id'].isin(result05['newest_id'])]
result06 = df006.groupby(['newest_id'])['browse_avg_price'].max().reset_index()


# 合并所有结果集
result = result01.append([result02, result03, result04, result05, result06], ignore_index=True)

#插入表

result['newest_name'] = result['newest_id']
result['direct_avg_price'] = 0
result['avg_price_rate'] = 0
result['browse_avg_price_sum'] = 0
result['browse_avg_price_count'] = 0
result['browse_count'] = 0
result['period'] = 'NULL'
result['dr'] = '0'
result['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
result['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
result['pal_name'] = 'clean_result'
result=result[['newest_id','newest_name','direct_avg_price','browse_avg_price','avg_price_rate','browse_avg_price_sum','browse_avg_price_count','browse_count','period','dr','create_time','update_time','pal_name']]

to_dws(result,table_name)



# result
print('>> Done!') #完毕

# 8026	96260
# 8001	82260
# 贝壳	70654
# 8002	63265
# 房天下	50594
# 8008	48936
# 8024	31825
# 8031	31066
# 吉屋	26447
# 居理新房	24330
# 8006	21385
# 安居客	16292
# 乐居	13729
# 8009	13150
# 8069	10263
# 8077	6654
# 365淘房	6637
# 8033	4515
# 8003	3953
# 焦点好房	3492
# 腾讯房产	3024
# 8062	2420
# 8013	2269
# 8034	2152
# 8037	1991
# 8058	1893
# 8011	1878
# 8065	1743
# 8061	1545
# 楼盘网	1240
# 8057	1033
# 觅房	1021
# 网易房产	976
# 8028	749
# 8059	724
# 深圳房地产	712
# 8080	669
# 8030	615
# 8095	515
# 我爱我家	486
# 8007	436
# 8129	433
# 凤凰网房产	360
# 8012	323
# 合房网	301
# 8137	291
# 8063	275
# 8005	243
# 乐有家	210
# 8027	201
# 8078	152
# 上海中原	134
# 8133	127
# 8114	124
# 8035	98
# 8121	98
# 8081	92
# 诸葛找房	92
# 8109	91
# 懂房帝	87
# 8082	81
# 8115	76
# 8108	50
# 购房网	47
# 8036	45
# 8104	43
# 8120	43
# 8167	31
# 8093	30
# 楼讯网	27
# 8079	16
# 8138	16
# 8130	15
# 8161	12
# 8171	9
# 8116	8
# 8123	8
# 8122	7
# 8125	7
# 8135	7
# 幸福里	5
# 8111	4
# 8118	4
# 8132	4
# 8140	4
# 8153	4
# 8159	4
# 8149	3
# 8154	3
# 8155	3
# 8038	2
# 8107	2
# 8119	2
# 8127	2
# 城市房产网	2
# 8092	1
# 8110	1
# 8131	1
# 8151	1
# 8156	1
# 8160	1
