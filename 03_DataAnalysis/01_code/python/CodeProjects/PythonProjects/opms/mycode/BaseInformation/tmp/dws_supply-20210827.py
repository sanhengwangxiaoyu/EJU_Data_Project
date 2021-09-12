# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 26 16:44:47 2021
@author: admin1
"""
import configparser
import os
import sys
from numpy.core.einsumfunc import _compute_size_by_dict
from numpy.lib.ufunclike import isneginf
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

con = MysqlClient(db_host,database,user,password)


# In[2]:
# 取55个城市
qua=con.query("select * from dws_db.dws_newest_city_qua where city_id in ('460200','310000','441900','442000','331100','360400','440600','130600','110000','320100','450100','360100','320600','350200','340100','610400','130200','330400','120000','330200','610300','320400','440100','320300','441300','510100','321000','320200','530100','330100','420100','440500','210100','370100','370800','460100','370300','440300','330300','330500','370600','440400','130100','350100','330600','441200','320500','610100','520100','360700','410100','500000','220100','430100','370200')")


# In[3]:
# 取供应套数
supply= con.query("select * from dws_db.dws_supply where dr=0")
ci_num= con.query("select period,sum(follow_people_num),cityid from dws_db.dws_supply ds where period_index = 2 and city_county_index = 2 and dr = 0 group by cityid,period")

# In[4]:
#截取列
df1=qua[['city_id','county_id','follow','period']]
#城市可区县分开
df1_rate_1 = df1[df1['county_id'].isna()]
df1_rate_2 = df1[~df1['county_id'].isna()]
#拼接
df2=pd.merge(df1_rate_2,df1_rate_1, how='left', on=['city_id','period'])
#求取比例
df2['rate']=df2['follow_x']/df2['follow_y']
df2 = df2[['city_id','county_id_x','period','rate','follow_x','follow_y']]


# In[5]:
#截取字段
df01=supply[['city_id','period','value','city_county_index','period_index']]
#获取城市的季度的总供应套数
df01_rate_1=df01[df01['city_county_index']=='1']
df01_rate_2=df01_rate_1[df01_rate_1['period_index']=='1']
 

# In[6]:
#拼接关注人数和供应套数
df02=pd.merge(df01_rate_2,df2, how='left', on=['city_id','period'])
df02[df02['rate'].isna()]




#转换数据类型
df02[['value']] = df02[['value']].astype('int')
#求取对应套数
df02['value_1']=df02['rate']*df02['value']
#转换数据类型
df02[['value_1']] = df02[['value_1']].astype('int')

# In[1000000]:
df02_test=df02[~df02['rate'].isna()]
df02_test


# In[7]:
#获取城市的供应套数
df4= df02.groupby(['city_id','period'])['value_1'].sum().reset_index()
#拼接修改列
df5=pd.merge(supply,df02, how='inner', on=['city_id','period'])
df6=df5[['city_name','county_name','city_id','period','value_y','local_issue_value','local_room_sum_value','cric_value','value_from_index','county_name_merge','city_county_index','period_index','update_time','dr','create_time']]
df6.columns=['city_name','county_name','city_id','period','value','local_issue_value','local_room_sum_value','cric_value','value_from_index','county_name_merge','city_county_index','period_index','update_time','dr','create_time']


# # In[100001]:
# #测试
# df_test2=df6[df6['city_id'] == '340100']
# df_test3=df5[df5['city_id'] == '340100']
# df_test4=supply[supply['city_id'] == '340100']
# df_test5=df02[df02['city_id'] == '340100']



# In[8]:
# 重新替换掉供应总套数
df7=pd.merge(df01_rate_2,df4, how='inner', on=['city_id','period'])
df8=df7[['city_name','county_name','city_id','period','value_1','local_issue_value','local_room_sum_value','cric_value','value_from_index','county_name_merge','city_county_index','period_index','update_time','dr','create_time']]
df8.columns=['city_name','county_name','city_id','period','value','local_issue_value','local_room_sum_value','cric_value','value_from_index','county_name_merge','city_county_index','period_index','update_time','dr','create_time']

# # In[100000]:
# #测试
# df_test1=df8[df8['city_id'] == '340100']



# In[9]:
# result=df6.append(df8,ignore_index=True)

# to_dws(result,'dws_supply')

df02.to_csv('C:\\Users\\86133\\Desktop\\df02.csv')



# In[10]:
#截取列
df001=supply[['city_id','period','value','follow_people_num','city_county_index','period_index','cityid']]
# # In[100000]:
# df001_test = df001[df001['city_id'] == '110102']
# # In[10]:
#获取月份数据
df002 = df001[df001['period_index'] == '2']
#城市可区县分开
df003_1 = ci_num
df003_2 = df002[df002['city_county_index'] == '2']
df003_3 = df002[df002['city_county_index'] == '1']
df003_3 = df003_3[['cityid','period','value']]
#拼接
df004=pd.merge(df003_2,df003_1, how='left', on=['cityid','period'])
#求取比例
df004.at[df004['value'] == '-' , 'value'] = 0
df004.at[df004['value'] == '' , 'value'] = 0
df004.at[df004['follow_people_num'].isna() , 'follow_people_num'] = 0
df004.at[df004['follow_people_num'] == '-' , 'follow_people_num'] = 0
df004[['value','follow_people_num','sum(follow_people_num)']] = df004[['value','follow_people_num','sum(follow_people_num)']].astype('int')
# df004['rate']=df004['value_x']/df004['value_y']
df004['rate']=df004['follow_people_num']/df004['sum(follow_people_num)']
# df004_test = df004[df004['city_id_x'] == '110102']
#按照比例分配区县人数
df005=pd.merge(df004,df003_3, how='left', on=['cityid','period'])
df005 = df005[['city_id','period','value_x','cityid','value_y','rate']]
df005[['value_y']] = df005[['value_y']].astype('int')
df005['value'] = (df005['value_y']*df005['rate']).astype('int')
# df005_test = df005[df005['city_id_x'] == '110102']
#获取区域的关注人数
co_follow_num = df005[['city_id_x','period','value','cityid','follow_people_num_x','city_county_index_x','period_index_x']]
co_follow_num.columns = ['city_id','period','value','cityid','follow_people_num','city_county_index','period_index']
co_follow_num_test = co_follow_num[co_follow_num['city_id'] == '110102']
#获取区域的数据
follow_num_r = supply[['city_name','county_name','city_id','period','local_issue_value','local_room_sum_value','cric_value','value_from_index','county_name_merge','city_county_index','period_index','update_time','dr','create_time','follow_people_num','cityid']]
m_follow_num_r = follow_num_r[follow_num_r['period_index'] == '2']
#城市可区县分开
ci_follow_num_r = m_follow_num_r[m_follow_num_r['city_county_index'] == '1']
co_follow_num_r = m_follow_num_r[m_follow_num_r['city_county_index'] == '2']
co_follow_num_r = co_follow_num_r.groupby(by = ['city_name','county_name','city_id','period','local_issue_value','local_room_sum_value','cric_value','value_from_index','county_name_merge','city_county_index','period_index','update_time','dr','create_time','cityid'], as_index=False)['follow_people_num'].max()
co_follow_num_r.at[co_follow_num_r['follow_people_num'] == '-' , 'follow_people_num'] = 0
co_follow_num_r.at[co_follow_num_r['follow_people_num'] == '' , 'follow_people_num'] = 0
co_follow_num_r[['follow_people_num']] = co_follow_num_r[['follow_people_num']].astype('int')
#合并最终结果集
co_follow_num_r = pd.merge(co_follow_num_r,co_follow_num,how='left',on=['city_id','period','follow_people_num','cityid','city_county_index','period_index'])
#整理列
co_r = co_follow_num_r[['city_name','county_name','city_id','period','value','local_issue_value','local_room_sum_value','cric_value','value_from_index','county_name_merge','city_county_index','period_index','update_time','dr','create_time','follow_people_num','cityid']]
co_r['dr'] = 0
# co_r.at[co_r['value'] == '-' , 'value'] = 0
co_r.at[co_r['value'] == '' , 'value'] = 0
co_r.at[co_r['value'].isna(), 'value'] = 0
co_r[['value']] = co_r[['value']].astype('int')


# In[11]:
#获取城市的数据
ci_follow_num = co_r.groupby(['period','cityid'])['value'].sum().reindex()
ci_follow_r = ci_follow_num_r.groupby(by = ['city_name','county_name','city_id','period','local_issue_value','local_room_sum_value','cric_value','value_from_index','county_name_merge','city_county_index','period_index','update_time','dr','create_time','cityid'], as_index=False)['follow_people_num'].max()
# ci_follow_num_r_test = ci_follow_r[['period']]
ci_follow_r = pd.merge(ci_follow_r,ci_follow_num,how='left',on=['period','cityid'])
#整理列
ci_r = ci_follow_r[['city_name','county_name','city_id','period','value','local_issue_value','local_room_sum_value','cric_value','value_from_index','county_name_merge','city_county_index','period_index','update_time','dr','create_time','follow_people_num','cityid']]
#合并最终结果
ci_r['dr'] = 0
result00 = co_r.append(ci_r,ignore_index=True)
result00['period'] = result00['period'].str.replace('-', '')

co_r_test = co_r[co_r['city_id'] == '310115']



# In[12]:
#截取列
df001=supply[['city_id','period','value','follow_people_num','city_county_index','period_index','cityid']]
# # In[100000]:
# df001_test = df001[df001['city_id'] == '110102']
# # In[10]:
#获取月份数据
df002 = df001[df001['period_index'] == '1']
#城市可区县分开
df003_1 = df002[df002['city_county_index'] == '1']
df003_2 = df002[df002['city_county_index'] == '2']
#拼接
df004=pd.merge(df003_2,df003_1, how='left', on=['cityid','period'])
#求取比例
df004.at[df004['value_x'] == '-' , 'value_x'] = 0
df004.at[df004['value_x'] == '' , 'value_x'] = 0
df004.at[df004['follow_people_num_y'] == '' , 'follow_people_num_y'] = 0
df004.at[df004['follow_people_num_y'].isna() , 'follow_people_num_y'] = 0
df004.at[df004['follow_people_num_y'] == '-' , 'follow_people_num_y'] = 0
df004.at[df004['follow_people_num_x'].isna() , 'follow_people_num_x'] = 0
df004.at[df004['follow_people_num_x'] == '-' , 'follow_people_num_x'] = 0
df004[['value_x','value_y','follow_people_num_x','follow_people_num_y']] = df004[['value_x','value_y','follow_people_num_x','follow_people_num_y']].astype('int')
# df004['rate']=df004['value_x']/df004['value_y']
df004['rate']=df004['follow_people_num_x']/df004['follow_people_num_y']
# df004_test = df004[df004['city_id_x'] == '110102']
#按照比例分配区县人数
df005 = df004[['city_id_x','period','value_x','cityid','value_y','follow_people_num_x','rate','city_county_index_x','period_index_x']]
df005['value'] = (df005['value_y']*df005['rate']).astype('int')
# df005_test = df005[df005['city_id_x'] == '110102']
#获取区域的关注人数
co_follow_num = df005[['city_id_x','period','value','cityid','follow_people_num_x','city_county_index_x','period_index_x']]
co_follow_num.columns = ['city_id','period','value','cityid','follow_people_num','city_county_index','period_index']
co_follow_num_test = co_follow_num[co_follow_num['city_id'] == '110102']
#获取区域的数据
follow_num_r = supply[['city_name','county_name','city_id','period','local_issue_value','local_room_sum_value','cric_value','value_from_index','county_name_merge','city_county_index','period_index','update_time','dr','create_time','follow_people_num','cityid']]
m_follow_num_r = follow_num_r[follow_num_r['period_index'] == '2']
#城市可区县分开
ci_follow_num_r = m_follow_num_r[m_follow_num_r['city_county_index'] == '1']
co_follow_num_r = m_follow_num_r[m_follow_num_r['city_county_index'] == '2']
co_follow_num_r = co_follow_num_r.groupby(by = ['city_name','county_name','city_id','period','local_issue_value','local_room_sum_value','cric_value','value_from_index','county_name_merge','city_county_index','period_index','update_time','dr','create_time','cityid'], as_index=False)['follow_people_num'].max()
co_follow_num_r.at[co_follow_num_r['follow_people_num'] == '-' , 'follow_people_num'] = 0
co_follow_num_r.at[co_follow_num_r['follow_people_num'] == '' , 'follow_people_num'] = 0
co_follow_num_r[['follow_people_num']] = co_follow_num_r[['follow_people_num']].astype('int')
#合并最终结果集
co_follow_num_r = pd.merge(co_follow_num_r,co_follow_num,how='left',on=['city_id','period','follow_people_num','cityid','city_county_index','period_index'])
#整理列
co_r = co_follow_num_r[['city_name','county_name','city_id','period','value','local_issue_value','local_room_sum_value','cric_value','value_from_index','county_name_merge','city_county_index','period_index','update_time','dr','create_time','follow_people_num','cityid']]
co_r['dr'] = 0
# co_r.at[co_r['value'] == '-' , 'value'] = 0
co_r.at[co_r['value'] == '' , 'value'] = 0
co_r.at[co_r['value'].isna(), 'value'] = 0
co_r[['value']] = co_r[['value']].astype('int')




# In[12]:
#删除数据
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
update_sql1 = "update dws_db.dws_supply set dr = 1  where period_index = 2 "
cur.execute(update_sql1)
conn.commit() # 提交记
conn.close() # 关闭数据库链接



# In[13]:
to_dws(result00,'dws_supply')
print('>> Done!') #完毕


# In[14]:
#设置格式
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
update_sql2 = "update dws_db.dws_supply set value = '-' where value = '0' "
cur.execute(update_sql2)
update_sql3 = "update dws_db.dws_supply set follow_people_num = '-' where follow_people_num = '0' or follow_people_num is null  "
cur.execute(update_sql3)
conn.commit() # 提交记
conn.close() # 关闭数据库链接



