#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Jun 26 16:44:47 2021
Update on 2021-06-15 17:48
        修改imei号的状态来源，修改前自己判断、修改该后 dws_imei_browse_tag
        修改城市id和区县id，修改前自己 dwb_newest_info ，修改后 dws_newest_info
        不使用dws_customer_cre
"""
import configparser
import os
import sys
from pandas.core import groupby
import pymysql
import pandas as pd
import numpy as np
from collections import Counter
import re
from sqlalchemy import create_engine
import datetime
from dateutil.relativedelta import relativedelta


cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
date_quarter = '2020Q4'    #  获取季度（统计周期）
start_date = '20201001'   #  获取取数的开始年月日
stop_date = '20210101'   #  获取取数结束的年月日
start_date_DF = datetime.datetime.strptime(start_date, "%Y%m%d")
end_date_DF = datetime.datetime.strptime(stop_date, "%Y%m%d")

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

# 城市项目总量与意向客户统计 # dws_newest_city_qua
# 读取当前季度的城市，楼盘信息：基础数据  城市id，区域，周期，均价
# 统计各个状态的楼盘数量
# 统计关注人数--->意向人数--->迫切人数
# 统计当季新增 当季留存


#意向客户总量#
con = MysqlClient(db_host,database,user,password)
# dwb_customer_browse_log  客户浏览楼盘日志表（每日增量） 
#                                                      imei          客户号
#                                                      county_id     区县日期  
#                                                      city_id       城市id 
#                                                      newest_id     楼盘id 
ori=con.query('''SELECT newest_id,city_id,county_id,imei FROM dwb_db.dwb_customer_browse_log where visit_date>='''+start_date+''' and visit_date<'''+stop_date)
#dws_newest_info  新房楼盘
#                                                      city_id         城市id  
#                                                      county_id       区县id  
#                                                      unit_price      均价
#                                                      newest_id       楼盘id 
#                                                      sales_state     销售状态
newest_id=con.query('''SELECT  sales_state,newest_id,city_id, county_id, unit_price  FROM dws_db.dws_newest_info''')
# In[1]:
# dws_newest_period_admit  准入楼盘表
#                                                      newest_id       楼盘id

admit = con.query('''select distinct newest_id from dws_db.dws_newest_period_admit where period = "'''+date_quarter+'''"  and dr = 0''')
# dws_customer_cre 意向客户增存留结果表
#                                   city_id       城市id
#                                   newest_id     楼盘id
#                                   exist         增/存量
#                                   imei_num      imei数量
#                                   period        统计周期
# cre = con.query('''select city_id,newest_id,exist,imei_num from dws_db.dws_customer_cre where period = "'''+date_quarter+'''"''')

# dws_imei_browse_tag  客户浏览标签结果表
#                                   imei            类似客户号的东西
#                                   concern        关注
#                                   intention      意向
#                                   urgent         迫切
#                                   cre            增存
ime = con.query('''select imei,concern,intention,urgent,cre from dws_db.dws_imei_browse_tag where period = "'''+date_quarter+'''"''')



# In[1]:
# 获取楼盘的城市和区县，以及浏览基本情况
grouped = pd.merge(admit, newest_id, how='left', on=['newest_id'])
grouped1 = grouped.groupby(['city_id','county_id'])['unit_price'].mean().reset_index()
grouped1.at[grouped1['unit_price'].isna(),'unit_price']=0
grouped1= grouped1[grouped1['unit_price']>0]
grouped1[['unit_price']] = grouped1[['unit_price']].astype('int')

# id为空值的数据
grouped002 = grouped
grouped002.at[grouped002['city_id'].isna(),'city_id']='0'
grouped002.at[grouped002['county_id'].isna(),'county_id']='0'
grouped2 = grouped002.groupby(['sales_state','city_id','county_id'])['newest_id'].count().reset_index()

# In[2]:
# 统计各个状态的楼盘数量
grouped2_for_sale= grouped2[grouped2['sales_state']=='待售']
grouped2_for_sale.rename(columns={'newest_id':'for_sale'},inplace=True)

grouped2_on_sale= grouped2[grouped2['sales_state']=='在售']
grouped2_on_sale.rename(columns={'newest_id':'on_sale'},inplace=True)

# grouped2_sell_out= grouped2[grouped2['sales_state']=='售罄']
# grouped2_sell_out.rename(columns={'newest_id':'sell_out'},inplace=True)
# In[11112]:
grouped2_on_sale111 = grouped2_on_sale[grouped2_on_sale['city_id'] == '120000']
grouped2_on_sale111

# In[2]:
grouped3 = pd.merge(grouped1, grouped2_for_sale, how='left', on=['city_id', 'county_id'])
grouped3 = pd.merge(grouped3, grouped2_on_sale, how='left', on=['city_id', 'county_id'])
# grouped3 = pd.merge(grouped3, grouped2_sell_out, how='left', on=['city_id', 'county_id'])
grouped3['sell_out'] = '0'
grouped3 = grouped3[['city_id','county_id','unit_price','for_sale','on_sale','sell_out']]

grouped3.at[grouped3['for_sale'].isna(),'for_sale']=0
grouped3.at[grouped3['on_sale'].isna(),'on_sale']=0
grouped3.at[grouped3['sell_out'].isna(),'sell_out']=0
# grouped3["total_count"] = grouped3['for_sale']+grouped3['on_sale']+grouped3['sell_out']
grouped3["total_count"] = grouped3['for_sale']+grouped3['on_sale']
grouped3[['for_sale', 'on_sale','total_count']] = grouped3[['for_sale', 'on_sale','total_count']].astype('int')
grouped3


# In[3]:
#统计关注人数--->意向人数--->迫切人数
#关联出city_id,county_id,状态
grouped4 = pd.merge(ori,ime ,how='left' ,on=['imei'])
grouped4 = grouped4[['newest_id','imei','concern','intention','urgent','cre']]
grouped5 = pd.merge(grouped4,grouped ,how='left' ,on=['newest_id'])
grouped5 = grouped5[['newest_id','imei','concern','intention','urgent','cre','city_id','county_id']]
# In[13]:
#关注人数

grouped5_concern_tmp =  grouped5[grouped5['concern'] == '关注']
grouped5_concern_tmp = grouped5_concern_tmp[['city_id','county_id','imei']].drop_duplicates()
grouped5_concern = grouped5_concern_tmp.groupby(['city_id','county_id'])['imei'].count().reset_index()

grouped5_intention_tmp =  grouped5[grouped5['intention'] == '意向']
grouped5_intention_tmp = grouped5_intention_tmp[['city_id','county_id','imei']].drop_duplicates()
grouped5_intention = grouped5_intention_tmp.groupby(['city_id','county_id'])['imei'].count().reset_index()

grouped5_urgent_tmp =  grouped5[grouped5['urgent'] == '迫切']
grouped5_urgent_tmp = grouped5_urgent_tmp[['city_id','county_id','imei']].drop_duplicates()
grouped5_urgent = grouped5_urgent_tmp.groupby(['city_id','county_id'])['imei'].count().reset_index()

grouped5_increase_tmp =  grouped5[grouped5['cre'] == '增长']
grouped5_increase_tmp = grouped5_increase_tmp[['city_id','county_id','imei']].drop_duplicates()
grouped5_increase = grouped5_increase_tmp.groupby(['city_id','county_id'])['imei'].count().reset_index()

grouped5_retained_tmp =  grouped5[grouped5['cre'] == '活跃']
grouped5_retained_tmp = grouped5_retained_tmp[['city_id','county_id','imei']].drop_duplicates()
grouped5_retained = grouped5_retained_tmp.groupby(['city_id','county_id'])['imei'].count().reset_index()

grouped5_concern.columns = ['city_id','county_id','concern']
grouped5_intention.columns = ['city_id','county_id','intention']
grouped5_urgent.columns = ['city_id','county_id','urgent']
grouped5_increase.columns = ['city_id','county_id','increase']
grouped5_retained.columns = ['city_id','county_id','retained']

grouped5_urgent

grouped6_1 = pd.merge(grouped3,grouped5_concern,how='left',on=['city_id','county_id'])
grouped6_2 = pd.merge(grouped6_1,grouped5_intention,how='left',on=['city_id','county_id'])
grouped6_3 = pd.merge(grouped6_2,grouped5_urgent,how='left',on=['city_id','county_id'])
grouped6_4 = pd.merge(grouped6_3,grouped5_increase,how='left',on=['city_id','county_id'])
grouped6_5 = pd.merge(grouped6_4,grouped5_retained,how='left',on=['city_id','county_id'])

grouped6_5['period']=date_quarter
grouped6_5

result = grouped6_5[['city_id','county_id','period','for_sale','on_sale','sell_out','total_count','concern','intention','urgent','increase','retained','unit_price']]
result.columns = ['city_id','county_id','period','for_sale','on_sale','sell_out','total_count','follow','intention','urgent','increase','retained','unit_price']
# 去空
result = result.dropna(subset=['period'])
result = result.drop_duplicates()
result.at[result['follow'].isna(),'follow']=0
result.at[result['intention'].isna(),'intention']=0
result.at[result['urgent'].isna(),'urgent']=0
result.at[result['increase'].isna(),'increase']=0
result.at[result['retained'].isna(),'retained']=0
result

# # ================================================添加城市的，上边是按照区域的================================================

# In[5]:
# 添加城市的，上边是按照区域的
# 获取楼盘的城市和区县，以及浏览基本情况
grouped01 = grouped.groupby(['city_id'])['unit_price'].mean().reset_index()
grouped01.at[grouped01['unit_price'].isna(),'unit_price']=0
grouped01[['unit_price']] = grouped01[['unit_price']].astype('int')

grouped02 = grouped.groupby(['sales_state','city_id'])['newest_id'].count().reset_index()

# In[6]:
# 统计各个状态的楼盘数量
grouped02_for_sale= grouped02[grouped02['sales_state']=='待售']
grouped02_for_sale.rename(columns={'newest_id':'for_sale'},inplace=True)

grouped02_on_sale= grouped02[grouped02['sales_state']=='在售']
grouped02_on_sale.rename(columns={'newest_id':'on_sale'},inplace=True)

# grouped02_sell_out= grouped02[grouped02['sales_state']=='售罄']
# grouped02_sell_out.rename(columns={'newest_id':'sell_out'},inplace=True)


grouped03 = pd.merge(grouped01, grouped02_for_sale, how='left', on=['city_id'])
grouped03 = pd.merge(grouped03, grouped02_on_sale, how='left', on=['city_id'])
# grouped03 = pd.merge(grouped03, grouped02_sell_out, how='left', on=['city_id'])
grouped03['sell_out'] = '0'
grouped03 = grouped03[['city_id','unit_price','for_sale','on_sale','sell_out']]

grouped03.at[grouped03['for_sale'].isna(),'for_sale']=0
grouped03.at[grouped03['on_sale'].isna(),'on_sale']=0
grouped03.at[grouped03['sell_out'].isna(),'sell_out']=0
# grouped03["total_count"] = grouped03['for_sale']+grouped03['on_sale']+grouped03['sell_out']
grouped03["total_count"] = grouped03['for_sale']+grouped03['on_sale']
grouped03[['for_sale', 'on_sale','total_count']] = grouped03[['for_sale', 'on_sale','total_count']].astype('int')
grouped03

# In[7]:

grouped05_concern_tmp =  grouped5[grouped5['concern'] == '关注']
grouped05_concern_tmp = grouped05_concern_tmp[['city_id','imei']].drop_duplicates()
grouped05_concern = grouped05_concern_tmp.groupby(['city_id'])['imei'].count().reset_index()

grouped05_intention_tmp =  grouped5[grouped5['intention'] == '意向']
grouped05_intention_tmp = grouped05_intention_tmp[['city_id','imei']].drop_duplicates()
grouped05_intention = grouped05_intention_tmp.groupby(['city_id'])['imei'].count().reset_index()

grouped05_urgent_tmp =  grouped5[grouped5['urgent'] == '迫切']
grouped05_urgent_tmp = grouped05_urgent_tmp[['city_id','imei']].drop_duplicates()
grouped05_urgent = grouped05_urgent_tmp.groupby(['city_id'])['imei'].count().reset_index()

grouped05_increase_tmp =  grouped5[grouped5['cre'] == '增长']
grouped05_increase_tmp = grouped05_increase_tmp[['city_id','imei']].drop_duplicates()
grouped05_increase = grouped05_increase_tmp.groupby(['city_id'])['imei'].count().reset_index()

grouped05_retained_tmp =  grouped5[grouped5['cre'] == '活跃']
grouped05_retained_tmp = grouped05_retained_tmp[['city_id','imei']].drop_duplicates()
grouped05_retained = grouped05_retained_tmp.groupby(['city_id'])['imei'].count().reset_index()

grouped05_concern.columns = ['city_id','concern']
grouped05_intention.columns = ['city_id','intention']
grouped05_urgent.columns = ['city_id','urgent']
grouped05_increase.columns = ['city_id','increase']
grouped05_retained.columns = ['city_id','retained']



grouped06_1 = pd.merge(grouped03,grouped05_concern,how='left',on=['city_id'])
grouped06_2 = pd.merge(grouped06_1,grouped05_intention,how='left',on=['city_id'])
grouped06_3 = pd.merge(grouped06_2,grouped05_urgent,how='left',on=['city_id'])
grouped06_4 = pd.merge(grouped06_3,grouped05_increase,how='left',on=['city_id'])
grouped06_5 = pd.merge(grouped06_4,grouped05_retained,how='left',on=['city_id'])

grouped06_5['period']=date_quarter
grouped06_5['county_id'] = ''

result0 = grouped06_5[['city_id','county_id','period','for_sale','on_sale','sell_out','total_count','concern','intention','urgent','increase','retained','unit_price']]
result0.columns = ['city_id','county_id','period','for_sale','on_sale','sell_out','total_count','follow','intention','urgent','increase','retained','unit_price']
# 去空
result0 = result0.dropna(subset=['period'])
result0 = result0.drop_duplicates()
result0.at[result0['follow'].isna(),'follow']=0
result0.at[result0['intention'].isna(),'intention']=0
result0.at[result0['urgent'].isna(),'urgent']=0
result0.at[result0['increase'].isna(),'increase']=0
result0.at[result0['retained'].isna(),'retained']=0
result0

# In[10]:
#合并
result = result.append(result0,ignore_index=True)
#  dws_newest_city_qua_test
to_dws(result,'dws_newest_city_qua')


# In[1000]:
# grouped.to_csv('C:\\Users\\86133\\Desktop\\grouped.csv')
confpath
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
update_sql = "UPDATE dws_db.dws_newest_city_qua SET county_id = NULL WHERE county_id = 'NULL' OR county_id = ''"
cur.execute(update_sql)
conn.commit() # 提交记
conn.close() # 关闭数据库链接
print('>> Done!') #完毕
