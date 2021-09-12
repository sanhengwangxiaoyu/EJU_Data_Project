#!/usr/bin/env python
# coding: utf-8

# In[1]:



# -*- coding: utf-8 -*-
"""
Created on Wed Apr  7 18:12:46 2021

@author: andy
"""

import sys
import pandas as pd
import numpy as np
import os
import pymysql
import configparser
from sqlalchemy import create_engine

cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值


# In[2]:


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
    engine = create_engine("mysql+pymysql://yangzhen:6V5_0rviExpxBzHj@172.28.36.77:3306/dws_db_prd?charset=utf8")
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)

# In[4]:


#不同人群竞品排名
con = MysqlClient(db_host,database,user,password)
date = con.query(" SELECT distinct visit_month FROM dwb_db.a_dwb_customer_browse_log  ")

#周维度的竞对指数列表
date




# In[5]:


quarter = list(date['visit_month'].drop_duplicates())
quarter


# In[ ]:


per_qua = pd.DataFrame()
for a in quarter:

    print(a)

    con = MysqlClient(db_host,database,user,password)
    data = con.query(" SELECT distinct newest_id,imei FROM  dwb_db.a_dwb_customer_browse_log where visit_month = '"+a+"'  ")
    compete_list=con.query("select city_id,newest_id,comp_id,confidence from dws_db_prd.dws_compete_list_qua where period='"+a+"'  ")
    #影响力
    #compete_list=con.query("select city_id,newest_id,comp_id,confidence from dws_db.dws_compete_list_qua where period='"+period+"' ")
    compete_list['confidence']=compete_list['confidence'].apply(lambda x:float(x))
    compete_list['confidence_rank']=compete_list.groupby(['city_id','newest_id'])['confidence'].rank(ascending=False,method='first')
    compete_assist=compete_list.groupby(['city_id'])['confidence'].agg(['min','max']).reset_index()
    compete_list=pd.merge(compete_list,compete_assist,on=['city_id'],how='left')

    #compete_list['confidence_level']=round((compete_list['confidence']-compete_list['min'])/(compete_list['max']-compete_list['min']))
    compete_list['confidence_l']=(compete_list['confidence']-compete_list['min'])/(compete_list['max']-compete_list['min'])
    compete_list['confidence_level'] = pd.qcut(compete_list['confidence_l'], 4,labels=False)
    compete_list=compete_list.drop(['min','max','confidence_l'],axis=1)
    #关注
    cus_filter=con.query('''select distinct imei,1 cou from dws_db_prd.dws_imei_browse_tag where concern='关注' ''')
    data_filter=pd.merge(data[['newest_id','imei']],cus_filter,how='inner',on=['imei'])
    data_filter = data_filter.groupby(['newest_id'])['imei'].count().reset_index()
    data_filter.rename(columns={'imei':'concern','newest_id':'comp_id'},inplace=True)
    compete_list=pd.merge(compete_list,data_filter,how='inner',on=['comp_id'])
    compete_list['concern_rank']=compete_list.groupby(['city_id','newest_id'])['concern'].rank(ascending=False,method='first')
    compete_assist=compete_list.groupby(['city_id'])['concern'].agg(['min','max']).reset_index()
    compete_list=pd.merge(compete_list,compete_assist,on=['city_id'],how='left')
    compete_list['concern_l']=(compete_list['concern']-compete_list['min'])/(compete_list['max']-compete_list['min'])
    compete_list['concern_level'] = pd.qcut(compete_list['concern_l'], 4,labels=False)
    compete_list=compete_list.drop(['min','max','concern_l'],axis=1)

    cus_filter=con.query('''select distinct imei,1 cou from dws_db_prd.dws_imei_browse_tag where  intention='意向' ''')
    data_filter=pd.merge(data[['newest_id','imei']],cus_filter,how='inner',on=['imei'])
    data_filter = data_filter.groupby(['newest_id'])['imei'].count().reset_index()
    data_filter.rename(columns={'imei':'intention','newest_id':'comp_id'},inplace=True)
    compete_list=pd.merge(compete_list,data_filter,how='inner',on=['comp_id'])
    compete_list['intention_rank']=compete_list.groupby(['city_id','newest_id'])['intention'].rank(ascending=False,method='first')
    compete_assist=compete_list.groupby(['city_id'])['intention'].agg(['min','max']).reset_index()
    compete_list=pd.merge(compete_list,compete_assist,on=['city_id'],how='left')
    compete_list['intention_l']=(compete_list['intention']-compete_list['min'])/(compete_list['max']-compete_list['min'])
    compete_list['intention_level'] = pd.qcut(compete_list['intention_l'], 4,labels=False)
    compete_list=compete_list.drop(['min','max','intention_l'],axis=1)

    cus_filter=con.query('''select distinct imei,1 cou from dws_db_prd.dws_imei_browse_tag where urgent='迫切' ''')
    data_filter=pd.merge(data[['newest_id','imei']],cus_filter,how='inner',on=['imei'])
    data_filter = data_filter.groupby(['newest_id'])['imei'].count().reset_index()
    data_filter.rename(columns={'imei':'urgent','newest_id':'comp_id'},inplace=True)
    compete_list=pd.merge(compete_list,data_filter,how='inner',on=['comp_id'])
    compete_list['urgent_rank']=compete_list.groupby(['city_id','newest_id'])['urgent'].rank(ascending=False,method='first')
    compete_assist=compete_list.groupby(['city_id'])['urgent'].agg(['min','max']).reset_index()
    compete_list=pd.merge(compete_list,compete_assist,on=['city_id'],how='left')
    compete_list['urgent_l']=(compete_list['urgent']-compete_list['min'])/(compete_list['max']-compete_list['min'])
    compete_list['urgent_level'] = pd.qcut(compete_list['urgent_l'], 4,labels=False)
    compete_list=compete_list.drop(['min','max','urgent_l'],axis=1)

    #增长
    cus_filter=con.query('''select distinct imei,1 cou from dws_db_prd.dws_imei_browse_tag where cre='增长' ''')
    data_filter=pd.merge(data[['newest_id','imei']],cus_filter,how='inner',on=['imei'])
    data_filter = data_filter.groupby(['newest_id'])['imei'].count().reset_index()
    data_filter.rename(columns={'imei':'increase','newest_id':'comp_id'},inplace=True)
    compete_list=pd.merge(compete_list,data_filter,how='inner',on=['comp_id'])
    compete_list['increase_rank']=compete_list.groupby(['city_id','newest_id'])['increase'].rank(ascending=False,method='first')
    compete_assist=compete_list.groupby(['city_id'])['increase'].agg(['min','max']).reset_index()
    compete_list=pd.merge(compete_list,compete_assist,on=['city_id'],how='left')
    compete_list['increase_l']=(compete_list['increase']-compete_list['min'])/(compete_list['max']-compete_list['min'])
    compete_list['increase_level'] = pd.qcut(compete_list['increase_l'], 4,labels=False)
    compete_list=compete_list.drop(['min','max','increase_l'],axis=1)

    #活跃

    cus_filter=con.query('''select distinct imei,1 cou from dws_db_prd.dws_imei_browse_tag where cre='活跃' ''')
    data_filter=pd.merge(data[['newest_id','imei']],cus_filter,how='inner',on=['imei'])
    data_filter = data_filter.groupby(['newest_id'])['imei'].count().reset_index()
    data_filter.rename(columns={'imei':'stock','newest_id':'comp_id'},inplace=True)
    compete_list=pd.merge(compete_list,data_filter,how='inner',on=['comp_id'])
    compete_list['stock_rank']=compete_list.groupby(['city_id','newest_id'])['stock'].rank(ascending=False,method='first')
    compete_assist=compete_list.groupby(['city_id'])['stock'].agg(['min','max']).reset_index()
    compete_list=pd.merge(compete_list,compete_assist,on=['city_id'],how='left')
    compete_list['stock_l']=(compete_list['stock']-compete_list['min'])/(compete_list['max']-compete_list['min'])
    compete_list['stock_level'] = pd.qcut(compete_list['stock_l'], 4,labels=False)
    compete_list=compete_list.drop(['min','max','stock_l'],axis=1)

    compete_list['period']= a
    compete_list=compete_list.fillna(3)
    print(compete_list.head(5))
    per_qua=per_qua.append(compete_list)

to_dws(per_qua,'dws_compete_list_sub')

# In[ ]:


per_qua


# In[ ]:





# In[ ]:





# In[ ]:




