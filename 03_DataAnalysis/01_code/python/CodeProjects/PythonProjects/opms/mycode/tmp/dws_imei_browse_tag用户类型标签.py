#!/usr/bin/env python
# coding: utf-8

# In[1]:


# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 14:53:31 2021

@author: admin1
"""


import configparser
import os
import sys
import pymysql
import pandas as pd
import numpy as np
from collections import Counter
import re
from sqlalchemy import create_engine
pd.set_option('display.max_columns',None)


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
con = MysqlClient(db_host,database,user,password)


def to_dws(result,table):
    engine = create_engine("mysql+pymysql://mysql:egSQ7HhxajHZjvdX@172.28.36.77:3306/dws_db?charset=utf8")
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)



# In[2]:


#sql
import datetime
from dateutil.relativedelta import relativedelta


# In[3]:


period = '2021Q2'
start_date = '2021-04-01'
end_date = '2021-07-01'
start_date1 = datetime.datetime.strptime(start_date, "%Y-%m-%d")
end_date1 = datetime.datetime.strptime(end_date, "%Y-%m-%d")
pre_start_date = start_date1 - relativedelta(months=+3)
pre_end_date = end_date1 - relativedelta(months=+3)
pre_start_date = str(pre_start_date)[0:10]
pre_end_date = str(pre_end_date)[0:10]
#print ('strs= %s' % (period))
#竞品概览-竞争力排名
#sql2 = " select *  from  dws_db.dws_compete_list_qua  where  period = '"+period+"' "
#sql = sql2.format(period)#转化后的sql语句
#comp_list=con.query(sql2)



# In[4]:


'''comp_list['lift']=comp_list['lift'].apply(lambda x:float(x))
comp_base=comp_list.groupby(['city_id','period','newest_id'])['lift'].agg(['min','max']).reset_index()
comp_rank=pd.merge(comp_list,comp_base,on=['city_id','period','newest_id'],how='left')
comp_rank['level']=round((comp_rank['lift']-comp_rank['min'])/(comp_rank['max']-comp_rank['min'])*3)'''


# In[5]:


#竞争关系-客户竞争指数
#imei标签-三度
ori=con.query(" SELECT city_id,newest_id,visit_date,imei, '"+period+"' period FROM dwb_db.a_dwb_customer_browse_log where visit_date>='"+start_date+"' and visit_date<'"+end_date+"' ")
imei_browse_tag=ori.groupby(['period','imei'])['newest_id'].count().reset_index()
imei_browse_tag.columns=['period','imei','cou']
imei_browse_tag[['cou']] = imei_browse_tag[['cou']].astype('int')
imei_browse_tag['concern']='关注'
imei_browse_tag['intention']=np.nan
imei_browse_tag['urgent']=np.nan
#imei_browse_tag['concern']=
imei_browse_tag.at[imei_browse_tag['cou']>3,'intention']='意向'




imei_browse_tag.at[imei_browse_tag['cou']>10,'urgent']='迫切'











# In[6]:


q3=con.query("SELECT customer imei,count(*) cou_l FROM odsdb.cust_browse_log_202004_202103  where date_format(idate,'%Y-%m-%d') >= '"+pre_start_date+"'  and date_format(idate,'%Y-%m-%d') <'"+pre_end_date+"'  group by customer ")
imei_browse_tag=pd.merge(imei_browse_tag,q3,on=['imei'],how='left')
imei_browse_tag.at[imei_browse_tag['cou']/imei_browse_tag['cou_l']<2,'urgent']=np.nan
#imei_browse_tag.at[imei_browse_tag['cou']/imei_browse_tag['cou_l']>=2,'urgent']='迫切'
imei_browse_tag=imei_browse_tag.drop('cou_l',axis=1)
imei_browse_tag=imei_browse_tag.drop('cou',axis=1)
q3 = None


# In[7]:


#imei标签-增存
his=con.query("SELECT distinct customer imei,'活跃' as cre FROM odsdb.cust_browse_log_202004_202103 where date_format(idate,'%Y-%m-%d') >= '"+pre_start_date+"'  and date_format(idate,'%Y-%m-%d') <'"+pre_end_date+"' ")
imei_browse_tag=pd.merge(imei_browse_tag,his,on='imei',how='left')
imei_browse_tag.at[imei_browse_tag['cre'].isna(),'cre']='增长'
his=None


# In[8]:


imei_browse_tag


# In[9]:


#imei_browse_tag.columns=['period','customer','concern','intention','urgent','cre']
to_dws(imei_browse_tag,'dws_imei_browse_tag')
imei_browse_tag


# In[ ]:





# In[ ]:




