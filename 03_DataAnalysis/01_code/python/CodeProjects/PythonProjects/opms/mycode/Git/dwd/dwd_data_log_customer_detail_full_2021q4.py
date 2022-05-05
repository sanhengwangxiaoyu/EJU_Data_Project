# In[]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 26 16:44:47 2021

给极光的客户表
"""
import configparser,re
from dataclasses import replace
import os
import sys,io
import pymysql
import pandas as pd
import numpy as np
from collections import Counter
import time
from sqlalchemy import create_engine
import getopt

##读取配置文件##
pymysql.install_as_MySQLdb()
cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "./conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
date_quater = '2021Q4'
table_name = 'dwd_data_log_customer_detail_full_2021q4'
database = 'dwd_db'

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

print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())  + '  reading data to : dwb_cust_high_brow_log')


# In[]
browse_log=con.query("select imei,newest_id,newest_name,visit_date,city_id from dwb_db.dwb_cust_high_brow_log where visit_month>='202110'")


#In[]
newest_info = con.query("select newest_id,city_id city from dws_db_prd.dws_newest_info group by newest_id,city_id ")

dim_gra = con.query("select city_name,city_id from dws_db_prd.dim_geography where city_name is not null group by city_name,city_id ")
dim_gra['city_id'] = dim_gra[['city_id']].astype(str)


#In[]
browse_log['visit_date'] = browse_log['visit_date'].apply(lambda x: str(x).replace('-',''))
result = pd.merge(browse_log,newest_info,how='left',on=['newest_id'])
result.at[~result['city'].isna(),'city_id'] = result['city']
result = pd.merge(result,dim_gra,how='left',on=['city_id'])
result['floor_name'] = result['newest_name']
result['idate'] = result['visit_date']
result['customer'] = result['imei']
result['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
result['period'] = '2021Q4'




# In[27]:
to_dws(result[['city_id','city_name','newest_id','floor_name','idate','customer','create_time','period']],table_name)
print('>>> Done')


# In[1000]:
# df.to_csv('C:\\Users\\86133\\Desktop\\df.csv')
#### 检查有没有不存在于之前的数据
# detail_log = con.query("select newest_id,customer,concat(newest_id,'-',customer) concat_newest_customer from dwd_db.dwd_data_log_customer_detail_full_2021q4 where create_time is null")

# con.close()

# result['concat_newest_customer'] = result['newest_id']+'-'+ result['customer']
# # result2 = detail_log[~detail_log['concat_newest_customer'].isin(result['concat_newest_customer'])]
# result2 = result[~result['concat_newest_customer'].isin(detail_log['concat_newest_customer'])]
# result2[['concat_newest_customer']].drop_duplicates()
# ### 8474

# result2 = result[~result['customer'].isin(detail_log['customer'])]
# result2[['customer']].drop_duplicates()

#In[]
result3 = result.groupby(['newest_id'])['customer'].count().reset_index()
result3 = result3[result3['customer']<20]


#In[]
result4 = result[result['newest_id'].isin(result3['newest_id'])]

to_dws(result4[['city_id','city_name','newest_id','floor_name','idate','customer','create_time','period']],table_name)
print('>>> Done')



#In[]
result5 = result.groupby(['newest_id'])['customer'].count().reset_index()
result5 = result5[(result5['customer']<10)]
result5

#In[]
result6 = result[result['newest_id'].isin(result5['newest_id'])]

to_dws(result6[['city_id','city_name','newest_id','floor_name','idate','customer','create_time','period']],table_name)
print('>>> Done')





#In[]
detail_log = con.query("select newest_id,count(customer) customers from dwd_db.dwd_data_log_customer_detail_full_2021q4 where create_time is not null group by newest_id having customers<20")

#In[]
detail_log2 = con.query("select city_id,city_name,newest_id,w_customer,floor_name,idate,customer,create_time,period from dwd_db.dwd_data_log_customer_detail_full_2021q4 where create_time is not null ")


#In[]
result_re = detail_log2[detail_log2['newest_id'].isin(detail_log['newest_id'])]
result_re
to_dws(result_re[['city_id','city_name','newest_id','floor_name','idate','customer','create_time','period']],table_name)
print('>>> Done')


