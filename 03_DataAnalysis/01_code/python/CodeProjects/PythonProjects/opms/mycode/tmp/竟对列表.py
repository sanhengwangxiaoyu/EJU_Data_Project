#!/usr/bin/env python
# coding: utf-8

# In[39]:


# -*- coding: utf-8 -*-
"""
Created on Wed Apr  7 18:12:46 2021

@author: admin1
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


# In[40]:


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


# In[101]:

# start_date = '2021-01-01'
# end_date = '2021-04-01'
# start_month = start_date[0:7]
# period = '2021Q1'
# year_id = period[0:4]
# quarter_id = period[5:6]

start_date = '2021-04-01'
end_date = '2021-07-01'
start_month = start_date[0:7]
period = '2021Q2'
year_id = period[0:4]
quarter_id = period[5:6]
#start_month
con = MysqlClient(db_host,database,user,password)

# In[102]:


data = con.query("select DISTINCT imei,newest_id from dwb_db.dwb_customer_browse_log \
     where visit_date>= '"+start_date+"'  and visit_date<'"+end_date+"' ")
if period == '2020Q4':
    newest_city = con.query(''' SELECT distinct newest_id,city_id FROM  dws_db.dws_newest_info where city_id in ('110000',
    '120000','130100','130200','130600','210100','220100','310000','320100',
    '320200','320500','320600','330100','330200','330400','340100',
    '350100','350200','370100','370200','370600','410100','420100',
    '430100','440100','440300','440600','441900','450100',
    '460100','500000','510100','530100','610100'
    ) ''')#2020Q4的34个城市
elif period == '2021Q2':            
    newest_city = con.query(''' SELECT distinct newest_id,city_id FROM  dwb_db.dwb_newest_info where city_id in ('500000',
    '420100','120000','430100','310000','510100','610100','440100',
    '210100','370200','320500','340100','330100','410100','370100',
    '440600','110000','450100','130100','360100','220100','370600',
    '320100','330200','320200','440300','530100','441900','520100',
    '350100','350200','610400','330400','460100','130200','320400',
    '460200','320600','440400','320300','370300','330600','360700',
    '370800','330500','610300','360400','130600','441200','330300',
    '440500','441300','321000','442000'
    ) ''')#2021Q2的54个城市
else:
    ''
data=pd.merge(data,newest_city,how='inner',on=['newest_id'])

data['newest_id']=data['newest_id'].apply(lambda x:str(x))
data['cou']=1

city_list=list(data['city_id'].drop_duplicates())
print(len(city_list))
city_apart=locals() #转字典
for i in range(len(city_list)):
    city_apart['floor_imei_'+city_list[i]]=data[data['city_id']==city_list[i]]

#city_detail=floor_imei_110000

def comp_list_cul(city_detail):

# groupby customer, 每个商品为一列
    basket = city_detail.pivot_table(values = 'cou',index=['imei'], columns=['newest_id'])
    tot=len(basket)
# 将nan填充成0
    basket = basket.fillna(0)   
    floor_list=list(basket.columns.values)
    a=pd.DataFrame()
    for i in floor_list:
        print(i)
        for j in floor_list:
#30个以上共同浏览imei进入计算#
            if (i!=j)&(len(basket[(basket['%s'%i]==1)&(basket['%s'%j]==1)])>=20):
                b=pd.DataFrame()
                b=b.append([[i,j]])
                b['支持度']=round(len(basket[(basket['%s'%i]==1)])/tot,6)
                b['置信度']=round(len(basket[(basket['%s'%i]==1)&(basket['%s'%j]==1)])/len(basket[basket['%s'%j]==1]),6)
                b['提升度']=round(b['置信度']/b['支持度'],6)
                a=a.append(b)
        #a.columns=['目标楼盘','竞品楼盘','支持度','置信度','提升度']  
    comp_list=a 
        #comp_list['filter']=a.groupby(['目标楼盘'])['支持度'].rank(ascending=False,method='first')
        #comp_list=comp_list[comp_list['filter']<=10]
    return comp_list


comp_list_res=pd.DataFrame()
for i in range(len(city_list)):
    print(city_list[i])
    city_comp_list=comp_list_cul(city_apart['floor_imei_'+city_list[i]])
    city_comp_list['city_id']=city_list[i]
    comp_list_res=comp_list_res.append(city_comp_list)
comp_list_res=comp_list_res.reset_index(drop=True)
#comp_list_res=comp_list_res.drop(['filter'],axis=1)
comp_list_res['period']= period
comp_list_res.columns=['newest_id','comp_id','support','confidence','lift','city_id','period']
#comp_list_res.groupby(['city_id']).count()
comp_list_res.drop_duplicates(inplace=True)

# newest_id降序  lift升序
comp_list_res.sort_values(['newest_id','lift'],ascending=[1,0],inplace=True)
comp_list_res['rank'] = comp_list_res.groupby(['newest_id'])['lift'].rank(ascending=False,method='dense')
comp_list_res


# In[12]:


def to_dws(result,table):
    engine = create_engine("mysql+pymysql://mysql:egSQ7HhxajHZjvdX@172.28.36.77:3306/dws_db?charset=utf8")
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)

to_dws(comp_list_res,'dws_compete_list_qua')


# In[104]:


#日期周期维度表读入

con = MysqlClient(db_host,database,user,password)

#date = con.query(" SELECT cal_date,Revised_year,Revised_quarter,Revised_month,Revised_week,concat(Revised_year,Revised_month) month_id,period week_id FROM  dws_db.dim_period_date where Revised_year='"+year_id+"' and Revised_quarter='"+quarter_id+"' ")
date = con.query(" SELECT cal_date,Revised_year,Revised_quarter,Revised_month,Revised_week,concat(Revised_year,'0',Revised_month) month_id,period week_id FROM  dws_db.dim_period_date where Revised_year='"+year_id+"' and Revised_quarter='"+quarter_id+"' ")

#date = con.query(" SELECT cal_date,Revised_year,Revised_quarter,Revised_month,Revised_week,concat(Revised_year,Revised_month) month_id,concat(Revised_year,'-',Revised_week) week_id FROM  dws_db.dim_date where Revised_year='"+year_id+"' and Revised_quarter='"+quarter_id+"' ")


#周月维度的函数，调整了起步参数和空值跳过
def comp_list_cul(city_detail):

# groupby customer, 每个楼盘为一列
    basket = city_detail.pivot_table(values = 'cou',index=['imei'], columns=['newest_id'])
    tot=len(basket)
# 将nan填充成0
    basket = basket.fillna(0)   
    floor_list=list(basket.columns.values)
    a=pd.DataFrame()
    for i in floor_list:
        #print(i)
        for j in floor_list:
#30个以上共同浏览imei进入计算#
            if (i!=j)&(len(basket[(basket['%s'%i]==1)&(basket['%s'%j]==1)])>=10):
                b=pd.DataFrame()
                b=b.append([[i,j]])
                b.columns=['目标楼盘','竞品楼盘']
                b['支持度']=round(len(basket[(basket['%s'%i]==1)])/tot,6)
                b['置信度']=round(len(basket[(basket['%s'%i]==1)&(basket['%s'%j]==1)])/len(basket[basket['%s'%j]==1]),6)
                b['提升度']=round(b['置信度']/b['支持度'],6)
                a=a.append(b)
            else:
                continue

    #a.columns=['目标楼盘','竞品楼盘','支持度','置信度','提升度']  
    comp_list=a          
    #comp_list['filter']=a.groupby(['目标楼盘'])['支持度'].rank(ascending=False,method='first')
    #comp_list=comp_list[comp_list['filter']<=10]
    return comp_list

#月维度的竞对列表
month_list = list(date['month_id'].drop_duplicates())
month = pd.DataFrame()

for a in month_list:    
    con = MysqlClient(db_host,database,user,password)
    data = con.query(" SELECT distinct newest_id,imei FROM  dwb_db.dwb_customer_browse_log where visit_month = '"+a+"' ")
    data = pd.merge(data,newest_city,how='inner',on=['newest_id'])
    data['newest_id'] = data['newest_id'].apply(lambda x:str(x))
    data['cou']=1
    city_list=list(data['city_id'].drop_duplicates())
    city_apart=locals()
    for i in range(len(city_list)):
        city_apart['floor_imei_'+city_list[i]]=data[data['city_id']==city_list[i]]
    comp_list_res=pd.DataFrame()
    print(a)
    for i in range(len(city_list)):
        print(city_list[i])
        city_comp_list=comp_list_cul(city_apart['floor_imei_'+city_list[i]])
        city_comp_list['city_id']=city_list[i]
        comp_list_res=comp_list_res.append(city_comp_list)
    comp_list_res=comp_list_res.reset_index(drop=True)
    #comp_list_res=comp_list_res.drop(['filter'],axis=1)
    comp_list_res['period']=a
    #comp_list_res.columns=['newest_id','comp_id','support','confidence','lift','city_id','period']
    month=month.append(comp_list_res)

month.drop_duplicates(inplace=True)

month.columns=['newest_id','comp_id','support','confidence','lift','city_id','period']
month['rank'] = month.groupby(['newest_id'])['lift'].rank(ascending=False,method='dense')


# In[20]:


def to_dws(result,table):
    engine = create_engine("mysql+pymysql://mysql:egSQ7HhxajHZjvdX@172.28.36.77:3306/dws_db?charset=utf8")
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)
to_dws(month, 'dws_compete_list_qua')


# In[ ]:





# In[21]:


#周维度的竞对列表
week_list = list(date['week_id'].drop_duplicates())
week = pd.DataFrame()
for a in week_list:
    con = MysqlClient(db_host,database,user,password)
    data = con.query(" SELECT distinct newest_id,imei FROM  dwb_db.dwb_customer_browse_log where current_week = '"+a+"' ")
    data=pd.merge(data,newest_city,how='inner',on=['newest_id'])
    data['newest_id']=data['newest_id'].apply(lambda x:str(x))
    data['cou']=1
    city_list=list(data['city_id'].drop_duplicates())
    city_apart=locals()
    for i in range(len(city_list)):
        city_apart['floor_imei_'+city_list[i]]=data[data['city_id']==city_list[i]]
    comp_list_res=pd.DataFrame()
    print(a)
    for i in range(len(city_list)):
        print(city_list[i])
        city_comp_list=comp_list_cul(city_apart['floor_imei_'+city_list[i]])
        city_comp_list['city_id']=city_list[i]
        comp_list_res=comp_list_res.append(city_comp_list)
    comp_list_res=comp_list_res.reset_index(drop=True)
    #comp_list_res=comp_list_res.drop(['filter'],axis=1)
    comp_list_res['period']=a
    #comp_list_res.columns=['newest_id','comp_id','support','confidence','lift','city_id','period']
    week=week.append(comp_list_res)

week.drop_duplicates(inplace=True)
week.columns=['newest_id','comp_id','support','confidence','lift','city_id','period']
week['rank'] = week.groupby(['newest_id'])['lift'].rank(ascending=False,method='dense')


# In[76]:

def to_dws(result,table):
    engine = create_engine("mysql+pymysql://mysql:egSQ7HhxajHZjvdX@172.28.36.77:3306/dws_db?charset=utf8")
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)
to_dws(week, 'dws_compete_list_qua')


# In[81]:


#周维度的竞对指数列表
week_list = list(date['week_id'].drop_duplicates())
week = pd.DataFrame()
for a in week_list:
    con = MysqlClient(db_host,database,user,password)
    data = con.query(" SELECT distinct newest_id,imei FROM  dwb_db.dwb_customer_browse_log where current_week = '"+a+"' ")
    compete_list=con.query("select city_id,newest_id,comp_id,confidence from dws_db.dws_compete_list_qua where period='"+a+"' ")
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
    cus_filter=con.query('''select distinct imei,1 cou from dws_db.dws_imei_browse_tag where concern='关注' ''')
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

    cus_filter=con.query('''select distinct imei,1 cou from dws_db.dws_imei_browse_tag where  intention='意向' ''')
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

    cus_filter=con.query('''select distinct imei,1 cou from dws_db.dws_imei_browse_tag where urgent='迫切' ''')
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
    cus_filter=con.query('''select distinct imei,1 cou from dws_db.dws_imei_browse_tag where cre='增长' ''')
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
    cus_filter=con.query('''select distinct imei,1 cou from dws_db.dws_imei_browse_tag where cre='活跃' ''')
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
    week=week.append(compete_list)


# In[82]:


week.drop_duplicates(inplace=True)
def to_dws(result,table):
    engine = create_engine("mysql+pymysql://mysql:egSQ7HhxajHZjvdX@172.28.36.77:3306/dws_db?charset=utf8")
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)
to_dws(week, 'dws_compete_list_sub')


# In[83]:





# In[105]:


#月维度的竞对列表
month_list = list(date['month_id'].drop_duplicates())
month = pd.DataFrame()
for a in month_list:
    con = MysqlClient(db_host,database,user,password)
    data = con.query(" SELECT distinct newest_id,imei FROM  dwb_db.dwb_customer_browse_log where visit_month = '"+a+"' ")
    compete_list=con.query("select city_id,newest_id,comp_id,confidence from dws_db.dws_compete_list_qua where period='"+a+"' ")
    print(a)
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
    cus_filter=con.query('''select distinct imei,1 cou from dws_db.dws_imei_browse_tag where concern='关注' ''')
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

    cus_filter=con.query('''select distinct imei,1 cou from dws_db.dws_imei_browse_tag where  intention='意向' ''')
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

    cus_filter=con.query('''select distinct imei,1 cou from dws_db.dws_imei_browse_tag where urgent='迫切' ''')
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
    cus_filter=con.query('''select distinct imei,1 cou from dws_db.dws_imei_browse_tag where cre='增长' ''')
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

    cus_filter=con.query('''select distinct imei,1 cou from dws_db.dws_imei_browse_tag where cre='活跃' ''')
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
    month=month.append(compete_list)


month.drop_duplicates(inplace=True)


# In[108]:


def to_dws(result,table):
    engine = create_engine("mysql+pymysql://mysql:egSQ7HhxajHZjvdX@172.28.36.77:3306/dws_db?charset=utf8")
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)
to_dws(month, 'dws_compete_list_sub')


# In[109]:


#不同人群竞品排名
con = MysqlClient(db_host,database,user,password)
data = con.query(" SELECT distinct newest_id,imei FROM  dwb_db.dwb_customer_browse_log where visit_date>= '"+start_date+"'  and visit_date<'"+end_date+"' ")

#city_list=list(compete_list['city_id'].drop_duplicates())

compete_list=con.query("select city_id,newest_id,comp_id,confidence from dws_db.dws_compete_list_qua where period='"+period+"' ")
compete_list

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
cus_filter=con.query('''select distinct imei,1 cou from dws_db.dws_imei_browse_tag where concern='关注' ''')
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


cus_filter=con.query('''select distinct imei,1 cou from dws_db.dws_imei_browse_tag where  intention='意向' ''')
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


cus_filter=con.query('''select distinct imei,1 cou from dws_db.dws_imei_browse_tag where urgent='迫切' ''')
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
cus_filter=con.query('''select distinct imei,1 cou from dws_db.dws_imei_browse_tag where cre='增长' ''')
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

cus_filter=con.query('''select distinct imei,1 cou from dws_db.dws_imei_browse_tag where cre='活跃' ''')
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

compete_list['period']= period
compete_list=compete_list.fillna(3)
compete_list.drop_duplicates(inplace=True)


# In[98]:


def to_dws(result,table):
    engine = create_engine("mysql+pymysql://mysql:egSQ7HhxajHZjvdX@172.28.36.77:3306/dws_db?charset=utf8")
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)
to_dws(compete_list, 'dws_compete_list_sub')



# In[ ]:




