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


#sql
import datetime
from dateutil.relativedelta import relativedelta

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

year_id = period[0:4]
quarter_id = period[5:6]
#start_month


# In[4]:


comp_list_full=con.query("select distinct city_id,newest_id,comp_id,period from dws_db.dws_compete_list_qua where  period = '"+period+"' ")

comp_confidence=con.query("select distinct comp_id newest_id,period,confidence from dws_db.dws_compete_list_qua where  period = '"+period+"' ")

imei_browse_tag=con.query("select imei,concern,intention,urgent,cre from dws_db.dws_imei_browse_tag where period = '"+period+"' ") 

comp_list = comp_list_full#[comp_list_full['period']==b]
#comp_list=con.query('''select * from dws_db.dws_compete_list_qua where period=%s'''%b)
if not comp_list.empty:

    newest_tag_agg=con.query(" SELECT distinct newest_id,imei FROM dwb_db.dwb_customer_browse_log where visit_date>='"+start_date+"' and visit_date<'"+end_date+"' ")
    newest_tag_agg['period'] = period

    newest_tag_agg=pd.merge(newest_tag_agg,imei_browse_tag,how='left',on=['imei'])
    newest_tag_concern=newest_tag_agg.groupby(['period','newest_id'])['concern'].count().reset_index()
    newest_tag_intention=newest_tag_agg.groupby(['period','newest_id'])['intention'].count().reset_index()
    newest_tag_urgent=newest_tag_agg.groupby(['period','newest_id'])['urgent'].count().reset_index()
    newest_tag_cre=newest_tag_agg.groupby(['period','newest_id','cre'])['imei'].count().reset_index()

    newest_tag_cre=newest_tag_cre.pivot_table(values='imei',index=['period','newest_id'],columns=['cre']).reset_index()
    newest_tag_cre.columns=['period','newest_id','increase','stock']
    newest_tag=pd.merge(newest_tag_concern,newest_tag_intention,how='left',on=['period','newest_id'])
    newest_tag=pd.merge(newest_tag,newest_tag_urgent,how='left',on=['period','newest_id'])
    newest_tag=pd.merge(newest_tag,newest_tag_cre,how='left',on=['period','newest_id'])
    comp_confidence1 = comp_confidence#[comp_confidence['period']==b]
    comp_confidence1=comp_confidence1.groupby(['newest_id','period']).mean().reset_index()
    newest_tag=pd.merge(newest_tag,comp_confidence1,how='left',on=['period','newest_id'])
    a = 0.04#random.randint(1,5)/1000
    newest_tag.fillna(a,inplace=True)
    #append楼盘指数标识
    dws_newest_browse_index = newest_tag #dws_newest_browse_index.append(newest_tag)
#comp_index1_c = comp_index1[['city_id','newest_id','comp_id','period','concern_l','intention_l''urgent_l','increase_l','stock_l','confidence_l']]

    comp_index=pd.merge(comp_list,newest_tag,on=['newest_id','period'],how='left')
    newest_tag.columns=['period','comp_id','concern_l','intention_l','urgent_l','increase_l','stock_l','confidence_l']
    comp_index1=pd.merge(comp_index,newest_tag,on=['comp_id','period'],how='left')
    comp_index1['confidence']=comp_index1['confidence'].apply(lambda x:float(x))
    comp_index_avg=comp_index1.groupby(['city_id','newest_id']).mean().reset_index()
    comp_index_avg['concern_l']=round(comp_index_avg['concern_l'],2)
    comp_index_avg['intention_l']=round(comp_index_avg['intention_l'],2)
    comp_index_avg['urgent_l']=round(comp_index_avg['urgent_l'],2)
    comp_index_avg['increase_l']=round(comp_index_avg['increase_l'],2)
    comp_index_avg['stock_l']=round(comp_index_avg['stock_l'],2)
    comp_index_avg['confidence_l']=round(comp_index_avg['confidence_l'],6)
    #comp_index_avg
    comp_index_avg.columns=['city_id','newest_id','newest_concern','newest_intention','newest_urgent','newest_increase','newest_stock','newest_confidence',
                            'avg_concern','avg_intention','avg_urgent','avg_increase','avg_stock','avg_confidence']
    comp_index1_c = comp_index1[['city_id','newest_id','comp_id','period','concern_l','intention_l','urgent_l','increase_l','stock_l','confidence_l']]

    comp_index_max=comp_index1_c.groupby(['city_id','newest_id']).max().reset_index()

    comp_index_max=comp_index_max[['city_id','newest_id','concern_l','intention_l','urgent_l','increase_l','stock_l','confidence_l']]
    comp_index_max.columns=['city_id','newest_id','max_concern','max_intention','max_urgent','max_increase','max_stock','max_confidence']
    comp_index_res=pd.merge(comp_index_avg,comp_index_max,on=['city_id','newest_id'],how='left')

    #comp_index_avg=comp_index1.groupby(['city_id','newest_id']).mean().reset_index()
    comp_index = comp_index_avg[['city_id','newest_id','newest_concern','newest_intention','newest_urgent','newest_increase','newest_stock','avg_confidence']]

    comp_index.drop_duplicates(subset=None, keep='first', inplace=True, ignore_index=False)
    comp_index.rename(columns={'avg_confidence':'newest_confidence'}, inplace=True)

    comp_index_min = comp_index.groupby(['city_id']).min().reset_index()
    comp_index_min.drop('newest_id',axis=1, inplace=True)
    comp_index_min.columns=['city_id','min_concern','min_intention','min_urgent','min_increase','min_stock','min_confidence']
    comp_index_max = comp_index.groupby(['city_id']).max().reset_index()
    comp_index_max.drop('newest_id',axis=1, inplace=True)
    comp_index_max.columns=['city_id','max_concern','max_intention','max_urgent','max_increase','max_stock','max_confidence']

    comp_index_01=pd.merge(comp_index,comp_index_min,on=['city_id'],how='left')
    comp_index_02=pd.merge(comp_index_01,comp_index_max,on=['city_id'],how='left')
    comp_index_res1 = comp_index_02 
    cat_cols = [col for col in comp_index_res1.columns if col not in ['city_id', 'newest_id']]
    for col in cat_cols:
        comp_index_res1[col] = comp_index_res1[col].astype('float64')

    comp_index_res1['confidence_index']=(comp_index_res1['newest_confidence']-comp_index_res1['min_confidence'])/(comp_index_res1['max_confidence']-comp_index_res1['min_confidence']) *5
    comp_index_res1['concern_index']=(comp_index_res1['newest_concern']-comp_index_res1['min_concern'])/(comp_index_res1['max_concern']-comp_index_res1['min_concern']) *5
    comp_index_res1['intention_index']=(comp_index_res1['newest_intention']-comp_index_res1['min_intention'])/(comp_index_res1['max_intention']-comp_index_res1['min_intention']) *5
    comp_index_res1['urgent_index']=(comp_index_res1['newest_urgent']-comp_index_res1['min_urgent'])/(comp_index_res1['max_urgent']-comp_index_res1['min_urgent']) *5
    comp_index_res1['increase_index']=(comp_index_res1['newest_increase']-comp_index_res1['min_increase'])/(comp_index_res1['max_increase']-comp_index_res1['min_increase']) *5
    comp_index_res1['stock_index']=(comp_index_res1['newest_stock']-comp_index_res1['min_stock'])/(comp_index_res1['max_stock']-comp_index_res1['min_stock']) *5
    comp_index_res2 = comp_index_res1[['city_id','newest_id','confidence_index','concern_index','intention_index','urgent_index','increase_index','stock_index']]
    comp_index_res0 = comp_index_res1[['city_id','newest_id']]
    comp_index_res0['comp_id'] = comp_index_res0['newest_id']
    comp_index_res01 = pd.concat([comp_list,comp_index_res0],axis=0)
    comp_index_res2.rename(columns={'newest_id':'comp_id'}, inplace=True)
    comp_index_02=pd.merge(comp_index_res01,comp_index_res2,on=['comp_id','city_id'],how='left')

    comp_index_02['concern_rank'] = comp_index_02.groupby(['newest_id'])['concern_index'].rank(ascending=False,method='dense')
    comp_index_02['intention_rank'] = comp_index_02.groupby(['newest_id'])['intention_index'].rank(ascending=False,method='dense')
    comp_index_02['urgent_rank'] = comp_index_02.groupby(['newest_id'])['urgent_index'].rank(ascending=False,method='dense')
    comp_index_02['increase_rank'] = comp_index_02.groupby(['newest_id'])['increase_index'].rank(ascending=False,method='dense')
    comp_index_02['stock_rank'] = comp_index_02.groupby(['newest_id'])['stock_index'].rank(ascending=False,method='dense')
    comp_index_02['confidence_rank'] = comp_index_02.groupby(['newest_id'])['confidence_index'].rank(ascending=False,method='dense')

    #求出每个楼盘的最大最小指数，并关联
    comp_index_06 = comp_index_02[['city_id','newest_id','comp_id','confidence_index','concern_index','intention_index','urgent_index','increase_index','stock_index']]
    comp_index_06.columns=['city_id','newest_id','comp_id','newest_confidence','newest_concern','newest_intention','newest_urgent','newest_increase','newest_stock']

    comp_index_07 = comp_index_06[comp_index_06['newest_id'] == comp_index_06['comp_id']]
    comp_index_07.drop('comp_id',axis=1, inplace=True)

    comp_index_index=comp_list[['city_id','newest_id']]
    comp_index_index.drop_duplicates(subset=None, keep='first', inplace=True, ignore_index=False)


    comp_index_06 = comp_index_06[comp_index_06['newest_id']!=comp_index_06['comp_id']]
    comp_index_avg_1 = comp_index_06.groupby(['city_id','newest_id']).mean().reset_index()
    #comp_index_avg_1.drop('comp_id',axis=1, inplace=True)

    comp_index_avg_1.columns=['city_id','newest_id','avg_confidence','avg_concern','avg_intention','avg_urgent','avg_increase','avg_stock']
    comp_index_max_1 = comp_index_06.groupby(['city_id','newest_id']).max().reset_index()
    comp_index_max_1.drop('comp_id',axis=1, inplace=True)
    comp_index_max_1.columns=['city_id','newest_id','max_confidence','max_concern','max_intention','max_urgent','max_increase','max_stock']

    comp_index_11=pd.merge(comp_index_07,comp_index_avg_1,on=['city_id','newest_id'],how='left')
    comp_index_12=pd.merge(comp_index_11,comp_index_max_1,on=['city_id','newest_id'],how='left')

    comp_index_index = comp_index_12
    #提取本楼盘信息和排名，插入数据库
    comp_index_03 = comp_index_02[['city_id','newest_id','comp_id','concern_rank','intention_rank','urgent_rank','increase_rank','stock_rank','confidence_rank']]
    comp_index_04 = comp_index_03[comp_index_03['newest_id'] == comp_index_03['comp_id']]
    comp_index_04.drop('comp_id',axis=1, inplace=True)
    comp_index_res['period'] = period
    comp_index_04['period'] = period
    comp_index_index['period'] = period

dws_analysis_index_value=comp_index_res
dws_analysis_index_rank=comp_index_04
dws_analysis_index_num=comp_index_index


#dws_analysis_index_value.dropna(axis=0, how='any', thresh=None, subset=None, inplace=True)
#dws_analysis_index_rank.dropna(axis=0, how='any', thresh=None, subset=None, inplace=True)
#dws_analysis_index_num.dropna(axis=0, how='any', thresh=None, subset=None, inplace=True)
def to_dws(result,table):
    engine = create_engine("mysql+pymysql://mysql:egSQ7HhxajHZjvdX@172.28.36.77:3306/dws_db?charset=utf8")
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)

to_dws(dws_analysis_index_value,'dws_analysis_index_value')
to_dws(dws_analysis_index_rank,'dws_analysis_index_rank')
to_dws(dws_analysis_index_num,'dws_analysis_index_num')


# In[20]:


#月指数
dws_analysis_index_value = pd.DataFrame()
dws_analysis_index_rank = pd.DataFrame()
dws_analysis_index_num = pd.DataFrame()
dws_newest_browse_index = pd.DataFrame()
#日期周期维度表读入

con = MysqlClient(db_host,database,user,password)

date = con.query(" SELECT cal_date,Revised_year,Revised_quarter,Revised_month,Revised_week,concat(Revised_year,'0',Revised_month) month_id,period week_id FROM  dws_db.dim_period_date where Revised_year='"+year_id+"' and Revised_quarter='"+quarter_id+"' ")
#date = con.query(" SELECT cal_date,Revised_year,Revised_quarter,Revised_month,Revised_week,concat(Revised_year,Revised_month) month_id,concat(Revised_year,'-',Revised_week) week_id FROM  dws_db.dim_period_date where Revised_year='"+year_id+"' and Revised_quarter='"+quarter_id+"' ")

#竞品概览-竞争力排名
month_list = list(date['month_id'].drop_duplicates())
month = pd.DataFrame()
for b in month_list:
    con = MysqlClient(db_host,database,user,password)
    #b = '2021-%s'%a 
    #a=1+x
    #b = '20210%s'%a 
    print(b)
    comp_list = con.query("select distinct city_id,newest_id,comp_id,period from dws_db.dws_compete_list_qua where  period = '"+b+"' ")
    #comp_list=con.query('''select * from dws_db.dws_compete_list_qua where period=%s'''%b)
    if not comp_list.empty:
        
        #comp_list['lift']=comp_list['lift'].apply(lambda x:float(x))
        #comp_base=comp_list.groupby(['city_id','period','newest_id'])['lift'].agg(['min','max']).reset_index()
        #comp_rank=pd.merge(comp_list,comp_base,on=['city_id','period','newest_id'],how='left')
        #comp_rank['level']=round((comp_rank['lift']-comp_rank['min'])/(comp_rank['max']-comp_rank['min'])*3)
        
        newest_tag_agg=con.query(" SELECT distinct newest_id,imei FROM dwb_db.dwb_customer_browse_log where visit_month = '"+b+"' ")
        newest_tag_agg['period'] = b
        #newest_tag_agg['newest_id']=newest_tag_agg['newest_id'].apply(lambda x:str(x))
        newest_tag_agg=pd.merge(newest_tag_agg,imei_browse_tag,how='left',on=['imei'])
        newest_tag_concern=newest_tag_agg.groupby(['period','newest_id'])['concern'].count().reset_index()
        newest_tag_intention=newest_tag_agg.groupby(['period','newest_id'])['intention'].count().reset_index()
        newest_tag_urgent=newest_tag_agg.groupby(['period','newest_id'])['urgent'].count().reset_index()
        newest_tag_cre=newest_tag_agg.groupby(['period','newest_id','cre'])['imei'].count().reset_index()

        newest_tag_cre=newest_tag_cre.pivot_table(values='imei',index=['period','newest_id'],columns=['cre']).reset_index()
        newest_tag_cre.columns=['period','newest_id','increase','stock']
        newest_tag=pd.merge(newest_tag_concern,newest_tag_intention,how='left',on=['period','newest_id'])
        newest_tag=pd.merge(newest_tag,newest_tag_urgent,how='left',on=['period','newest_id'])
        newest_tag=pd.merge(newest_tag,newest_tag_cre,how='left',on=['period','newest_id'])
        comp_confidence1 = con.query("select distinct comp_id newest_id,period,confidence from dws_db.dws_compete_list_qua where  period = '"+b+"' ")
        comp_confidence1=comp_confidence1.groupby(['newest_id','period']).mean().reset_index()
        newest_tag=pd.merge(newest_tag,comp_confidence1,how='left',on=['period','newest_id'])
        a = 0.04#random.randint(1,5)/1000
        newest_tag.fillna(a,inplace=True)
        #append楼盘指数标识
        dws_newest_browse_index = dws_newest_browse_index.append(newest_tag)
        
        
        
        comp_index=pd.merge(comp_list,newest_tag,on=['newest_id','period'],how='left')
        newest_tag.columns=['period','comp_id','concern_l','intention_l','urgent_l','increase_l','stock_l','confidence_l']
        comp_index1=pd.merge(comp_index,newest_tag,on=['comp_id','period'],how='left')
        comp_index1['confidence']=comp_index1['confidence'].apply(lambda x:float(x))
        comp_index_avg=comp_index1.groupby(['city_id','newest_id']).mean().reset_index()
        comp_index_avg['concern_l']=round(comp_index_avg['concern_l'],2)
        comp_index_avg['intention_l']=round(comp_index_avg['intention_l'],2)
        comp_index_avg['urgent_l']=round(comp_index_avg['urgent_l'],2)
        comp_index_avg['increase_l']=round(comp_index_avg['increase_l'],2)
        comp_index_avg['stock_l']=round(comp_index_avg['stock_l'],2)
        comp_index_avg['confidence_l']=round(comp_index_avg['confidence_l'],6)
        #comp_index_avg
        comp_index_avg.columns=['city_id','newest_id','newest_concern','newest_intention','newest_urgent','newest_increase','newest_stock','newest_confidence',
                                'avg_concern','avg_intention','avg_urgent','avg_increase','avg_stock','avg_confidence']
        comp_index1_c = comp_index1[['city_id','newest_id','comp_id','period','concern_l','intention_l','urgent_l','increase_l','stock_l','confidence_l']]

        comp_index_max=comp_index1_c.groupby(['city_id','newest_id']).max().reset_index()

        comp_index_max=comp_index_max[['city_id','newest_id','concern_l','intention_l','urgent_l','increase_l','stock_l','confidence_l']]
        comp_index_max.columns=['city_id','newest_id','max_concern','max_intention','max_urgent','max_increase','max_stock','max_confidence']
        comp_index_res=pd.merge(comp_index_avg,comp_index_max,on=['city_id','newest_id'],how='left')

        #comp_index_avg=comp_index1.groupby(['city_id','newest_id']).mean().reset_index()
        comp_index = comp_index_avg[['city_id','newest_id','newest_concern','newest_intention','newest_urgent','newest_increase','newest_stock','avg_confidence']]

        comp_index.drop_duplicates(subset=None, keep='first', inplace=True, ignore_index=False)
        comp_index.rename(columns={'avg_confidence':'newest_confidence'}, inplace=True)

        comp_index_min = comp_index.groupby(['city_id']).min().reset_index()
        comp_index_min.drop('newest_id',axis=1, inplace=True)
        comp_index_min.columns=['city_id','min_concern','min_intention','min_urgent','min_increase','min_stock','min_confidence']
        comp_index_max = comp_index.groupby(['city_id']).max().reset_index()
        comp_index_max.drop('newest_id',axis=1, inplace=True)
        comp_index_max.columns=['city_id','max_concern','max_intention','max_urgent','max_increase','max_stock','max_confidence']

        comp_index_01=pd.merge(comp_index,comp_index_min,on=['city_id'],how='left')
        comp_index_02=pd.merge(comp_index_01,comp_index_max,on=['city_id'],how='left')
        comp_index_res1 = comp_index_02 
        cat_cols = [col for col in comp_index_res1.columns if col not in ['city_id', 'newest_id']]
        for col in cat_cols:
            comp_index_res1[col] = comp_index_res1[col].astype('float64')

        comp_index_res1['confidence_index']=(comp_index_res1['newest_confidence']-comp_index_res1['min_confidence'])/(comp_index_res1['max_confidence']-comp_index_res1['min_confidence']) *5
        comp_index_res1['concern_index']=(comp_index_res1['newest_concern']-comp_index_res1['min_concern'])/(comp_index_res1['max_concern']-comp_index_res1['min_concern']) *5
        comp_index_res1['intention_index']=(comp_index_res1['newest_intention']-comp_index_res1['min_intention'])/(comp_index_res1['max_intention']-comp_index_res1['min_intention']) *5
        comp_index_res1['urgent_index']=(comp_index_res1['newest_urgent']-comp_index_res1['min_urgent'])/(comp_index_res1['max_urgent']-comp_index_res1['min_urgent']) *5
        comp_index_res1['increase_index']=(comp_index_res1['newest_increase']-comp_index_res1['min_increase'])/(comp_index_res1['max_increase']-comp_index_res1['min_increase']) *5
        comp_index_res1['stock_index']=(comp_index_res1['newest_stock']-comp_index_res1['min_stock'])/(comp_index_res1['max_stock']-comp_index_res1['min_stock']) *5
        comp_index_res2 = comp_index_res1[['city_id','newest_id','confidence_index','concern_index','intention_index','urgent_index','increase_index','stock_index']]
        comp_index_res0 = comp_index_res1[['city_id','newest_id']]
        comp_index_res0['comp_id'] = comp_index_res0['newest_id']
        comp_index_res01 = pd.concat([comp_list,comp_index_res0],axis=0)
        comp_index_res2.rename(columns={'newest_id':'comp_id'}, inplace=True)
        comp_index_02=pd.merge(comp_index_res01,comp_index_res2,on=['comp_id','city_id'],how='left')

        comp_index_02['concern_rank'] = comp_index_02.groupby(['newest_id'])['concern_index'].rank(ascending=False,method='dense')
        comp_index_02['intention_rank'] = comp_index_02.groupby(['newest_id'])['intention_index'].rank(ascending=False,method='dense')
        comp_index_02['urgent_rank'] = comp_index_02.groupby(['newest_id'])['urgent_index'].rank(ascending=False,method='dense')
        comp_index_02['increase_rank'] = comp_index_02.groupby(['newest_id'])['increase_index'].rank(ascending=False,method='dense')
        comp_index_02['stock_rank'] = comp_index_02.groupby(['newest_id'])['stock_index'].rank(ascending=False,method='dense')
        comp_index_02['confidence_rank'] = comp_index_02.groupby(['newest_id'])['confidence_index'].rank(ascending=False,method='dense')

        #求出每个楼盘的最大最小指数，并关联
        comp_index_06 = comp_index_02[['city_id','newest_id','comp_id','confidence_index','concern_index','intention_index','urgent_index','increase_index','stock_index']]
        comp_index_06.columns=['city_id','newest_id','comp_id','newest_confidence','newest_concern','newest_intention','newest_urgent','newest_increase','newest_stock']

        comp_index_07 = comp_index_06[comp_index_06['newest_id'] == comp_index_06['comp_id']]
        comp_index_07.drop('comp_id',axis=1, inplace=True)

        comp_index_index=comp_list[['city_id','newest_id']]
        comp_index_index.drop_duplicates(subset=None, keep='first', inplace=True, ignore_index=False)


        comp_index_06 = comp_index_06[comp_index_06['newest_id']!=comp_index_06['comp_id']]
        comp_index_avg_1 = comp_index_06.groupby(['city_id','newest_id']).mean().reset_index()
        #comp_index_avg_1.drop('comp_id',axis=1, inplace=True)

        comp_index_avg_1.columns=['city_id','newest_id','avg_confidence','avg_concern','avg_intention','avg_urgent','avg_increase','avg_stock']
        comp_index_max_1 = comp_index_06.groupby(['city_id','newest_id']).max().reset_index()
        comp_index_max_1.drop('comp_id',axis=1, inplace=True)
        comp_index_max_1.columns=['city_id','newest_id','max_confidence','max_concern','max_intention','max_urgent','max_increase','max_stock']

        comp_index_11=pd.merge(comp_index_07,comp_index_avg_1,on=['city_id','newest_id'],how='left')
        comp_index_12=pd.merge(comp_index_11,comp_index_max_1,on=['city_id','newest_id'],how='left')
        #comp_index_13=pd.merge(comp_index_12,comp_index_04,on=['city_id','newest_id','period'],how='left')

        comp_index_index = comp_index_12
        #提取本楼盘信息和排名，插入数据库
        comp_index_03 = comp_index_02[['city_id','newest_id','comp_id','concern_rank','intention_rank','urgent_rank','increase_rank','stock_rank','confidence_rank']]
        comp_index_04 = comp_index_03[comp_index_03['newest_id'] == comp_index_03['comp_id']]
        comp_index_04.drop('comp_id',axis=1, inplace=True)
        comp_index_res['period']=b
        comp_index_04['period'] = b
        comp_index_index['period'] =b
        dws_analysis_index_value=dws_analysis_index_value.append(comp_index_res)
        dws_analysis_index_rank=dws_analysis_index_rank.append(comp_index_04)
        dws_analysis_index_num=dws_analysis_index_num.append(comp_index_index)
        

dws_analysis_index_num = dws_analysis_index_num[dws_analysis_index_num['newest_confidence'].notnull()]


to_dws(dws_analysis_index_value,'dws_analysis_index_value')
to_dws(dws_analysis_index_rank,'dws_analysis_index_rank')
to_dws(dws_analysis_index_num,'dws_analysis_index_num')


# In[16]:


#周指数
dws_analysis_index_value = pd.DataFrame()
dws_analysis_index_rank = pd.DataFrame()
dws_analysis_index_num = pd.DataFrame()
dws_newest_browse_index = pd.DataFrame()
#日期周期维度表读入

con = MysqlClient(db_host,database,user,password)

date = con.query(" SELECT cal_date,Revised_year,Revised_quarter,Revised_month,Revised_week,concat(Revised_year,Revised_month) month_id,period week_id FROM  dws_db.dim_period_date where Revised_year='"+year_id+"' and Revised_quarter='"+quarter_id+"' ")
#date = con.query(" SELECT cal_date,Revised_year,Revised_quarter,Revised_month,Revised_week,concat(Revised_year,Revised_month) month_id,concat(Revised_year,'-',Revised_week) week_id FROM  dws_db.dim_date where Revised_year='"+year_id+"' and Revised_quarter='"+quarter_id+"' ")


#竞品概览-竞争力排名
week_list = list(date['week_id'].drop_duplicates())
week = pd.DataFrame()
for b in week_list:
    con = MysqlClient(db_host,database,user,password)
    #b = '2021-%s'%a 
    #a=1+x
    #b = '20210%s'%a 
    print(b)
    comp_list = con.query("select distinct city_id,newest_id,comp_id,period from dws_db.dws_compete_list_qua where  period = '"+b+"' ")
    #comp_list=con.query('''select * from dws_db.dws_compete_list_qua where period=%s'''%b)
    if not comp_list.empty:
        
        #comp_list['lift']=comp_list['lift'].apply(lambda x:float(x))
        #comp_base=comp_list.groupby(['city_id','period','newest_id'])['lift'].agg(['min','max']).reset_index()
        #comp_rank=pd.merge(comp_list,comp_base,on=['city_id','period','newest_id'],how='left')
        #comp_rank['level']=round((comp_rank['lift']-comp_rank['min'])/(comp_rank['max']-comp_rank['min'])*3)
        
        newest_tag_agg=con.query(" SELECT distinct newest_id,imei FROM dwb_db.dwb_customer_browse_log where current_week = '"+b+"' ")
        newest_tag_agg['period'] = b
        #newest_tag_agg['newest_id']=newest_tag_agg['newest_id'].apply(lambda x:str(x))
        newest_tag_agg=pd.merge(newest_tag_agg,imei_browse_tag,how='left',on=['imei'])
        newest_tag_concern=newest_tag_agg.groupby(['period','newest_id'])['concern'].count().reset_index()
        newest_tag_intention=newest_tag_agg.groupby(['period','newest_id'])['intention'].count().reset_index()
        newest_tag_urgent=newest_tag_agg.groupby(['period','newest_id'])['urgent'].count().reset_index()
        newest_tag_cre=newest_tag_agg.groupby(['period','newest_id','cre'])['imei'].count().reset_index()

        newest_tag_cre=newest_tag_cre.pivot_table(values='imei',index=['period','newest_id'],columns=['cre']).reset_index()
        newest_tag_cre.columns=['period','newest_id','increase','stock']
        newest_tag=pd.merge(newest_tag_concern,newest_tag_intention,how='left',on=['period','newest_id'])
        newest_tag=pd.merge(newest_tag,newest_tag_urgent,how='left',on=['period','newest_id'])
        newest_tag=pd.merge(newest_tag,newest_tag_cre,how='left',on=['period','newest_id'])
        comp_confidence1 = con.query("select distinct comp_id newest_id,period,confidence from dws_db.dws_compete_list_qua where  period = '"+b+"' ")
        comp_confidence1=comp_confidence1.groupby(['newest_id','period']).mean().reset_index()
        newest_tag=pd.merge(newest_tag,comp_confidence1,how='left',on=['period','newest_id'])
        a = 0.04#random.randint(1,5)/1000
        newest_tag.fillna(a,inplace=True)
        #append楼盘指数标识
        dws_newest_browse_index = dws_newest_browse_index.append(newest_tag)
        
        
        
        comp_index=pd.merge(comp_list,newest_tag,on=['newest_id','period'],how='left')
        newest_tag.columns=['period','comp_id','concern_l','intention_l','urgent_l','increase_l','stock_l','confidence_l']
        comp_index1=pd.merge(comp_index,newest_tag,on=['comp_id','period'],how='left')
        comp_index1['confidence']=comp_index1['confidence'].apply(lambda x:float(x))
        comp_index_avg=comp_index1.groupby(['city_id','newest_id']).mean().reset_index()
        comp_index_avg['concern_l']=round(comp_index_avg['concern_l'],2)
        comp_index_avg['intention_l']=round(comp_index_avg['intention_l'],2)
        comp_index_avg['urgent_l']=round(comp_index_avg['urgent_l'],2)
        comp_index_avg['increase_l']=round(comp_index_avg['increase_l'],2)
        comp_index_avg['stock_l']=round(comp_index_avg['stock_l'],2)
        comp_index_avg['confidence_l']=round(comp_index_avg['confidence_l'],6)
        #comp_index_avg
        comp_index_avg.columns=['city_id','newest_id','newest_concern','newest_intention','newest_urgent','newest_increase','newest_stock','newest_confidence',
                                'avg_concern','avg_intention','avg_urgent','avg_increase','avg_stock','avg_confidence']
        comp_index_max=comp_index.groupby(['city_id','newest_id']).max().reset_index()

        comp_index_max=comp_index_max[['city_id','newest_id','concern','intention','urgent','increase','stock','confidence']]
        comp_index_max.columns=['city_id','newest_id','max_concern','max_intention','max_urgent','max_increase','max_stock','max_confidence']
        comp_index_res=pd.merge(comp_index_avg,comp_index_max,on=['city_id','newest_id'],how='left')
        
        #comp_index_avg=comp_index1.groupby(['city_id','newest_id']).mean().reset_index()
        comp_index = comp_index_avg[['city_id','newest_id','newest_concern','newest_intention','newest_urgent','newest_increase','newest_stock','avg_confidence']]

        comp_index.drop_duplicates(subset=None, keep='first', inplace=True, ignore_index=False)
        comp_index.rename(columns={'avg_confidence':'newest_confidence'}, inplace=True)

        comp_index_min = comp_index.groupby(['city_id']).min().reset_index()
        comp_index_min.drop('newest_id',axis=1, inplace=True)
        comp_index_min.columns=['city_id','min_concern','min_intention','min_urgent','min_increase','min_stock','min_confidence']
        comp_index_max = comp_index.groupby(['city_id']).max().reset_index()
        comp_index_max.drop('newest_id',axis=1, inplace=True)
        comp_index_max.columns=['city_id','max_concern','max_intention','max_urgent','max_increase','max_stock','max_confidence']

        comp_index_01=pd.merge(comp_index,comp_index_min,on=['city_id'],how='left')
        comp_index_02=pd.merge(comp_index_01,comp_index_max,on=['city_id'],how='left')
        comp_index_res1 = comp_index_02 
        cat_cols = [col for col in comp_index_res1.columns if col not in ['city_id', 'newest_id']]
        for col in cat_cols:
            comp_index_res1[col] = comp_index_res1[col].astype('float64')

        comp_index_res1['confidence_index']=(comp_index_res1['newest_confidence']-comp_index_res1['min_confidence'])/(comp_index_res1['max_confidence']-comp_index_res1['min_confidence']) *5
        comp_index_res1['concern_index']=(comp_index_res1['newest_concern']-comp_index_res1['min_concern'])/(comp_index_res1['max_concern']-comp_index_res1['min_concern']) *5
        comp_index_res1['intention_index']=(comp_index_res1['newest_intention']-comp_index_res1['min_intention'])/(comp_index_res1['max_intention']-comp_index_res1['min_intention']) *5
        comp_index_res1['urgent_index']=(comp_index_res1['newest_urgent']-comp_index_res1['min_urgent'])/(comp_index_res1['max_urgent']-comp_index_res1['min_urgent']) *5
        comp_index_res1['increase_index']=(comp_index_res1['newest_increase']-comp_index_res1['min_increase'])/(comp_index_res1['max_increase']-comp_index_res1['min_increase']) *5
        comp_index_res1['stock_index']=(comp_index_res1['newest_stock']-comp_index_res1['min_stock'])/(comp_index_res1['max_stock']-comp_index_res1['min_stock']) *5
        comp_index_res2 = comp_index_res1[['city_id','newest_id','confidence_index','concern_index','intention_index','urgent_index','increase_index','stock_index']]
        comp_index_res0 = comp_index_res1[['city_id','newest_id']]
        comp_index_res0['comp_id'] = comp_index_res0['newest_id']
        comp_index_res01 = pd.concat([comp_list,comp_index_res0],axis=0)
        comp_index_res2.rename(columns={'newest_id':'comp_id'}, inplace=True)
        comp_index_02=pd.merge(comp_index_res01,comp_index_res2,on=['comp_id','city_id'],how='left')
        
        comp_index_02['concern_rank'] = comp_index_02.groupby(['newest_id'])['concern_index'].rank(ascending=False,method='dense')
        comp_index_02['intention_rank'] = comp_index_02.groupby(['newest_id'])['intention_index'].rank(ascending=False,method='dense')
        comp_index_02['urgent_rank'] = comp_index_02.groupby(['newest_id'])['urgent_index'].rank(ascending=False,method='dense')
        comp_index_02['increase_rank'] = comp_index_02.groupby(['newest_id'])['increase_index'].rank(ascending=False,method='dense')
        comp_index_02['stock_rank'] = comp_index_02.groupby(['newest_id'])['stock_index'].rank(ascending=False,method='dense')
        comp_index_02['confidence_rank'] = comp_index_02.groupby(['newest_id'])['confidence_index'].rank(ascending=False,method='dense')
        
        #求出每个楼盘的最大最小指数，并关联
        comp_index_06 = comp_index_02[['city_id','newest_id','comp_id','confidence_index','concern_index','intention_index','urgent_index','increase_index','stock_index']]
        comp_index_06.columns=['city_id','newest_id','comp_id','newest_confidence','newest_concern','newest_intention','newest_urgent','newest_increase','newest_stock']

        comp_index_07 = comp_index_06[comp_index_06['newest_id'] == comp_index_06['comp_id']]
        comp_index_07.drop('comp_id',axis=1, inplace=True)

        comp_index_index=comp_index_res[['city_id','newest_id']]
        comp_index_index.drop_duplicates(subset=None, keep='first', inplace=True, ignore_index=False)

        
        comp_index_06 = comp_index_06[comp_index_06['newest_id']!=comp_index_06['comp_id']]
        comp_index_avg_1 = comp_index_06.groupby(['city_id','newest_id']).mean().reset_index()
        #comp_index_avg_1.drop('comp_id',axis=1, inplace=True)

        comp_index_avg_1.columns=['city_id','newest_id','avg_confidence','avg_concern','avg_intention','avg_urgent','avg_increase','avg_stock']
        comp_index_max_1 = comp_index_06.groupby(['city_id','newest_id']).max().reset_index()
        comp_index_max_1.drop('comp_id',axis=1, inplace=True)
        comp_index_max_1.columns=['city_id','newest_id','max_confidence','max_concern','max_intention','max_urgent','max_increase','max_stock']

        comp_index_11=pd.merge(comp_index_07,comp_index_avg_1,on=['city_id','newest_id'],how='left')
        comp_index_12=pd.merge(comp_index_11,comp_index_max_1,on=['city_id','newest_id'],how='left')
        #comp_index_13=pd.merge(comp_index_12,comp_index_04,on=['city_id','newest_id','period'],how='left')

        comp_index_index = comp_index_12
        #提取本楼盘信息和排名，插入数据库
        comp_index_03 = comp_index_02[['city_id','newest_id','comp_id','concern_rank','intention_rank','urgent_rank','increase_rank','stock_rank','confidence_rank']]
        comp_index_04 = comp_index_03[comp_index_03['newest_id'] == comp_index_03['comp_id']]
        comp_index_04.drop('comp_id',axis=1, inplace=True)
        comp_index_res['period']=b
        comp_index_04['period'] = b
        comp_index_index['period'] =b
        dws_analysis_index_value=dws_analysis_index_value.append(comp_index_res)
        dws_analysis_index_rank=dws_analysis_index_rank.append(comp_index_04)
        dws_analysis_index_num=dws_analysis_index_num.append(comp_index_index)
        
dws_analysis_index_value.dropna(axis=0, how='any', thresh=None, subset=None, inplace=True)
dws_analysis_index_rank.dropna(axis=0, how='any', thresh=None, subset=None, inplace=True)
dws_analysis_index_num.dropna(axis=0, how='any', thresh=None, subset=None, inplace=True)
to_dws(dws_analysis_index_value,'dws_analysis_index_value')
to_dws(dws_analysis_index_rank,'dws_analysis_index_rank')
to_dws(dws_analysis_index_num,'dws_analysis_index_num')



