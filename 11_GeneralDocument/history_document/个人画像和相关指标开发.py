#!/usr/bin/env python
# coding: utf-8

# In[1]:


# -*- coding: utf-8 -*-
"""
Created on Fri Mar 26 16:44:47 2021

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


# In[2]:


def to_dws(result,table):
    engine = create_engine("mysql+pymysql://mysql:egSQ7HhxajHZjvdX@172.28.36.77:3306/dws_db?charset=utf8")
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)


# In[18]:


#意向客户总量#
con = MysqlClient(db_host,database,user,password)
ori=con.query('''SELECT imei,visit_date date,newest_id,pv FROM dwb_db.dwb_customer_browse_log where visit_date>=20201001 and visit_date<20210101''')
#意向客户总量#
newest_id=con.query('''select distinct newest_id,city_id city,county_id from dwb_db.dwb_newest_info''')
admit = con.query('''select distinct newest_id from dws_db.dws_newest_period_admit where period = '2020Q4' ''')
grouped2 = pd.merge(admit, newest_id, how='left', on=['newest_id'])
ori = pd.merge(grouped2, ori, how='inner', on=['newest_id'])
ori.rename(columns={'newest_id':'newest'},inplace=True)


#city_test=ori[['city','newest','imei']].drop_duplicates()
#data1 = pd.DataFrame([list(i) for i in res],columns=columnNames)
#ori.columns=['imei','date','city','county_id','newest','pv']
ori['date']=pd.to_datetime(ori['date'],format='%Y-%m-%d').dt.date

#to_dws(cus_sum_res,'dws_customer_sum')
cus_sum=ori.groupby(['city','newest']).agg({'imei':pd.Series.nunique}).reset_index()
cus_sum.columns=['city_name','newest_name','cou_imei']
cus_sum_city=cus_sum.groupby('city_name')['cou_imei'].mean().reset_index()
cus_sum_city.columns=['city_name','city_avg']
cus_sum_city['city_avg']=round(cus_sum_city['city_avg'],2)
cus_sum_res=pd.merge(cus_sum,cus_sum_city,how="left",on=['city_name'])
cus_sum_res['ratio']=round(cus_sum_res['cou_imei']/cus_sum_res['city_avg']-1,4)
cus_sum_res['period']='2020Q4'
cus_sum_res=cus_sum_res[['city_name','newest_name','period','cou_imei','city_avg','ratio']]
cus_sum_res.columns=['city_id','newest_id','period','cou_imei','city_avg','ratio']

#cus_sum_res


# In[ ]:





# In[19]:


to_dws(cus_sum_res,'dws_customer_sum')#画像首页意向用户数总量


# In[20]:


las=con.query('''select customer,concat(substr(idate,1,4),"-",substr(idate,5,2),"-",substr(idate,7,2)),city_name,floor_name,pv from odsdb.cust_browse_log_202006
union all
select customer,concat(substr(idate,1,4),"-",substr(idate,5,2),"-",substr(idate,7,2)),city_name,floor_name,pv from odsdb.cust_browse_log_202007
union all
select customer,concat(substr(idate,1,4),"-",substr(idate,5,2),"-",substr(idate,7,2)),city_name,floor_name,pv from odsdb.cust_browse_log_202008
''')
las.columns=['imei','date','city','newest','pv']
las['date']=pd.to_datetime(las['date'],format='%Y-%m-%d').dt.date


# In[21]:


#增存留占比#
cus_list=ori[['city','newest','imei']].drop_duplicates()
cus_last=las[['imei']].drop_duplicates()
cus_last['exist']="存量"
cus_list=pd.merge(cus_list,cus_last,how='left',on=['imei'])
cus_list.at[cus_list['exist'].isna(),'exist']="增量"


# In[22]:


cus_list


# In[23]:


cus_cre_res=cus_list.groupby(['city','newest','exist']).count().reset_index()
cus_cre_res['period']='2020Q4'
cus_cre_res.columns=['city_id','newest_id','exist','imei_num','period']
cus_cre_res


# In[24]:


cus_cre_res[cus_cre_res['newest_id'] == '4522a59909f23aa86b940f4c436f6340']


# In[25]:


to_dws(cus_cre_res,'dws_customer_cre')


# In[26]:


#意向客户趋势-按周
cus_combi_ori=ori.append(las,ignore_index=True)
cus_week=cus_combi_ori
cus_week['week']=cus_week['date'].apply(lambda x:x.strftime('%Y-%W'))
cus_week=cus_week[cus_week['week']>='2020-39']
cus_week=cus_week[['city','newest','week','imei']].drop_duplicates()
cus_week['last_week']=cus_week.sort_values(by=['imei','week']).groupby(['city','newest','imei']).shift(1)
cus_week.at[cus_week['last_week'].isna(),'last_week']="增量"
cus_week.at[cus_week['last_week']!="增量",'last_week']="存量"
cus_week_nn = cus_week[cus_week['week']>'2020-39']
cus_week_res=cus_week_nn.groupby(['city','newest','week','last_week'])['imei'].count().reset_index()
cus_week_res['period']='2020Q4'
cus_week_res.columns=['city_id','newest_id','week','exist','imei_num','period']
#to_dws(cus_week_res,'dws_customer_week')


# In[27]:


to_dws(cus_week_res,'dws_customer_week')


# In[28]:


#意向客户趋势-按月
cus_mon=cus_combi_ori
cus_mon['month']=cus_mon['date'].apply(lambda x:x.strftime('%Y%m'))
cus_mon=cus_mon[cus_mon['month']>='202009']
cus_mon=cus_mon[['city','newest','month','imei']].drop_duplicates()
cus_mon['last_month']=cus_mon.sort_values(by=['imei','month']).groupby(['city','newest','imei']).shift(1)
cus_mon.at[cus_mon['last_month'].isna(),'last_month']="增量"
cus_mon.at[cus_mon['last_month']!="增量",'last_month']="存量"
cus_mon_res=cus_mon.groupby(['city','newest','month','last_month'])['imei'].count().reset_index()
cus_mon_res['period']='2020Q4'
cus_mon_res.columns=['city_id','newest_id','month','exist','imei_num','period']
cus_mon_res = cus_mon_res[cus_mon_res['month']>'202009']
cus_mon_res


# In[29]:


cus_mon_res


# In[30]:


cus_mon_res


# In[31]:


to_dws(cus_mon_res,'dws_customer_month')


# In[39]:


a = str(city_list[0])
city_test[city_test['city']==a]


# In[ ]:





# In[ ]:





# In[42]:


#基本特征#
city_test=ori[['city','newest','imei']].drop_duplicates()
city_test['city'] = city_test['city'].astype("str")
city_list=list(ori['city'].drop_duplicates())
city_apart=locals()
for i in range(len(city_list)):
    city_apart['imei_tag_'+city_list[i]]=city_test[city_test['city']==city_list[i]]


# In[ ]:





# In[31]:


def summary_cul(city_detail):
    city_detail=pd.merge(city_detail,summary_tag,how='left',on='imei')
    a=pd.DataFrame()
    b=pd.DataFrame()
    c=pd.DataFrame()
#年龄#
    a=city_detail.groupby(['city','newest','age'])['imei'].count().reset_index()
    a.columns=['city','newest','tag_value','res_value']
    c=a.groupby(['city','newest'])['res_value'].sum().reset_index()
    c.columns=['city','newest','sum_value']
    a=pd.merge(a,c,how='left',on=['city','newest'])
    a['ratio']=a['res_value']/a['sum_value']
    a['tag']='年龄分布'
    b=b.append(a)
#职业#
    a=city_detail.groupby(['city','newest','career'])['imei'].count().reset_index()
    a.columns=['city','newest','tag_value','res_value']
    c=a.groupby(['city','newest'])['res_value'].sum().reset_index()
    c.columns=['city','newest','sum_value']
    a=pd.merge(a,c,how='left',on=['city','newest'])
    a['ratio']=a['res_value']/a['sum_value']
    a['tag']='职业分布'
    b=b.append(a)
#性别#
    a=city_detail.groupby(['city','newest','sex'])['imei'].count().reset_index()
    a.columns=['city','newest','tag_value','res_value']
    c=a.groupby(['city','newest'])['res_value'].sum().reset_index()
    c.columns=['city','newest','sum_value']
    a=pd.merge(a,c,how='left',on=['city','newest'])
    a['ratio']=a['res_value']/a['sum_value']
    a['tag']='性别分布'
    b=b.append(a)
#家庭结构#
    a=city_detail.groupby(['city','newest','family'])['imei'].count().reset_index()
    a.columns=['city','newest','tag_value','res_value']
    c=a.groupby(['city','newest'])['res_value'].sum().reset_index()
    c.columns=['city','newest','sum_value']
    a=pd.merge(a,c,how='left',on=['city','newest'])
    a['ratio']=a['res_value']/a['sum_value']
    a['tag']='家庭结构'
    b=b.append(a)
#学历等级#
    a=city_detail.groupby(['city','newest','education'])['imei'].count().reset_index()
    a.columns=['city','newest','tag_value','res_value']
    c=a.groupby(['city','newest'])['res_value'].sum().reset_index()
    c.columns=['city','newest','sum_value']
    a=pd.merge(a,c,how='left',on=['city','newest'])
    a['ratio']=a['res_value']/a['sum_value']
    a['tag']='学历等级'
    b=b.append(a)
    return b


# In[32]:


summary_tag=con.query('''SELECT imei,age_group age,career,case when sex='男' then '男性' when sex='女' then '女性' end sex,case when marriage='未婚' then '未婚人士' 
when (marriage='已婚' and have_child='有') then '有孩家庭'
when marriage='已婚' then '两人世界' end family,case when education ='高' then '高' when education ='中' then '中' when education ='低' then '低' else null end education
FROM dwb_db.dwb_customer_imei_tag where period='2021Q1'  ''')


# In[33]:


#summary_tag


# In[34]:


#清洗职业数据
def function_career(x):
    output=None
    if x=='IT从业人员' :
        output='专业技术人员'
    elif x=='IT从业者' :
        output='专业技术人员'
    elif x=='O2O从业人员' :
        output='服务人员'
    elif x=='个人店主' :
        output='私营企业主'
    elif x=='中小商家' :
        output='私营企业主'
    elif x=='中小学生' :
        output='其他'
    elif x=='人力资源人员' :
        output='职员'
    elif x=='企业人员' :
        output='职员'
    elif x=='会计' :
        output='专业技术人员'
    elif x=='保险从业人员' :
        output='职员'
    elif x=='保险代理人' :
        output='职员'
    elif x=='公务人员' :
        output='公务员'
    elif x=='公务员' :
        output='公务员'
    elif x=='其他' :
        output='其他'
    elif x=='农业从业人员' :
        output='农业劳动者'
    elif x=='初高中生' :
        output='其他'
    elif x=='医护人员' :
        output='专业技术人员'
    elif x=='医生' :
        output='专业技术人员'
    elif x=='司机' :
        output='服务人员'
    elif x=='售后服务员' :
        output='服务人员'
    elif x=='大学生' :
        output='其他'
    elif x=='导游' :
        output='服务人员'
    elif x=='建筑人员' :
        output='工人'
    elif x=='建筑行业从业人员' :
        output='专业技术人员'
    elif x=='建造师' :
        output='专业技术人员'
    elif x=='律师' :
        output='专业技术人员'
    elif x=='快递员' :
        output='服务人员'
    elif x=='房产中介' :
        output='职员'
    elif x=='房地产行业从业人员' :
        output='职员'
    elif x=='房地产销售人员' :
        output='职员'
    elif x=='护士' :
        output='专业技术人员'
    elif x=='教练' :
        output='专业技术人员'
    elif x=='机场工作人员' :
        output='职员'
    elif x=='汽车站工作人员' :
        output='职员'
    elif x=='派送员' :
        output='服务人员'
    elif x=='消防人员' :
        output='专业技术人员'
    elif x=='涉农人员' :
        output='农业劳动者'
    elif x=='港口工作人员' :
        output='职员'
    elif x=='火车站工作人员' :
        output='职员'
    elif x=='白领' :
        output='职员'
    elif x=='科研工作者' :
        output='专业技术人员'
    elif x=='网约车司机' :
        output='服务人员'
    elif x=='老师' :
        output='专业技术人员'
    elif x=='装修人员' :
        output='工人'
    elif x=='警务人员' :
        output='公务员'
    elif x=='证券基金从业人员' :
        output='职员'
    elif x=='货车运输人员' :
        output='工人'
    elif x=='退休' :
        output='其他'
    elif x=='配送员' :
        output='服务人员'
    elif x=='金融行业从业人员' :
        output='职员'
    elif x=='铁路工作人员' :
        output='事业单位'
    elif x=='银行职员' :
        output='事业单位'
    elif x=='销售人员' :
        output='职员'
    return output
 
summary_tag['career'] = summary_tag['career'].apply(lambda x: function_career(x))


# In[18]:


summary_res=pd.DataFrame()
for i in range(len(city_list)):
    city_summary=summary_cul(city_apart['imei_tag_'+city_list[i]])
    summary_res=summary_res.append(city_summary)


# In[19]:


summary_res['ratio']=round(summary_res['ratio'],4)
summary_res=summary_res.reset_index(drop=True)
summary_res['period']='2021Q1'
summary_res.columns=['city_id','newest_id','tag_value','value_num','total_num','ratio','tag_name','period']
to_dws(summary_res,'dws_tag_basic')


# In[20]:


#购买能力
purchase_tag=con.query('''SELECT imei,consume_power,income_level,have_car,have_house,mobile_brand,mobile_model,
case when mobile_value<1000 then '1000以下' 
when mobile_value<2000 then '1000-2000' 
when mobile_value<3000 then '2000-3000' 
when mobile_value<4000 then '3000-4000' 
when mobile_value<5000 then '4000-5000' 
when mobile_value<10000 then '5000-10000' 
when mobile_value>=10000 then '10000以上'
end mobile_value,travel_dest_type,long_traffic_prefer,
case when hotel_duration<=2 then '2天以内'
when hotel_duration<=5 then '3-5天'
when hotel_duration<=8 then '6-8天'
when hotel_duration>8 then '9天以上' end travel_duration,hotel_level_prefer
FROM  dwb_db.dwb_customer_imei_tag where period='2021Q1' ''')
purchase_tag['mobile_brand']=purchase_tag['mobile_brand'].apply(lambda x:re.split('-',str(x))[0])


# In[21]:


def purchase_cul(city_detail):
    city_detail=pd.merge(city_detail,purchase_tag,how='left',on='imei')
    a=pd.DataFrame()
    b=pd.DataFrame()
    c=pd.DataFrame()
#消费力#
    a=city_detail.groupby(['city','newest','consume_power'])['imei'].count().reset_index()
    a.columns=['city','newest','tag_value','res_value']
    c=a.groupby(['city','newest'])['res_value'].sum().reset_index()
    c.columns=['city','newest','sum_value']
    a=pd.merge(a,c,how='left',on=['city','newest'])
    a['ratio']=a['res_value']/a['sum_value']
    a['tag']='消费力'
    b=b.append(a)
#收入水平#
    a=city_detail.groupby(['city','newest','income_level'])['imei'].count().reset_index()
    a.columns=['city','newest','tag_value','res_value']
    c=a.groupby(['city','newest'])['res_value'].sum().reset_index()
    c.columns=['city','newest','sum_value']
    a=pd.merge(a,c,how='left',on=['city','newest'])
    a['ratio']=a['res_value']/a['sum_value']
    a['tag']='收入水平'
    b=b.append(a)
#有房#
    a=city_detail.groupby(['city','newest','have_house'])['imei'].count().reset_index()
    a.columns=['city','newest','tag_value','res_value']
    c=a.groupby(['city','newest'])['res_value'].sum().reset_index()
    c.columns=['city','newest','sum_value']
    a=pd.merge(a,c,how='left',on=['city','newest'])
    a['ratio']=a['res_value']/a['sum_value']
    a['tag']='有无房产'
    b=b.append(a)
#有车#
    city_detail['have_car'] = city_detail['have_car'].apply(lambda x: function_car(x))
    a=city_detail.groupby(['city','newest','have_car'])['imei'].count().reset_index()
    a.columns=['city','newest','tag_value','res_value']
    c=a.groupby(['city','newest'])['res_value'].sum().reset_index()
    c.columns=['city','newest','sum_value']
    a=pd.merge(a,c,how='left',on=['city','newest'])
    a['ratio']=a['res_value']/a['sum_value']
    a['tag']='有无车辆'
    b=b.append(a)
#手机价位分布#
    a=city_detail.groupby(['city','newest','mobile_value'])['imei'].count().reset_index()
    a.columns=['city','newest','tag_value','res_value']
    c=a.groupby(['city','newest'])['res_value'].sum().reset_index()
    c.columns=['city','newest','sum_value']
    a=pd.merge(a,c,how='left',on=['city','newest'])
    a['ratio']=a['res_value']/a['sum_value']
    a['tag']='手机价位'
    b=b.append(a)    
#旅游消费#
    a=city_detail.groupby(['city','newest','travel_dest_type'])['imei'].count().reset_index()
    a.columns=['city','newest','tag_value','res_value']
    c=a.groupby(['city','newest'])['res_value'].sum().reset_index()
    c.columns=['city','newest','sum_value']
    a=pd.merge(a,c,how='left',on=['city','newest'])
    a['ratio']=a['res_value']/a['sum_value']
    a['tag']='旅游消费'
    b=b.append(a)    
#长途交通#
    a=city_detail[['city','newest','imei','long_traffic_prefer']].dropna()
    a=a.groupby(['city','newest'])['long_traffic_prefer'].apply(lambda x:','.join(x)).reset_index()
    a['traffic_cou']=a['long_traffic_prefer'].apply(lambda x:re.split(',',str(x)))
    a['traffic_res']=a['traffic_cou'].apply(lambda x:dict(Counter(x)))
    c=pd.DataFrame()
    for i in range(len(a)):
        a1=pd.DataFrame()
        a1['tag_value']=list(a.at[i,'traffic_res'])
        a1['res_value']=list(a.at[i,'traffic_res'].values())
        a1['city']=a.at[i,'city']
        a1['newest']=a.at[i,'newest']
        a1['sum_value']=sum(a.at[i,'traffic_res'].values())
        c=c.append(a1)
        c=c[['city','newest','tag_value','res_value','sum_value']]
    c['ratio']=c['res_value']/c['sum_value']
    c['tag']='长途交通'
    b=b.append(c)    
#度假时长#
    a=city_detail.groupby(['city','newest','travel_duration'])['imei'].count().reset_index()
    a.columns=['city','newest','tag_value','res_value']
    c=a.groupby(['city','newest'])['res_value'].sum().reset_index()
    c.columns=['city','newest','sum_value']
    a=pd.merge(a,c,how='left',on=['city','newest'])
    a['ratio']=a['res_value']/a['sum_value']
    a['tag']='度假时长'
    b=b.append(a)    
#酒店消费分析#
    a=city_detail.groupby(['city','newest','hotel_level_prefer'])['imei'].count().reset_index()
    a.columns=['city','newest','tag_value','res_value']
    c=a.groupby(['city','newest'])['res_value'].sum().reset_index()
    c.columns=['city','newest','sum_value']
    a=pd.merge(a,c,how='left',on=['city','newest'])
    a['ratio']=a['res_value']/a['sum_value']
    a['tag']='酒店消费分析'
    b=b.append(a)   
    return b


# In[22]:


def function_car(x):
    output=None
    if x=='N' :
        output='N'
    elif x=='Y' :
        output='Y'
    return output


# In[23]:


purchase_res=pd.DataFrame()
for i in range(len(city_list)):
    city_purchase = purchase_cul(city_apart['imei_tag_'+city_list[i]])    
    purchase_res=purchase_res.append(city_purchase)
purchase_res=purchase_res.reset_index(drop=True)
purchase_res['period']='2021Q1'
purchase_res['ratio']=round(purchase_res['ratio'],4)
purchase_res.columns=['city_id','newest_id','tag_value','value_num','total_num','ratio','tag_name','period']
#to_dws(purchase_res,'dws_tag_purchase')


# In[73]:


'''def purchase_mobile_cul(city_detail):
    city_detail=pd.merge(city_detail,purchase_tag,how='left',on='imei')
    a=pd.DataFrame()
    b=pd.DataFrame()
    #c=pd.DataFrame()
#手机品牌型号占比#
    city_detail['mobile_brand'] = city_detail['mobile_brand'].apply(lambda x: function_mobile(x))
    a=city_detail.groupby(['city','newest','mobile_brand','mobile_model'])['imei'].count()
    a.columns=['city','newest','mobile_brand','mobile_model','res_value']
    b=b.append(a)
    return b'''


# In[176]:


#购买能力
con = MysqlClient(db_host,database,user,password)
ori=con.query('''SELECT imei,visit_date,city_id,county_id,newest_id,pv FROM dwb_db.dwb_customer_browse_log where visit_date>=20201001 and visit_date<20210101''')
ori.columns=['imei','date','city','county_id','newest','pv']
ori['date']=pd.to_datetime(ori['date'],format='%Y-%m-%d').dt.date


# In[177]:


ori


# In[178]:


city_test=ori[['city','newest','imei']].drop_duplicates()
aplle_mobile=con.query(''' select city_id,newest_id,'苹果' mobile_brand,'苹果' mobile_model,count(distinct imei) res_value from dwb_db.dwb_customer_browse_log where  source='苹果' and visit_date>=20201001 and visit_date<20210101 group by city_id,newest_id ''')
aplle_mobile.columns=['city','newest','mobile_brand','mobile_model','res_value']

#purchase_mobile_res.groupby(['mobile_brand'])['res_value'].sum()


# In[179]:


def function_mobile(x):
    output=None
    if x=='华为' :
        output='华为'
    elif x=='荣耀' :
        output='华为'
    elif x=='三星' :
        output='三星'
    elif x=='小米' :
        output='小米'
    elif x=='VIVO' :
        output='VIVO'        
    elif x=='OPPO' :
        output='OPPO'
    else:
        output='其他'
    return output


# In[180]:


'''purchase_mobile_res=pd.DataFrame()

for i in range(len(city_list)):
    try:
        city_purchase_mobile=purchase_mobile_cul(city_apart['imei_tag_'+city_list[i]])    
    except:
        print(city_list[i]+"空tag")
    purchase_mobile_res=purchase_mobile_res.append(city_purchase_mobile)'''


# In[181]:


#city_test

city_test_tag=pd.merge(city_test,purchase_tag,how='left',on='imei')

city_test_tag['mobile_brand'] = city_test_tag['mobile_brand'].apply(lambda x: function_mobile(x))
a=city_test_tag.groupby(['city','newest','mobile_brand','mobile_model'])['imei'].count().reset_index()
a


# In[182]:


a.columns=['city','newest','mobile_brand','mobile_model','res_value']
a


# In[183]:


a = pd.concat([a, aplle_mobile], axis=0)
d=a.groupby(['city','newest','mobile_brand'])['res_value'].sum().reset_index()
d.columns=['city','newest','mobile_brand','brand_value']
a=pd.merge(a,d,how='left',on=['city','newest','mobile_brand'])


# In[184]:


a


# In[185]:


c=a.groupby(['city','newest'])['res_value'].sum().reset_index()
c.columns=['city','newest','sum_value']
a=pd.merge(a,c,how='left',on=['city','newest'])
a


# In[186]:


a['brand_ratio']=a['brand_value']/a['sum_value']
a['model_ratio']=a['res_value']/a['brand_value']
a['tag']='手机品牌型号占比'


# In[187]:


a


# In[188]:


#purchase_mobile_res.groupby(['newest']).sum()
#purchase_mobile_res.mobile_brand.describe()
#purchase_mobile_res
a.drop_duplicates(inplace=True)
a


# In[189]:


purchase_mobile_res = a


# In[190]:


#purchase_mobile_res[purchase_mobile_res['mobile_brand']=='苹果']
a[a['newest']=='0109639c53f8fe047886c5c777ced440']


# In[191]:


purchase_mobile_res['model_ratio'] = purchase_mobile_res['model_ratio'].astype(float)


# In[192]:


purchase_mobile_res=purchase_mobile_res.reset_index(drop=True)
purchase_mobile_res['period']='2020Q4'
purchase_mobile_res['brand_ratio']=round(purchase_mobile_res['brand_ratio'],4)
purchase_mobile_res['model_ratio']=round(purchase_mobile_res['model_ratio'],4)
purchase_mobile_res.columns=['city_id','newest_id','mobile_brand','mobile_model','model_num','brand_num','total_num','brand_ratio','model_ratio','tag_name','period']


# In[193]:


to_dws(purchase_mobile_res,'dws_tag_purchase_mobile')


# In[ ]:





# In[ ]:





# In[ ]:





# In[26]:


#生活方式

lifestyle_traffic_tag=con.query('''SELECT imei,workday_traffic,holiday_traffic FROM dwb_db.dwb_customer_imei_tag where ori_table='origin_estate.ori_jiguang_personal_tag_i' ''')


# In[24]:


def lifestyle_traffic_cul(city_detail):
    city_detail=pd.merge(city_detail,lifestyle_traffic_tag,how='left',on='imei')
    a=pd.DataFrame()
    b=pd.DataFrame()
    c=pd.DataFrame()
    d=pd.DataFrame()
    e=pd.DataFrame()
    f=pd.DataFrame()
#工作日出行占比#
    a=city_detail[['city','newest','imei','workday_traffic']].dropna()
    a=a.groupby(['city','newest'])['workday_traffic'].apply(lambda x:','.join(x)).reset_index()
    a['traffic_cou']=a['workday_traffic'].apply(lambda x:re.split(',',str(x)))
    a['traffic_res']=a['traffic_cou'].apply(lambda x:dict(Counter(x)))
    c=pd.DataFrame()
    for i in range(len(a)):
        a1=pd.DataFrame()
        a1['tag_value']=list(a.at[i,'traffic_res'])
        a1['workday_value']=list(a.at[i,'traffic_res'].values())
        a1['city']=a.at[i,'city']
        a1['newest']=a.at[i,'newest']
        a1['workday_sum']=sum(a.at[i,'traffic_res'].values())
        c=c.append(a1)
        c=c[['city','newest','tag_value','workday_value','workday_sum']]
    c['workday_ratio']=c['workday_value']/c['workday_sum']
#节假日出行占比#
    d=city_detail[['city','newest','imei','holiday_traffic']].dropna()
    d=d.groupby(['city','newest'])['holiday_traffic'].apply(lambda x:','.join(x)).reset_index()
    d['traffic_cou']=d['holiday_traffic'].apply(lambda x:re.split(',',str(x)))
    d['traffic_res']=d['traffic_cou'].apply(lambda x:dict(Counter(x)))
    e=pd.DataFrame()
    for i in range(len(d)):
        d1=pd.DataFrame()
        d1['tag_value']=list(d.at[i,'traffic_res'])
        d1['holiday_value']=list(d.at[i,'traffic_res'].values())
        d1['city']=d.at[i,'city']
        d1['newest']=d.at[i,'newest']
        d1['holiday_sum']=sum(d.at[i,'traffic_res'].values())
        e=e.append(d1)
        e=e[['city','newest','tag_value','holiday_value','holiday_sum']]
    e['holiday_ratio']=e['holiday_value']/e['holiday_sum']
    f=pd.merge(c,e,how='outer',on=['city','newest','tag_value'])
    b=b.append(f)
    return b


# In[27]:


lifestyle_traffic_res=pd.DataFrame()
for i in range(len(city_list)):
    try:
        city_lifestyle_traffic=lifestyle_traffic_cul(city_apart['imei_tag_'+city_list[i]])    
    except:
        print(city_list[i]+"空tag")
    lifestyle_traffic_res=lifestyle_traffic_res.append(city_lifestyle_traffic)
lifestyle_traffic_res=lifestyle_traffic_res.reset_index(drop=True)
lifestyle_traffic_res['period']='2020Q4'
lifestyle_traffic_res['workday_ratio']=round(lifestyle_traffic_res['workday_ratio'],4)
lifestyle_traffic_res['holiday_ratio']=round(lifestyle_traffic_res['holiday_ratio'],4)
lifestyle_traffic_res.columns=['city_id','newest_id','tag_value','workday_value_num','workday_total_num','workday_ratio','holiday_value_num','holiday_total_num','holiday_ratio','period']
to_dws(lifestyle_traffic_res,'dws_tag_lifestyle_traffic')


# In[3]:


# -*- coding: utf-8 -*-
"""
Created on Fri Mar 26 16:44:47 2021

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
    engine = create_engine("mysql+pymysql://mysql:egSQ7HhxajHZjvdX@172.28.56.90:3306/dws_db?charset=utf8")
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)


# In[4]:


con = MysqlClient(db_host,database,user,password)
df=con.query('''SELECT * FROM dws_db.dws_tag_purchase_mobile''')


# In[6]:


df.drop('id',axis=1, inplace=True)


# In[7]:


df.drop_duplicates(subset=None, keep='first', inplace=False)


# In[8]:


to_dws(df,'dws_tag_purchase_mobile_tmp')


# In[ ]:





# In[ ]:




