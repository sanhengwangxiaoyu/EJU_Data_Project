#In[]

import sys
from typing import Tuple
import pandas as pd
import numpy as np
import time
import configparser
import os
from pandas.core import groupby
import pymysql



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
        cur.close()
        return res,columnNames
    def close(self):
        self.conn.close()

# con = MysqlClient(db_host,database,user,password)

pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
engine = create_engine('mysql+mysqldb://'+'mysql'+':'+'egSQ7HhxajHZjvdX'+'@'+'172.28.36.77'+':'+'3306'+'/'+'dws_db')# 初始化引擎
con = MysqlClient(db_host,database,user,password)



# 购房特征-总价分布
# 需求：每个楼盘的用户都看过哪些总价区间，浏览占比是多少
# 逻辑：先把每个楼盘都有哪些用户筛选出来，再把这些用户每个季度看过的区间以及对应的浏览量求和，再匹配对应的楼盘

# 取每个用户各季度浏览过的楼盘次数
period_value = '2020Q4'
start_date = '2020-10-01'
end_date = '2021-01-01'
start_idate = '20191001'
end_idate = '20201001'

# period_value = '2021Q1'
# start_date = '2021-01-01'
# end_date = '2021-04-01'
# start_idate = '20200101'
# end_idate = '20210101'

# period_value = '2021Q2'
# start_date = '2021-04-01'
# end_date = '2021-07-01'
# start_idate = '20200701'
# end_idate = '20210701'

#In[]

res,columnNames = con.query(" SELECT imei,newest_id,visit_date FROM dwb_db.dwb_customer_browse_log where visit_date >= '"+start_date+"' and visit_date<'"+end_date+"' ")

df = pd.DataFrame([list(i) for i in res],columns=columnNames)
df.visit_date = pd.to_datetime(df.visit_date)
df['quarter'] = pd.PeriodIndex(df.visit_date, freq='Q')

df0 = df.groupby(['imei','newest_id','quarter'])['visit_date'].count().to_frame('browse_number').reset_index()

# 城市对应表并补充缺失值
res,columnNames = con.query('''
SELECT 
distinct
city_id,city_name,city_level_desc
from 
dws_db.dim_geography 
where city_level_desc is not null and  city_level_desc <> ''
''')
df1 = pd.DataFrame([list(i) for i in res],columns=columnNames)
# 补4个没有城市等级的城市
# dfn = pd.DataFrame([['610100','西安市', '二线城市'], ['442000','中山市', '三线城市'],['512000','资阳市','四线城市'],['422800','恩施土家族苗族自治州','四线城市']], columns=('city_id','city_name','city_level_desc'))
# df1=df1.append(dfn).reset_index(drop=True)
# df1['city_id'] = df1['city_id'].apply(pd.to_numeric)

# 取楼盘信息关联城市
res,columnNames = con.query('''
SELECT newest_id,city_id FROM dwb_db.dwb_newest_info WHERE dr=0 
''')
df2 = pd.DataFrame([list(i) for i in res],columns=columnNames)
df2['city_id'] = df2['city_id'].apply(pd.to_numeric)
df2 = pd.merge(df2,df1,how='inner',on='city_id')


# 取楼盘户型信息并关联城市信息-校验用
res,columnNames = con.query('''
SELECT newest_id,layout_price FROM dwb_db.dwb_newest_layout WHERE dr=0 and layout_price>20
''')
df3 = pd.DataFrame([list(i) for i in res],columns=columnNames)
df3 = pd.merge(df3,df2,how='inner',on='newest_id')

# 总价区间与户型区间归类
condlist=[
np.logical_and(df3.city_level_desc=='一线城市',df3.layout_price<270),
np.logical_and(df3.city_level_desc=='一线城市',df3.layout_price<360),
np.logical_and(df3.city_level_desc=='一线城市',df3.layout_price<460),
np.logical_and(df3.city_level_desc=='一线城市',df3.layout_price<625),
np.logical_and(df3.city_level_desc=='一线城市',df3.layout_price<1050),
np.logical_and(df3.city_level_desc=='一线城市',df3.layout_price>=1050),

np.logical_and(df3.city_level_desc=='二线城市',df3.layout_price<100),
np.logical_and(df3.city_level_desc=='二线城市',df3.layout_price<135),
np.logical_and(df3.city_level_desc=='二线城市',df3.layout_price<170),
np.logical_and(df3.city_level_desc=='二线城市',df3.layout_price<220),
np.logical_and(df3.city_level_desc=='二线城市',df3.layout_price<320),
np.logical_and(df3.city_level_desc=='二线城市',df3.layout_price>=320),

np.logical_and(df3.city_level_desc=='三线城市',df3.layout_price<65),
np.logical_and(df3.city_level_desc=='三线城市',df3.layout_price<100),
np.logical_and(df3.city_level_desc=='三线城市',df3.layout_price<135),
np.logical_and(df3.city_level_desc=='三线城市',df3.layout_price<180),
np.logical_and(df3.city_level_desc=='三线城市',df3.layout_price<260),
np.logical_and(df3.city_level_desc=='三线城市',df3.layout_price>=260)
] 
choicelist=[
'270以下','270~360','360~460','460~625','625~1050','1050以上', 
'100以下','100~135','135~170','170~220','220~320','320以上', 
'65以下','65~100','100~135','135~180','180~260','260以上'
]
aa=np.select(condlist,choicelist)
df3['layout_price1'] = aa


# 关联每个用户看过的楼盘都是什么户型
df4 = pd.merge(df0,df3,how='inner',on='newest_id')


# 每个用户每个季度看过的总价都有哪些,每个总价区间看过几次
df5 = df4.groupby(['imei','city_id','quarter','city_level_desc','layout_price1'])['browse_number'].sum().reset_index()
df5
# 每个用户看过的户型都有哪些,每个户型区间看过几次
# df6 = df4.groupby(['imei','city_id','quarter','city_level_desc','room1'])['browse_number'].sum().reset_index()
# df6

# 每个用户各季度浏览过哪些楼盘关联浏览过的总价区间
col_n = ['imei','newest_id','quarter']
df7 = pd.DataFrame(df,columns = col_n).drop_duplicates()
df7 = pd.merge(df7,df2,how='inner',on='newest_id' )

df8 = pd.merge(df7, df5,how='inner',on=['imei','quarter','city_id','city_level_desc'])
df8 = df8.groupby(['city_id','newest_id','city_level_desc','quarter','layout_price1'])['browse_number'].sum().reset_index()

df9 = df8.groupby(['newest_id','city_level_desc','quarter'])['browse_number'].sum().reset_index()
df10 = pd.merge(df8, df9,how='inner',on=['newest_id','quarter','city_level_desc'])
df10['percent']=df10['browse_number_x']/df10['browse_number_y']

col_x = ['city_id','newest_id','city_level_desc','quarter','layout_price1','percent']
df11 = pd.DataFrame(df10,columns = col_x)
df11.rename(columns={'layout_price1': 'interval'}, inplace=True)
df11['type'] = 'layout_price'
df11=df11.astype('str')

df11.to_sql('dws_newest_layout_price',engine,index=False,if_exists='append')

#In[]

# 需求：用户浏览目标楼盘各区间的数量/用户浏览所有楼盘这个区间的数量
# 购房特征-均价分布

# 楼盘表取均值 直接过滤均价2000元以下的脏数据
res,columnNames = con.query('''
SELECT
newest_id,
city_id,
CASE WHEN unit_price<2000 THEN NULL ELSE unit_price END unit_price
FROM 
dwb_db.dwb_newest_info
''')
unit0= pd.DataFrame([list(i) for i in res],columns=columnNames)
unit0['city_id'] = unit0['city_id'].apply(pd.to_numeric)
unit0 = pd.merge(unit0,df2,how='inner',on=['newest_id','city_id'])
unit0
condlist=[
np.logical_and(unit0.city_level_desc=='一线城市',unit0.unit_price<25000),
np.logical_and(unit0.city_level_desc=='一线城市',unit0.unit_price<37000),
np.logical_and(unit0.city_level_desc=='一线城市',unit0.unit_price<46000),
np.logical_and(unit0.city_level_desc=='一线城市',unit0.unit_price<56000),
np.logical_and(unit0.city_level_desc=='一线城市',unit0.unit_price<82000),
np.logical_and(unit0.city_level_desc=='一线城市',unit0.unit_price>=82000),

np.logical_and(unit0.city_level_desc=='二线城市',unit0.unit_price<10000),
np.logical_and(unit0.city_level_desc=='二线城市',unit0.unit_price<13000),
np.logical_and(unit0.city_level_desc=='二线城市',unit0.unit_price<15000),
np.logical_and(unit0.city_level_desc=='二线城市',unit0.unit_price<20000),
np.logical_and(unit0.city_level_desc=='二线城市',unit0.unit_price<25000),
np.logical_and(unit0.city_level_desc=='二线城市',unit0.unit_price>=25000),

np.logical_and(unit0.city_level_desc=='三线城市',unit0.unit_price<10000),
np.logical_and(unit0.city_level_desc=='三线城市',unit0.unit_price<13000),
np.logical_and(unit0.city_level_desc=='三线城市',unit0.unit_price<15000),
np.logical_and(unit0.city_level_desc=='三线城市',unit0.unit_price<20000),
np.logical_and(unit0.city_level_desc=='三线城市',unit0.unit_price<25000),
np.logical_and(unit0.city_level_desc=='三线城市',unit0.unit_price>=25000),
]  #参数一，定义三个限制条件
# print(condlist)
choicelist=[
'2.5以下','2.5-3.7','3.7-4.6','4.6-5.6','5.6-8.2','8.2以上', 
'1以下','1-1.3','1.3-1.5','1.5-2','2-2.5','2.5以上', 
'1以下','1-1.3','1.3-1.5','1.5-2','2-2.5','2.5以上', 
]
# print(choicelist)
unit1=np.select(condlist,choicelist)
unit0['interval'] = unit1

# 获取每个用户浏览过的目标楼盘区间数量/目标楼盘所有用户浏览过相同区间的数量
unit1 = pd.merge(df0,unit0,how='inner',on='newest_id')
# unit1 = unit1[unit1['unit_price'].notnull()]
unit2 = unit1.merge(unit1,how='inner',on=['imei','quarter','city_level_desc'])
unit2['browse_number_y'] = unit2['browse_number_y'].apply(pd.to_numeric)
unit2 = unit2[unit2['unit_price_y'].notnull()]
unit3 = unit2.groupby(['newest_id_x','quarter','city_id_x','interval_y','city_level_desc'])['browse_number_y'].sum().reset_index()
unit4 = unit3.groupby(['newest_id_x','quarter','city_id_x','city_level_desc'])['browse_number_y'].sum().reset_index()
unit5 = unit3.merge(unit4,how='inner',on=['newest_id_x','quarter','city_id_x'])
unit5['percent'] = unit5['browse_number_y_x']/unit5['browse_number_y_y']

unit5.rename(columns={'newest_id_x':'newest_id','city_id_x':'city_id','interval_y':'interval','city_level_desc_x':'city_level_desc'},inplace=True)

col_x = ['city_id','newest_id','city_level_desc','quarter','interval','percent']
unit6 = pd.DataFrame(unit5,columns = col_x)
unit6['type'] = 'unit_price'
unit6=unit6.astype('str')


unit6.to_sql('dws_newest_layout_price',engine,index=False,if_exists='append')


#In[]

# 需求：用户浏览目标楼盘各区间的数量/用户浏览所有楼盘这个区间的数量
# 购房特征-面积分布
# 楼盘表取面积
res,columnNames = con.query('''
SELECT
DISTINCT
a0.newest_id,a0.layout_area
FROM 
dwb_db.dwb_newest_layout a0
JOIN dwb_db.dwb_newest_info a1 ON a1.newest_id=a0.newest_id
JOIN (SELECT city_id FROM  dwb_db.dwb_customer_browse_log GROUP BY city_id) a2 ON a2.city_id=a1.city_id 
where a0.layout_area is not NULL
''')
area = pd.DataFrame([list(i) for i in res],columns=columnNames)
area = pd.merge(area, df2,how='inner',on='newest_id')
area
# 区间归类
area.loc[area['layout_area']<70,'interval'] = '70以下'
area.loc[(area['layout_area']<90) & (area['layout_area']>70),'interval'] = '70-90'
area.loc[(area['layout_area']<110) & (area['layout_area']>90),'interval'] = '90-110'
area.loc[(area['layout_area']<130) & (area['layout_area']>110),'interval'] = '110-130'
area.loc[(area['layout_area']<150) & (area['layout_area']>130),'interval'] = '130-150'
area.loc[(area['layout_area']<200) & (area['layout_area']>150),'interval'] = '150-200'
area.loc[(area['layout_area']<300) & (area['layout_area']>200),'interval'] = '200-300'
area.loc[area['layout_area']>300,'interval'] = '300以上'

city_test=area
city_list=list(area['city_id'].drop_duplicates())
len(city_list)
city_apart=locals()

for i in range(len(city_list)):
    city_apart[str(city_list[i])]=city_test[city_test['city_id']==city_list[i]]
# city_apart['370200']
city_apart


summary_tag = pd.merge(df0,df0,how='inner',on=['imei','quarter'])
summary_tag

def summary_cul(city_detail):
    print(str(city_list[i]))
    city_detail=pd.merge(city_detail,summary_tag,how='inner',left_on='newest_id',right_on='newest_id_y')
    b=pd.DataFrame()
    # c=pd.DataFrame()

#面积#
    a=city_detail.groupby(['newest_id_x','quarter','interval'])['browse_number_y'].sum().reset_index()
    b=b.append(a)
    return b
summary_res=pd.DataFrame()


for i in range(len(city_list)):
    city_summary=summary_cul(city_apart[str(city_list[i])])
    summary_res=summary_res.append(city_summary)
summary_res=summary_res.reset_index(drop=True)
summary_res['newest_id_x']= summary_res['newest_id_x'].astype('str')
summary_res['quarter']= summary_res['quarter'].astype('str')
summary_res['browse_number_y']= summary_res['browse_number_y'].astype('int')
summary_res = summary_res.groupby(['newest_id_x','quarter','interval'])['browse_number_y'].sum().reset_index()

# 计算总量
summary_res1 = summary_res.groupby(['newest_id_x','quarter'])['browse_number_y'].sum().reset_index()
summary_res1['newest_id_x']= summary_res1['newest_id_x'].astype('str')
summary_res1['quarter']= summary_res1['quarter'].astype('str')
summary_res1['browse_number_y']= summary_res1['browse_number_y'].astype('int')
summary_res1
summary_res1.info()
summary_res2=pd.merge(summary_res,summary_res1,how='inner',on=['newest_id_x','quarter'])
summary_res2.head(10)
summary_res2['percent'] = summary_res2['browse_number_y_x']/summary_res2['browse_number_y_y']
summary_res2['type'] = 'layout_area'
summary_res2
summary_res3 = summary_res2.merge(df2,how='inner',left_on=['newest_id_x'],right_on=['newest_id'])
summary_res3
summary_res4 = ['city_id','newest_id','city_level_desc','quarter','interval','percent','type']
summary_res5 = pd.DataFrame(summary_res3,columns=summary_res4)
summary_res5

summary_res5.to_sql('dws_newest_layout_price',engine,index=False,if_exists='append')



#In[]

# 购房特征-户型分布
# 楼盘表取户型
res,columnNames = con.query('''
SELECT
a0.newest_id,a0.room
FROM 
dwb_db.dwb_newest_layout a0
where a0.room is not null
''')
room = pd.DataFrame([list(i) for i in res],columns=columnNames)
room = pd.merge(room, df2,how='inner',on='newest_id')
room

room.loc[room['room']==1,'interval'] = '1室'
room.loc[room['room']==2,'interval'] = '2室'
room.loc[room['room']==3,'interval'] = '3室'
room.loc[room['room']>=4,'interval'] = '4室以上'
room

# 获取每个用户浏览过的目标楼盘区间数量/目标楼盘所有用户浏览过相同区间的数量
room1 = pd.merge(df0,room,how='inner',on='newest_id')
room1
room2 = room1.groupby(['imei','newest_id','city_id','city_level_desc','interval','quarter'])['browse_number'].sum().reset_index()
room2
# room3 = room1.groupby(['imei','city_level_desc','interval','quarter'])['browse_number'].sum().reset_index()
room3 = room1.groupby(['imei','quarter'])['browse_number'].sum().reset_index()
room3
# room4 = pd.merge(room2,room3,how='inner',on=['imei','city_level_desc','interval','quarter'])
room4 = pd.merge(room2,room3,how='inner',on=['imei','quarter'])
room4


# 分子
room5 = room4.groupby(['imei','quarter','interval'])['browse_number_x'].sum().reset_index()
room5 
# 楼盘与IMEI关联关系
df6 = ['imei','newest_id','quarter']
df7 = pd.DataFrame(df,columns = df6).drop_duplicates().reset_index(drop=True)
df8 = df7.merge(room5,how='inner',on=['imei','quarter'])
df9 = df8.groupby(['newest_id','quarter','interval'])['browse_number_x'].sum().reset_index()
df10 = df9.groupby(['newest_id','quarter'])['browse_number_x'].sum().reset_index()
df11 = df9.merge(df10,how='inner',on=['newest_id','quarter'])
df11['percent'] = df11['browse_number_x_x']/df11['browse_number_x_y']
room5 = df11.merge(df2,how='inner',on='newest_id')
col_x = ['city_id','newest_id','city_level_desc','quarter','interval','percent']
room6 = pd.DataFrame(room5,columns = col_x)
room6['type'] = 'room'

room6=room6.astype('str')
room6
room6.to_sql('dws_newest_layout_price',engine,index=False,if_exists='append')


#In[]

# 需求：用户关注周期占比
# 逻辑：目标楼盘用户（最后关注日期-最早关注日期）的区间数量/总关注人数

# period_value = '2021Q2'
# start_date = '2021-04-01'
# end_date = '2021-07-01'
# start_idate = '20200701'
# end_idate = '20210701'




browse = pd.merge(df[['imei','newest_id','quarter']],df2,how='inner',on='newest_id').drop_duplicates().reset_index(drop=True)
# browse.visit_date = pd.to_datetime(browse.visit_date)
browse

res,columnNames = con.query(" select customer imei,max(idate) max_idate,min(idate) min_idate from\
     odsdb.cust_browse_log_201801_202106 where idate>= '"+start_idate+"' AND idate < '"+end_idate+"' group by customer ")
idate = pd.DataFrame([list(i) for i in res],columns=columnNames)
idate.max_idate = pd.to_datetime(idate.max_idate)
idate.min_idate = pd.to_datetime(idate.min_idate)

browse2 = browse.groupby(['newest_id'])['imei'].count().reset_index()
browse2

browse3 = pd.merge(browse,idate,how='inner',on=['imei'])
browse3['visit_date1'] =browse3['max_idate']-browse3['min_idate']

l = browse3.visit_date1.astype("str").str.split().str[0]
n = l.astype("int")
browse3['day']=n

browse3.loc[(browse3['day']>=0) & (browse3['day']<=30),'interval'] = '1个月内'
browse3.loc[(browse3['day']>=31) & (browse3['day']<=90),'interval'] = '1至3个月'
browse3.loc[(browse3['day']>=91) & (browse3['day']<=180),'interval'] = '3至6个月'
browse3.loc[(browse3['day']>=181),'interval'] = '6个月以上'

browse4 = browse3.groupby(['newest_id','city_id','city_level_desc','interval'])['imei'].count().reset_index()
browse44 = browse4.groupby(['newest_id'])['imei'].sum().reset_index()

browse5 = pd.merge(browse4,browse44,how='inner',on=['newest_id'])
browse5['percent']=browse5['imei_x']/browse5['imei_y']
browse5['quarter']= period_value
browse5

col_x = ['city_id','newest_id','city_level_desc','quarter','interval','percent']
browse6 = pd.DataFrame(browse5,columns = col_x)
browse6['type'] = 'layout_visit'
browse6=browse6.astype('str')
browse6
browse6.to_sql('dws_newest_layout_price',engine,index=False,if_exists='append')

#In[]

# 标签表取用户小区均价区间字段并初步清洗标签标准名
res,columnNames = con.query("SELECT imei,resi_district_price FROM dwb_db.dwb_customer_imei_tag where resi_district_price is not null and period= '"+period_value+"'")
df = pd.DataFrame([list(i) for i in res],columns=columnNames)

df = df.drop('resi_district_price', axis=1).join(df['resi_district_price'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename('resi_district_price')).drop_duplicates().reset_index(drop=True)

df['resi_district_price1'] = 'Unknown'
df.loc[df['resi_district_price']=='6000-8000','resi_district_price1'] = '0.6w-0.8w'
df.loc[df['resi_district_price']=='4000-6000','resi_district_price1'] = '0.4w-0.6w'
df.loc[df['resi_district_price']=='2000-4000','resi_district_price1'] = '0.4w以下'
df.loc[df['resi_district_price']=='8000-10000','resi_district_price1'] = '0.8w-1w'
df.loc[df['resi_district_price']=='2000以下','resi_district_price1'] = '0.4w以下'
df.loc[df['resi_district_price']=='4000以下','resi_district_price1'] = '0.4w以下'
df.loc[df['resi_district_price']=='1.6-1.8w','resi_district_price1'] = '1.6w-1.8w'
df.loc[df['resi_district_price1']=='Unknown','resi_district_price1'] = df['resi_district_price']


# 城市对应表并补充缺失值
res,columnNames = con.query('''
SELECT 
distinct
city_id,city_name,city_level_desc
from 
dw.dim_geography 
where city_level_desc is not null and  city_level_desc <> ''
''')
df1 = pd.DataFrame([list(i) for i in res],columns=columnNames)

# 浏览表读取,并贴上季度
res,columnNames = con.query("SELECT imei,newest_id,visit_date FROM dwb_db.dwb_customer_browse_log where  visit_date >= '"+start_date+"' and visit_date<'"+end_date+"'")
df2 = pd.DataFrame([list(i) for i in res],columns=columnNames)
df2.visit_date = pd.to_datetime(df2.visit_date)
df2['quarter'] = pd.PeriodIndex(df2.visit_date, freq='Q')


df1=df1.astype('str')
# 楼盘表获取楼盘所在城市并关联所属几线城市
res,columnNames = con.query('''
SELECT newest_id,city_id FROM dws_db.dws_newest_info WHERE dr=0
''')
df3 = pd.DataFrame([list(i) for i in res],columns=columnNames)
df3 = pd.merge(df3,df1,how='inner',on='city_id')

# 浏览表关联楼盘所在城市是几线,关联用户所住小区均价
df4 = df2.merge(df3,how='inner',on=['newest_id']).merge(df[['imei','resi_district_price1']],how='inner',on=['imei'])
df5 = df4


df5.loc[(df5['city_level_desc']=='一线城市') & (df5['resi_district_price1']=='8w-9w'),'resi_district_price'] = '8w以上'
df5.loc[(df5['city_level_desc']=='一线城市') & (df5['resi_district_price1']=='9w-10w'),'resi_district_price'] = '8w以上'
df5.loc[(df5['city_level_desc']=='一线城市') & (df5['resi_district_price1']=='10w以上'),'resi_district_price'] = '8w以上'
df5.loc[(df5['city_level_desc']=='一线城市') & (df5['resi_district_price1']=='0.4w以下'),'resi_district_price'] = '2w以下'
df5.loc[(df5['city_level_desc']=='一线城市') & (df5['resi_district_price1']=='0.4w-0.6w'),'resi_district_price'] = '2w以下'
df5.loc[(df5['city_level_desc']=='一线城市') & (df5['resi_district_price1']=='0.6w-0.8w'),'resi_district_price'] = '2w以下'
df5.loc[(df5['city_level_desc']=='一线城市') & (df5['resi_district_price1']=='0.8w-1w'),'resi_district_price'] = '2w以下'
df5.loc[(df5['city_level_desc']=='一线城市') & (df5['resi_district_price1']=='1w-1.2w'),'resi_district_price'] = '2w以下'
df5.loc[(df5['city_level_desc']=='一线城市') & (df5['resi_district_price1']=='1.2w-1.4w'),'resi_district_price'] = '2w以下'
df5.loc[(df5['city_level_desc']=='一线城市') & (df5['resi_district_price1']=='1.4w-1.6w'),'resi_district_price'] = '2w以下'
df5.loc[(df5['city_level_desc']=='一线城市') & (df5['resi_district_price1']=='1.6w-1.8w'),'resi_district_price'] = '2w以下'
df5.loc[(df5['city_level_desc']=='一线城市') & (df5['resi_district_price1']=='1.8w-2w'),'resi_district_price'] = '2w以下'

df5.loc[(df5['city_level_desc']=='二线城市') & (df5['resi_district_price1']=='0.4w以下'),'resi_district_price'] = '0.6w以下'
df5.loc[(df5['city_level_desc']=='二线城市') & (df5['resi_district_price1']=='0.4w-0.6w'),'resi_district_price'] = '0.6w以下'
df5.loc[(df5['city_level_desc']=='二线城市') & (df5['resi_district_price1']=='0.6w-0.8w'),'resi_district_price'] = '0.6w-1w'
df5.loc[(df5['city_level_desc']=='二线城市') & (df5['resi_district_price1']=='0.8w-1w'),'resi_district_price'] = '0.6w-1w'
df5.loc[(df5['city_level_desc']=='二线城市') & (df5['resi_district_price1']=='1.6w-1.8w'),'resi_district_price'] = '1.6w-2w'
df5.loc[(df5['city_level_desc']=='二线城市') & (df5['resi_district_price1']=='1.8w-2w'),'resi_district_price'] = '1.6w-2w'
df5.loc[(df5['city_level_desc']=='二线城市') & (df5['resi_district_price1']=='3w-4w'),'resi_district_price'] = '3w以上'
df5.loc[(df5['city_level_desc']=='二线城市') & (df5['resi_district_price1']=='4w-5w'),'resi_district_price'] = '3w以上'
df5.loc[(df5['city_level_desc']=='二线城市') & (df5['resi_district_price1']=='5w-6w'),'resi_district_price'] = '3w以上'
df5.loc[(df5['city_level_desc']=='二线城市') & (df5['resi_district_price1']=='6w-7w'),'resi_district_price'] = '3w以上'
df5.loc[(df5['city_level_desc']=='二线城市') & (df5['resi_district_price1']=='7w-8w'),'resi_district_price'] = '3w以上'
df5.loc[(df5['city_level_desc']=='二线城市') & (df5['resi_district_price1']=='8w-9w'),'resi_district_price'] = '3w以上'
df5.loc[(df5['city_level_desc']=='二线城市') & (df5['resi_district_price1']=='9w-10w'),'resi_district_price'] = '3w以上'
df5.loc[(df5['city_level_desc']=='二线城市') & (df5['resi_district_price1']=='10w以上'),'resi_district_price'] = '3w以上'

df5.loc[(df5['city_level_desc']=='三线城市') & (df5['resi_district_price1']=='0.4w以下'),'resi_district_price'] = '0.6w以下'
df5.loc[(df5['city_level_desc']=='三线城市') & (df5['resi_district_price1']=='0.4w-0.6w'),'resi_district_price'] = '0.6w以下'
df5.loc[(df5['city_level_desc']=='三线城市') & (df5['resi_district_price1']=='0.6w-0.8w'),'resi_district_price'] = '0.6w-0.8w'
df5.loc[(df5['city_level_desc']=='三线城市') & (df5['resi_district_price1']=='0.8w-1w'),'resi_district_price'] = '0.8w-1w'
df5.loc[(df5['city_level_desc']=='三线城市') & (df5['resi_district_price1']=='1w-1.2w'),'resi_district_price'] = '1w-1.2w'
df5.loc[(df5['city_level_desc']=='三线城市') & (df5['resi_district_price1']=='1.2w-1.4w'),'resi_district_price'] = '1.2w-1.6w'
df5.loc[(df5['city_level_desc']=='三线城市') & (df5['resi_district_price1']=='1.4w-1.6w'),'resi_district_price'] = '1.2w-1.6w'
df5.loc[(df5['city_level_desc']=='三线城市') & (df5['resi_district_price1']=='1.6w-1.8w'),'resi_district_price'] = '1.6w-2w'
df5.loc[(df5['city_level_desc']=='三线城市') & (df5['resi_district_price1']=='1.8w-2w'),'resi_district_price'] = '1.6w-2w'
df5.loc[(df5['city_level_desc']=='三线城市') & (df5['resi_district_price1']=='2w-3w'),'resi_district_price'] = '2w-3w'
df5.loc[(df5['city_level_desc']=='三线城市') & (df5['resi_district_price1']=='3w-4w'),'resi_district_price'] = '3w以上'
df5.loc[(df5['city_level_desc']=='三线城市') & (df5['resi_district_price1']=='4w-5w'),'resi_district_price'] = '3w以上'
df5.loc[(df5['city_level_desc']=='三线城市') & (df5['resi_district_price1']=='5w-6w'),'resi_district_price'] = '3w以上'
df5.loc[(df5['city_level_desc']=='三线城市') & (df5['resi_district_price1']=='6w-7w'),'resi_district_price'] = '3w以上'
df5.loc[(df5['city_level_desc']=='三线城市') & (df5['resi_district_price1']=='7w-8w'),'resi_district_price'] = '3w以上'
df5.loc[(df5['city_level_desc']=='三线城市') & (df5['resi_district_price1']=='8w-9w'),'resi_district_price'] = '3w以上'
df5.loc[(df5['city_level_desc']=='三线城市') & (df5['resi_district_price1']=='9w-10w'),'resi_district_price'] = '3w以上'
df5.loc[(df5['city_level_desc']=='三线城市') & (df5['resi_district_price1']=='10w以上'),'resi_district_price'] = '3w以上'

df5.resi_district_price.fillna(value=df5['resi_district_price1'],inplace = True)

# 居住小区均价结果表
df6=df5.groupby(['newest_id','city_level_desc','resi_district_price','city_id','city_name','quarter'])['imei'].count().to_frame('imei_count').reset_index()

df7 = df6.groupby(['newest_id','quarter'])['imei_count'].sum().reset_index()

df8 = pd.merge(df6,df7,how='inner',on=['newest_id','quarter'])
df8['percent']=df8['imei_count_x']/df8['imei_count_y']
df8.rename(columns={'resi_district_price': 'interval'}, inplace=True)

col_x = ['city_id','newest_id','city_level_desc','quarter','interval','percent']
df9 = pd.DataFrame(df8,columns = col_x)
df9['type'] = 'resi_district'
df9=df9.astype('str')


df9.to_sql('dws_newest_layout_price',engine,index=False,if_exists='append')


#In[]


# 线下门店消费类型占比
# 标签表取用户线下门店偏好并清洗偏好归类
# 后台标签	对应前端
# 交通行业	生活服务
# 娱乐行业	休闲娱乐
# 快递行业	生活服务
# 快餐行业	餐饮
# 教育培训行业	生活服务
# 购物行业	零售
# 金融行业	生活服务
# 餐饮分类	餐饮

res,columnNames = con.query("SELECT imei,offline_shop_prefer FROM dwb_db.dwb_customer_imei_tag WHERE offline_shop_prefer IS NOT NULL  and period = '"+period_value+"'")
df = pd.DataFrame([list(i) for i in res],columns=columnNames)

df = df.drop('offline_shop_prefer', axis=1).join(df['offline_shop_prefer'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename('offline_shop_prefer')).drop_duplicates().reset_index(drop=True)

df.loc[df['offline_shop_prefer']=='交通行业','offline_shop_prefer1'] = '生活服务'
df.loc[df['offline_shop_prefer']=='娱乐行业','offline_shop_prefer1'] = '休闲娱乐'
df.loc[df['offline_shop_prefer']=='快递行业','offline_shop_prefer1'] = '生活服务'
df.loc[df['offline_shop_prefer']=='快餐行业','offline_shop_prefer1'] = '餐饮'
df.loc[df['offline_shop_prefer']=='教育培训行业','offline_shop_prefer1'] = '生活服务'
df.loc[df['offline_shop_prefer']=='购物行业','offline_shop_prefer1'] = '零售'
df.loc[df['offline_shop_prefer']=='金融行业','offline_shop_prefer1'] = '生活服务'
df.loc[df['offline_shop_prefer']=='餐饮分类','offline_shop_prefer1'] = '餐饮'
df.loc[df['offline_shop_prefer']=='房地产行业','offline_shop_prefer1'] = '生活服务'
df.loc[df['offline_shop_prefer']=='住宿行业','offline_shop_prefer1'] = '生活服务'
df.loc[df['offline_shop_prefer']=='体育行业','offline_shop_prefer1'] = '生活服务'
df.loc[df['offline_shop_prefer']=='医院','offline_shop_prefer1'] = '生活服务'

# 浏览表读取,并贴上季度
res,columnNames = con.query("SELECT imei,newest_id,visit_date FROM dwb_db.dwb_customer_browse_log where visit_date >= '"+start_date+"' and visit_date<'"+end_date+"' ")
df2 = pd.DataFrame([list(i) for i in res],columns=columnNames)
df2.visit_date = pd.to_datetime(df2.visit_date)
df2['quarter'] = pd.PeriodIndex(df2.visit_date, freq='Q')
df3 = pd.merge(df2,df,how='inner',on='imei').groupby(['newest_id','quarter','offline_shop_prefer1'])['imei'].count().reset_index(name='number')
df4 = df3.groupby(['newest_id','quarter'])['number'].sum().reset_index()
df5 = pd.merge(df3,df4,how='inner',on=['newest_id','quarter'])
df5['Proportion'] = round(df5['number_x']/df5['number_y'],4)
df5=df5.astype('str') 


df5.to_sql('dws_newest_shop_prefer',engine,index=False,if_exists='append')
#In[]


























#In[]

# 酒店类型占比
# 浏览表读取,并贴上季度
res,columnNames = con.query(" SELECT imei,newest_id,visit_date FROM dwb_db.dwb_customer_browse_log where visit_date >= '"+start_date+"' and visit_date<'"+end_date+"'")
df = pd.DataFrame([list(i) for i in res],columns=columnNames)
df.visit_date = pd.to_datetime(df.visit_date)
df['quarters'] = pd.PeriodIndex(df.visit_date, freq='Q')

df0 = ['imei','newest_id','quarters']
df00 = pd.DataFrame(df,columns=df0) 
df00.drop_duplicates(subset=['imei','newest_id','quarters'],keep=False,inplace=True)
df = df00.reset_index(drop=True)

# 酒店标签并清洗
res,columnNames = con.query('''
SELECT DISTINCT imei,hotel_level_prefer 
FROM dwb_db.dwb_customer_imei_tag 
WHERE hotel_level_prefer IS NOT NULL  
''')
df1 = pd.DataFrame([list(i) for i in res],columns=columnNames)
df2 = df1.drop('hotel_level_prefer',axis=1).join(df1['hotel_level_prefer']\
.str.split(',',expand=True).stack().reset_index(level=1,drop=True).rename('hotel_level_prefer'))\
.drop_duplicates().reset_index(drop=True)

df3 = df.merge(df2,how='inner',on='imei')
df3.loc[df3['hotel_level_prefer']=='高档型','hotel_level_prefer1'] = '星级酒店'
df3.loc[df3['hotel_level_prefer']=='豪华型','hotel_level_prefer1'] = '星级酒店'
df3.loc[df3['hotel_level_prefer']=='经济型','hotel_level_prefer1'] = '快捷酒店'
df3.loc[df3['hotel_level_prefer']=='快捷酒店','hotel_level_prefer1'] = '快捷酒店'
df3.loc[df3['hotel_level_prefer']=='民宿','hotel_level_prefer1'] = '民宿'
df3.loc[df3['hotel_level_prefer']=='青年旅馆','hotel_level_prefer1'] = '青年旅舍'
df3.loc[df3['hotel_level_prefer']=='舒适型','hotel_level_prefer1'] = '快捷酒店'
df3.loc[df3['hotel_level_prefer']=='星级酒店','hotel_level_prefer1'] = '星级酒店'

df4 = df3.groupby(['newest_id','quarters','hotel_level_prefer1'])['imei'].count().to_frame('hotel_num').reset_index()
df5 = df4.groupby(['newest_id','quarters'])['hotel_num'].sum().to_frame('hotel_num_all').reset_index()
df6 = df4.merge(df5,how='inner',on=['newest_id','quarters'])

res,columnNames = con.query('''SELECT * FROM dws_db.dws_newest_info where dr=0''')
df7 = pd.DataFrame([list(i) for i in res],columns=columnNames)
df8 = df6.merge(df7[['newest_id','city_id']],how='inner',on=['newest_id'])
df8['rate'] = df8['hotel_num']/df8['hotel_num_all']
df8.rename(columns={'hotel_level_prefer1':'types'}, inplace=True)
df9 = ['city_id','quarters','newest_id','types','rate']
df9 = pd.DataFrame(df8,columns=df9)
df9 = df9.astype('str')


df9.to_sql('dws_visithoteltyperate_quarter',engine,index=False,if_exists='append')

#In[]

#dim_period表开发
# 这块不跑

# a = pd.read_excel(r'C:\Users\yangz\Desktop\易居\文件\date.xlsx')
# pd.to_datetime(a['date'],format='%Y-%m-%d').dt.date
# a['period'] =  pd.PeriodIndex(a.date, freq='Q')

# a['week0'] = pd.to_datetime(a.date).dt.weekofyear
# a['week1'] = 'na'
# a.loc[a['week0']<10,'week1'] = '0'+a['week0'].astype('str')
# a.loc[a['week1']=='na','week1'] = a['week0'].astype('str')
# a.info()
# a['week'] = pd.to_datetime(a.date).dt.year.astype('str')+'-'+a['week1'].astype('str')
# a
# # a['week'] = pd.to_datetime(a.date).dt.year.astype('str')+'-'+pd.to_datetime(a.date).dt.weekofyear.astype('str')

# a['month'] = pd.to_datetime(a.date).dt.month.astype('int')
# a['month1'] = 'na'
# a.loc[a['month']<10,'month1'] = '0'+a['month'].astype('str')
# a.loc[a['month1']=='na','month1'] = a['month']
# a['year_month'] = pd.to_datetime(a.date).dt.year.astype('str')+a['month1'].astype('str')

# week = a['week'].drop_duplicates().to_frame('period').reset_index()
# week['date_type'] = 'W'

# year_month = a['year_month'].drop_duplicates().to_frame('period').reset_index()
# year_month['date_type'] = 'M'

# period = a['period'].drop_duplicates().reset_index()
# period['date_type'] = 'Q'


# res = pd.DataFrame(columns=('period','date_type'))
# res = res.append(period[['period','date_type']])
# res = res.append(week[['period','date_type']])
# res = res.append(year_month[['period','date_type']])
# print(res)
# res.to_sql('dim_period',engine,index=False,if_exists='append')

