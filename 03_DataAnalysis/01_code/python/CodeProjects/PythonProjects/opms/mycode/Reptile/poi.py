#In[] 

import sys
import pandas as pd
import numpy as np
import time
import configparser
import os
from pandas.core import groupby
import pymysql
import re

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
engine = create_engine('mysql+mysqldb://'+'yangzhen'+':'+'6V5_0rviExpxBzHj'+'@'+'172.28.36.77'+':'+'3306'+'/'+'dws_db_prd')# 初始化引擎
con = MysqlClient(db_host,database,user,password)



#In[] :poi配套信息明细

# poi配套信息明细
res,columnNames = con.query('''
select g.city_id,t.business_type poi_type,t.mall_name poi_name,substring_index(t.gd_lnglat,',',-1) as lat,substring_index(t.gd_lnglat,',',1) as lng 
from dwb_db.dwb_newest_poi_mall_info t LEFT JOIN dw_v2.dim_geography g on g.grade=3 and t.ori_city=g.city_short 
having lng <>'' and lat <>'' and poi_type <>''
union
select g.city_id,t.poi_type,t.poi_name,substring_index(t.lnglat,',',-1) as lat,substring_index(t.lnglat,',',1) as lng  
from dwb_db.dwb_newest_poi_gd_info t LEFT JOIN dw_v2.dim_geography g on g.grade=3 and t.city_name=g.city_short
having lng <>'' and lat <>'' and poi_type <>''
''')
poi = pd.DataFrame([list(i) for i in res],columns=columnNames)
poi_null = poi[(poi['city_id'].isnull())]
poi_notnull = poi[(poi['city_id'].notnull())]
# poi_null = poi
poi_null['a'] = 'a'

# 获取城市中心点经纬度与id
res,columnNames = con.query('''
select city_id,city_name,lat,lng from dwb_db.dim_geography where grade =3
''')
city_lng = pd.DataFrame([list(i) for i in res],columns=columnNames)
city_lng['a'] = 'a'


city_merge = pd.merge(poi_null,city_lng,how='inner',on='a')
city_merge

# 通过经纬度计算距离
from geopy.distance import geodesic
city_merge['distance(m)'] = city_merge.apply(lambda x:geodesic((x.lat_y,x.lng_y),(x.lat_x,x.lng_x)).km,axis=1)
city_merge

city_merge_re = city_merge.groupby(by=['poi_type','poi_name','lng_x','lat_x'],as_index=False)['distance(m)'].min().reset_index(drop=True)
city_merge_re1 = city_merge_re.merge(city_merge[['poi_type','poi_name','lng_x','lat_x','distance(m)','city_id_y']],how='left',on=['poi_type','poi_name','lng_x','lat_x','distance(m)'])
city_merge_re1 = city_merge_re1.rename(columns={'city_id_y':'city_id','lng_x':'lng','lat_x':'lat'}) 
city_merge_re1 = city_merge_re1[['city_id','poi_type','poi_name','lat','lng']]

# 空city_id的结果表city_merge_re1
# city_merge_re1.info()
# 把补好city_id的df拼接到有city_id的df中
poi_notnull['city_id'] = poi_notnull['city_id'].astype('int')
city_merge_all = poi_notnull.append(city_merge_re1).reset_index(drop=True)


# 获取准入表中的楼盘经纬度与city_idx`
res,columnNames = con.query('''
SELECT distinct a.* FROM (
SELECT a0.newest_id,a1.city_id,a1.gd_lat lat,a1.gd_lng lng  
FROM dwb_db.a_dws_newest_period_admit a0  
LEFT JOIN dwb_db.a_dwb_newest_info a1 ON a1.newest_id=a0.newest_id 
) a 
WHERE a.newest_id IN (SELECT DISTINCT newest_id FROM dwb_db.a_dws_newest_period_admit WHERE dr=0 AND newest_id NOT IN (SELECT DISTINCT newest_id FROM dws_db_prd.dws_tag_purchase_poi)
)
AND a.lat>0
''')
newest_city = pd.DataFrame([list(i) for i in res],columns=columnNames)
newest_city = newest_city[newest_city['city_id'].notnull()]
newest_city['city_id'] = newest_city['city_id'].astype('int')
# 以城市id关联楼盘与poi信息

city_merge_all_newest = city_merge_all.merge(newest_city,how='inner',on='city_id')

city_merge_all_newest

city_merge_all_newest['lng_y']=city_merge_all_newest['lng_y'].astype('float')
city_merge_all_newest['lat_y']=city_merge_all_newest['lat_y'].astype('float')
city_merge_all_newest['lng_x']=city_merge_all_newest['lng_x'].astype('float')
city_merge_all_newest['lat_x']=city_merge_all_newest['lat_x'].astype('float')


# 通过经纬度计算距离
city_merge_all_newest['pure_distance'] = city_merge_all_newest.apply(lambda x:geodesic((x.lat_y,x.lng_y),(x.lat_x,x.lng_x)).km,axis=1)
city_merge_all_newest
# 项目周边半径500m、1km、1.5km、2km、2.5km、3km范围的配套数据准备
city_merge_all_newest_5 = city_merge_all_newest[city_merge_all_newest['pure_distance']<=10]

city_merge_all_newest_6 = city_merge_all_newest_5.rename(columns={'poi_type':'tag_value','poi_name':'tag_detail','lat_x':'lat','lng_x':'lng'})
city_merge_all_newest_6 = city_merge_all_newest_6[['city_id','newest_id','tag_value','tag_detail','pure_distance','lng','lat']]


def function(a):
    if '专科医院' in a:
        return '专科医院'
    elif '综合医院' in a:
        return '综合医院'
    elif '公交车站' in a:
        return '公交车站'
    elif '学校' in a:
        return '学校'
    elif '购物中心' in a:
        return '商场'
    elif '独立百货' in a:
        return '商场'
    elif '商业街' in a:
        return '商场'
    elif '步行街' in a:
        return '商场' 
    elif '城市综合体' in a:
        return '商场'         
    elif '文旅商业' in a:
        return '商场'
    elif '地铁' in a:
        return '地铁站'
    elif '宾馆' in a:
        return '酒店'
    elif '酒店' in a:
        return '酒店'
    elif '配套零售商业-酒店' in a:
        return '酒店'
    else:
        return ''

city_merge_all_newest_6['tag_value2'] = city_merge_all_newest_6['tag_value'].apply(lambda x: function(x))
city_merge_all_newest_7 = city_merge_all_newest_6[city_merge_all_newest_6['tag_value2']!='']

city_merge_all_newest_7.to_sql('dws_tag_purchase_poi',engine,index=False,if_exists='append')


#In[] :poi评分 全删全导

# 取poi评分结果表
res,columnNames = con.query('''
SELECT newest_id,tag_value,tag_detail,pure_distance FROM dws_db_prd.dws_tag_purchase_poi
''')
df = pd.DataFrame([list(i) for i in res],columns=columnNames)


df1 = df.drop('tag_value', axis=1).join(df['tag_value'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename('tag_value'))
df1.loc[df1['tag_value']=='专科医院','type'] = '综合专科医院'
df1.loc[df1['tag_value']=='综合医院','type'] = '综合专科医院'
df1.loc[df1['tag_value']=='地铁站','type'] = '地铁'
df1.loc[df1['tag_value']=='公交车站','type'] = '公交'
df1.loc[df1['tag_value']=='学校','type'] = '学校'
df1.loc[df1['tag_value']=='购物中心','type'] = '购物中心'
df1.loc[df1['tag_value']=='独立百货','type'] = '百货'
df1.loc[df1['tag_value']=='商业街','type'] = '商业街'
df1.loc[df1['tag_value']=='宾馆酒店','type'] = '酒店'
df1.loc[df1['tag_value']=='文旅商业','type'] = '文旅商业'
df1.loc[df1['tag_value']=='其他','type'] = '其他'

df2 = df1.groupby(['newest_id','type','pure_distance'])['newest_id'].count().to_frame('number').reset_index()
df3 = df2.groupby(['newest_id','type'])['pure_distance'].min().reset_index()
df3['pure_distance']=df3['pure_distance'].astype('str').astype('float')

df3.loc[(df3['type']=='地铁') & (df3['pure_distance']<=3),'score'] = 0.375
df3.loc[(df3['type']=='地铁') & (df3['pure_distance']<=2),'score'] = 0.75
df3.loc[(df3['type']=='地铁') & (df3['pure_distance']<=1),'score'] = 1.5

df3.loc[(df3['type']=='公交') & (df3['pure_distance']<=3),'score'] = 0.125
df3.loc[(df3['type']=='公交') & (df3['pure_distance']<=2),'score'] = 0.25
df3.loc[(df3['type']=='公交') & (df3['pure_distance']<=1),'score'] = 0.5

df3.loc[(df3['type']=='学校') & (df3['pure_distance']<=5),'score'] = 0.25
df3.loc[(df3['type']=='学校') & (df3['pure_distance']<=3),'score'] = 0.5
df3.loc[(df3['type']=='学校') & (df3['pure_distance']<=2),'score'] = 1

df3.loc[(df3['type']=='综合专科医院'),'score'] = 0.5

df3.loc[(df3['type']=='购物中心') & (df3['pure_distance']<=5),'score'] = 0.5
df3.loc[(df3['type']=='购物中心') & (df3['pure_distance']<=3),'score'] = 1
df3.loc[(df3['type']=='购物中心') & (df3['pure_distance']<=2),'score'] = 1.5

df3.loc[(df3['type']=='百货') & (df3['pure_distance']<=5),'score'] = 0.4
df3.loc[(df3['type']=='百货') & (df3['pure_distance']<=3),'score'] = 0.8
df3.loc[(df3['type']=='百货') & (df3['pure_distance']<=2),'score'] = 1.2

df3.loc[(df3['type']=='商业街') & (df3['pure_distance']<=5),'score'] = 0.3
df3.loc[(df3['type']=='商业街') & (df3['pure_distance']<=3),'score'] = 0.6
df3.loc[(df3['type']=='商业街') & (df3['pure_distance']<=2),'score'] = 0.9

def bussness(x):
    if x in ['购物中心','百货','商业街']:
        return '商业'
    else:
        return x

df3['type_1'] = df3['type'].apply(lambda x: bussness(x) )

df4 = df3.sort_values('score',ascending=False).groupby(['newest_id','type_1'],as_index=False).first()
df4 = df4.groupby(['newest_id'])['score'].sum().reset_index()


# 获取楼盘对应city_id
res,columnNames = con.query('''
SELECT newest_id,city_id FROM dws_db.dws_newest_info where dr=0
''')
df5 = pd.DataFrame([list(i) for i in res],columns=columnNames)
df5

df6= pd.merge(df4,df5,how='left',on='newest_id')
df6
df7 = df6.groupby('city_id')['score'].sum().reset_index()
df8 = df6.groupby(['city_id']).size().to_frame('city_count').reset_index()
df9 = pd.merge(df7,df8,how='inner',on='city_id')
df9['city_score']=round(df9['score']/df9['city_count'],2)

df10 = pd.merge(df6,df9[['city_score','city_id']],how='left',on='city_id')
df10['higt_score']=round((df10['score']-df10['city_score'])/df10['city_score'],4)

res,columnNames = con.query('''
SELECT distinct newest_id FROM dws_db.dws_newest_period_admit WHERE dr=0
''')
df11 = pd.DataFrame([list(i) for i in res],columns=columnNames)

df12 = df10.merge(df11,how='inner',on='newest_id').drop_duplicates()

df12.to_sql('dws_tag_purchase_poi_score',engine,index=False,if_exists='append')

# 校验
# df13 = df12
# df13['score_int'] = df13['score'].astype('int')
# df14 = df13.groupby(['score_int'])['newest_id'].count().to_frame('num')
# df14


# %%
res,columnNames = con.query('''
 SELECT distinct period FROM dwb_db.a_dws_newest_period_admit
''')
date = pd.DataFrame([list(i) for i in res],columns=columnNames)

period_value = list(date['period'].drop_duplicates())

property1 = pd.DataFrame()

for i in period_value:
    print(i)
    res,columnNames = con.query('''
    SELECT DISTINCT  
    a0.newest_id,a2.city_id,
    CASE WHEN a1.land_area IS NULL OR a1.land_area='' THEN 0 ELSE a1.land_area END  land_area ,a2.property_comp,
    CASE WHEN a2.property_fee IS NULL OR a2.property_fee='' THEN  0 ELSE a2.property_fee END property_fee
    FROM dwb_db.a_dws_newest_period_admit a0
    LEFT JOIN dwb_db.a_dwb_newest_info a1 ON a1.newest_id=a0.newest_id 
    LEFT JOIN dws_db_prd.dws_newest_property a2 on a2.newest_id=a0.newest_id
    WHERE a2.property_comp IS NOT NULL AND a2.property_comp NOT LIKE '%暂无%' 
    ''')
    wy = pd.DataFrame([list(i) for i in res],columns=columnNames)

    wy['property_fee']=wy['property_fee'].astype('str').astype('float')
    wy['land_area']=wy['land_area'].astype('str').astype('float')
    # wy['property_id']=wy['property_id'].astype('str')
    wy['city_id']=wy['city_id'].astype('str')

    # 物业公司服务最多的城市
    wy0 = wy.groupby(['property_comp','city_id']).size().to_frame('newest_city_num').reset_index()
    wy0.sort_values('newest_city_num',ascending=False,inplace=True)
    wy0.drop_duplicates(subset=['property_comp'],keep='first',inplace=True)
    wy0 = wy0.reset_index(drop=True)

    # 物业费评分
    wy1 = wy.groupby('property_comp')['property_fee'].agg(['min','max']).reset_index()

    # 服务面积评分
    wy2 = wy.groupby('property_comp')['land_area'].max().to_frame('area_max').reset_index()

    # 服务楼盘数评分
    wy3 = wy.groupby('property_comp')['newest_id'].count().to_frame('newest_number').reset_index()

    wy4 = wy0.merge(wy1[['property_comp','max']],how='left',on='property_comp')
    wy5 = wy4.groupby('city_id')['max'].agg(['min','max']).reset_index()
    wy6 = pd.merge(wy4,wy5,how='left',on='city_id') 
    wy6.rename(columns={'max_x': 'property_fee','min': 'city_fee_min','max_y': 'city_fee_max'}, inplace=True)
    wy7 = wy6.merge(wy2[['property_comp','area_max']],how='left',on='property_comp')
    wy8 = wy7.groupby('city_id')['area_max'].agg(['min','max']).reset_index()
    wy9 = pd.merge(wy7,wy8,how='left',on='city_id')
    wy9.rename(columns={'max': 'city_area_max','min': 'city_area_min'}, inplace=True)
    wy10 = pd.merge(wy9,wy3,how='left',on='property_comp')
    wy11 = wy10.groupby('city_id')['newest_number'].agg(['min','max']).reset_index()
    wy12 = pd.merge(wy10,wy11,how='left',on='city_id')
    wy12.rename(columns={'max': 'city_newest_max','min': 'city_newest_min'}, inplace=True)

    wy12['property_fee_score']=(wy12['property_fee']-wy12['city_fee_min'])/(wy12['city_fee_max']-wy12['city_fee_min'])
    wy12['property_area_score']=(wy12['area_max']-wy12['city_area_min'])/(wy12['city_area_max']-wy12['city_area_min'])
    wy12['property_newest_score']=(wy12['newest_number']-wy12['city_newest_min'])/(wy12['city_newest_max']-wy12['city_newest_min'])
    wy12 = wy12.fillna(0)
    wy12['property_all_score']=wy12['property_fee_score']+wy12['property_area_score']+wy12['property_newest_score']

    wy13 = ['property_comp','city_id','property_all_score']
    wy14 = pd.DataFrame(wy12,columns=wy13)
    wy14['quarter'] = i
    wy14.rename(columns={'property_all_score': 'score'}, inplace=True)

    wy14['score'] = round(wy14['score'].astype('float'),2)+2
    wy14 = wy14[wy14['property_comp'].notnull()]


    res,columnNames = con.query("\
    SELECT DISTINCT a.newest_id,a.period quarter,b.property_comp,a.city_id \
    FROM dwb_db.a_dws_newest_period_admit a \
    LEFT JOIN dws_db_prd.dws_newest_property b ON b.newest_id=a.newest_id \
    WHERE a.dr=0 AND a.period ='"+i+"' ")

    wy15 = pd.DataFrame([list(i) for i in res],columns=columnNames)
    wy16 = wy15.merge(wy14[['score','property_comp','quarter']],how='left',on=['property_comp','quarter'])

    # 1.1.3需求增加高于城市均值百分比字段
    wy_mean = wy16.groupby(['city_id','quarter'])['score'].mean().to_frame('mean_score').reset_index()
    wy16['score'] = wy16['score'].fillna(0)
    wy_merge = wy16.merge(wy_mean,how='left',on=['city_id','quarter'])
    wy_merge['city_mean_score'] = (wy_merge['score']-wy_merge['mean_score'])/wy_merge['mean_score']
    wy_merge = wy_merge[['newest_id','city_id','score','quarter','mean_score','city_mean_score']]
    wy_merge=wy_merge.astype('str')
    property1 = property1.append(wy_merge)

property1.to_sql('dws_newest_property_score',engine,index=False,if_exists='append')
