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
date_quarter = '2020Q4'    #  获取季度（统计周期）


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
# 创建一个engine，用作最后结果插入mysql表中
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database)# 获取sqlalchemy链接
con = MysqlClient(db_host,database,user,password)


#dws_tag_purchase_poi_score 配套信息评分表#
#dws_newest_property_score 物业评分表#



# 取评分结果表
#   获取到的信息有： newest_id   楼盘id   例如: 1aaeb4748cb225cea69a87e2b319ed0f
#                   tag_value   标签值   例如： 城市综合体,购物中心,社区商业,商业裙楼
#                   tag_detail   配套详情 例如： 宝文中心  
#                   pure_distance 直线距离 例如： 1-5              
res,columnNames = con.query('''
SELECT newest_id,tag_value,tag_detail,pure_distance FROM dws_db.dws_tag_purchase_poi 
''')
df = pd.DataFrame([list(i) for i in res],columns=columnNames)

# 对标签值进行拆分，获取每个楼盘的标签  值：城市综合体,购物中心,社区商业,商业裙楼
df1 = df.drop('tag_value', axis=1).join(df['tag_value'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename('tag_value'))

# 新建一列type，并替换个别标签值
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

# 统计楼盘附近标签值的数量（配套设施）
df2 = df1.groupby(['newest_id','type','pure_distance'])['newest_id'].count().to_frame('number').reset_index()
# 获取楼盘附近最近的标签值（标签值就是配套设施，以后都用标签值来代替）
df3 = df2.groupby(['newest_id','type'])['pure_distance'].min().reset_index()
# 修改pure_distance的数据类型
df3['pure_distance']=df3['pure_distance'].astype('str').astype('int')

# 新建一列score: 根据标签值和距离给与的权重值
# 地铁
df3.loc[(df3['type']=='地铁') & (df3['pure_distance']==1),'score'] = 1.5
df3.loc[(df3['type']=='地铁') & (df3['pure_distance']==2),'score'] = 0.75
df3.loc[(df3['type']=='地铁') & (df3['pure_distance']==3),'score'] = 0.375
# 公交
df3.loc[(df3['type']=='公交') & (df3['pure_distance']==1),'score'] = 0.5
df3.loc[(df3['type']=='公交') & (df3['pure_distance']==2),'score'] = 0.25
df3.loc[(df3['type']=='公交') & (df3['pure_distance']==3),'score'] = 0.125
# 学校
df3.loc[(df3['type']=='学校') & (df3['pure_distance']<=2),'score'] = 1
df3.loc[(df3['type']=='学校') & (df3['pure_distance']<=3),'score'] = 0.5
df3.loc[(df3['type']=='学校') & (df3['pure_distance']<=5),'score'] = 0.25
# 综合医院
df3.loc[(df3['type']=='综合专科医院'),'score'] = 0.25
# 购物中心
df3.loc[(df3['type']=='购物中心') & (df3['pure_distance']<=2),'score'] = 1.5
df3.loc[(df3['type']=='购物中心') & (df3['pure_distance']<=3),'score'] = 1
df3.loc[(df3['type']=='购物中心') & (df3['pure_distance']<=5),'score'] = 0.5
# 百货
df3.loc[(df3['type']=='百货') & (df3['pure_distance']<=2),'score'] = 1.2
df3.loc[(df3['type']=='百货') & (df3['pure_distance']<=3),'score'] = 0.8
df3.loc[(df3['type']=='百货') & (df3['pure_distance']<=5),'score'] = 0.4
# 商业街
df3.loc[(df3['type']=='商业街') & (df3['pure_distance']<=2),'score'] = 0.9
df3.loc[(df3['type']=='商业街') & (df3['pure_distance']<=3),'score'] = 0.6
df3.loc[(df3['type']=='商业街') & (df3['pure_distance']<=5),'score'] = 0.3
# 文旅商业
df3.loc[(df3['type']=='文旅商业') & (df3['pure_distance']<=2),'score'] = 0.6
df3.loc[(df3['type']=='文旅商业') & (df3['pure_distance']<=3),'score'] = 0.4
df3.loc[(df3['type']=='文旅商业') & (df3['pure_distance']<=5),'score'] = 0.2
# 酒店
df3.loc[(df3['type']=='酒店') & (df3['pure_distance']<=2),'score'] = 0.6
df3.loc[(df3['type']=='酒店') & (df3['pure_distance']<=3),'score'] = 0.4
df3.loc[(df3['type']=='酒店') & (df3['pure_distance']<=5),'score'] = 0.2
# 其他
df3.loc[(df3['type']=='其他') & (df3['pure_distance']<=2),'score'] = 0.3
df3.loc[(df3['type']=='其他') & (df3['pure_distance']<=3),'score'] = 0.2
df3.loc[(df3['type']=='其他') & (df3['pure_distance']<=5),'score'] = 0.1

# 获取每个楼盘的权重值总和score
df4 = df3.groupby(['newest_id'])['score'].sum().reset_index()

# 获取楼盘对应city_id
#                   newest_id     楼盘id
#                   city_id       城市id
res,columnNames = con.query('''
SELECT newest_id,city_id FROM dwb_db.dwb_newest_info 
''')
df5 = pd.DataFrame([list(i) for i in res],columns=columnNames)

# 和之前的结果左连接，获取city_id,。得到一个楼盘id，城市id，楼盘的权重值这么一个数据集
df6= pd.merge(df4,df5,how='left',on='newest_id')
#  统计每个城市权重值总和 score
df7 = df6.groupby('city_id')['score'].sum().reset_index()
#  统计城市的数量
df8 = df6.groupby(['city_id']).size().to_frame('city_count').reset_index()
#  计算出城市平均评分， score 权重总值（楼盘评分）/每个城市的楼盘数量
df9 = pd.merge(df7,df8,how='inner',on='city_id')
df9['city_score']=round(df9['score']/df9['city_count'],2,)
# 将之前计算的df6和df9进行合并，使用城市进行关联，获取城市的平均评分
df10 = pd.merge(df6,df9[['city_score','city_id']],how='left',on='city_id')
# 计算获得高于城市平均评分，公式--> （楼盘权重值-城市平均评分）/城市平均评分
df10['higt_score']=round((df10['score']-df10['city_score'])/df10['city_score'],4)
#  新加列 quarter 赋值为2020年第四季度
df10['quarter'] = date_quarter
# 将数据加载到dws_tag_purchase_poi_score表中
df10.to_sql('dws_tag_purchase_poi_score',engine,index=False,if_exists='append')




# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 



# 物业评分
# 获取物业费，服务过的楼盘数，服务过的总平方数
# dws_newest_property_rs   楼盘与物业公司关联表   
#                               newest_id       楼盘id
#                               property_id     省份城市id
# dwb_newest_info          新房楼盘
#                                land_area      占地面积
#                                city_id        城市id
#                                newest_id      楼盘id
# ori_newest_info_main  新房-贝壳未去重结果集
#                                property_fee    物业费
#                                uuid           楼盘uuid
res,columnNames = con.query('''
SELECT a0.newest_id,a0.property_id,a1.land_area,a2.property_fee,a1.city_id
FROM dws_db.dws_newest_property_rs a0
LEFT JOIN dwb_db.dwb_newest_info a1 ON a1.newest_id=a0.newest_id
LEFT JOIN odsdb.ori_newest_info_main a2 ON a2.uuid=a0.newest_id
WHERE a2.property_fee>0 AND a1.land_area>1000
''')
wy = pd.DataFrame([list(i) for i in res],columns=columnNames)

# 转换数据类型
wy['property_fee']=wy['property_fee'].astype('str').astype('float')
wy['land_area']=wy['land_area'].astype('str').astype('float')
wy['property_id']=wy['property_id'].astype('str')
wy['city_id']=wy['city_id'].astype('str')

# 物业公司服务最多的城市·
# 根据统计一下以城市为单位的楼盘数量，newest_city_num
wy0 = wy.groupby(['property_id','city_id']).size().to_frame('newest_city_num').reset_index()
# 按照求出的楼盘数量进行排序，降序排序
wy0.sort_values('newest_city_num',ascending=False,inplace=True)
# 删除重复项，仅保留最后一次出现的重复项。
wy0.drop_duplicates(subset=['property_id'],keep='first',inplace=True)
wy0 = wy0.reset_index(drop=True)


# 物业费评分,获取每个省份物业费的最大值和最小值
wy1 = wy.groupby('property_id')['property_fee'].agg(['min','max']).reset_index()

# 服务面积评分，获取每个物业的最大面积  area_max
wy2 = wy.groupby('property_id')['land_area'].max().to_frame('area_max').reset_index()
# 服务楼盘数评分
#获取每个物业的楼盘数量  newest_number
wy3 = wy.groupby('property_id')['newest_id'].count().to_frame('newest_number').reset_index()
# 根据物业id关联数据集wy0，wy1。获得最大物业费和楼盘数量
wy4 = wy0.merge(wy1[['property_id','max']],how='inner',on='property_id')
# 获取每个物业物业费的最大值和最小值
wy5 = wy4.groupby('city_id')['max'].agg(['min','max']).reset_index()
# 获得项目数量，和物业最大物业费，城市最小和最大物业费
wy6 = pd.merge(wy4,wy5,how='inner',on='city_id')
# 修改表名
wy6.rename(columns={'max_x': 'property_fee','min': 'city_fee_min','max_y': 'city_fee_max'}, inplace=True)
# 将wy6再进行合并，添加最大城市最大面积和物业id
wy7 = wy6.merge(wy2[['property_id','area_max']],how='inner',on='property_id')
#  接下来再把面积再整一遍
#  算出每个城市的最大面积物业和最小面积物业
wy8 = wy7.groupby('city_id')['area_max'].agg(['min','max']).reset_index()
#  合并数据集，获得每个城市的最大物业面积和最小物业面积，以及物业费
wy9 = pd.merge(wy7,wy8,how='inner',on='city_id')
#  替换列名
wy9.rename(columns={'max': 'city_area_max','min': 'city_area_min'}, inplace=True)
#  再把楼盘的数量再理一遍
#  合并wy3获取楼盘数量
wy10 = pd.merge(wy9,wy3,how='inner',on='property_id')
#  根据城市获取每个城市的最大楼盘数量和最小楼盘数量
wy11 = wy10.groupby('city_id')['newest_number'].agg(['min','max']).reset_index()
#  合并数据集，讲最大楼盘数量和最小楼盘数量进行合并
wy12 = pd.merge(wy10,wy11,how='inner',on='city_id')
#  修改列名
wy12.rename(columns={'max': 'city_newest_max','min': 'city_newest_min'}, inplace=True)


#  获取评分   物业： （每个物业的最大物业费-城市的最小物业费）/（城市的最大物业费-城市的最小物业费）
wy12['property_fee_score']=(wy12['property_fee']-wy12['city_fee_min'])/(wy12['city_fee_max']-wy12['city_fee_min'])
#  获取评分   面积 （每个物业的最大面积-城市的最小面积）/（城市的最大面积-城市的最小面积）
wy12['property_area_score']=(wy12['area_max']-wy12['city_area_min'])/(wy12['city_area_max']-wy12['city_area_min'])
#  获取评分   楼盘数量 （每个物业的最大物业费-城市的最小物业费）/（城市的最大物业费-城市的最小物业费）
wy12['property_newest_score']=(wy12['newest_number']-wy12['city_newest_min'])/(wy12['city_newest_max']-wy12['city_newest_min'])
#  为空时 用0添加
wy12 = wy12.fillna(0)
# 添加列property_all_score，值是面积物业费楼盘数量的和
wy12['property_all_score']=wy12['property_fee_score']+wy12['property_area_score']+wy12['property_newest_score']

#  新建数据集
wy13 = ['property_id','city_id','property_all_score']
wy14 = pd.DataFrame(wy12,columns=wy13)
#  新加列 quarter 赋值为2020年第四季度
wy14['quarter'] = date_quarter
wy14.rename(columns={'property_all_score': 'score'}, inplace=True)
#  转换数据类型
wy14=wy14.astype('str')
#  添加到dws_newest_property_score中
wy14.to_sql('dws_newest_property_score',engine,index=False,if_exists='append')



















