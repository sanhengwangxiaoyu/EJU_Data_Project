#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-

import configparser
import os
import sys
from numpy.lib.function_base import append
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
# database = cf.get("Mysql", "database")  # 获取dbname对应的值
database = 'temp_db'

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


# In[1]:
#意向客户总量#
con = MysqlClient(db_host,database,user,password)
# ori_newest_info_base_new_20210609  临时的楼盘基础清洗后的表 
#                                                      id
#                                                      main_id
#                                                      uuid
#                                                      platform
#                                                      url
#                                                      newest_name
#                                                      city_id
#                                                      city_name
#                                                      county_id
#                                                      county_name
#                                                      block_name
#                                                      ori_lnglat
#                                                      address
#                                                      sale_address
#                                                      sale_phone
#                                                      alias        别名
#                                                      former_name
#                                                      layout
#                                                      total_price
#                                                      unit_price           楼盘单价
#                                                      recent_opening_time    最近开盘时间
#                                                      recent_delivery_time   最近交房时间
#                                                      issue_date           发证日期
#                                                      issue_number
#                                                      developer
#                                                      investor
#                                                      brander
#                                                      land_area     总占地面积
#                                                      building_area   总建筑面积
#                                                      arch_style
#                                                      green_rate     绿化率
#                                                      volume_rate
#                                                      building_type
#                                                      right_term
#                                                      property_type    物业类型
#                                                      property_sub
#                                                      household_num
#                                                      property_comp
#                                                      property_fee
#                                                      park_rate
#                                                      park_num
#                                                      park_price
#                                                      orientation
#                                                      building_num
#                                                      avg_distance
#                                                      ong_park_num
#                                                      ung_park_num
#                                                      build_density
#                                                      floor_num
#                                                      ambitus
#                                                      bike_park_number
#                                                      agent
#                                                      decoration
#                                                      layout_pic
#                                                      estate_pic
#                                                      advantage
#                                                      disadvantages
#                                                      pay_meth
#                                                      sale_status
#                                                      gd_city     高德城市
#                                                      gd_county   高德区县
#                                                      gd_lnglat   高德经纬度
#                                                      gd_lng      高德经度
#                                                      gd_lat      高度维度
#                                                      gd_office_name
#                                                      is_accurate
#                                                      heat_mode
#                                                      power_mode
#                                                      water_mode
#                                                      ring_locate
#                                                      `characters`
#                                                      deco_level
#                                                      mate_equip
#                                                      designer
#                                                      builder
#                                                      progress
#                                                      provide_sche       动态文章 
#                                                      land_time          拿地时间
#                                                      create_time
#                                                      update_time
#                                                      r_id
#                                                      remark
#                                                      alias_json
#                                                      flag
ori=con.query('''SELECT * FROM dwd_db.ori_newest_info_base_new_20210609''')

#dim_geography   地理位置维度表
#                                                      city_name   城市名字
#                                                      city_id     城市id
#                                                      grade       级别
city=con.query('''SELECT city_name,city_id FROM dws_db.dim_geography WHERE grade=3''')




# In[2]:
#问题清洗


# 别名：（去掉），--待定--（无意义的字符去掉）   alias        别名
ori['alias'] = ori['alias'].str.replace('别名：', '')
ori['alias'] = ori['alias'].str.replace('--待定--', '')

# 均价和日期格式处理
#                                                      unit_price           楼盘单价
#                                                      recent_opening_time    最近开盘时间
#                                                      recent_delivery_time   最近交房时间
#                                                      issue_date           发证日期
#  汉字
ori['unit_price'] = ori['unit_price'].str.replace('[\u4e00-\u9fa5]', '')
ori['recent_opening_time'] = ori['recent_opening_time'].str.replace('[\u4e00-\u9fa5]', '')
ori['recent_delivery_time'] = ori['recent_delivery_time'].str.replace('[\u4e00-\u9fa5]', '')
ori['issue_date'] = ori['issue_date'].str.replace('[\u4e00-\u9fa5]', '')
#  特殊符号
ori['unit_price'] = ori['unit_price'].str.replace('\/', '')
ori['issue_date'] = ori['issue_date'].str.replace('-', '')
#  非常规日期处理
#  统一时间格式
ori['recent_delivery_time_num'] = ori['recent_delivery_time'].map(lambda x:len(x))
ori7 = ori[ori['recent_delivery_time_num'] == 7]
ori = ori[ori['recent_delivery_time_num'] != 7]
# ori = ori[ori['recent_delivery_time_num'] == 10]
# ori10 = ori[ori['recent_delivery_time_num'] != 10]
# ori_10['recent_delivery_time'] = ''
ori7['recent_delivery_time'] = ori7['recent_delivery_time']+"-"+'01'
# 拼接
ori = ori.append(ori7,ignore_index=True)
# 去掉单位
#                                                      land_area     总占地面积
#                                                      building_area   总建筑面积
ori['land_area'] = ori['land_area'].str.replace('㎡', '')
ori['building_area'] = ori['building_area'].str.replace('㎡', '')
ori['land_area'] = ori['land_area'].str.replace('-', '')
ori['building_area'] = ori['building_area'].str.replace('-', '')


# 绿化率去掉汉字和无意义的字符，保留百分比；
#                                                      green_rate     绿化率
ori['green_rate'] = ori['green_rate'].str.replace('[\u4e00-\u9fa5]', '')
ori['green_rate'] = ori['green_rate'].str.replace('\%', '')


#物业类型清洗
#                                                     property_type    物业类型
#                                                     property_sub
ori_01 = ori.loc[ori['property_type'].str.contains("宅")]
ori = ori.loc[~ori['property_type'].str.contains("宅")]
ori_01['property_sub'] = '住宅类'

ori_02 = ori.loc[ori['property_type'].str.contains("寓")]
ori = ori.loc[~ori['property_type'].str.contains("寓")]
ori_02['property_sub'] = '公寓'

ori_03 = ori.loc[ori['property_type'].str.contains("商")]
ori = ori.loc[~ori['property_type'].str.contains("商")]
ori_03['property_sub'] = '商住'

ori_04 = ori.loc[ori['property_type'].str.contains("写字")]
ori = ori.loc[~ori['property_type'].str.contains("写字")]
ori_04['property_sub'] = '商住'

ori_05 = ori.loc[ori['property_type'].str.contains("别")]
ori = ori.loc[~ori['property_type'].str.contains("别")]
ori_05['property_sub'] = '住宅类'

# ori_06 = ori.loc[ori['provide_sche'].str.contains("商业")]
# ori = ori.loc[~ori['provide_sche'].str.contains("商业")]
# ori_01['property_sub'] = '商住'

# ori_07 = ori.loc[ori['provide_sche'].str.contains("商业类")]
# ori = ori.loc[~ori['provide_sche'].str.contains("商业类")]
# ori_01['property_sub'] = '商住'

ori = pd.concat([ori_01,ori_02,ori_03,ori_04,ori_05])


# 移动列
#                                                      gd_city     高德城市
#                                                      gd_county   高德区县
#                                                      gd_lnglat   高德经纬度
#                                                      gd_lng      高德经度
#                                                      gd_lat      高度维度
#                                                      provide_sche       动态文章 
#                                                      land_time          拿地时间


ori['gd_city'] = ''
ori['gd_city'] = ori['gd_county']

ori['gd_county'] = ''
ori['gd_county'] = ori['gd_lnglat']

ori['gd_lnglat'] = ''
ori['gd_lnglat'] = ori['gd_lng']

ori['gd_lng'] = ''
ori['gd_lat'] = ''
ori['gd_lng'] = ori['gd_lnglat'].map(lambda x:x. split(',')[0])
ori['gd_lat'] = ori['gd_lnglat'].map(lambda x:x. split(',')[1])

ori['provide_sche'] = ''
ori['provide_sche'] = ori['land_time']

ori['land_time'] = ''

# 经纬度脏数据处理
ori['gd_lng'] = pd.to_numeric(ori['gd_lng']).astype(int) 
ori_lng = ori[ori['gd_lng'] <= 90 ]
ori = ori[ori['gd_lng'] > 90 ]

# ori_lat = ori[ori['gd_lat'] > '90' ]
# ori = ori[ori['gd_lng'] <= '90' ]

# ori_lngat = pd.concat([ori_lng,ori_lat])

ori_lng['gd_lng'] = ''
ori_lng['gd_lat'] = ''
ori_lng['gd_lng'] = ori_lng['gd_lnglat'].map(lambda x:x. split(',')[1])
ori_lng['gd_lat'] = ori_lng['gd_lnglat'].map(lambda x:x. split(',')[0])

ori['gd_lng'] = ''
ori['gd_lat'] = ''
ori['gd_lng'] = ori['gd_lnglat'].map(lambda x:x. split(',')[0])
ori['gd_lat'] = ori['gd_lnglat'].map(lambda x:x. split(',')[1])


ori = ori.append(ori_lng,ignore_index=True)



#In[3]:
#city_id不对，换成正常的
grouped = pd.merge(ori, city, how='left', on=['city_name'])
grouped = grouped[["main_id","uuid","platform","url","newest_name","city_id_y","city_name","county_id","county_name","block_name","ori_lnglat","address","sale_address","sale_phone","alias","former_name","layout","total_price","unit_price","recent_opening_time","recent_delivery_time","issue_date","issue_number","developer","investor","brander","land_area","building_area","arch_style","green_rate","volume_rate","building_type","right_term","property_type","property_sub","household_num","property_comp","property_fee","park_rate","park_num","park_price","orientation","building_num","avg_distance","ong_park_num","ung_park_num","build_density","floor_num","ambitus","bike_park_number","agent","decoration","layout_pic","estate_pic","advantage","disadvantages","pay_meth","sale_status","gd_city","gd_county","gd_lnglat","gd_lng","gd_lat","gd_office_name","is_accurate","heat_mode","power_mode","water_mode","ring_locate","characters","deco_level","mate_equip","designer","builder","progress","provide_sche","land_time","create_time","update_time","r_id","remark","alias_json","flag"]]
grouped.columns = ["main_id","uuid","platform","url","newest_name","city_id","city_name","county_id","county_name","block_name","ori_lnglat","address","sale_address","sale_phone","alias","former_name","layout","total_price","unit_price","recent_opening_time","recent_delivery_time","issue_date","issue_number","developer","investor","brander","land_area","building_area","arch_style","green_rate","volume_rate","building_type","right_term","property_type","property_sub","household_num","property_comp","property_fee","park_rate","park_num","park_price","orientation","building_num","avg_distance","ong_park_num","ung_park_num","build_density","floor_num","ambitus","bike_park_number","agent","decoration","layout_pic","estate_pic","advantage","disadvantages","pay_meth","sale_status","gd_city","gd_county","gd_lnglat","gd_lng","gd_lat","gd_office_name","is_accurate","heat_mode","power_mode","water_mode","ring_locate","characters","deco_level","mate_equip","designer","builder","progress","provide_sche","land_time","create_time","update_time","r_id","remark","alias_json","flag"]

grouped
to_dws(grouped,'ori_newest_info_base_new_20210609_test')

# grouped.to_csv('C:\\Users\\86133\\Desktop\\grouped.csv')
