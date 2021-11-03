# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Jul 12 17:44:47 2021
  许可证套数	issue_room
  许可证面积	issue_area
  Change on Oct 28 13:29:00 2021
    去除三亚,单独搞三亚

"""
import configparser
import os
import sys,io
from numpy.lib.function_base import append
import pymysql
import pandas as pd
import numpy as np
from collections import Counter
import re
from sqlalchemy import create_engine
import getopt
import time

##读取配置文件##
pymysql.install_as_MySQLdb()
cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

##设置变量初始值##
user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
date_quarter = '2021Q3'   # 季度
table_name = 'dwb_newest_issue_offer' # 要插入的表名称
database = 'dwb_db'


# In[2]:
##通过输入的参数的方式获取变量值##  如果不需要使用输入参数的方式，可以不用这段
opts,args=getopt.getopt(sys.argv[1:],"t:q:d:c:",["city_id","database=","table=","quarter="])
for opts,arg in opts:
  if opts=="-t" or opts=="--table": # 获取输入参数 -t或者--table 后的值
    table_name = arg
  elif opts=="-q" or opts=="--quarter":  # 获取输入参数 -1或者--quarter 后的值
    date_quarter = arg
  elif opts=="-d" or opts=="--database":  # 获取输入参数 -1或者--quarter 后的值
    database = arg
  elif opts=="-c" or opts=="--city_id":  # 获取输入参数 -1或者--quarter 后的值
    city_id = arg


# In[3]:
##重置时间格式
start_date = str(pd.to_datetime(date_quarter))[0:10]   #截取成yyyy-MM-dd
end_date =  str(pd.to_datetime(date_quarter) + pd.offsets.QuarterEnd(0))[0:10]      #截取成yyyy-MM-dd

##mysql连接配置##
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

##mysql写入执行##
def to_dws(result,table):
    engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database+'?charset=utf8')
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)
##room_sum清洗逻辑
def sum_str(s):
    new_str = ""  		#创建一个空字符串
    for ch in s:
	    if ch.isdigit():		#字符串中的方法，可以直接判断ch是否是数字
		    new_str += ch
	    else:
		    new_str += " "
    sub_list = new_str.split()   #对新的字符串切片
    num_list = list(map(int, sub_list)) 	#map方法，使列表中的元素按照指定方式转变
    res  = sum(num_list)
    # print(res)
    return res



##正式代码##
"""
1> 获取数据信息：dws_newest_info，dws_newest_period_admit
    通过admit筛选准入楼盘信息，通过dws_newest_info获取楼盘具体信息
"""
con = MysqlClient(db_host,database,user,password)


# In[4]:
# city_newest_deal  新楼盘交易数据表 
#      url          楼盘网址
#      gd_city      城市名称
#      floor_name   楼盘名字 
#      business     楼盘开发商
#      address      楼盘地址
#      issue_code   预售证
#      issue_date_clean  发证时间
#      room_sum      房间数量
#      building_code 建筑编号
#      issue_area    预售证面积
# city_id = '东莞'
# if 'city_id' in locals().keys() :
#        issue_offer=con.query("select url ,gd_city ,floor_name ,business ,address ,issue_code , issue_date_clean ,replace(substr(issue_date_clean,1,7),'-','') issue_month , '"+date_quarter+"' issue_quarter ,case when room_sum is null then 0 when room_sum = '' then 0 when room_sum = 'None' then 0 else room_sum end room_sum,building_code ,case when issue_area = 'None' then 0 when issue_area = '' then 0 else substr(issue_area,1,8) end issue_area from odsdb.city_newest_deal where issue_code is not null and issue_code != '' and issue_date_clean between '"+start_date+"' and '"+end_date+"' and city_name = '"+city_id+"'")
# else:
issue_offer=con.query("select url ,gd_city ,floor_name ,business ,address ,issue_code , issue_date_clean ,replace(substr(issue_date_clean,1,7),'-','') issue_month , '"+date_quarter+"' issue_quarter ,case when room_sum is null then 0 when room_sum = '' then 0 when room_sum = 'None' then 0 else room_sum end room_sum,building_code ,case when issue_area = 'None' then 0 when issue_area = '' then 0 else substr(issue_area,1,8) end issue_area ,room_code from odsdb.city_newest_deal where issue_code is not null and issue_code != '' and issue_date_clean between '"+start_date+"' and '"+end_date+"'")


# In[5]:
########每个城市的预售证粒度不一样，所以统计的供应数量方式也不一样###########
########################################################################
########################################################################
#按照room_sum统计一份套数 （深圳 佛山 嘉兴 南宁 九江 保定 长沙 东莞 海口 合肥 青岛 沈阳 厦门 淄博 福州）
#   深圳市
df_sz = issue_offer[issue_offer['gd_city'] == '深圳市']
df_sz = df_sz[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url']].drop_duplicates()
df_sz[['room_sum']] = df_sz[['room_sum']].astype('int')
#   嘉兴市
df_jx = issue_offer[issue_offer['gd_city'] == '嘉兴市']
df_jx = df_jx.loc[~df_jx['building_code'].str.contains("地下")]
df_jx = df_jx.loc[~df_jx['building_code'].str.contains("办公")]
df_jx = df_jx.loc[~df_jx['building_code'].str.contains("车库")]
df_jx = df_jx.loc[~df_jx['building_code'].str.contains("商业")]
df_jx = df_jx.loc[~df_jx['building_code'].str.contains("其他")]
df_jx = df_jx.loc[~df_jx['floor_name'].str.contains("地下")]
df_jx = df_jx.loc[~df_jx['floor_name'].str.contains("办公")]
df_jx = df_jx.loc[~df_jx['floor_name'].str.contains("车库")]
df_jx = df_jx.loc[~df_jx['floor_name'].str.contains("商业")]
df_jx = df_jx.loc[~df_jx['floor_name'].str.contains("其他")]
df_jx = df_jx[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url']].drop_duplicates()
df_jx[['room_sum']] = df_jx[['room_sum']].astype('int')
#   南宁市
df_nn = issue_offer[issue_offer['gd_city'] == '南宁市']
df_nn = df_nn[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url']].drop_duplicates()
df_nn[['room_sum']] = df_nn[['room_sum']].astype('int')
#   佛山市
df_fs = issue_offer[issue_offer['gd_city'] == '佛山市']
df_fs = df_fs.groupby(['gd_city','floor_name','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url'])['issue_code'].max().reset_index()
df_fs = df_fs[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url']].drop_duplicates()
df_fs[['room_sum']] = df_fs[['room_sum']].astype('int')
#   九江市
df_jj =  issue_offer[issue_offer['gd_city'] == '九江市']
df_jj = df_jj[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url']].drop_duplicates()
df_jj[['room_sum']] = df_jj[['room_sum']].astype('int')
#   保定市
df_bd =  issue_offer[issue_offer['gd_city'] == '保定市']
df_bd['room_sum'] = df_bd['room_sum'].apply(lambda x:sum_str(x))
df_bd = df_bd[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url']].drop_duplicates()
df_bd[['room_sum']] = df_bd[['room_sum']].astype('int')
#   长沙市
df_cs =  issue_offer[issue_offer['gd_city'] == '长沙市']
df_cs = df_cs[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url']].drop_duplicates()
df_cs[['room_sum']] = df_cs[['room_sum']].astype('int')
#   东莞市
df_dg =  issue_offer[issue_offer['gd_city'] == '东莞市']
df_dg = df_dg.loc[~df_dg['building_code'].str.contains("地下")]
df_dg = df_dg[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url']].drop_duplicates()
df_dg[['room_sum']] = df_dg[['room_sum']].astype('int')
#   海口市
df_hk =  issue_offer[issue_offer['gd_city'] == '海口市']
df_hk = df_hk.loc[~df_hk['building_code'].str.contains("地下")]
df_hk = df_hk.loc[~df_hk['building_code'].str.contains("办公")]
df_hk = df_hk.loc[~df_hk['building_code'].str.contains("综合")]
df_hk = df_hk.loc[~df_hk['building_code'].str.contains("商业")]
df_hk = df_hk.loc[~df_hk['building_code'].str.contains("商务")]
df_hk = df_hk.loc[~df_hk['building_code'].str.contains("商住")]
df_hk = df_hk.loc[~df_hk['building_code'].str.contains("中心")]
df_hk['room_sum'] = df_hk['room_sum'].apply(lambda x:re.sub("\D","",x))
df_hk = df_hk[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url']].drop_duplicates()
# 相同楼盘不同预售证清洗 ：只取一个预售证的套数
df_hk_tmp1 = df_hk[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','issue_area','business','address','url']]
df_hk_tmp2 = df_hk[['gd_city','floor_name','issue_code','room_sum']]
# 只取一个预售证的套数
df_hk_tmp2['last_issue_code']=df_hk_tmp2.sort_values(by=['gd_city','floor_name','issue_code','room_sum']).groupby(['gd_city','floor_name','room_sum'])['issue_code'].shift(1)
df_hk_tmp2.at[~df_hk_tmp2['last_issue_code'].isna(),'room_sum'] = 0
df_hk_tmp2 = df_hk_tmp2[['gd_city','floor_name','issue_code','room_sum']]
# 合并
df_hk = pd.merge(df_hk_tmp1,df_hk_tmp2,how='inner',on=['gd_city','floor_name','issue_code'])
df_hk[['room_sum']] = df_hk[['room_sum']].astype('int')
test = df_hk.groupby(['gd_city'])['room_sum'].sum().reset_index()

#   合肥市
df_hf =  issue_offer[issue_offer['gd_city'] == '合肥市']
df_hf = df_hf.loc[~df_hf['building_code'].str.contains("地下")]
df_hf = df_hf.loc[~df_hf['building_code'].str.contains("办公")]
df_hf = df_hf.loc[~df_hf['building_code'].str.contains("综合")]
df_hf = df_hf.loc[~df_hf['building_code'].str.contains("商业")]
df_hf = df_hf.loc[~df_hf['building_code'].str.contains("商务")]
df_hf = df_hf.loc[~df_hf['building_code'].str.contains("商住")]
df_hf = df_hf.loc[~df_hf['building_code'].str.contains("中心")]
df_hf['room_sum'] = df_hf['room_sum'].apply(lambda x:re.sub("\D","",x))
df_hf = df_hf[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url']].drop_duplicates()
df_hf.at[df_hf['room_sum'] == '' , 'room_sum'] = '0'
df_hf[['room_sum']] = df_hf[['room_sum']].astype('int')
df_hf_test = df_hf.groupby(['gd_city'])['room_sum'].sum().reset_index()
#   青岛市
df_qd =  issue_offer[issue_offer['gd_city'] == '青岛市']
df_qd['room_sum'] = df_qd['room_sum'].apply(lambda x:re.sub("\D","",x))
df_qd = df_qd[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url']].drop_duplicates()
df_qd[['room_sum']] = df_qd[['room_sum']].astype('int')
#   沈阳市
df_sheny =  issue_offer[issue_offer['gd_city'] == '沈阳市']
df_sheny['room_sum'] = df_sheny['room_sum'].apply(lambda x:re.sub("\D","",x))
df_sheny = df_sheny[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url']].drop_duplicates()
df_sheny[['room_sum']] = df_sheny[['room_sum']].astype('int')
#   厦门市
df_xm =  issue_offer[issue_offer['gd_city'] == '厦门市']
df_xm = df_xm[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url']].drop_duplicates()
df_xm[['room_sum']] = df_xm[['room_sum']].astype('int')
#   淄博市
df_zb =  issue_offer[issue_offer['gd_city'] == '淄博市']
df_zb = df_zb[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url']].drop_duplicates()
df_zb[['room_sum']] = df_zb[['room_sum']].astype('int')
#   福州市
df_fz =  issue_offer[issue_offer['gd_city'] == '福州市']
df_fz = df_fz.loc[~df_fz['building_code'].str.contains("地下")]
df_fz = df_fz.loc[~df_fz['building_code'].str.contains("办公")]
df_fz = df_fz.loc[~df_fz['building_code'].str.contains("综合")]
df_fz = df_fz.loc[~df_fz['building_code'].str.contains("商业")]
df_fz = df_fz.loc[~df_fz['building_code'].str.contains("商务")]
df_fz = df_fz.loc[~df_fz['building_code'].str.contains("商住")]
df_fz = df_fz.loc[~df_fz['building_code'].str.contains("中心")]
df_fz = df_fz.groupby(['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address'])['url'].max().reset_index()
df_fz[['room_sum']] = df_fz[['room_sum']].astype('int')
#   上海市
df_sh =  issue_offer[issue_offer['gd_city'] == '上海市']
df_sh = df_sh[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url']].drop_duplicates()




# In[6]:
########################################################################
########################################################################
########################################################################
# 按照issue_area统计一份套数 （ 无锡市 成都 贵阳 惠州 武汉 济南  肇庆）
#    无锡市
df_wx =  issue_offer[issue_offer['gd_city'] == '无锡市']
df_wx['issue_area'] = df_wx['issue_area'].map(lambda x:x. split('.')[0])
df_wx['issue_area'] = df_wx['issue_area'].apply(lambda x:re.sub("\D","",x))
df_wx[['room_sum']] = (df_wx[['issue_area']].astype('float')/60).astype('int')
df_wx = df_wx[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url']].drop_duplicates()
#    成都市
df_cd =  issue_offer[issue_offer['gd_city'] == '成都市']
df_cd['issue_area'] = df_cd['issue_area'].map(lambda x:x. split('.')[0])
df_cd['issue_area'] = df_cd['issue_area'].apply(lambda x:re.sub("\D","",x))
df_cd.at[df_cd['issue_area'] == '','issue_area']=0
df_cd[['room_sum']] = (df_cd[['issue_area']].astype('float')/180).astype('int')
df_cd = df_cd[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url']].drop_duplicates()
#    贵阳市
df_gy =  issue_offer[issue_offer['gd_city'] == '贵阳市']
df_gy['issue_area'] = df_gy['issue_area'].map(lambda x:x. split('.')[0])
df_gy['issue_area'] = df_gy['issue_area'].apply(lambda x:re.sub("\D","",x))
df_gy.at[df_gy['issue_area'] == '','issue_area']=0
df_gy[['room_sum']] = (df_gy[['issue_area']].astype('float')/90).astype('int')
df_gy = df_gy[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url']].drop_duplicates()
#    惠州市
df_hz =  issue_offer[issue_offer['gd_city'] == '惠州市']
df_hz['issue_area'] = df_hz['issue_area'].map(lambda x:x. split('.')[0])
df_hz['issue_area'] = df_hz['issue_area'].apply(lambda x:re.sub("\D","",x))
df_hz.at[df_hz['issue_area'] == '','issue_area']=0
df_hz[['room_sum']] = (df_hz[['issue_area']].astype('float')/120).astype('int')
df_hz = df_hz[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url']].drop_duplicates()
#    武汉市
df_wh =  issue_offer[issue_offer['gd_city'] == '武汉市']
df_wh['issue_area'] = df_wh['issue_area'].map(lambda x:x. split('.')[0])
df_wh['issue_area'] = df_wh['issue_area'].apply(lambda x:re.sub("\D","",x))
df_wh.at[df_wh['issue_area'] == '','issue_area']=0
df_wh[['room_sum']] = (df_wh[['issue_area']].astype('float')/120).astype('int')
df_wh = df_wh[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url']].drop_duplicates()
#    济南市
df_jn =  issue_offer[issue_offer['gd_city'] == '济南市']
df_jn['issue_area'] = df_jn['issue_area'].map(lambda x:x. split('.')[0])
df_jn['issue_area'] = df_jn['issue_area'].apply(lambda x:re.sub("\D","",x))
df_jn.at[df_jn['issue_area'] == '','issue_area']=0
df_jn[['room_sum']] = (df_jn[['issue_area']].astype('float')/120).astype('int')
df_jn = df_jn[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url']].drop_duplicates()
#    肇庆市
df_zq =  issue_offer[issue_offer['gd_city'] == '肇庆市']
df_zq['issue_area'] = df_zq['issue_area'].map(lambda x:x. split('.')[0])
df_zq['issue_area'] = df_zq['issue_area'].apply(lambda x:re.sub("\D","",x))
df_zq.at[df_zq['issue_area'] == '','issue_area']=0
df_zq[['room_sum']] = (df_zq[['issue_area']].astype('float')/120).astype('int')
df_zq = df_zq[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url']].drop_duplicates()


# In[7]:
########上边都是以预售证粒度统计的房间数量，下边是以建筑编号为粒度统计房间数量#######
########################################################################
########################################################################
#按照room_sum统计一份套数 （深圳 佛山 嘉兴 南宁 九江 保定 长沙 东莞 海口 合肥 青岛 沈阳 厦门 淄博 无锡市 成都 贵阳 惠州 武汉 济南  肇庆 徐州 扬州）
#   宝鸡市
df_bj =  issue_offer[issue_offer['gd_city'] == '宝鸡市']
df_bj = df_bj[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url','building_code']].drop_duplicates()
df_bj[['room_sum']] = df_bj[['room_sum']].astype('int')
# df_bj_test = df_bj.groupby(['gd_city'])['room_sum'].sum().reset_index()
#   北京市
df_beij =  issue_offer[issue_offer['gd_city'] == '北京市']
df_beij = df_beij[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url','building_code']].drop_duplicates()
df_beij[['room_sum']] = df_beij[['room_sum']].astype('int')
#   长春市
df_cc =  issue_offer[issue_offer['gd_city'] == '长春市']
df_cc['room_sum'] = df_cc['room_sum'].apply(lambda x:re.sub("\D","",x)).astype('int')
df_cc = df_cc[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url','building_code']].drop_duplicates()
df_cc[['room_sum']] = df_cc[['room_sum']].astype('int')
#   广州市
df_gz =  issue_offer[issue_offer['gd_city'] == '广州市']
df_gz = df_gz[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url','building_code']].drop_duplicates()
df_gz = df_gz.loc[~df_gz['building_code'].str.contains("地下")]
df_gz[['room_sum']] = df_gz[['room_sum']].astype('int')
#   南京市
df_nj =  issue_offer[issue_offer['gd_city'] == '南京市']
df_nj['room_sum'] = df_nj['room_sum'].apply(lambda x:re.sub("\D","",x))
df_nj = df_nj[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url','building_code']].drop_duplicates()
df_nj[['room_sum']] = df_nj[['room_sum']].astype('int')
#   三亚市
# df_sy =  issue_offer[issue_offer['gd_city'] == '三亚市']
# df_sy['room_sum'] = df_sy['room_sum'].apply(lambda x:re.sub("\D","",x))
# df_sy = df_sy[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url','building_code']].drop_duplicates()
# df_sy[['room_sum']] = df_sy[['room_sum']].astype('int')
#   唐山市
df_ts =  issue_offer[issue_offer['gd_city'] == '唐山市']
df_ts = df_ts[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url','building_code']].drop_duplicates()
df_ts[['room_sum']] = df_ts[['room_sum']].astype('int')
#   中山市
df_zs =  issue_offer[issue_offer['gd_city'] == '中山市']
df_zs = df_zs[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url','building_code']].drop_duplicates()
df_zs[['room_sum']] = df_zs[['room_sum']].astype('int')
#   宁波市
df_nb =  issue_offer[issue_offer['gd_city'] == '宁波市']
df_nb = df_nb[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url','building_code']].drop_duplicates()
df_nb = df_nb.loc[~df_nb['building_code'].str.contains("地下")]
df_nb = df_nb.loc[~df_nb['building_code'].str.contains("商业")]
df_nb = df_nb.loc[~df_nb['building_code'].str.contains("办公")]
df_nb = df_nb.loc[~df_nb['building_code'].str.contains("商铺")]
df_nb[['room_sum']] = df_nb[['room_sum']].astype('int')
#   石家庄市
df_sjz =  issue_offer[issue_offer['gd_city'] == '石家庄市']
df_sjz = df_sjz[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url','building_code']].drop_duplicates()
df_sjz[['room_sum']] = df_sjz[['room_sum']].astype('int')
#   珠海市
df_zh =  issue_offer[issue_offer['gd_city'] == '珠海市']
df_zh = df_zh[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url','building_code']].drop_duplicates()
df_zh[['room_sum']] = df_zh[['room_sum']].astype('int')
#   徐州市
df_xz =  issue_offer[issue_offer['gd_city'] == '徐州市']
df_xz = df_xz[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url','building_code']].drop_duplicates()
df_xz[['room_sum']] = df_xz[['room_sum']].astype('int')
#   扬州市
df_yz =  issue_offer[issue_offer['gd_city'] == '扬州市']
df_yz = df_yz[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url','building_code','room_code']].drop_duplicates()
df_yz = df_yz.groupby(['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','issue_area','business','address','url','building_code'])['room_sum'].count().reset_index()
df_yz[['room_sum']] = df_yz[['room_sum']].astype('int')
df_yz = df_yz[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url','building_code']]
#   温州市
df_wz =  issue_offer[issue_offer['gd_city'] == '温州市']
df_wz = df_wz[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url','building_code']].drop_duplicates()
df_wz = df_wz.loc[~df_wz['building_code'].str.contains("地下")]
df_wz = df_wz.loc[~df_wz['building_code'].str.contains("商业")]
df_wz = df_wz.loc[~df_wz['building_code'].str.contains("办公")]
df_wz = df_wz.loc[~df_wz['building_code'].str.contains("商铺")]
df_wz = df_wz.loc[~df_wz['floor_name'].str.contains("商")]
df_wz[['room_sum']] = df_wz[['room_sum']].astype('int')
# df_wz.groupby(['gd_city'])['room_sum'].sum().reset_index()

#   咸阳市
df_xy =  issue_offer[issue_offer['gd_city'] == '咸阳市']
df_xy = df_xy[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url','building_code']].drop_duplicates()
df_xy = df_xy.loc[~df_xy['building_code'].str.contains("地下")]
df_xy = df_xy.loc[~df_xy['building_code'].str.contains("商业")]
df_xy = df_xy.loc[~df_xy['building_code'].str.contains("办公")]
df_xy = df_xy.loc[~df_xy['building_code'].str.contains("商铺")]
df_xy['room_sum'] = df_xy['room_sum'].astype('str').apply(lambda x:x.split('非住宅')[0])
df_xy['room_sum'] = df_xy['room_sum'].apply(lambda x:re.sub("\D","",x))
df_xy[['room_sum']] = df_xy[['room_sum']].astype('int')
# df_xy.groupby(['gd_city'])['room_sum'].sum().reset_index()


# In[8]:
# 统计房间数量算供应套数（西安）
#   西安市
# df_xa =  issue_offer[issue_offer['gd_city'] == '西安市']
# df_xa = df_xa.loc[~df_xa['building_code'].str.contains("地下")]
# df_xa = df_xa.loc[~df_xa['building_code'].str.contains("办公")]
# df_xa = df_xa.loc[~df_xa['building_code'].str.contains("综合")]
# df_xa = df_xa.loc[~df_xa['building_code'].str.contains("商业")]
# df_xa = df_xa.loc[~df_xa['building_code'].str.contains("商务")]
# df_xa = df_xa.loc[~df_xa['building_code'].str.contains("商住")]
# df_xa = df_xa.loc[~df_xa['building_code'].str.contains("中心")]
# df_xa = df_xa.loc[~df_xa['room_code'].str.contains("配套")]
# df_xa = df_xa.loc[~df_xa['room_code'].str.contains("物业")]
# df_xa = df_xa.loc[~df_xa['room_code'].isin(['','None'])]
# df_xa = df_xa[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','url','building_code','room_code']].drop_duplicates()
# df_xa = df_xa.groupby(['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','issue_area','business','address','url','building_code'])['room_sum'].count().reset_index()
# df_xa[['room_sum']] = df_xa[['room_sum']].astype('int')
# df_xa_test = df_xa.groupby(['gd_city'])['room_sum'].sum().reset_index()


# result = df_sz.append(df_jx,ignore_index=True)
# result = result.append(df_nn,ignore_index=True)
# result = result.append(df_fs,ignore_index=True)
# result = result.append(df_wx,ignore_index=True)
# result = result.append(df_cc,ignore_index=True)
# result = result.append(df_cd,ignore_index=True)
# result = result.append(df_gy,ignore_index=True)
# result = result.append(df_hz,ignore_index=True)
# result = result.append(df_wh,ignore_index=True)
# result = result.append(df_jn,ignore_index=True)
# result = result.append(df_jj,ignore_index=True)
# result = result.append(df_bj,ignore_index=True)
# result = result.append(df_bd,ignore_index=True)
# result = result.append(df_beij,ignore_index=True)
# result = result.append(df_gz,ignore_index=True)
# result = result.append(df_cs,ignore_index=True)
# result = result.append(df_dg,ignore_index=True)
# result = result.append(df_hk,ignore_index=True)
# result = result.append(df_hf,ignore_index=True)
# result = result.append(df_nj,ignore_index=True)
# result = result.append(df_qd,ignore_index=True)
# result = result.append(df_sy,ignore_index=True)
# result = result.append(df_sheny,ignore_index=True)
# result = result.append(df_ts,ignore_index=True)
# result = result.append(df_xm,ignore_index=True)
# result = result.append(df_zq,ignore_index=True)
# result = result.append(df_zs,ignore_index=True)
# result = result.append(df_zb,ignore_index=True)
# result = result.append(df_nb,ignore_index=True)
# result = result.append(df_zh,ignore_index=True)
# result = result.append(df_xz,ignore_index=True)
# result = result.append(df_yz,ignore_index=True)
# result = result.groupby(['gd_city'])['room_sum'].sum().reset_index()


# In[9]:
###合并结果集
# 深圳 佛山 嘉兴 南宁 九江 保定 长沙 东莞 海口 合肥 青岛 沈阳 厦门 淄博 无锡市 成都 贵阳 惠州 武汉 济南  肇庆  福州
reult1 = pd.concat([df_zq,df_jn,df_wh,df_hz,df_gy,df_cd,df_wx,df_zb,df_xm,df_sheny,df_qd,df_hf,df_hk,df_dg,df_cs,df_bd,df_jj,df_fs,df_nn,df_jx,df_sz,df_fz,df_sh])
# 宝鸡 北京 长春 广州 南京 三亚 唐山 中山 宁波 石家庄 珠海 徐州 扬州
# reult2 = pd.concat([df_xy,df_wz,df_yz,df_xz,df_zh,df_sjz,df_nb,df_zs,df_ts,df_sy,df_nj,df_gz,df_cc,df_beij,df_bj,])
reult2 = pd.concat([df_xy,df_wz,df_yz,df_xz,df_zh,df_sjz,df_nb,df_zs,df_ts,df_nj,df_gz,df_cc,df_beij,df_bj,])
reult2 = reult2.groupby(['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','issue_area','business','address','url'])['room_sum'].sum().reset_index()
# 合并
result = reult1.append(reult2,ignore_index=True)
# 更新标识
result['dr'] = 0
result['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
result['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
# reult = reult.groupby(['gd_city'])['room_sum'].sum().reset_index()
# 格式化列
result = result[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','dr','create_time','update_time','url']]
result.columns = ['city','newest_name','issue_code','issue_date','issue_month','issue_quarter','issue_room','issue_area','business','address','dr','create_time','update_time','newest_url']
# 格式化面积
result['issue_area'] = result['issue_area'].astype('str').apply(lambda x:x.split('.')[0])
result['issue_area'] = result['issue_area'].apply(lambda x:re.sub("\D","",x))
result['newest_id'] = '待定'
result['formal_newest_name'] = '待定'
result['alias_name'] = '待定'
result = result[['newest_id','newest_name','address','business','formal_newest_name','alias_name','city','issue_code','issue_date','issue_month','issue_quarter','issue_room','issue_area','dr','create_time','update_time','newest_url']]
result.columns = ['newest_id','newest_name','address','developer','formal_newest_name','alias_name','city','issue_code','issue_date','issue_month','issue_quarter','issue_room','issue_area','dr','create_time','update_time','newest_url']

# In[1000000]:
# 手动插入缺失数据
# result = df_fz
# # 更新标识
# result['dr'] = 0
# result['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
# result['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
# # reult = reult.groupby(['gd_city'])['room_sum'].sum().reset_index()
# # 格式化列
# result = result[['gd_city','floor_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','business','address','dr','create_time','update_time','url']]
# result.columns = ['city','newest_name','issue_code','issue_date','issue_month','issue_quarter','issue_room','issue_area','business','address','dr','create_time','update_time','newest_url']
# # 格式化面积
# result['issue_area'] = result['issue_area'].astype('str').apply(lambda x:x.split('.')[0])
# result['issue_area'] = result['issue_area'].apply(lambda x:re.sub("\D","",x))
# result = result[result['city']=='沈阳市']
# result = result[result['city']== '咸阳市']

# In[10]:
# 加载到新表 dwb_newest_issue_offer
# result.drop_duplicates(inplace=True)
to_dws(result,table_name)
# df_wz.to_csv('C:\\Users\\86133\\Desktop\\df_wz.csv')
# result
print('>>>>>>>Done')

