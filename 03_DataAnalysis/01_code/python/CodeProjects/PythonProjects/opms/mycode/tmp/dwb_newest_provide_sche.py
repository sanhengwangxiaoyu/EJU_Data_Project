# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Jul 12 17:44:47 2021
  清洗动态信息
"""
import configparser
import os
import sys
import pymysql
import pandas as pd
import time
from sqlalchemy import create_engine
import getopt
import re

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
table_name = 'ori_newest_provide_sche' # 要插入的表名称
database = 'odsdb'

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
#创建数据库连接
con = MysqlClient(db_host,database,user,password)


# In[4]:
# ori_newest_provide_sche 楼盘动态原始信息表
# 数据网址	url || uuid
# 楼盘名	newest_name
# tag标签	sche_tag || 周期
# title标题	provide_title
# date发布时间	provide_date
# content内容	provide_sche
# 时间清洗结果	date_clean
# dim_housing主键 housing_id 
sche=con.query("select url,newest_name,sche_tag,provide_title,provide_date,provide_sche,date_clean,create_time,update_time from odsdb.ori_newest_provide_sche")


# In[5]:
# dws_newest_info   新房楼盘
#     楼盘id	newest_id
#     楼盘名称	newest_name
#     楼盘别名	alias_name
newest_id=con.query("select url,uuid,max(id) housing_id from odsdb.ori_newest_info_base where remark not like '%删除%' or remark is null or flag is not null group by url,uuid having count(1) = 1")
newest_id.drop_duplicates(inplace=True)


# In[9]:
result = pd.merge(sche,newest_id,how='left',on=['url'])
result.at[result['url'] == "https://newhouse.fang.com/loupan/1010767645.htm", 'uuid'] = '1f841150b19cffecd20eef967b9af891'
result.at[result['url'] == "https://esf.fang.com/loupan/1010665775.htm", 'uuid'] = '03bfdba3d9d1462a36ac7962c79c2b37'
result.at[result['url'] == "https://bj.fang.ke.com/loupan/p_rzyfaadaq/", 'uuid'] = '13b7809cc0fe949888a23e17ef00761b'
result.at[result['url'] == "https://bj.fang.ke.com/loupan/p_zhfdggbmbjq/", 'uuid'] = '18580d45e65e015859b24119a906bbb5'
result.at[result['url'] == "http://bj.jiwu.com/loupan/959109.html", 'uuid'] = '1ec584adb374e21f18edc17cbfaa1de7'
result.at[result['url'] == "https://newhouse.fang.com/loupan/1010123313.htm", 'uuid'] = '2662be9c5f20cfcc2c3820251feae93c'
result.at[result['url'] == "//cs.newhouse.fang.com/loupan/2710189802.htm", 'uuid'] = 'd5d0d13ba965c11193336a93c1992720'
result.at[result['url'] == "//jining.newhouse.fang.com/loupan/2418171159.htm", 'uuid'] = 'ae9fcdfe22751a9df46fd85212e8e4a4'
result.at[result['url'] == "//cd.newhouse.fang.com/loupan/3211156112.htm", 'uuid'] = '67730c1e1c1fae40d5d7aeceae4fd473'
result.at[result['url'] == "//cq.newhouse.fang.com/loupan/3110123416.htm", 'uuid'] = '8ce25aa3ec1d17e5046ab634a2b31c3c'
result.at[result['url'] == "https://hz.fang.ke.com/loupan/p_jhysjnbkvgm/", 'uuid'] = '0c24777de25d65d6f82532e6847bc2ec'
result.at[result['url'] == "https://sz.julive.com/project/20049202.html#around_map", 'uuid'] = '017b370630aa4b9db1ce79a40af97318'
result.at[result['url'] == "https://sz.julive.com/project/20078961.html#around_map", 'uuid'] = '0354fdcd20c71b429791aeaadc5dfb50'
result.at[result['url'] == "//zz.newhouse.fang.com/loupan/2510148771.htm", 'uuid'] = '0645e793cbf3f0c189d99233de7962fa'
result.at[result['url'] == "//cq.newhouse.fang.com/loupan/3110123954.htm", 'uuid'] = '07efee8c665444cb0c2a49a38e4d9cf4'
result.at[result['url'] == "https://km.fang.ke.com/loupan/p_bgykynyxablyp/", 'uuid'] = '08d2e1aae71f9c17f70dd54978289fbd'
result.at[result['url'] == "https://hz.fang.ke.com/loupan/p_gkdfjcxfbkqva/", 'uuid'] = '0911f303d1e8f7a6159b5b536c8b2804'
result.at[result['url'] == "http://km.jiwu.com/loupan/586987.html", 'uuid'] = '12dad824cec29f5cd960a2f37b5a2d91'
result.at[result['url'] == "https://fz.fang.ke.com/loupan/p_tjhfbkmnv/", 'uuid'] = '13712ea97b7ab0cefd6c65113ea4dd79'
result.at[result['url'] == "//cs.newhouse.fang.com/loupan/2710190536.htm", 'uuid'] = '162d2bef1d825c5a67c30025003c4fd3'
result.at[result['url'] == "https://sz.julive.com/project/20041308.html#around_map", 'uuid'] = '183d8341624f7b7482f1c7284867bd2b'
result.at[result['url'] == "https://sz.julive.com/project/20049202.html#around_map", 'uuid'] = '1b33a26dae6d94354e32d66ffcc43242'
result.at[result['url'] == "https://hf.fang.ke.com/loupan/p_mmsfabcnp/", 'uuid'] = '1bb30f0ae747434fb223747fed81063c'
result.at[result['url'] == "https://sz.julive.com/project/20038194.html#around_map", 'uuid'] = '1e2c8343e9648e7739b3ce1f2197e023'
result.at[result['url'] == "https://xianyang.julive.com/project/20053580.html#around_map", 'uuid'] = '1f6c27c9ce456562fff05acd3349804f'
result.at[result['url'] == "//cq.newhouse.fang.com/loupan/3110123954.htm", 'uuid'] = '227a152d6b1b675a266677184e5c092b'
result.at[result['url'] == "https://km.fang.ke.com/loupan/p_bgykynyxablyp/", 'uuid'] = '23e910d3be06b8fb39f84c3d96362e25'
result.at[result['url'] == "https://wh.fang.ke.com/loupan/p_wthgjsqblkbe/", 'uuid'] = '25fd9b1a0662cc83e466882c4c16c090'
result.at[result['url'] == "//hf.newhouse.fang.com/loupan/2111193920.htm", 'uuid'] = '290935128823c3aabfdae33f34a07b0d'
result.at[result['url'] == "https://cq.fang.ke.com/loupan/p_hrxdcablas/", 'uuid'] = '2cb0482be207ad789ded14760c2f3f3e'
result.at[result['url'] == "https://cq.fang.ke.com/loupan/p_bdzyhyfabhan/", 'uuid'] = '308f1a7de173af537f84c2fe37515483'
result.at[result['url'] == "https://xm.fang.ke.com/loupan/p_thsmxyzabkvl/", 'uuid'] = '3b6a819a5ba21b27f91991ef11bd6411'
result.at[result['url'] == "https://sz.julive.com/project/20039666.html#around_map", 'uuid'] = '3d78ffd0f00a602bd7d6caeb1e260da4'
result.at[result['url'] == "https://sz.julive.com/project/20039313.html#around_map", 'uuid'] = '3ed496cffdc1bb27fee072aabfc78750'
result.at[result['url'] == "https://xianyang.julive.com/project/20048568.html#around_map", 'uuid'] = '40054f061b0988b5bc4c043a1e3bd358'
result.at[result['url'] == "//suzhou.newhouse.fang.com/loupan/1822201096.htm", 'uuid'] = '443d6828eac5e8d198f8b64dfc069889'
result.at[result['url'] == "https://cs.fang.ke.com/loupan/p_aajvp/", 'uuid'] = '44c0c7337ccd49368a61d7d05b564814'
result.at[result['url'] == "https://fs.fang.ke.com/loupan/p_bgyfthwabhbu/", 'uuid'] = '485c1c22ed2ddda1dae458839948f873'
result.at[result['url'] == "https://cq.fang.ke.com/loupan/p_hrxdcablas/", 'uuid'] = '49f508b5c1725624fd510ec11c5c3839'
result.at[result['url'] == "//xm.newhouse.fang.com/loupan/2212125884.htm", 'uuid'] = '50adec1344b8639c0a6818dae343ca3c'
result.at[result['url'] == "https://sz.julive.com/project/20040893.html#around_map", 'uuid'] = '5112b84bf334fa55e7c452b4ae1c620d'
result.at[result['url'] == "https://dg.fang.ke.com/loupan/p_jdcnyjaazst/", 'uuid'] = '5263a8a8e4f898f79b6b339a8e8d817d'
result.at[result['url'] == "https://sz.fang.ke.com/loupan/p_xsdggbljhe/", 'uuid'] = '5625b8d8229a2cfe587569098f70b720'
result.at[result['url'] == "https://xm.fang.ke.com/loupan/p_thsmxyzabkvl/", 'uuid'] = '5e4196c416b0eb0b558b9eec719ff60e'
result.at[result['url'] == "//cs.newhouse.fang.com/loupan/2710190256.htm", 'uuid'] = '5e52db09ec0149f74fe462e1bf09bf95'
result.at[result['url'] == "https://sz.julive.com/project/20039587.html#around_map", 'uuid'] = '5e690831186dbb2d3ac5418dcd108fb1'
result.at[result['url'] == "https://zz.fang.ke.com/loupan/p_gljbjuuz/", 'uuid'] = '5e6aa3663444a6b519cfb85667b52c5e'
result.at[result['url'] == "https://sz.fang.ke.com/loupan/p_xsdggbljhe/", 'uuid'] = '5ebddef1af245117fcaf517afbea0289'
result.at[result['url'] == "https://sz.julive.com/project/20049244.html#around_map", 'uuid'] = '60ec83daf8b6ad8cf26e59cdfa123a59'
result.at[result['url'] == "https://fz.fang.ke.com/loupan/p_tjhfbkmnv/", 'uuid'] = '65b5a9f1f100e76e7d37c12c27de7aff'
result.at[result['url'] == "//cs.newhouse.fang.com/loupan/2710189578.htm", 'uuid'] = '6626a8fee4bfa203192cb5d307b5bc33'
result.at[result['url'] == "https://xianyang.julive.com/project/20048572.html#around_map", 'uuid'] = '70be5a2b575c90c037e1fba6ff2f4993'
result.at[result['url'] == "https://qd.fang.ke.com/loupan/p_hecccdfxfblode/", 'uuid'] = '70c06fa6f3199862f4759c8f849fcaa6'
result.at[result['url'] == "https://dg.fang.ke.com/loupan/p_jdcnyjaazst/", 'uuid'] = '7621ebf363545b4870e210abd2c5f3d0'
result.at[result['url'] == "//cq.newhouse.fang.com/loupan/3110123590.htm", 'uuid'] = '76448b96c8de6bcc0744e6c10f9fcb87'
result.at[result['url'] == "https://sz.fang.ke.com/loupan/p_hryhlsababo/", 'uuid'] = '764ae7e58031af24022809918963ab1a'
result.at[result['url'] == "https://su.fang.ke.com/loupan/p_hrjywablmv/", 'uuid'] = '7a5796819f398dcb1cdf25979edb16a4'
result.at[result['url'] == "https://su.julive.com/project/20124536.html#around_map", 'uuid'] = '7da440b62ac8d4ff19df606f51370b3c'
result.at[result['url'] == "//zz.newhouse.fang.com/loupan/2510148771.htm", 'uuid'] = '7f0e99db2cdef6fe1296968850ec1650'
result.at[result['url'] == "//hf.newhouse.fang.com/loupan/2110177976.htm", 'uuid'] = '802fb260536d182f3806cc3d3ef5fd66'
result.at[result['url'] == "https://fs.fang.ke.com/loupan/p_bgyfthwabhbu/", 'uuid'] = '8525bc570eca9ddad21730d15a10b257'
result.at[result['url'] == "//xm.newhouse.fang.com/loupan/2212125884.htm", 'uuid'] = '85436db2c9081ac4190fe145727bb69a'
result.at[result['url'] == "https://cq.fang.ke.com/loupan/p_ldxlyyabkab/", 'uuid'] = '89169a854961aa1e7889fdcd8351f2c6'
result.at[result['url'] == "https://su.fang.ke.com/loupan/p_szyhggblcqt/", 'uuid'] = '8babc440acaa118ffa2a74571d61f5f9'
result.at[result['url'] == "http://km.jiwu.com/loupan/586987.html", 'uuid'] = '8c7b021a041f973dacd564de233e28b9'
result.at[result['url'] == "//cq.newhouse.fang.com/loupan/3110123590.htm", 'uuid'] = '8e4fec7834e9942b8583a6aa6dc990e9'
result.at[result['url'] == "https://cq.fang.ke.com/loupan/p_ldxlyyabkab/", 'uuid'] = '9049aa9915cf87dace1f950a2dc44329'
result.at[result['url'] == "https://tj.fang.ke.com/loupan/p_aaizl/", 'uuid'] = '94ba24396c97a2de200fde89433cde03'
result.at[result['url'] == "https://cq.fang.ke.com/loupan/p_zsyjfbjezl/", 'uuid'] = '950001e1c61dacb4ff4cb541980a21b7'
result.at[result['url'] == "//cs.newhouse.fang.com/loupan/2710189578.htm", 'uuid'] = '9a99c6c4625c7a9c948596e31ac14578'
result.at[result['url'] == "https://hk.fang.ke.com/loupan/p_bhbsbcmrw/", 'uuid'] = '9b94958d625b37d4e8c17766ddf65fe1'
result.at[result['url'] == "https://cs.fang.ke.com/loupan/p_aajvp/", 'uuid'] = '9e62c61875b690e138c746adcc7b3ffb'
result.at[result['url'] == "https://zz.fang.ke.com/loupan/p_gljbjuuz/", 'uuid'] = 'a0d0fc17ad7264d60e807aa688346587'
result.at[result['url'] == "//nn.newhouse.fang.com/loupan/2910196244.htm", 'uuid'] = 'a5db80ea474d696bbd1be3ce9dd8a6ef'
result.at[result['url'] == "https://hk.fang.ke.com/loupan/p_bhbsbcmrw/", 'uuid'] = 'a66efa04c24374ea80c6f25db990cff1'
result.at[result['url'] == "//suzhou.newhouse.fang.com/loupan/1822201096.htm", 'uuid'] = 'a7c84438c1cee5e1b3025216dc6ed62d'
result.at[result['url'] == "https://su.julive.com/project/20124536.html#around_map", 'uuid'] = 'a8e59fb072273d2930b4811a300c578f'
result.at[result['url'] == "//cq.newhouse.fang.com/loupan/3110123904.htm", 'uuid'] = 'aa5ede96a53c3eace9acc245062d5478'
result.at[result['url'] == "https://sh.fang.ke.com/loupan/p_rclgyhbjbaw/", 'uuid'] = 'aa878de731e7aebc9aaf5a89ee224f99'
result.at[result['url'] == "https://su.fang.ke.com/loupan/p_szyhggblcqt/", 'uuid'] = 'aaac24b377f52f121c8a239a0066ded7'
result.at[result['url'] == "//suzhou.newhouse.fang.com/loupan/1822201868.htm", 'uuid'] = 'aea4ca6e3647ba9427193fc044d9efd8'
result.at[result['url'] == "https://cq.fang.ke.com/loupan/p_zsyjfbjezl/", 'uuid'] = 'b194c9097464782a92ec7593e5b6c35b'
result.at[result['url'] == "https://xianyang.julive.com/project/20053577.html#around_map", 'uuid'] = 'b3be01421baaf90e4cd73a4150e12268'
result.at[result['url'] == "https://cq.fang.ke.com/loupan/p_bjcjyxtabikx/", 'uuid'] = 'b61ec98e903ca4ff5ff157f6df16e10c'
result.at[result['url'] == "https://bj.fang.ke.com/loupan/p_gcbsjafmee/", 'uuid'] = 'b855eb57c7e7b88596371f4a6096bca9'
result.at[result['url'] == "https://sh.fang.ke.com/loupan/p_rclgyhbjbaw/", 'uuid'] = 'c1d1b310515aea1c65e7715c7d552aa5'
result.at[result['url'] == "https://bj.fang.ke.com/loupan/p_gcbsjafmee/", 'uuid'] = 'c3d483d8c7bd8065dc236b143bc6c4aa'
result.at[result['url'] == "https://hz.fang.ke.com/loupan/p_rcwfzcabqyo/", 'uuid'] = 'cada9377cc6c3be8fea4408e4cacf8f2'
result.at[result['url'] == "//hf.newhouse.fang.com/loupan/2110177976.htm", 'uuid'] = 'cb8b9d1009b6983e005d106da722a173'
result.at[result['url'] == "//cq.newhouse.fang.com/loupan/3110123904.htm", 'uuid'] = 'd1ff6afe6bad2291ea897c71c763e2ea'
result.at[result['url'] == "https://sz.julive.com/project/20039666.html#around_map", 'uuid'] = 'd53a49f0fb0df770e9464b3adce5c0cc'
result.at[result['url'] == "https://hz.fang.ke.com/loupan/p_rcwfzcabqyo/", 'uuid'] = 'd5bb797ab879688a2e73ca4005090a2d'
result.at[result['url'] == "https://qd.fang.ke.com/loupan/p_ydxfabbxb/", 'uuid'] = 'd62e2297919e83394091735d1e847cb6'
result.at[result['url'] == "https://hf.fang.ke.com/loupan/p_mmsfabcnp/", 'uuid'] = 'e22549604b8ec94c69d730a08987d381'
result.at[result['url'] == "https://hz.fang.ke.com/loupan/p_gkdfjcxfbkqva/", 'uuid'] = 'e49dd39ff74500fd146bfda924b06105'
result.at[result['url'] == "https://sz.julive.com/project/20122285.html#around_map", 'uuid'] = 'e59c34fb2ec347283eddf8eeecebf7c1'
result.at[result['url'] == "//suzhou.newhouse.fang.com/loupan/1822201868.htm", 'uuid'] = 'ead4bc2a9020a2b513f97d9af5e190a8'
result.at[result['url'] == "//cs.newhouse.fang.com/loupan/2710190536.htm", 'uuid'] = 'ebf9c30d89e41bce406cb520c2cc1490'
result.at[result['url'] == "//hf.newhouse.fang.com/loupan/2111193920.htm", 'uuid'] = 'ec8e39a7a01f444223b94823c44205ae'
result.at[result['url'] == "https://cq.fang.ke.com/loupan/p_bjcjyxtabikx/", 'uuid'] = 'ecdd9b432800b4e4ad53194c7417f737'
result.at[result['url'] == "https://sz.fang.ke.com/loupan/p_hryhlsababo/", 'uuid'] = 'f12320ed71ba0006c7e31aa8bbd9c7c0'
result.at[result['url'] == "https://tj.fang.ke.com/loupan/p_aaizl/", 'uuid'] = 'f4be3215eb194b6f4d3bc22b54470051'
result.at[result['url'] == "https://xianyang.julive.com/project/20048550.html#around_map", 'uuid'] = 'f6f5fa2b6978ef996bf1dc9c6dc45462'
result.at[result['url'] == "https://cq.fang.ke.com/loupan/p_bdzyhyfabhan/", 'uuid'] = 'f83e8b875bccee1d931aab0f3ce18b52'
result.at[result['url'] == "https://sz.julive.com/project/20040877.html#around_map", 'uuid'] = 'f852610e161e8407da1f2b74c9147adc'
result.at[result['url'] == "https://sz.julive.com/project/20041444.html#around_map", 'uuid'] = 'f8d81588650245c769848be40b042ba4'
result.at[result['url'] == "https://su.fang.ke.com/loupan/p_hrjywablmv/", 'uuid'] = 'fae6eaf4293c207b79635a8df667d593'
result.at[result['url'] == "https://qd.fang.ke.com/loupan/p_ydxfabbxb/", 'uuid'] = 'fb53eca6ba34e2cf68d18faebdbaf216'
result.at[result['url'] == "//cs.newhouse.fang.com/loupan/2710190256.htm", 'uuid'] = 'fdacf7b786ed75f49bbc1664ff9a4c66'

result = result[['uuid','newest_name','sche_tag','provide_title','provide_date','provide_sche','date_clean','create_time','update_time','url']]

result.columns = ['url','newest_name','sche_tag','provide_title','provide_date','provide_sche','date_clean','create_time','update_time','housing_id']


# In[]

#数据加载到mysql表里去
to_dws(result,'ori_newest_provide_sche')


# In[11]:
# sche.to_csv('C:\\Users\\86133\\Desktop\\sche.csv')

