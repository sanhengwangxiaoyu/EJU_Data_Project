
#%%
from cgi import print_environ_usage
from gettext import find
from json.tool import main
from tkinter import N, Y
from unittest import result
from pandas.core.frame import DataFrame
from sqlalchemy import create_engine
from threading import Thread
import sys,pandas as pd,numpy as np,os,pymysql,configparser,json,datetime,re



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
    engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database+'?charset=utf8')
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)

def str_type_splits(s,i):
    if '=' in s:
        if i == 1:
            return float(s.split('=')[i])
        else :
             return s.split('=')[i]
    elif len(re.findall(r'\(\d{1,3}.\d{1,20}, \d{1,3}.\d{1,20}\)',s)) >0 and i == 1:
        return re.findall(r'\(\d{1,3}.\d{1,20}, \d{1,3}.\d{1,20}\)',s)[0]
    elif len(re.findall(r'\(\d{1,3}.\d{1,20}, \d{1,3}.\d{1,20}\)',s)) >0 and i == 0:
            return re.sub(r'\(\d{1,3}.\d{1,20}, \d{1,3}.\d{1,20}\)','',s)
    else :
        if i == 0:
            return s
        else :
            return np.nan

#In[]

#################################################################################
#################################################################################
#################################################################################
#################################################################################
#################################################################################
# # 群体标签拆解
# data = con.query("select order_id,tag_id,jdata from origin_estate.ori_jiguang_group_tag_i where dr = 0 and period = '2021Q4' ")
data = con.query("select order_id,tag_id,jdata from origin_estate.ori_jike_group_tag_i where dr = 0 and period = '2021Q4'")
tag_name = con.query("select tag_id,tag_type,tag_name,tag_en_name from origin_estate.acq_jike_tag where dr =0 and tag_clazz in ('G','H') ")
#字符串去空格
data['tag_id'] = data['tag_id'].apply(lambda x: x.strip())
tag_name['tag_id'] = tag_name['tag_id'].apply(lambda x: x.strip())


#In[]
####群体标签未key=value的方式，取ley
#先将数据转换出来
df_data_standby = pd.merge(data,tag_name,how='left',on=['tag_id'])  #获取名字
# #一行转多行
#切分数组
df_data_standby = df_data_standby.drop('jdata',axis=1).join(df_data_standby['jdata'].str.split('}, {',expand=True).stack().reset_index(level=1,drop=True).rename('jdata')).reset_index(drop=True)
#标准化数据
df_data_standby['jdata'] = df_data_standby['jdata'].apply(lambda x :re.sub(r'\{|\}|\[|\]','',x)) #同意数据格式
### 将单行和多行标签值分开
df_data = df_data_standby.groupby(['order_id','tag_id'])['jdata'].count().reset_index().sort_values('jdata') 
df_data.rename(columns={'jdata':'jdata_num'},inplace=True)
df_data = pd.merge(df_data,df_data_standby,how='inner',on=['order_id','tag_id']) #将数据合并到一起



#In[]
#需要拆多行的数据
df_data_a = df_data[df_data['jdata_num'] == 1]  ##筛选
df_data_a['jdata'] = df_data_a.apply(lambda x: x.jdata.split('), ') if x.tag_id == 'J00107_G' else x.jdata.split(', ') ,axis=1) #字符串转换为列表
# df_data_a['data'] = df_data_a['jdata'].apply(lambda x:len(x)) # 标签的数量
df_data_a = df_data_a.explode('jdata').drop_duplicates().reset_index(drop=True) #列表爆炸开
df_data_a['jdata'] = df_data_a.apply(lambda x: str(x.jdata)+')' if x.tag_id == 'J00107_G' else x.jdata ,axis=1)
df_data_a['data'] = df_data_a['jdata'].apply(lambda x: str_type_splits(x,1))
df_data_a['jdata'] = df_data_a['jdata'].apply(lambda x: str_type_splits(x,0))
df_data_a['period'] = '2021Q4'
df_data_a['city_name'] = np.nan
df_data_a['lng_lat'] = df_data_a['data'].apply(lambda x: re.sub(r'\(|\)','',x) if ',' in str(x) else np.nan)
df_data_a.rename(columns={'jdata':'index','order_id':'house_id'},inplace=True)
df_data_a


#In[]
df_data_c = df_data[df_data['jdata_num'] != 1].reset_index()  ##筛选
df_data_c['jdata'] = df_data_c['jdata'].apply(lambda x: '{"'+x.replace('=','" : "').replace(', ','", "')+'"}')
df_data_c['jdata'] = df_data_c['jdata'].map(lambda x: eval(x))
df_data_c[['aa','bb','cc','dd','poi_id','a','b','c','d','e','f','j','h','i','g']] = pd.DataFrame(df_data_c['jdata'].values.tolist())
df_data_c.at[df_data_c['d'].isna(),'d']=df_data_c['i']
df_data_c.at[df_data_c['a'].isna(),'a']=df_data_c['f']
df_data_c.at[df_data_c['b'].isna(),'b']=df_data_c['j']
df_data_c.at[df_data_c['c'].isna(),'c']=df_data_c['h']
df_data_c.at[df_data_c['e'].isna(),'e']=df_data_c['g']
df_data_c.at[df_data_c['e'].isna(),'e']=df_data_c['dd']
df_data_c['index'] = df_data_c['d'].astype(str)+'$$'+df_data_c['a'].astype(str)+'$$'+df_data_c['e'].astype(str)+'$$'+df_data_c['c'].astype(str)
df_data_c.rename(columns={'b':'data','order_id':'house_id','d':'city_name','e':'lng_lat'},inplace=True)
df_data_c['period'] = '2021Q4'
###计算占比
df_data_c.at[df_data_c['aa'].isna(),'aa']=0
df_data_c['aa'] = df_data_c[['aa']].astype(float).astype(int)
df_data_c2 = df_data_c.groupby(['tag_id'])['aa'].sum().reset_index()
df_data_c2.rename(columns={'aa':'data_sum'},inplace=True)
df_data_c2 = df_data_c2[df_data_c2['data_sum']!=0]
df_data_c = pd.merge(df_data_c,df_data_c2,how='left',on=['tag_id'])
df_data_c['rate'] = df_data_c.apply(lambda x: '%.2f' % float(x.aa/x.data_sum),axis=1)
df_data_c.at[df_data_c['rate']>'0.0','data']=df_data_c['rate']
df_data_c.at[df_data_c['rate']>'0.0','index']=df_data_c['cc']
result = df_data_a[['index','data','tag_name','house_id','period','city_name','lng_lat']].append(df_data_c[['index','data','tag_name','house_id','period','city_name','lng_lat']],ignore_index=False)
result['create_time'] = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d')
result['dr'] = 0
result['data'] = result['data'].apply(lambda x:re.sub(r'%','',str(x)) if re.sub(r'%','',str(x)) != 'nan' else x)
# result.at[result['data']=='nan','data'] = np.nan
result


##科学计数法和nan
#In[]
database = 'dwd_db'
result
to_dws(result,'dwd_customer_group_tag_20220315')


#In[]
# pd.DataFrame(df_data_c['jdata'].values.tolist()).to_csv(r'C:\Users\86133\Desktop\result.csv')
