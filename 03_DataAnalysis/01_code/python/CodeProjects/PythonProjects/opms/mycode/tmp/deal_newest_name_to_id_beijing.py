# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Seq 08 15:44:47 2021
  预售证楼盘和现有楼盘对照

"""
import configparser,os,pymysql,pandas as pd,re,time
from sqlalchemy import create_engine
from difflib import SequenceMatcher#导入库
import numpy as np

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
cityid = '110000'
cityname = '北京'
date_quarter = '2018Q1'   # 季度
table_name = 'dwb_newest_issue_offer' # 要插入的表名称
database = 'dwb_db'

##重置时间格式
start_date = str(pd.to_datetime(date_quarter))[0:10]   #截取成yyyy-MM-dd
end_date =  str(pd.to_datetime(date_quarter) + pd.offsets.QuarterEnd(0))[0:10]      #截取成yyyy-MM-dd


# In[3]:
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

def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()#引用ratio方法，返回序列相似性的度量
con = MysqlClient(db_host,database,user,password)


# In[4]:
# newest_deal=con.query("select floor_name newest_name,address,business developer from odsdb.city_newest_deal where city_name = '三亚' and issue_date_clean >= '2018-01-01' group by floor_name,address,business")
# 获取当前城市的所有预售证信息
issue_offer=con.query("select url ,gd_city ,floor_name newest_name,business developer,address ,issue_code , issue_date_clean ,replace(substr(issue_date_clean,1,7),'-','') issue_month , '"+date_quarter+"' issue_quarter ,case when room_sum is null then 0 when room_sum = '' then 0 when room_sum = 'None' then 0 else room_sum end room_sum,building_code ,case when issue_area = 'None' then 0 when issue_area = '' then 0 else substr(issue_area,1,8) end issue_area ,room_code from odsdb.city_newest_deal where issue_code is not null and issue_code != '' and issue_date_clean between '"+start_date+"' and '"+end_date+"' and city_name = '"+cityname+"'")

# 获取最全的楼盘名和楼盘id
newest_name=con.query("select tt1.*,tt2.developer,tt2.newest_name formal_newest_name,tt2.alias_name from (select newest_id,alias newest_name,address from (select min(id) housing_id,uuid newest_id,newest_name,address,developer from dwb_db.dim_housing where city_id = '"+cityid+"' and uuid is not null group by uuid,newest_name,address,developer) t1 left join (select housing_id,alias from dwb_db.dim_housing_alias) t2 on t1.housing_id=t2.housing_id union select t1.newest_id,case when alias_name is null then t1.newest_name else alias_name end newest_name,address from (select newest_id,newest_name,address from dws_db_prd.dws_newest_info where city_id = '"+cityid+"' and newest_id is not null group by newest_id,newest_name,address) t1 left join (select newest_id,alias_name from dws_db_prd.dws_newest_alias) t2 on t1.newest_id=t2.newest_id ) tt1 left join (select a1.*,b1.newest_name,b1.alias_name from (select uuid newest_id,developer from dwb_db.dim_housing where city_id = '"+cityid+"' and uuid is not null group by uuid,developer) a1 left join (select newest_id,newest_name,alias_name from dws_db_prd.dws_newest_info where dr = 0 and city_id = '"+cityid+"' and newest_id is not null group by newest_id,newest_name,alias_name) b1 on a1.newest_id = b1.newest_id) tt2 on tt1.newest_id = tt2.newest_id")


# In[]:
newest_deal = issue_offer[['newest_name','address','developer']].drop_duplicates(inplace=False)
###    直接根据楼盘名和公司关联上的
df_1 = pd.merge(newest_deal,newest_name,how='inner',on=['newest_name','developer'])
df_1['address'] = df_1['address_x'].apply(lambda x:  df_1['address_y'] if (x is None or x == '')  else x)
###    直接根据楼盘名关联上的
df_2_deal = newest_deal[~newest_deal['newest_name'].isin(df_1['newest_name'])]
df_2_name = newest_name[~newest_name['newest_id'].isin(df_1['newest_id'])]
df_2 = pd.merge(df_2_deal,df_2_name,how='inner',on=['newest_name'])
df_2['address'] = df_2['address_x'].apply(lambda x:  df_2['address_y'] if (x is None or x == '')  else x)
df_2['developer'] = df_2['developer_x'].apply(lambda x:  df_2['developer_y'] if (x is None or x == '')  else x)
###    直接根据公司名关联上的
df_3_deal = df_2_deal[~df_2_deal['newest_name'].isin(df_2['newest_name'])]
df_3_name = df_2_name[~df_2_name['newest_id'].isin(df_2['newest_id'])]
df_3 = pd.merge(df_3_deal,df_3_name,how='inner',on=['developer'])
#计算楼盘名字的匹配率
df_3['name_com_rate'] = df_3.apply(lambda x:similarity(x.newest_name_x,x.newest_name_y),axis=1)
df_3 = df_3[df_3['name_com_rate']>0.2]
#获取每个楼盘名字最大的匹配率
df_3_max_rate = df_3.groupby(['newest_name_x','address_x','developer'])['name_com_rate'].max().reset_index()
#获取结果
df_3 = pd.merge(df_3,df_3_max_rate,how='inner',on=['newest_name_x','address_x','developer','name_com_rate'])
df_3['address']=np.nan
df_3.at[df_3['address_x']=='','address']=df_3['address_y']
df_3.at[df_3['address_x']!='','address']=df_3['address_x']
df_3['newest_name']=np.nan
df_3.at[df_3['newest_name_x']=='','newest_name']=df_3['newest_name_y']
df_3.at[df_3['newest_name_x']!='','newest_name']=df_3['newest_name_x']
### 无法关联的楼盘
df_4_deal = df_3_deal[~df_3_deal['newest_name'].isin(df_3['newest_name_x'])]
df_4_deal['newest_id'] = np.nan
df_4_deal['formal_newest_name'] = np.nan
df_4_deal['alias_name'] = np.nan
##合并所有楼盘id的数据
df1 = df_1[['newest_id','newest_name','address','developer','formal_newest_name','alias_name']]
df1 = df1.append([df_2[['newest_id','newest_name','address','developer','formal_newest_name','alias_name']],df_3[['newest_id','newest_name','address','developer','formal_newest_name','alias_name']],df_4_deal[['newest_id','newest_name','address','developer','formal_newest_name','alias_name']]],ignore_index=True)


#In[]
df_sy =  issue_offer[issue_offer['gd_city'] == '三亚市']
df_sy['room_sum'] = df_sy['room_sum'].apply(lambda x:re.sub("\D","",x))
df_sy = df_sy[['gd_city','newest_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','developer','address','url','building_code']].drop_duplicates()
df_sy[['room_sum']] = df_sy[['room_sum']].astype('int')
df_sy = df_sy.groupby(['gd_city','newest_name','issue_code','issue_date_clean','issue_month','issue_quarter','issue_area','developer','address','url'])['room_sum'].sum().reset_index()
df_sy['dr'] = 0
df_sy['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
df_sy['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
# 格式化列
df_sy = df_sy[['gd_city','newest_name','issue_code','issue_date_clean','issue_month','issue_quarter','room_sum','issue_area','dr','create_time','update_time','url']]
df_sy.columns = ['city','newest_name','issue_code','issue_date','issue_month','issue_quarter','issue_room','issue_area','dr','create_time','update_time','newest_url']
# 格式化面积
df_sy['issue_area'] = df_sy['issue_area'].astype('str').apply(lambda x:x.split('.')[0])
df_sy['issue_area'] = df_sy['issue_area'].apply(lambda x:re.sub("\D","",x)) 


#In[]
##预售证和楼盘id合并
result = pd.merge(df1,df_sy,how='left',on=['newest_name'])



# In[10]:
# 加载到新表 dwb_newest_issue_offer
# result.drop_duplicates(inplace=True)
to_dws(result,'dwb_newest_issue_offer')
# df_4_deal.to_csv('C:\\Users\\86133\\Desktop\\df_4_deal.csv')
# result
print('>>>>>>>Done')

