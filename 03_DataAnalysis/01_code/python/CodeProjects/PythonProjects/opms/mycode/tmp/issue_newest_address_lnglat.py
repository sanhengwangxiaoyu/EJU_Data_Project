#In[]
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




#In[]




###创建比较函数
def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()#引用ratio方法，返回序列相似性的度量
###读取excel到dataframe中
# df=pd.read_excel('C:\\Users\\86133\\Desktop\\newest_address_lnglat.xlsx',engine='openpyxl')
df=pd.read_excel('C:\\Users\\86133\\Desktop\\newest_city_lng_lat.xlsx',engine='openpyxl')

###截取指定列
df = df[['newest_id','newest_name','city_name','address','change_lng_lat','Unnamed: 11']]
###过滤脏数据
df = df[~df['Unnamed: 11'].isnull()]
###修改列名
df.columns=['newest_id','newest_name','city_name','address','lng_lat','search_newest_name']
###进行比较
df['name_com_rate'] = df.apply(lambda x:similarity(x.newest_name,x.search_newest_name),axis=1)



# %%
##基本准确的
df_true = df[df['name_com_rate']>0.8]
##不确定的
df_true_level_1 = df[(df['name_com_rate']<=0.8)&(df['name_com_rate']>=0.4)]
df_true_level_2 = df[(df['name_com_rate']<0.4)&(df['name_com_rate']>0.0)]
##错误的
df_false = df[df['name_com_rate']==0.0]

###加载数据
df_true.to_excel('C:\\Users\\86133\\Desktop\\df_true.xlsx')
df_true_level_1.to_excel('C:\\Users\\86133\\Desktop\\df_true_level_1.xlsx')
df_true_level_2.to_excel('C:\\Users\\86133\\Desktop\\df_true_level_2.xlsx')
df_false.to_excel('C:\\Users\\86133\\Desktop\\df_false.xlsx')


