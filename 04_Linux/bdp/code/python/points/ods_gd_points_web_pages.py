 #In[]
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Nov 23 09:16:47 2021
    切分数据到MySQL表  ods_gd_points_web_pages
"""

import json,re,configparser,os,pymysql,pandas as pd,json,numpy as np,time
from sqlalchemy import create_engine

pymysql.install_as_MySQLdb()
cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块
user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
table_name = 'ods_gd_points_web_pages'
database = 'odsdb'
input_file = '/bdp/conf/mongoDB/test.txt'
# input_file = 'C:\\Users\\86133\\Desktop\\result.text'



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
con = MysqlClient(db_host,database,user,password)
f =open(input_file,encoding='utf-8') #打开‘product.json’的json文件
res=f.read()  #读文件


#In[]

res = res.split("versions do not match\n")[1]  #切分出json数据
res = res.replace('ObjectId(','').replace('),\n\t"resource_list"',',\n\t"resource_list"').replace('_id','id').replace('\t','').replace('\n','').replace('ISODate(','').replace('),"appid"',',"appid"').replace('}{"id"','},{"id"')  # 替换脏字符
res = '['+res+']'
#####转换为Json
df = json.loads(res)
#####转换为DtaFrame
df1 = pd.json_normalize(df)

#================================================================================================
######清洗resource_list
df1['resource_list_ods'] = df1['resource_list']  # 留作备份查看
#####将备份数据从json转为字符串形式
df1['resource_list_ods'] = df1['resource_list_ods'].map(lambda x: "Document["+str(x)+"]")
df1['resource_list_ods'] = df1['resource_list_ods'].map(lambda x: str(x).replace(':','='))
df1['resource_list_ods'] = df1['resource_list_ods'].map(lambda x: str(x).replace('\'',''))
#####清洗resource_list为空的不用处理,最后直接添加
df2 = df1[df1['resource_list_ods'] != "Document[[]]"]
#####去除[]转换为json
df2['resource_list'] = df2['resource_list'].map(lambda x: str(x)[1:])
df2['resource_list'] = df2['resource_list'].map(lambda x: str(x)[:-1])
#####拆分清洗resource_list
obj = re.compile(r"'params':.*?}",re.S)   ### 将参数的json单独拿出来存放到一个字段,之后处理
#将一个resource_list 拆分为多个
df3 = df2.drop('resource_list',axis=1).join(df2['resource_list'].str.split('}, {',expand=True).stack().reset_index(level=1,drop=True).rename('resource_list')).reset_index(drop=True)
#取出resource_list_params并转为字符串并存放在单独字段中,避免影响后续处理
df3['resource_list_params'] = df3['resource_list'].map(lambda x: obj.findall(x))
df3['resource_list_params'] = df3['resource_list_params'].map(lambda x: str(x).replace('\'','`'))
#resource_list去除resource_list_params并处理成json的样式
df3['resource_list'] = df3['resource_list'].map(lambda x: str(x)+"}")
df3['resource_list'] = df3['resource_list'].map(lambda x: re.sub(", 'params':.*?}",'',x))
df3['resource_list'] = df3['resource_list'].map(lambda x: "{"+str(x).replace('}','').replace('{','')+"}")
#将处理好的dataframe整个转为json
df4 = (df3.to_json(orient = "records",force_ascii=False))
#将json文件中错误的地方纠正
df4 = df4.replace('\'','"').replace('","resource_list_params"',',"resource_list_params"').replace('resource_list":"{"name"','resource_list":{"name"')
#####转换为字典
df5 = json.loads(df4)
#####转换为DtaFrame
df6 = pd.json_normalize(df5)

#================================================================================================
#####不放过任何一条数据
result = df1[df1['resource_list_ods'] == "Document[[]]"]

#####合并
"""
    先修改列名，然后再添加缺失的列
  最后合并，加载到mysql中
"""
#改列名
df6.columns=['ods_id','create_time','app_id','url','full_url','pre_url','speed_type','is_first_in','mark_page','mark_user','load_time','dns_time','tcp_time','dom_time','total_res_size','white_time','redirect_time','unload_time','request_time','analysisdom_time','ready_time','screenwidth','screenheight','app_mobile','data_v','resource_list','resource_list_params','resource_list_name','resource_list_methond','resource_list_type','resource_list_duration','resource_list_decodebodysize','resource_list_nexthotprotocol']
#提取数据
df6 = df6[['ods_id','app_id','create_time','url','full_url','pre_url','speed_type','is_first_in','mark_page','mark_user','load_time','dns_time','tcp_time','dom_time','resource_list','resource_list_name','resource_list_methond','resource_list_type','resource_list_duration','resource_list_decodebodysize','resource_list_nexthotprotocol','resource_list_params','total_res_size','white_time','redirect_time','unload_time','request_time','analysisdom_time','ready_time','screenwidth','screenheight','app_mobile']]
#增加缺失列
result['resource_list_name'] = np.nan
result['resource_list_methond'] = np.nan
result['resource_list_type'] = np.nan
result['resource_list_duration'] = np.nan
result['resource_list_decodebodysize'] = np.nan
result['resource_list_nexthotprotocol'] = np.nan
result['resource_list_params'] = np.nan
#修改列名
result['ods_id'] = result['id']
result['app_id'] = result['appid']
result['analysisdom_time'] = result['analysisDom_time']
#提取数据
result = result[['ods_id','app_id','create_time','url','full_url','pre_url','speed_type','is_first_in','mark_page','mark_user','load_time','dns_time','tcp_time','dom_time','resource_list','resource_list_name','resource_list_methond','resource_list_type','resource_list_duration','resource_list_decodebodysize','resource_list_nexthotprotocol','resource_list_params','total_res_size','white_time','redirect_time','unload_time','request_time','analysisdom_time','ready_time','screenwidth','screenheight','app_mobile']]
#合并数据
result = result.append(df6,ignore_index=False)
#添加更新标识
result['dr'] = 0
result['load_data_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
result['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
#脏数据处理
# result['resource_list_params'] = result['resource_list_params'].map(lambda x: "[Doc["+str(x)+"]")
result['resource_list'] = result['resource_list'].map(lambda x: "[Doc["+str(x)+"]")
#加载数据
to_dws(result,table_name)
print('>>>ETL Done!!!!!!')
