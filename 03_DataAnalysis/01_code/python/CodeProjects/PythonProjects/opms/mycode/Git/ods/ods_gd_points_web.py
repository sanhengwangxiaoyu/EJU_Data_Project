#In[]
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Nov 23 09:16:47 2021
    切分数据到MySQL表  ods_gd_points_web_*
Changed
    2021-12-06resouece_list新增result处理
"""

import json,re,configparser,os,pymysql,pandas as pd,json,numpy as np,time
from sqlalchemy import create_engine
print("START TIME : "+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) )


pymysql.install_as_MySQLdb()
cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块
user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
# table_name = 'ods_gd_points_web_pages'
# database = 'odsdb'
# input_file = '/data/ods/luohw_*'
input_file = 'C:\\Users\\86133\\Desktop\\ods_gd_points_web_pages_20220307'
input_table = 'ods_gd_points_web_pages'


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
f = open(input_file,encoding='utf-8') #打开‘product.json’的json文件
res = f.read()  #读文件
# newres = res[:6]




#In[]
####将第一层拆分
data = res.split("versions do not match\n")[1]  #切分出json数据
data = data.replace('ObjectId(','').replace('),\n\t"resource_list"',',\n\t"resource_list"').replace('_id','id').replace('\t','').replace('\n','').replace('ISODate(','').replace('),"appid"',',"appid"').replace('}{"id"','},{"id"').replace('"),"create_time" :','","create_time" :').replace('"),"type" :','","type" :').replace('"),"duration" :','","duration" :').replace('"),"top_pages" :','","top_pages" :')    # 替换脏字符
data = '['+data+']'
#####转换为Json
df = json.loads(data)
#####转换为DtaFrame
df1 = pd.json_normalize(df)



#In[]
####判断是那张表
print("本次加载数据表为:  "+input_table)


if input_table == 'ods_gd_points_web_statis' :
    if len(df1) >0 :
        df1_tmp = df1
        ######清洗   top_pages
        df1_tmp['top_pages_ods'] = df1_tmp['top_pages']  # 留作备份查看
        #####将备份数据从json转为字符串形式
        df1_tmp['top_pages_ods'] = df1_tmp['top_pages_ods'].map(lambda x: "Document["+str(x)+"]")
        df1_tmp['top_pages_ods'] = df1_tmp['top_pages_ods'].map(lambda x: str(x).replace(':','='))
        df1_tmp['top_pages_ods'] = df1_tmp['top_pages_ods'].map(lambda x: str(x).replace('\'',''))
        # #####清洗  top_pages  为空的不用处理,最后直接添加
        # df2 = df1[df1['top_pages_ods'] != "Document[[]]"]
        df2 = df1_tmp
        #####去除[]转换为json
        df2['top_pages'] = df2['top_pages'].map(lambda x: str(x)[1:])
        df2['top_pages'] = df2['top_pages'].map(lambda x: str(x)[:-1])
        #将一个  top_pages 拆分为多个
        df3 = df2.drop('top_pages',axis=1).join(df2['top_pages'].str.split('}, {',expand=True).stack().reset_index(level=1,drop=True).rename('top_pages')).reset_index(drop=True)
        # top_pages  处理成json的样式
        df3['top_pages'] = df3['top_pages'].map(lambda x: str(x)+"}")
        df3['top_pages'] = df3['top_pages'].map(lambda x: "{"+str(x).replace('}','').replace('{','').replace('id\': \'url\': ','id_url\': ')+"}")
    
        #将处理好的dataframe整个转为json
        df4 = (df3.to_json(orient = "records",force_ascii=False))
        #将json文件中错误的地方纠正
        df4 = df4.replace(':"{',':{').replace('}"','}').replace('\'','"')
        #####转换为字典
        df5 = json.loads(df4)
        #####转换为DtaFrame
        df6 = pd.json_normalize(df5)
        #####不放过任何一条数据
        # df6_tmp = df1[df1['top_pages_ods'] == "Document[[]]"]
        # df6_tmp['top_pages.id_url'] = np.nan
        # df6_tmp['top_pages.count'] =np.nan
        # df6 = df6.append(df6_tmp,ignore_index=False)


        ######清洗   top_jump_out
        df6['top_jump_out_ods'] = df6['top_jump_out']  # 留作备份查看
        ####将备份数据从json转为字符串形式
        df6['top_jump_out_ods'] = df6['top_jump_out_ods'].map(lambda x: "Document["+str(x)+"]")
        df6['top_jump_out_ods'] = df6['top_jump_out_ods'].map(lambda x: str(x).replace(':','='))
        df6['top_jump_out_ods'] = df6['top_jump_out_ods'].map(lambda x: str(x).replace('\'',''))
        # #####清洗  top_pages  top_jump_out,最后直接添加
        # df7 = df6[df6['top_jump_out_ods'] != "Document[[]]"]
        df7 = df6
        #####去除[]转换为json
        df7['top_jump_out'] = df7['top_jump_out'].map(lambda x: str(x)[1:])
        df7['top_jump_out'] = df7['top_jump_out'].map(lambda x: str(x)[:-1])
        #将一个  top_jump_out 拆分为多个
        df7 = df7.drop('top_jump_out',axis=1).join(df7['top_jump_out'].str.split('}, {',expand=True).stack().reset_index(level=1,drop=True).rename('top_jump_out')).reset_index(drop=True)
        # top_jump_out  处理成json的样式
        df7['top_jump_out'] = df7['top_jump_out'].map(lambda x: str(x)+"}")
        df7['top_jump_out'] = df7['top_jump_out'].map(lambda x: "{"+str(x).replace('}','').replace('{','').replace('\'id\': \'value\': ','\'id_value\': ')+"}")
    
        #将处理好的dataframe整个转为json
        df8 = (df7.to_json(orient = "records",force_ascii=False))
        #将json文件中错误的地方纠正
        df8 = df8.replace(':"{',':{').replace('}"','}').replace('\'','"')
        #####转换为字典
        df9 = json.loads(df8)
        #####转换为DtaFrame
        df10 = pd.json_normalize(df9)
        #####不放过任何一条数据
        # df10_tmp = df6[df6['top_jump_out_ods'] == "Document[[]]"]
        # df10_tmp['top_jump_out.id_value'] = np.nan
        # df10_tmp['top_jump_out.count'] =np.nan
        # df10 = df10.append(df10_tmp,ignore_index=False)


        ######清洗   top_jump_out
        df10['top_browser_ods'] = df10['top_browser']  # 留作备份查看
        ####将备份数据从json转为字符串形式
        df10['top_browser_ods'] = df10['top_browser_ods'].map(lambda x: "Document["+str(x)+"]")
        df10['top_browser_ods'] = df10['top_browser_ods'].map(lambda x: str(x).replace(':','='))
        df10['top_browser_ods'] = df10['top_browser_ods'].map(lambda x: str(x).replace('\'',''))
        df11 = df10
        #####去除[]转换为json
        df11['top_browser'] = df11['top_browser'].map(lambda x: str(x)[1:])
        df11['top_browser'] = df11['top_browser'].map(lambda x: str(x)[:-1])
        #将一个  top_browser 拆分为多个
        df12 = df11.drop('top_browser',axis=1).join(df11['top_browser'].str.split('}, {',expand=True).stack().reset_index(level=1,drop=True).rename('top_browser')).reset_index(drop=True)
        # top_browser  处理成json的样式
        df12['top_browser'] = df12['top_browser'].map(lambda x: str(x)+"}")
        df12['top_browser'] = df12['top_browser'].map(lambda x: "{"+str(x).replace('}','').replace('{','').replace('\'id\': \'browser\': ','\'id_browser\': ')+"}")
    
        #将处理好的dataframe整个转为json
        df13 = (df12.to_json(orient = "records",force_ascii=False))
        #将json文件中错误的地方纠正
        df13 = df13.replace(':"{',':{').replace('}"','}').replace('\'','"')
        #####转换为字典
        df14 = json.loads(df13)
        #####转换为DtaFrame
        df14 = pd.json_normalize(df14)



        ######清洗   provinces
        df14['provinces_ods'] = df14['provinces']  # 留作备份查看
        ####将备份数据从json转为字符串形式
        df14['provinces_ods'] = df14['provinces_ods'].map(lambda x: "Document["+str(x)+"]")
        df14['provinces_ods'] = df14['provinces_ods'].map(lambda x: str(x).replace(':','='))
        df14['provinces_ods'] = df14['provinces_ods'].map(lambda x: str(x).replace('\'',''))
        df15 = df14
        #####去除[]转换为json
        df15['provinces'] = df15['provinces'].map(lambda x: str(x)[1:])
        df15['provinces'] = df15['provinces'].map(lambda x: str(x)[:-1])
        #将一个  provinces 拆分为多个
        df15 = df15.drop('provinces',axis=1).join(df15['provinces'].str.split('}, {',expand=True).stack().reset_index(level=1,drop=True).rename('provinces')).reset_index(drop=True)
        # provinces  处理成json的样式
        df15['provinces'] = df15['provinces'].map(lambda x: str(x)+"}")
        df15['provinces'] = df15['provinces'].map(lambda x: "{"+str(x).replace('}','').replace('{','').replace('\'id\': \'province\': ','\'id_province\': ').replace('None','\'暂无\'')+"}")
        #将处理好的dataframe整个转为json
        df16 = (df15.to_json(orient = "records",force_ascii=False))
        #将json文件中错误的地方纠正
        df16 = df16.replace(':"{',':{').replace('}"','}').replace('\'','"')
        #####转换为字典
        df17 = json.loads(df16)
        #####转换为DtaFrame
        df18 = pd.json_normalize(df17)
        result = df18
        #改列名
        result = result.rename(columns={'id':'ods_id','appid':'app_id','__v':'data_v','top_pages.id_url':'top_pages_id_url','top_pages.count':'top_pages_count','top_jump_out.id_value':'top_jump_out_id_value','top_jump_out.count':'top_jump_out_count','top_browser.id_browser':'top_browser_id_browser','top_browser.count':'top_browser_count','provinces.id_province':'provinces_id_province','provinces.count':'provinces_count','top_pages_ods':'top_pages','top_jump_out_ods':'top_jump_out','top_browser_ods':'top_browser','provinces_ods':'provinces'})
        #添加更新标识
        result['dr'] = 0
        result['load_data_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
        result['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
        result = result.drop(['data_v'],axis=1)
    else :
        result = df1

elif input_table == 'ods_gd_points_web_axios' :
    result = df1
    #改列名
    result = result.rename(columns={'id':'ods_id','appid':'app_id','__v':'data_v'})
    #添加更新标识
    result['dr'] = 0
    result['load_data_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
    result['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
    result = result[['app_id','create_time','url','speed_type','method','duration','decoded_body_size','options','full_url','call_url','mark_page','mark_user','app_mobile','dr','load_data_time','update_time','ods_id','response']]

elif input_table == 'ods_gd_points_web_pvuvip' :
    result = df1
    #改列名
    result = result.rename(columns={'id':'ods_id','appid':'app_id','__v':'data_v'})
    #添加更新标识
    result['dr'] = 0
    result['load_data_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
    result['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
    result['depth'] = np.nan
    result['bounce'] = np.nan
    result = result[['app_id','pv','uv','ip','ajax','bounce','depth','flow','type','create_time','dr','load_data_time','update_time','ods_id']]

elif input_table == 'ods_gd_points_web_environ' :
    result = df1
    #改列名
    result = result.rename(columns={'id':'ods_id','appid':'app_id','__v':'data_v'})
    #添加更新标识
    result['dr'] = 0
    result['load_data_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
    result['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
    result['county'] = result['province']
    result = result[['app_id','create_time','url','mark_page','mark_user','mark_uv','browser','borwser_version','system','system_version','ip','county','province','city','app_mobile','dr','load_data_time','update_time','ods_id']]

elif input_table == 'ods_gd_points_web_pages' :
    #================================================================================================
    data = df1.explode('resource_list', ignore_index=True) 
    data = data[~data['resource_list'].isna()]
    data[['resource_list_name','resource_list_method','resource_list_type','resource_list_duration','resource_list_decodedBodySize','resource_list_nextHopProtocol','resource_list_params','resource_list_result']] = pd.DataFrame(data['resource_list'].values.tolist()) 

    # ######清洗resource_list
    # df1['resource_list_ods'] = df1['resource_list']  # 留作备份查看
    # #####将备份数据从json转为字符串形式
    # df1['resource_list_ods'] = df1['resource_list_ods'].map(lambda x: "Document["+str(x)+"]")
    # df1['resource_list_ods'] = df1['resource_list_ods'].map(lambda x: str(x).replace(':','='))
    # df1['resource_list_ods'] = df1['resource_list_ods'].map(lambda x: str(x).replace('\'',''))
    # #####清洗resource_list为空的不用处理,最后直接添加
    # df2 = df1[df1['resource_list_ods'] != "Document[[]]"]
    # #####去除[]转换为json
    # df2['resource_list'] = df2['resource_list'].map(lambda x: str(x)[1:])
    # df2['resource_list'] = df2['resource_list'].map(lambda x: str(x)[:-1])
    # #####拆分清洗resource_list
    # obj = re.compile(r"'params':.*?result",re.S) ### 将参数的json单独拿出来存放到一个字段,之后处理
    # #将一个resource_list 拆分为多个
    # df3 = df2.drop('resource_list',axis=1).join(df2['resource_list'].str.split('}, {\'name\':',expand=True).stack().reset_index(level=1,drop=True).rename('resource_list')).reset_index(drop=True)
    # #取出resource_list_params并转为字符串并存放在单独字段中,避免影响后续处理
    # df3['resource_list_params'] = df3['resource_list'].map(lambda x: obj.findall(x))
    # df3['resource_list_params'] = df3['resource_list_params'].map(lambda x: str(x).replace('\'','`').replace(', `result',''))
    # #resource_list去除resource_list_params并处理成json的样式
    # df3['resource_list'] = df3['resource_list'].map(lambda x: str(x)+"}")
    # df3['resource_list'] = df3['resource_list'].map(lambda x: re.sub(", 'params':.*?result",', \'result',x))
    # df3['resource_list'] = df3['resource_list'].map(lambda x: "{'name':"+str(x).replace('}','').replace('{','').replace('\'name\':','')+"}")
    # #将处理好的dataframe整个转为json
    # df4 = (df3.to_json(orient = "records",force_ascii=False))
    # #将json文件中错误的地方纠正
    # df4 = df4.replace('\'','"').replace('","resource_list_params"',',"resource_list_params"').replace('resource_list":"{"name"','resource_list":{"name"')
    # #2021-12-06resouece_list新增result处理
    # df4 = df4.replace('"result": True','"result": "True"').replace('"result": False','"result": "False"')
    # #####转换为字典
    # df5 = json.loads(df4)
    # #####转换为DtaFrame
    # df6 = pd.json_normalize(df5)

    # #================================================================================================
    # #####不放过任何一条数据
    # result = df1[df1['resource_list_ods'] == "Document[[]]"]
    # #####合并
    # """
    #     先修改列名，然后再添加缺失的列
    # 最后合并，加载到mysql中
    # """
    #改列名
    # df6 = data.rename(columns={'id':'ods_id','appid':'app_id','__v':'data_v','resource_list_ods':'resource_list','resource_list.name':'resource_list_name','resource_list.method':'resource_list_methond','resource_list.type':'resource_list_type','resource_list.duration':'resource_list_duration','resource_list.decodedBodySize':'resource_list_decodebodysize','resource_list.nextHopProtocol':'resource_list_nexthotprotocol','analysisDom_time':'analysisdom_time','track.behaviorTag':'track_behaviorTag'})
    df6 = data.rename(columns={'id':'ods_id','appid':'app_id','__v':'data_v','resource_list_ods':'resource_list','resource_list_method':'resource_list_methond','resource_list_type':'resource_list_type','resource_list_duration':'resource_list_duration','resource_list_decodedBodySize':'resource_list_decodebodysize','resource_list_nextHopProtocol':'resource_list_nexthotprotocol','analysisDom_time':'analysisdom_time','track.behaviorTag':'track_behaviorTag'})
    #提取数据
    # result = df6.drop(['data_v','resource_list.result'],axis=1)
    result = df6.drop(['data_v','resource_list_result'],axis=1)

    # #增加缺失列
    # result['resource_list_name'] = np.nan
    # result['resource_list_methond'] = np.nan
    # result['resource_list_type'] = np.nan
    # result['resource_list_duration'] = np.nan
    # result['resource_list_decodebodysize'] = np.nan
    # result['resource_list_nexthotprotocol'] = np.nan
    # result['resource_list_params'] = np.nan
    # # result['track_behaviorTag'] = np.nan
    #修改列名
    # result = result.rename(columns={'id':'ods_id','appid':'app_id','__v':'data_v','analysisDom_time':'analysisdom_time','track.behaviorTag':'track_behaviorTag'})
    # #提取数据
    # result = result.drop(['data_v','resource_list_ods'],axis=1)
    # #合并数据
    # result = result.append(df6,ignore_index=False)
    #添加更新标识
    result['dr'] = 0
    result['load_data_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
    result['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
    #脏数据处理
    result['resource_list_params'] = result['resource_list_params'].map(lambda x: "[Doc["+str(x)+"]")
    result['resource_list'] = result['resource_list'].map(lambda x: "[Doc["+str(x)+"]")
else :
    print("缺少输入参数!!!!!")


#In[]
print("LOAD DATA : "+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) )
#加载数据
if input_table == 'ods_gd_points_web_statis' :
    to_dws(result,'ods_gd_points_web_statis')
elif input_table == 'ods_gd_points_web_axios' :
    to_dws(result,'ods_gd_points_web_axios')
elif input_table == 'ods_gd_points_web_pvuvip' :
    to_dws(result,'ods_gd_points_web_pvuvip')
elif input_table == 'ods_gd_points_web_environ' :
    to_dws(result,'ods_gd_points_web_environ')
elif input_table == 'ods_gd_points_web_pages' :
    to_dws(result,'ods_gd_points_web_pages')
else:
    print("没有加载数据的结果表")

print('>>>>>>>>>>>>>>>>ETL Done!!!!!!')
# df

print("END TIME : "+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) )






































# ######===============================================以下为测试dataX导出的ongDB数据


#In[]

# import configparser,os,sys,pymysql,pandas as pd,json,numpy as np
# from re import split
# from sqlalchemy import create_engine

# pymysql.install_as_MySQLdb()
# cf = configparser.ConfigParser()
# path = os.path.abspath(os.curdir)
# confpath = path + "/conf/config4.ini"
# cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块
# user = cf.get("Mysql", "user")  # 获取user对应的值
# password = cf.get("Mysql", "password")  # 获取password对应的值
# db_host = cf.get("Mysql", "host")  # 获取host对应的值
# database = cf.get("Mysql", "database")  # 获取dbname对应的值
# table_name = 'ods_gd_points_web_pages'
# database = 'odsdb'
# # input_file = '/data/ods'
# input_file = 'luohw__73a9418d_0348_416d_867c_f2ed3db9b476'

# # -*- coding: utf-8 -*-
# class MysqlClient:
#     def __init__(self, db_host,database,user,password):
#         """
#         create connection to hive server
#         """
#         self.conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
#     def query(self, sql):
#         """
#         query
#         """
#         cur = self.conn.cursor()
#         cur.execute(sql)
#         res = cur.fetchall()
#         columnDes = cur.description #获取连接对象的描述信息
#         columnNames = [columnDes[i][0] for i in range(len(columnDes))]
#         data = pd.DataFrame([list(i) for i in res],columns=columnNames)
#         cur.close()
#         return data
#     def close(self):
#         self.conn.close()

# def to_dws(result,table):
#     engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database+'?charset=utf8')
#     result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)

# con = MysqlClient(db_host,database,user,password)



# ####读取的文档
# data = pd.read_csv(input_file, sep='\t', header=None)
# #####修改列名
# data.columns=['ods_id','app_id','create_time','url','full_url','pre_url','speed_type','is_first_in','mark_page','mark_user','load_time','dns_time','tcp_time','dom_time','resource_list','total_res_size','white_time','redirect_time','unload_time','request_time','analysisdom_time','ready_time','screenwidth','screenheight','app_mobile']
# #####保留原始数据
# data['resource_list_ods'] = data['resource_list']
# # df = df[df['mark_user'] == 'xPhzc4RtZG1637562894972']
# # df = df[df['mark_user'] == 'Td4p4Ta4Tx1634288215188']
# # df = df[df['app_id'] == 'a2wQJt21634186115834']
# # df = df[df['create_time'] == "2021-11-23"]
# #####筛选
# df = data[data['resource_list'] != "[]"]
# ####拆除mongoDB文档类型的字段
# df1 = df.drop('resource_list',axis=1).join(df['resource_list'].str.split(', Document{',expand=True).stack().reset_index(level=1,drop=True).rename('resource_list')).reset_index(drop=True)
# #####手动替换字符，以达到json格式的数据
# df1['resource_list'] = df1['resource_list'].str.replace('Document{', '')
# # df1['resource_list'] = df1['resource_list'].str.replace('}}', '}}')
# df1['resource_list'] = df1['resource_list'].str.replace('}}', '}')
# df1['resource_list'] = df1['resource_list'].str.replace('\[{', '{')
# df1['resource_list'] = df1['resource_list'].str.replace('}\]', '}')
# df1['resource_list'] = df1['resource_list'].str.replace('{name=', '{\'name\':\'')
# df1['resource_list'] = df1['resource_list'].str.replace(', method=', '\', \'method\':\'')
# df1['resource_list'] = df1['resource_list'].str.replace(', type=', '\', \'type\':\'')
# df1['resource_list'] = df1['resource_list'].str.replace(', duration=', '\', \'duration\':\'')
# df1['resource_list'] = df1['resource_list'].str.replace(', decodedBodySize=', '\', \'decodedBodySize\':\'')
# df1['resource_list'] = df1['resource_list'].str.replace(', nextHopProtocol=', '\', \'nextHopProtocol\':\'')
# df1['resource_list'] = df1['resource_list'].str.replace(', params=', '\', \'params\':\'')
# df1['resource_list'] = df1['resource_list'].str.replace('}', '\'}')
# df1['resource_list'] = df1['resource_list'].str.replace('\'}\'}', '}\'}')
# # df1['resource_list'] = df1['resource_list'].str.replace('\[\]', '\'')
# # df1['resource_list'] = df1['resource_list'].str.replace('=\'}', '=无}')
# #####to_json
# df2 = (df1.to_json(orient = "records",force_ascii=False))
# #####清除脏字符
# df2 = df2.replace(':"{',':{').replace('"}','}').replace('\'','"')
# #####转换为Json
# df2 = json.loads(df2)
# #####转换为DtaFrame
# df3 = pd.json_normalize(df2)
# ##==================================================================================
# #####不放过任何一条数据
# df4 = data[data['resource_list'] == "[]"]



# # %%

# #####合并
# """
#     先修改列名，然后再添加缺失的列
#   最后合并，加载到mysql中
# """
# df3.columns=['ods_id','app_id','create_time','url','full_url','pre_url','speed_type','is_first_in','mark_page','mark_user','load_time','dns_time','tcp_time','dom_time','total_res_size','white_time','redirect_time','unload_time','request_time','analysisdom_time','ready_time','screenwidth','screenheight','app_mobile','resource_list','resource_list_name','resource_list_methond','resource_list_type','resource_list_duration','resource_list_decodebodysize','resource_list_nexthotprotocol','resource_list_params']
# result = df3[['ods_id','app_id','create_time','url','full_url','pre_url','speed_type','is_first_in','mark_page','mark_user','load_time','dns_time','tcp_time','dom_time','resource_list','resource_list_name','resource_list_methond','resource_list_type','resource_list_duration','resource_list_decodebodysize','resource_list_nexthotprotocol','resource_list_params','total_res_size','white_time','redirect_time','unload_time','request_time','analysisdom_time','ready_time','screenwidth','screenheight','app_mobile']]


# df4['resource_list_name'] = np.nan
# df4['resource_list_methond'] = np.nan
# df4['resource_list_type'] = np.nan
# df4['resource_list_duration'] = np.nan
# df4['resource_list_decodebodysize'] = np.nan
# df4['resource_list_nexthotprotocol'] = np.nan
# df4['resource_list_params'] = np.nan
# df5 = df4[['ods_id','app_id','create_time','url','full_url','pre_url','speed_type','is_first_in','mark_page','mark_user','load_time','dns_time','tcp_time','dom_time','resource_list','resource_list_name','resource_list_methond','resource_list_type','resource_list_duration','resource_list_decodebodysize','resource_list_nexthotprotocol','resource_list_params','total_res_size','white_time','redirect_time','unload_time','request_time','analysisdom_time','ready_time','screenwidth','screenheight','app_mobile']]

# result = result.append(df5,ignore_index=False)


# to_dws(result,table_name)

# print('>>>ETL Done!!!!!!')



#In[]






# df1.to_csv('C:\\Users\\86133\\Desktop\\df1.csv')






















# ######===============================================以下为测试python MongDB代码
# # In[]:
# #!/usr/bin/env python
# # coding: utf-8
# # -*- coding: utf-8 -*-

# from json.decoder import JSONDecodeError
# from numpy import maximum
# import pymysql,pandas as pd,json


# ##设置变量初始值##
# user = 'root'
# password = '000000'
# db_host = '47.96.87.7'
# database = 'ST'

# ##mysql连接配置##
# # -*- coding: utf-8 -*-
# class MysqlClient:
#     def __init__(self, db_host,database,user,password):
#         """
#         create connection to hive server
#         """
#         self.conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
#     def query(self, sql):
#         """
#         query
#         """
#         cur = self.conn.cursor()
#         cur.execute(sql)
#         res = cur.fetchall()
#         columnDes = cur.description #获取连接对象的描述信息
#         columnNames = [columnDes[i][0] for i in range(len(columnDes))]
#         data = pd.DataFrame([list(i) for i in res],columns=columnNames)
#         cur.close()
#         return data
#     def close(self):
#         self.conn.close()

# con = MysqlClient(db_host,database,user,password)

# deal=con.query("select @rowNo:=@rowNo+1 id,a.* from fang_detail a,(select @rowNo:=0) b limit 10")



# #In[]
# ##basic_info    record  planning    sale    peitao  trends
# deal_basic_info = deal[['id','newest_id','basic_info']]
# deal_basic_info[['id']] = deal_basic_info[['id']].astype('int')
# deal_basic_info = (deal_basic_info.to_json(orient = "records",force_ascii=False))
# deal_basic_info = deal_basic_info.replace(':"{',':{').replace('"}','}').replace('\'','"')
# # deal_basic_info
# json_datas_basic_info = json.loads(deal_basic_info)
# result_basic_info = pd.json_normalize(json_datas_basic_info)
# result_basic_info


# #In[]
# deal_record = deal[['id','newest_id','record']]
# deal_record[['id']] = deal_record[['id']].astype('int')
# deal_record = (deal_record.to_json(orient = "records",force_ascii=False))
# # deal_record
# deal_record = deal_record.replace(':"{',':{').replace('"}','}').replace('\'','"')
# # deal_record
# json_datas_record = json.loads(deal_record)
# # json_datas
# tmp_record = pd.json_normalize(json_datas_record)
# # pd.json_normalize(json_datas,'record.楼盘纪事',['id','newest_id','record.分期信息'])
# tmp_record.columns = ['id','newest_id','record_story','record_split']
# # pd.json_normalize(tmp_record,'record_story',['id','newest_id','record_split'])
# tmp_record_split = tmp_record[['id','newest_id','record_split']]


# # %%
# #!/usr/bin/env python
# # coding: utf-8
# # -*- coding: utf-8 -*-
# import pymongo,pandas as pd,re
# import datetime
# import json
# from bson import ObjectId

# class JSONEncoder(json.JSONEncoder):
#     def default(self, o):
#         if isinstance(o,ObjectId):
#             return str(o)
#         return json.JSONEncoder.default(self,o)
 
# myclient = pymongo.MongoClient("mongodb://10.122.144.202:27017/")
# mydb = myclient["performance-prod"]
# mycol = mydb["web_pages_a2wqjt21634186115834"]
# result = mycol.find_one({},{"_id": 1,"resource_list":1})
# result1 =JSONEncoder().encode(result)



# j = json.dumps(result1)
# deal_basic_info = j.replace('_id','id')
# json_datas_basic_info = json.loads(deal_basic_info)

# result_basic_info = pd.json_normalize(json_datas_basic_info)

# result_basic_info

# # # %%
# #!/usr/bin/env python
# # coding: utf-8
# # -*- coding: utf-8 -*-
# import pymongo,pandas as pd,re
# import datetime
# import json
# from bson import ObjectId

# class JSONEncoder(json.JSONEncoder):
#     def default(self, o):
#         if isinstance(o,ObjectId):
#             return str(o)
#         return json.JSONEncoder.default(self,o)
 
# myclient = pymongo.MongoClient("mongodb://10.122.144.202:27017/")
# mydb = myclient["performance-prod"]
# mycol = mydb["web_pages_a2wqjt21634186115834"]
# result = mycol.find_one({},{"_id": 0,"resource_list":1})
# # result =JSONEncoder().encode(result)



# j = json.dumps(result)
# deal_basic_info = j.replace('{"resource_list": ','').replace('}]}','}]')
# json_datas_basic_info = json.loads(deal_basic_info)
# result_basic_info = pd.json_normalize(json_datas_basic_info)

# result_basic_info




# # %%
# print(result)
# print('type result===',type(result))
# cal_index =0
# for i in result:
#     cal_index = cal_index +1
#     if cal_index == 4:  #先打个三个地区出来看看情况
#         break
#     #print('type i =',type(i))
#     # i.pop('_id') #不如这个key和value不去掉的话，会报ObjectId错误的问题，因为这个是MongoDB里面的类，json不认识，你的自定定义这个类来处理
#     j = json.dumps(i,ensure_ascii=False)
#     print(j)
# # for x in mycol.find():
# #   print(x)


