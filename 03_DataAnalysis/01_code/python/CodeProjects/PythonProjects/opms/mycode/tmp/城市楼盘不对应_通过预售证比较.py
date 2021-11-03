# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Seq278 10:31:47 2021
  城市楼盘不对应_通过预售证比较

"""
import configparser,os,pymysql,pandas as pd
from sqlalchemy import create_engine

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
database = 'temp_db'


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

def to_df_field(county_issue,df_base,index):
    df_field = pd.DataFrame(columns=('newest_name','city_id','address','issue_number'))
    for x in county_issue:
        if index == 1:
            df_base = df_base[~df_base['issue_number'].str.contains(x)]
            df_field = df_base
        elif index == 0:
            df_tmp = df_base[df_base['issue_number'].str.contains(x)]
            if df_field.empty:
                df_field = df_tmp
            else:
                df_field = df_tmp.append(df_field,ignore_index=True)
    return df_field

con = MysqlClient(db_host,database,user,password)



# In[4]:
# 浏览日志表imei明细
newest_base=con.query("select newest_name,city_id,address,issue_number from odsdb.ori_newest_info_base")

newest_info=con.query("select newest_name,newest_id,city_id,address from dws_db_prd.dws_newest_info where city_id in ('110000','310000','440300','440100') and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit group by newest_id)")

newest_base = newest_base[newest_base['issue_number'] != '']


# In[]:
#切分
#北京
df_base_beijing = newest_base[newest_base['city_id'] == 110000]
#城市预售证比较判断
df_base_beijing_field = df_base_beijing.loc[~df_base_beijing['issue_number'].str.contains("京房")]

#上海
df_base_shanghai = newest_base[newest_base['city_id'] == 310000]
shanghai_county_issue = {'诸','长售','增城','预许','吴','德','浔','余','甬','盐','桐','太','苏','善','绍','启','平','莱','昆','京','建字','嘉秀','嘉售','嘉南','嘉港','即','惠','湖','海','皋','鄂州','慈','常','\(安\)','安售','榕'}
df_base_shanghai_filed = to_df_field(shanghai_county_issue,df_base_shanghai,0)

#深圳
df_base_shenzhen = newest_base[newest_base['city_id'] == 440300]
shenzhen_county_issue = {'渝国','徐房','太','蓉','津国','济建','房许','禅房','筑商','郑房','深'}
df_base_shenzhen_filed = to_df_field(shenzhen_county_issue,df_base_shenzhen,1)

#广州
df_base_gaunghzou = newest_base[newest_base['city_id'] == 440100]
gaungzhou_county_issue = {'佛','南','北建','博','禅','德','莞','恩','高','清','惠东','惠湾','惠阳','陵','南房','三房','顺','四','台','桐','肇','中'}
df_base_gaunghzou_filed = to_df_field(gaungzhou_county_issue,df_base_gaunghzou,0)


#In[]
#合并
df = df_base_beijing_field.append([df_base_shanghai_filed,df_base_shenzhen_filed,df_base_gaunghzou_filed],ignore_index=True).groupby(['newest_name','city_id','address','issue_number']).count().reset_index()
newest_info['city_id'] = newest_info['city_id'].astype(int)
result = pd.merge(df,newest_info,how='inner',on=['newest_name','city_id'])
result = result.groupby(['newest_id','newest_name','city_id'])['issue_number'].count().reset_index()
result = result[['newest_id','newest_name','city_id']]


#In[]
to_dws(result,'newest_city_fialed_info')



#In[]
##
df = newest_base


# In[10]:
result.to_csv('C:\\Users\\86133\\Desktop\\result.csv')


# result
print('>>>>>>>Done')



