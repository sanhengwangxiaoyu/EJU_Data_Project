
#%%
"""
百度根据楼盘名爬取经纬度和城市
    1、查询出所有上线楼盘
    2、根据楼盘名字调用百度api爬取数据
    3、爬取下来的数据和本地数据比较，选取相似度最高的


"""

import requests,json,time, pandas as pd,os,pymysql,configparser
from sqlalchemy import create_engine
from difflib import SequenceMatcher#导入库

cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
date_q = '2018Q4'

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
    engine = create_engine("mysql+pymysql://yangzhen:6V5_0rviExpxBzHj@172.28.36.77:3306/dws_db_prd?charset=utf8")
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)

def similarity(a,b):
    return SequenceMatcher(None,a,b).ratio()#引用ratio方法，返回序列相似性的度量


#%%

# newest = con.query("SELECT DISTINCT a.newest_id,a.newest_name,b.city_name,a.address FROM dws_newest_info a JOIN dws_db_prd.dim_geography b ON b.grade=3 AND b.city_id=a.city_id WHERE a.newest_id IN (SELECT DISTINCT newest_id FROM dws_newest_period_admit WHERE dr=0)  and a.newest_id not in (select newest_id from city_detail_baidu ) and newest_id  in (select newest_id from temp_db.newest_city_fialed_info)")
newest = con.query("select tt1.newest_id,tt1.newest_name,tt1.city_name,tt1.address from (SELECT a.newest_id,a.newest_name,b.city_name,a.address FROM dws_newest_info a INNER JOIN dws_db_prd.dim_geography b ON b.grade=3 AND b.city_id=a.city_id WHERE a.newest_id IN (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id having max(period) = '2018Q1')) tt1 left join (select newest_id from city_detail_baidu) tt2 on tt1.newest_id = tt2.newest_id where tt2.newest_id is null group by tt1.newest_id,tt1.newest_name,tt1.city_name,tt1.address")
newest_ls = newest.newest_name.drop_duplicates()
# newest_ls = {'恒天广场','谱洋府','金地都会艺境','雅居乐江玥','嘉汇四季阳光','颐和雅园','正商启盛华庭','公园壹号','招商雍荣府','中国湾','学仕嘉园','紫薇丽都','阳光城丽景公馆','赣电贵府','华科印象台北','格力两江总部公园','钧明城','新城璟郡澜庭','复地花屿城','万科-中天-宏远魅力之城','首城国际','浅山','天健公馆','乐清蝴蝶广场','旭辉华宇南宸府','弘阳云筑','万隆伟业京学府','弘阳时光里','中铁诺德心悦湾','北京华发中央公园','保利玥府','翡丽公园','新长海广场','京基鹭府','卓越华宸世纪','美的碧桂园观澜宸府','成都恒大悦府','龙湖云颂','碧桂园-天玺','鸿通公园1988九熙府一区','新中宇维萨','徐州铜山万达广场','恒大湖山半岛','千山-半岛国际','国正-天境湾','博景湾花园','南益名悦湾','中国核建锦城','仁恒峦山美地','恒茂城市之光','海棠盛世','清远恒大银湖城','富力悦山湖','融科花满庭','蓝光林肯公园','壹方天第','新城香溢紫郡','天空之城','泰达汉郡豪庭','蔚蓝卡地亚花园城','中辅小时代','碧桂园中心天宸','耀辰公馆','五矿绿城-御园','天房麗山','济南世茂璀璨时代','汉北广场','博林腾瑞','保利西汇智慧云城'}
newest_ls = list(newest_ls)
city_detail = pd.DataFrame()
for n in newest_ls:
    try:
        print(n)
        url = 'https://api.map.baidu.com/place/v2/suggestion'   # 输入API问号前固定不变的部分
        params = {  'query': n,
                    # 'region':'苏州',
                    'ak': 'v06Ob0IHtxFjW6lNN6Xu0dfXLczGCol2',
                    # 'ak': 'IIHc5lyh5GpgV22zyf1ix9nnIqthVg27',
                    # 'city_limit':True,
                    'output':'json'
                    }                # 将两个参数放入字典
        res = requests.get(url, params)
        res.text
        jd = json.loads(res.text)      # 将json数据转化为Python字典格式
        # for i in range(len(jd['result'])):
        #     try:
        #         # print(i)
        #         coords = jd['result'][i]
        #         name = coords['name']
        #         lat = coords['location']['lat']
        #         lng = coords['location']['lng']
        #         province = coords['province']
        #         city = coords['city']
        #         district = coords['district']
        #         address = coords['address']
        #         tag = coords['tag']
        #         city_detail1 = pd.DataFrame([n])
        #         # city_detail1['city_y'] = pd.DataFrame([name])
        #         city_detail1['name'] = pd.DataFrame([name])
        #         city_detail1['lat'] = pd.DataFrame([lat])
        #         city_detail1['lng'] = pd.DataFrame([lng])
        #         city_detail1['province'] = pd.DataFrame([province])
        #         city_detail1['city'] = pd.DataFrame([city])
        #         city_detail1['district'] = pd.DataFrame([district])
        #         city_detail1['address'] = pd.DataFrame([address])
        #         city_detail1['tag'] = pd.DataFrame([tag])
        #         city_detail = city_detail.append(city_detail1)
        #         city_detail_re = city_detail.merge(newest,how='left',left_on=[0],right_on=['newest_name'])
        #     except: print("Error encounted with Line 1: " + '')
    except: print("Error encounted with Line 2: " + '')
city_detail_re = city_detail.merge(newest,how='left',left_on=[0],right_on=['newest_name'])
city_detail_re['address_x'] = city_detail_re['address_x'].apply(lambda x: x.split('-')[-1])
city_detail_re['xx'] = city_detail_re.apply(lambda x:similarity(x.address_x,x.address_y),axis=1)
city_detail_re = city_detail_re[city_detail_re['tag']=='住宅区']
city_detail_re.sort_values(['newest_name','address_y','xx'],ascending=False,inplace=True)
city_detail_re1 = city_detail_re.groupby(['newest_name','address_y']).head(1).reset_index(drop=True)
city_detail_re1.drop(0,axis=1,inplace=True)
print('>>>> load_data')
to_dws(city_detail_re1,'city_detail_baidu')
time.sleep(60)


