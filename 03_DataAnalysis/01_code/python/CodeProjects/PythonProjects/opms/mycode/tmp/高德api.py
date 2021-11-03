
import requests,json,requests,json,pandas as pd,os,pymysql,time
import configparser
from sqlalchemy import create_engine

##设置配置信息##
pymysql.install_as_MySQLdb()
##读取配置文件##
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

def Dnull(str):
    if str is None :
        str = ""
    elif len(str) == 0:
        str = ""
    else :
        str
    return str

con = MysqlClient(db_host,database,user,password)
newest = con.query("select newest_name,address,city city_name from temp_db.tmp_newest_in_city_error where dr = 0 and newest_id not in ('7dae2029d10cfa1a6b24fd0060ecc79a','84be99387c460a2b850fc79919c62158','53234bfe2375747ca4e53da30ebf671d','d1b4c797211ffe5ca12890bfaf5104b5','331daaa40ed5d03efedd17a2c5f861e4','cd83c7cbc9c6e84d549929dc4e0d28d5','5a85bd3178d40eeb16a1a05970fd058f','b60a33ce77c8a91710df9b0ac1a4c2bc','c5469d16b150bdd87dcd3003834f0b46','dedcec9cd7bce87082806070b0cdea72','3c7fd0ec1628524d2a0949c5598a1afc','b0ade1b6f389c0c4e91d1b285bb7c804','55c58a9195dedcfb325c4121c0fae5a4','5f5581e6b2b0a3e3d095abe3835690fc','7a42a72d4b6c3091cf340890158472b1','8896b66014148af9915e632aa45b0a83','8dc361a06dcd7d1e92a2314fac06b0e9','665ccf58b75432e513f9febd8b571473','16d2dda8bdae4a99cbaa18e5b5d1d15e','18e88bddb3d5e7185284cc6e755a581c','fe434ab9eac2afb8205b39a1f453a817','0d69530ce541ada658f6766ab2a6f8e4','a1e7206ec14317977bd86a4e3ff6f068','f0749f5a41b2369989dca779bff1fd54','873fa36f4b2800def2f08927dc9f5523','a5328926a6370c26ead474c38700a5cd','4e99d8e8b5c6c9e238e57431fd9bde84','d72b5abc155c03f376cfb36a510dcb36','1e25b358785aea629b1d3103546caa27','0af473d22ebdac2c9abeae6b6f276930','cd518390fadf8a05afc2ae2ecc47420f','9f3631b112a83efec293fd2bcb94d126','9d009b2a363d17ebcb03eac972aefc57','25c1705574bc2268d8435a681b8bce59','cd27da2967e34d977ddbdf5de2a0cd47','af8bc6f61bc35578ce852090a69dfd7f','3dc69ed9cc129164c39a40fc75be0ce7','eebeeec5205a145035b9589ce106fa4b','abde2394b37a0e5e7fa47827bf61c1cc','a9be5388107833177d4fb49052654751','bc9691e02a48fe49c2d0cacfad6d7b61')")

# newest_test = newest[newest['newest_name'].isin(['龙胤花园','中梁暨阳时光','健康小镇','绿地城'])]
city_detail = pd.DataFrame()

for index,row in newest.iterrows():
    url = 'https://restapi.amap.com/v3/geocode/geo'   # 输入API问号前固定不变的部分
    params = { 'key': 'ecc2bff0af86a26467024657ff163c76',
        #    'key': '61d7e8cf769016c6904b3cea7b719e3d',
           'address': row['address'],
           'city': row['city_name'] }                # 将两个参数放入字典
    res = requests.get(url, params)
    res.text
    jd = json.loads(res.text)      # 将json数据转化为Python字典格式
    print(row['newest_name'])
    for i in range(len(jd['geocodes'])):
        coords = jd['geocodes'][i]
        lat = coords['location'].split(',')[-1]
        lng = coords['location'].split(',')[0]
        province = coords['province']
        city = coords['city']
        district = coords['district']
        address = coords['formatted_address']
        city_detail1 = pd.DataFrame([row])
        city_detail1['gd_lat'] = lat
        city_detail1['gd_lng'] = lng
        city_detail1['gd_province'] = Dnull(province)
        city_detail1['gd_city'] = Dnull(city)
        city_detail1['gd_district'] = Dnull(district)
        city_detail1['gd_address'] = Dnull(address)
        city_detail = city_detail.append(city_detail1)

# city_detail_re = pd.merge(newest,city_detail,how='left',on=['newest_id','newest_name','city_id','address','lng','lat','city_name'])

city_detail_re = city_detail[['newest_name','address','city_name','gd_city','gd_district','gd_lat','gd_lng']]
city_detail_re['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 

to_dws(city_detail_re,'crawler_city_newest_lnglat_gd')


city_detail.to_csv('C:\\Users\\86133\\Desktop\\city_detail.csv')