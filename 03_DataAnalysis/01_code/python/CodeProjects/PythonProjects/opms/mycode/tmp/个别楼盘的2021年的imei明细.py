# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Seq278 09:01:47 2021
  个别楼盘的2021年的imei明细

"""
import configparser,os,pymysql,pandas as pd,re,time
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
date_quarter = '2021Q3' 

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
con = MysqlClient(db_host,database,user,password)


# In[4]:
# 浏览日志表imei明细
# issue_offer=con.query("SELECT newest_id,imei FROM dwb_db.a_dwb_customer_browse_log where visit_date>='"+start_date+"' and visit_date<='"+end_date+"' and newest_id in ('83f23fd918398971c7295b3c07ed1967','7eb9ef20aea0b19c97fe839420e4350d','73d65da4561d6fcf738af55ff3c21467','495f6fbb3f35bdd14c7d4efb4022004a','8417be8224c97a16ce4935d8fc95a0e5','e86a44841446dd9a2edc06a6b22bd924','50976be57f9a8c5058813d3a6d34b01d','c10d68db25053e2e0bc0746e2ecfc2f6','05ed67003a563f6df6188423dddf9a64','08612952d30126bb87cf50554cf8cf60','7bef6dfa7fca742e5622dc2b25470300','783d390239928e4bbdd352004f8587dd','9aa57c09bf9c11eb86162cea7f6c2bde','1d427991cff480ac5b8b54c17b5b6d08','459f83383975b96704dea6e5a632b769','9ae729fcbf9c11eb86162cea7f6c2bde','9b8bb916bf9c11eb86162cea7f6c2bde','2bff59fe941afe48e1298945f0be3e88','5be89fa35ffcc06a267ce0da6fc38789','b493054673950d870d65ce6b62e11b69','9c053f0bbf9c11eb86162cea7f6c2bde','a28b6653c400c1f8509764079d1349d0','9a6ab320bf9c11eb86162cea7f6c2bde','a02255ccee0910d630369043f1a8ec5a','9ada1d3dbf9c11eb86162cea7f6c2bde')")

issue_offer=con.query("select customer imei,floor_name from odsdb.cust_browse_log_202107 where floor_name in ('唐山碧桂园凤凰星宸','碧桂园凤凰星宸','万科和颂轩','深国际万科和颂轩','锦绣海湾城九期珑悦湾','锦绣海湾城','锦绣海湾城十期','锦绣海湾城九期','万科城花园','广州增城万科城','广州万科城','集美嘉悦,金科集美嘉悦','天伦云境天澄,云境天澄','新力翡翠湾','翡翠湾','翡翠湾雅园','光明当代拾光里','中基·文博府','文博苑','中基文博府','咸阳龙湖-上城','咸阳龙湖・上城','龙湖・上城','龙湖•上城','龙湖上城','龙湖-上城','龙湖·天璞','龙湖天璞','中交白兰春晓','中交白兰春晓苑','白兰春晓中交','中铁琉森水岸天境','中铁·琉森水岸天境','中铁•琉森水岸','中铁琉森水岸','琉森水岸','颐璟名庭','碧桂园•松湖明珠','碧桂园松湖天悦','龙湖-春江天越','龙湖·春江天越','龙湖•春江天越','龙湖春江天越','保利-和光尘樾','保利·和光尘樾','保利•和光尘樾','保利和光尘樾','天津金地藝墅家','藝墅家·酩悦','金地兿墅家-酩悦','金地兿墅家·酩悦','金地墅家','金地艺墅家','金地藝墅家','金地藝墅家-酩悦','金地藝墅家·酩悦','金地藝墅家•酩悦','金地藝墅家酩悦','华润置地御华府','御华府','大东海-晋棠府','大东海·晋棠府','大东海•晋棠府','大东海晋棠府','保利领秀前城冠江墅组团','保利•领秀前城','保利·领秀前城禧悦都','保利领秀前城','世茂滨江府','滨江府1913','滨江府','润和滨江府','凤汇壹品居','凤汇壹品居宸园','旭辉宝龙凤汇壹品','新城明昱东方','明昱东方','泾河雅居乐花园,雅居乐北城雅郡') union all select customer imei,floor_name from odsdb.cust_browse_log_202108 where floor_name in ('唐山碧桂园凤凰星宸','碧桂园凤凰星宸','万科和颂轩','深国际万科和颂轩','锦绣海湾城九期珑悦湾','锦绣海湾城','锦绣海湾城十期','锦绣海湾城九期','万科城花园','广州增城万科城','广州万科城','集美嘉悦,金科集美嘉悦','天伦云境天澄,云境天澄','新力翡翠湾','翡翠湾','翡翠湾雅园','光明当代拾光里','中基·文博府','文博苑','中基文博府','咸阳龙湖-上城','咸阳龙湖・上城','龙湖・上城','龙湖•上城','龙湖上城','龙湖-上城','龙湖·天璞','龙湖天璞','中交白兰春晓','中交白兰春晓苑','白兰春晓中交','中铁琉森水岸天境','中铁·琉森水岸天境','中铁•琉森水岸','中铁琉森水岸','琉森水岸','颐璟名庭','碧桂园•松湖明珠','碧桂园松湖天悦','龙湖-春江天越','龙湖·春江天越','龙湖•春江天越','龙湖春江天越','保利-和光尘樾','保利·和光尘樾','保利•和光尘樾','保利和光尘樾','天津金地藝墅家','藝墅家·酩悦','金地兿墅家-酩悦','金地兿墅家·酩悦','金地墅家','金地艺墅家','金地藝墅家','金地藝墅家-酩悦','金地藝墅家·酩悦','金地藝墅家•酩悦','金地藝墅家酩悦','华润置地御华府','御华府','大东海-晋棠府','大东海·晋棠府','大东海•晋棠府','大东海晋棠府','保利领秀前城冠江墅组团','保利•领秀前城','保利·领秀前城禧悦都','保利领秀前城','世茂滨江府','滨江府1913','滨江府','润和滨江府','凤汇壹品居','凤汇壹品居宸园','旭辉宝龙凤汇壹品','新城明昱东方','明昱东方','泾河雅居乐花园,雅居乐北城雅郡') union all select customer imei,floor_name from odsdb.cust_browse_log_202109 where floor_name in ('唐山碧桂园凤凰星宸','碧桂园凤凰星宸','万科和颂轩','深国际万科和颂轩','锦绣海湾城九期珑悦湾','锦绣海湾城','锦绣海湾城十期','锦绣海湾城九期','万科城花园','广州增城万科城','广州万科城','集美嘉悦,金科集美嘉悦','天伦云境天澄,云境天澄','新力翡翠湾','翡翠湾','翡翠湾雅园','光明当代拾光里','中基·文博府','文博苑','中基文博府','咸阳龙湖-上城','咸阳龙湖・上城','龙湖・上城','龙湖•上城','龙湖上城','龙湖-上城','龙湖·天璞','龙湖天璞','中交白兰春晓','中交白兰春晓苑','白兰春晓中交','中铁琉森水岸天境','中铁·琉森水岸天境','中铁•琉森水岸','中铁琉森水岸','琉森水岸','颐璟名庭','碧桂园•松湖明珠','碧桂园松湖天悦','龙湖-春江天越','龙湖·春江天越','龙湖•春江天越','龙湖春江天越','保利-和光尘樾','保利·和光尘樾','保利•和光尘樾','保利和光尘樾','天津金地藝墅家','藝墅家·酩悦','金地兿墅家-酩悦','金地兿墅家·酩悦','金地墅家','金地艺墅家','金地藝墅家','金地藝墅家-酩悦','金地藝墅家·酩悦','金地藝墅家•酩悦','金地藝墅家酩悦','华润置地御华府','御华府','大东海-晋棠府','大东海·晋棠府','大东海•晋棠府','大东海晋棠府','保利领秀前城冠江墅组团','保利•领秀前城','保利·领秀前城禧悦都','保利领秀前城','世茂滨江府','滨江府1913','滨江府','润和滨江府','凤汇壹品居','凤汇壹品居宸园','旭辉宝龙凤汇壹品','新城明昱东方','明昱东方','泾河雅居乐花园,雅居乐北城雅郡') ")

newest=con.query("select alias_name floor_name,newest_id from dws_db_prd.dws_newest_alias where newest_id in ('83f23fd918398971c7295b3c07ed1967','7eb9ef20aea0b19c97fe839420e4350d','73d65da4561d6fcf738af55ff3c21467','495f6fbb3f35bdd14c7d4efb4022004a','8417be8224c97a16ce4935d8fc95a0e5','e86a44841446dd9a2edc06a6b22bd924','50976be57f9a8c5058813d3a6d34b01d','c10d68db25053e2e0bc0746e2ecfc2f6','05ed67003a563f6df6188423dddf9a64','08612952d30126bb87cf50554cf8cf60','7bef6dfa7fca742e5622dc2b25470300','783d390239928e4bbdd352004f8587dd','9aa57c09bf9c11eb86162cea7f6c2bde','1d427991cff480ac5b8b54c17b5b6d08','459f83383975b96704dea6e5a632b769','9ae729fcbf9c11eb86162cea7f6c2bde','9b8bb916bf9c11eb86162cea7f6c2bde','2bff59fe941afe48e1298945f0be3e88','5be89fa35ffcc06a267ce0da6fc38789','b493054673950d870d65ce6b62e11b69','9c053f0bbf9c11eb86162cea7f6c2bde','a28b6653c400c1f8509764079d1349d0','9a6ab320bf9c11eb86162cea7f6c2bde','a02255ccee0910d630369043f1a8ec5a','9ada1d3dbf9c11eb86162cea7f6c2bde')") 

issue_offer = pd.merge(issue_offer,newest,how='left',on=['floor_name'])


# In[]:
df = issue_offer.groupby(['newest_id','imei']).count().reset_index()
df['period'] = date_quarter


# In[10]:
df.to_csv('C:\\Users\\86133\\Desktop\\df.csv')


# result
print('>>>>>>>Done')



#In[]
browse_imei=con.query("SELECT newest_id,imei FROM dwb_db.a_dwb_customer_browse_log where visit_date>='2021-01-01' and visit_date<='2021-06-30' and newest_id in ('83f23fd918398971c7295b3c07ed1967','7eb9ef20aea0b19c97fe839420e4350d','73d65da4561d6fcf738af55ff3c21467','495f6fbb3f35bdd14c7d4efb4022004a','8417be8224c97a16ce4935d8fc95a0e5','e86a44841446dd9a2edc06a6b22bd924','50976be57f9a8c5058813d3a6d34b01d','c10d68db25053e2e0bc0746e2ecfc2f6','05ed67003a563f6df6188423dddf9a64','08612952d30126bb87cf50554cf8cf60','7bef6dfa7fca742e5622dc2b25470300','783d390239928e4bbdd352004f8587dd','9aa57c09bf9c11eb86162cea7f6c2bde','1d427991cff480ac5b8b54c17b5b6d08','459f83383975b96704dea6e5a632b769','9ae729fcbf9c11eb86162cea7f6c2bde','9b8bb916bf9c11eb86162cea7f6c2bde','2bff59fe941afe48e1298945f0be3e88','5be89fa35ffcc06a267ce0da6fc38789','b493054673950d870d65ce6b62e11b69','9c053f0bbf9c11eb86162cea7f6c2bde','a28b6653c400c1f8509764079d1349d0','9a6ab320bf9c11eb86162cea7f6c2bde','a02255ccee0910d630369043f1a8ec5a','9ada1d3dbf9c11eb86162cea7f6c2bde')")

jike_imei = con.query("select imei from temp_db.jike_mingxi")


#In[]
df1 = browse_imei.groupby(['imei']).count().reset_index()
df2 = jike_imei.groupby(['imei']).count().reset_index()
df2 = df2['imei'].apply(lambda x: x[0:14]).reset_index()
df1[df1['imei'].isin(df2['imei'])]


