
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

# # 单体标签解析
data = con.query("select id,jdata from  origin_estate.ori_jike_personal_tag_i where dr = 0")

tag_name = con.query("select tag_id,tag_type,tag_name,tag_en_name from odsdb.acq_jike_tag where dr =0 and tag_clazz = 'P' ")


#In[]
def str_transform(s):
    if s == '是':
        return 'Y'
    elif s == '否':
        return 'N'
    elif s == '无':
        return '无房'
    elif s == '有':
        return '有房'
    else :
        return s
df_jdata = data
df_jdata['data'] = df_jdata['jdata'].apply(lambda x: '{"'+re.sub(r'\{|\}|\[|\]|\'\',|, \'\'|\'|:\d{1,9}天','',x.replace('"','\'').replace('=','" : "').replace(', J','", "J'))+'"}')
df_jdata['data'] = df_jdata['data'].map(lambda x: eval(x))
df_jdata = pd.DataFrame(df_jdata['data'].values.tolist())
df_jdata_col = df_jdata.columns.tolist()
tag_name_col = tag_name['tag_id'].values.tolist()
surplus_col = [x for x in df_jdata_col if x not in tag_name_col]
surplus_col.remove('imei')
df_jdata = df_jdata.drop(surplus_col,axis=1)
df_jdata.rename(columns={"id":"ori_id","J00085" : "sex", "J00081" : "age_group", "J00087" : "career", "J00084" : "is_college_stu", "J00086" : "education", "J00080" : "marriage", "J00083" : "have_child", "J00045" : "have_car", "J00050" : "income_level", "J00051" : "consume_power", "J00092" : "mobile_model", "J00088" : "mobile_brand", "J00093" : "system", "J00062" : "consume_prefer", "J00082" : "is_chinese", "J00091" : "mobile_value", "J00041" : "active_city", "J00024" : "workday_traffic", "J00025" : "holiday_traffic", "J00065" : "estate_app", "J00094" : "estate_app_freq", "J00047" : "decorate_demand", "J00027" : "long_traffic_prefer", "J00008" : "travel_abroad", "J00043" : "resi_district_price", "J00106" : "mall_prefer", "J00104" : "active_comarea", "J00042" : "have_house", "J00046" : "car_brand", "J00019" : "in_province_travel", "J00020" : "out_province_travel", "J00023" : "have_pet", "J00077" : "bank_prefer", "J00075" : "invest_prefer", "J00063" : "loan_prefer", "J00064" : "lottery_prefer", "J00079" : "live_prefer", "J00078" : "game_prefer", "J00072" : "health_care", "J00074" : "entertainment", "J00073" : "food_category", "J00071" : "convenience", "J00101" : "social_active_level", "J00070" : "social_prefer", "J00068" : "educate_prefer", "J00059" : "offline_brand_prefer", "J00058" : "offline_shop_prefer", "J00060" : "offline_brand_type", "J00112" : "is_visit_mall", "J00033" : "is_visit_gym", "J00124" : "is_visiti_hotel", "J00123" : "hotel_duration", "J00103" : "hotel_prefer_poi", "J00120" : "ent_prefer_poi", "J00118" : "ent_str_prefer_poi", "J00004" : "food_prefer_type", "J00003" : "food_prefer_poi", "J00034" : "gas_prefer_poi", "J00035" : "car_repair_poi", "J00119" : "weekend_entertain", "J00113" : "weekent_purchase", "J00029" : "poi_prefer_cate", "J00030" : "poi_str_prefer_cate", "J00021" : "is_weekend_travel", "J00017" : "travel_dest_type", "J00028" : "long_traffic_str_prefer", "J00014" : "travel_abroad_dest", "J00010" : "travel_month_prefer", "J00005" : "bustrip_city", "J00015" : "travel_prefer_poi", "J00013" : "travel_poi_duration", "J00102" : "resi_comarea", "J00130" : "insp_house_city", "J00131" : "insp_house_county", "J00132" : "insp_house_poi", "J00127" : "insp_house_freq", "J00126" : "hotel_brand_prefer", "J00125" : "hotel_level_prefer", "J00107" : "mall_prefer_lnglat", "J00133" : "insp_house_lnglat", "J00089" : "equip_cate", "J00040" : "resi_address", "J00039" : "work_address", "J00038" : "resi_province", "J00037" : "resi_city", "J00036" : "resi_county", "J00022" : "travel_intention_level", "J00069" : "manage_money_level", "J00066" : "realestate_app_prefer", "J00067" : "shopping_prefer", "J00076" : "shopping_online_prefer", "J00044" : "estate_rent", "J00007" : "travel_country", "J00134" : "app_install_count", "J00129" : "week_inspection_days", "J00061" : "consume_time_prefer", "J00105" : "month_shopping_count", "J00009" : "travel_time_prefer", "J00018" : "travel_count_prefer", "J00016" : "interest_type_prefer", "J00121" : "hotel_price_prefer", "J00026" : "short_traffic_type", "J00001" : "restaurant_customer_order", "J00002" : "hallfood_avg_consum_time", "J00006" : "frequent_switch_city", "J00011" : "vacation_duration_prefer", "J00012" : "foreign_tourism_top3", "J00031" : "daytime_consum_place_top10", "J00032" : "night_consum_place_top10", "J00048" : "rent_will", "J00049" : "buy_house_will", "J00052" : "rest_consume_level", "J00053" : "prent_child_consume_level", "J00054" : "beauty_care_consume_level", "J00055" : "recreat_entert_consume_level", "J00056" : "clothing_shoes_consume_level", "J00057" : "retail_consumpt_consume_level", "J00095" : "threemon_visit_rental_app_num", "J00096" : "threemon_visit_rental_app_newdate", "J00097" : "threemon_install_rental_app_num", "J00098" : "threemon_visit_rentalcompre_app_num", "J00099" : "threemon_visit_rentalcompre_app_newdate", "J00100" : "threemon_install_rentalcompre_app_num", "J00108" : "often_child_stores_avg_price", "J00109" : "often_child_brands_avg_price", "J00110" : "often_shop_stores_avg_price", "J00111" : "offline_shop_prefer_level", "J00114" : "often_life_server_stores_avg_price", "J00115" : "often_life_server_brands_avg_price", "J00116" : "often_recreat_entert_stores_avg_price", "J00117" : "often_recreat_entert_brands_avg_price", "J00122" : "hotel_brand_perfer_compare_avg_price", "J00128" : "month_distinc_visit_house_num"},inplace=True)
df_jdata['date'] = '20220322'
df_jdata['ori_table'] = 'origin_estate.ori_jike_personal_tag_i'
df_jdata['have_car'] = df_jdata['have_car'].apply(lambda x : str_transform(x))
df_jdata['have_house'] = df_jdata['have_house'].apply(lambda x : str_transform(x))

# df_jdata.to_csv(r'C:\Users\86133\Desktop\result.csv')
database = 'dwb_db'
to_dws(df_jdata,'dwb_customer_imei_tag_20220322')

df_jdata

