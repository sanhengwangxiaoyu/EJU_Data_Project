# -*- codeing = utf-8 -*-
# @Time : 2021/6/22 17:02
# @Author :SunHZ
# @File : Newest_Lng_Lat.py
# @Software: PyCharm
import json

import pymysql
import requests


# def get_poi(address):
#     # 输入API问号前固定不变的部分
#     url='https://restapi.amap.com/v5/place/text'
#     # 将两个参数放入字典
#     params = {'key': 'fb91b25523f85a7e5cadd0540c20071a',
#               'address': address}
#     res = requests.get(url, params)
#     # 输出结果为json，将其转为字典格式
#     jsons = json.loads(res.text)
#     return jsons



def gd_jsons(address,city):
    # 输入API问号前固定不变的部分
    url='http://restapi.amap.com/v3/geocode/geo'
    # 将两个参数放入字典
    params = {'key': 'fb91b25523f85a7e5cadd0540c20071a',
              'address': address,
              'city':city}
    res = requests.get(url, params)
    # 输出结果为json，将其转为字典格式
    jsons = json.loads(res.text)
    if jsons['geocodes'] == []:
        # 高德城市
        gd_city = ''
        #高德区域
        gd_county = ''
        #高德经纬度
        gd_lnglat = ''
        #高德经度
        gd_lng = '0'
        #高德纬度
        gd_lat = '0'
        # 城区编号
        gd_region_id = ''
        return gd_city,gd_county,gd_lnglat,gd_lng,gd_lat,gd_region_id
    else:
        #高德城市
        gd_city = jsons['geocodes'][0]['city']
        #高德区域
        gd_county = jsons['geocodes'][0]['district']
        #高德经纬度
        gd_lnglat = jsons['geocodes'][0]['location']
        #高德经度
        gd_lng = gd_lnglat.split(",")[0]
        #高德纬度
        gd_lat = gd_lnglat.split(",")[1]
        # 城区编号
        gd_region_id = jsons['geocodes'][0]['adcode']
        #返回
        return gd_city,gd_county,gd_lnglat,gd_lng,gd_lat,gd_region_id

# def temp_lng_lat():

#     insert_sql = "INSERT INTO temp_db.temp_lng_lat(newest_id,city_id,lng,lat,newest_name,address)VALUES (%s,%s,%s,%s,%s,%s)"

#     db = pymysql.connect(host="172.28.36.77",user="mysql",password="egSQ7HhxajHZjvdX",database="dws_db",charset='utf8')
#     # 使用 cursor() 方法创建一个游标对象 cursor
#     cursor = db.cursor()
#     # 使用 execute()  方法执行 SQL 查询
#     sql = (" SELECT a.*,b.newest_name,b.address FROM ( SELECT a0.newest_id,a1.city_id,a1.lat,a1.lng FROM dws_db.dws_newest_period_admit a0 "
#            " LEFT JOIN dws_db.dws_newest_info a1 ON a1.newest_id=a0.newest_id WHERE a0.dr=0 AND a0.newest_id NOT IN "
#            " (SELECT newest_id FROM dwb_db.dwb_newest_info WHERE lat>90)"
#            " UNION "
#            " SELECT a0.newest_id,a1.city_id,a1.lng lat,a1.lat lng "
#            " FROM dws_db.dws_newest_period_admit a0  "
#            " LEFT JOIN dws_db.dws_newest_info a1 ON a1.newest_id=a0.newest_id  "
#            " WHERE a0.dr=0 AND a0.newest_id IN (SELECT newest_id FROM dwb_db.dwb_newest_info WHERE lat>90) "
#            " ) a LEFT JOIN dws_db.dws_newest_info b ON b.newest_id=a.newest_id "
#            " WHERE a.newest_id IN (SELECT DISTINCT newest_id FROM dws_newest_period_admit "
#            " WHERE dr=0 AND newest_id NOT IN (SELECT DISTINCT newest_id FROM dws_tag_purchase_poi_copy1))"
#            " AND a.lat=0")
#     cursor.execute(sql)
#     datas = cursor.fetchall()
#     for data in datas:
#         jsons = gd_jsons(data[5])
#         lng = jsons[3]
#         lat = jsons[4]
#         values = (str(data[0]), str(data[1]),str(lng),str(lat),str(data[4]),str(data[5]))
#         cursor.execute(insert_sql,values)
#         print(insert_sql)
#         db.commit()
#     db.close()

# def update_gd_city():
#     db = pymysql.connect(host="172.28.36.77", user="mysql", password="egSQ7HhxajHZjvdX", database="odsdb",charset='utf8')
#     # 使用 cursor() 方法创建一个游标对象 cursor
#     cursor = db.cursor()
#     # 使用 execute()  方法执行 SQL 查询
#     '''
#     sql = " select uuid,city_id,newest_name,address,city_name,gd_city,gd_county from ori_newest_info_cust_202106 where uuid in " \
#           " (select uuid from ori_newest_info_base where (city_id,newest_name) in (" \
#           " select city_id,newest_name from ori_newest_info_base where remark is null group by city_id,newest_name having count(uuid)>1" \
#           " ) and remark is null ) order by city_id,newest_name "
#     '''
#     sql = " select uuid,city_id,newest_name,address,city_name,gd_city,gd_county,gd_lng,gd_lat from ori_newest_info_cust_202106 " \
#           " where city_name != gd_city "
#     cursor.execute(sql)
#     datas = cursor.fetchall()
#     for data in datas:
#         json = gd_jsons(data[3])
#         if(json[0] != ''):
#             city = json[0]
#             county = json[1]
#             gd_lnglat = json[2]
#             gd_lng = json[3]
#             gd_lat = json[4]
#             if city == []:
#                 city = ''
#             if county == []:
#                 county = ''
#             update_sql =  " update ori_newest_info_cust_202106 " \
#                           " set gd_city = '"+city+"',gd_county = '"+county+"',gd_lng = '"+gd_lng+"',gd_lat = '"+gd_lat+"',gd_lnglat = '"+gd_lnglat+"' " \
#                           " where uuid = '"+data[0]+"' "
#             print(update_sql)
#             cursor.execute(update_sql)
#             db.commit()

#update_gd_city()




db = pymysql.connect(host="172.28.36.77", user="mysql", password="egSQ7HhxajHZjvdX", database="odsdb",charset='utf8')
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()
# 使用 execute()  方法执行 SQL 查询
sql = " select uuid,city_id,newest_name,address,city_name,gd_city,gd_county,gd_lng,gd_lat from ori_newest_info_cust_202106 " \
        " where city_name != gd_city "
cursor.execute(sql)
datas = cursor.fetchall()
for data in datas:
    json = gd_jsons(data[0],data[1])
    if(json[0] != ''):
        city = json[0]
        county = json[1]
        gd_lnglat = json[2]
        gd_lng = json[3]
        gd_lat = json[4]
        if city == []:
            city = ''
        if county == []:
            county = ''



# json = gd_jsons('双云公路和云溪北路交叉口','嘉兴市')
print(json)
