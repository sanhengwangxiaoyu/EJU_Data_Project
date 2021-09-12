# -*- codeing = utf-8 -*-
# @Time : 2021/6/25 18:41
# @Author :SunHZ
# @File : gd_poi.py
# @Software: PyCharm


from urllib.parse import quote
from urllib import request
import json

# 根据城市名称和分类关键字获取poi数据
import pymysql


def getpois(keywords):
    i = 1
    poilist = []
    while True:  # 使用while循环不断分页获取数据
        result = getpoi_page(keywords, i)
        # print(result)
        result = json.loads(result)  # 将字符串转换为json
        if result['count'] == '0':
            break
        hand(poilist, result)
        i = i + 1
    return poilist

# 将返回的poi数据装入集合返回
def hand(poilist, result):
    pois = result['pois']
    for i in range(len(pois)):
        poilist.append(pois[i])

# 单页获取pois
def getpoi_page(keywords, page):
    req_url = poi_search_url + "?key=" + amap_web_key + '&extensions=all&keywords=' + quote(
        keywords) + '&offset=25' + '&page=' + str(
        page) + '&output=json'
    data = ''
    with request.urlopen(req_url) as f:
        data = f.read()
        data = data.decode('utf-8')
    return data


def update_newest_info():
    db = pymysql.connect(host="172.28.36.77", user="mysql", password="egSQ7HhxajHZjvdX", database="odsdb",charset='utf8')
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    # 使用 execute()  方法执行 SQL 查询
    sql = " select uuid,city_id,newest_name,address,city_name,gd_city,gd_county,gd_lng,gd_lat from ori_newest_info_cust_202106 " \
          " where city_name != gd_city "
    cursor.execute(sql)
    datas = cursor.fetchall()
    for data in datas:
        poi = data[3]
        pois = getpois(poi)
        if pois != []:
            gd_city = pois[0]['cityname']
            gd_county = pois[0]['adname']
            gd_lnglat = pois[0]['location']
            gd_lng = gd_lnglat.split(",")[0]
            gd_lat = gd_lnglat.split(",")[1]
            #print(gd_city, gd_county, gd_lnglat, gd_lng, gd_lat)
            update_sql =  " update ori_newest_info_cust_202106 " \
                          " set gd_city = '"+gd_city+"',gd_county = '"+gd_county+"',gd_lng = '"+gd_lng+"',gd_lat = '"+gd_lat+"',gd_lnglat = '"+gd_lnglat+"' " \
                          " where uuid = '"+data[0]+"' "
            print(update_sql)
            cursor.execute(update_sql)
            db.commit()



if __name__ == '__main__':

    poi_search_url = "http://restapi.amap.com/v3/place/text"
    amap_web_key = 'fb91b25523f85a7e5cadd0540c20071a'
    #update_newest_info()


    poi  = '归十东路保利城'
    pois = getpois(poi)
    print(pois)
    if pois != []:
        gd_city = pois[0]['cityname']
        gd_county = pois[0]['adname']
        gd_lnglat = pois[0]['location']
        gd_adcode = pois[0]['adcode']
        gd_lng = gd_lnglat.split(",")[0]
        gd_lat = gd_lnglat.split(",")[1]
        print(gd_city,gd_county,gd_lnglat,gd_lng,gd_lat,gd_adcode)







