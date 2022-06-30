# -*- codeing = utf-8 -*-
# @Time : 2021/5/31 15:21
# @Author :SunHZ
# @File : Get_Newest_Info_Base.py
# @Software: PyCharm


import json
import re
import pymysql
import requests
from requests.exceptions import RequestException
from lxml import etree

session = requests.session()
session.verify = False
requests.packages.urllib3.disable_warnings()

#部分城市
citys = {
    '上海市':'https://sh.fang.ke.com',
    '广州市':'https://gz.fang.ke.com',
    '深圳市':'https://sz.fang.ke.com',
    '北京市':'https://bj.fang.ke.com',
    '东莞市':'https://dg.fang.ke.com',
    '佛山市':'https://fs.fang.ke.com',
    '苏州市':'https://su.fang.ke.com',
    '青岛市':'https://qd.fang.ke.com',
    '长沙市':'https://cs.fang.ke.com',
    '天津市':'https://tj.fang.ke.com',
    '郑州市':'https://zz.fang.ke.com',
    '西安市':'https://xa.fang.ke.com',
    '合肥市':'https://hf.fang.ke.com',
    '重庆市':'https://cq.fang.ke.com',
    '成都市':'https://cd.fang.ke.com',
    '沈阳市':'https://sy.fang.ke.com',
    '杭州市':'https://hz.fang.ke.com',
    '武汉市':'https://wh.fang.ke.com',
    '南京市':'https://nj.fang.ke.com',
    '惠州市':'https://hui.fang.ke.com',
    '中山市':'https://zs.fang.ke.com',
    '泉州市':'https://quanzhou.fang.ke.com',
    '无锡市':'https://wx.fang.ke.com',
    '石家庄市':'https://sjz.fang.ke.com',
    '南宁市':'https://nn.fang.ke.com',
    '昆明市':'https://km.fang.ke.com',
    '济南市':'https://jn.fang.ke.com',
    '大连市':'https://dl.fang.ke.com',
    '哈尔滨市':'https://hrb.fang.ke.com',
    '长春市':'https://cc.fang.ke.com',
    '福州市':'https://fz.fang.ke.com',
    '海口市':'https://hk.fang.ke.com',
    '宁波市':'https://nb.fang.ke.com',
    '厦门市':'https://xm.fang.ke.com',
    '唐山市':'https://ts.fang.ke.com',
    '保定市':'https://bd.fang.ke.com',
    '南通市':'https://nt.fang.ke.com',
    '嘉兴市':'https://jx.fang.ke.com',
    '肇庆市':'https://zq.fang.ke.com',
    '珠海市':'https://zh.fang.ke.com',
    '汕头市':'https://st.fang.ke.com',
    '贵阳市':'https://gy.fang.ke.com',
    '三亚市':'https://san.fang.ke.com',
    '扬州市':'https://yz.fang.ke.com',
    '徐州市':'https://xz.fang.ke.com',
    '常州市':'https://changzhou.fang.ke.com',
    '南昌市':'https://nc.fang.ke.com',
    '九江市':'https://jiujiang.fang.ke.com',
    '淄博市':'https://zb.fang.ke.com',
    '宝鸡市':'https://baoji.fang.ke.com',
    '咸阳市':'https://xianyang.fang.ke.com',
    '温州市':'https://wz.fang.ke.com',
    '湖州市':'https://huzhou.fang.ke.com',
    '丽水市':'https://lishui.fang.ke.com',
    '绍兴市':'https://sx.fang.ke.com',
    '赣州市':'https://ganzhou.fang.ke.com',
    '烟台市':'https://yt.fang.ke.com',
    '济宁市':'https://jining.fang.ke.com',
}

def get_one_page(url):
    headers = {"Content-Type": "application/x-www-form-urlencoded",
               "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) "
                             "Chrome/69.0.3497.100 Safari/537.36",
               "Referer": "http://san,fang.ke.com/"}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.text
        return response.text
    except RequestException:
        return None

def parse_one_page(html, offset):
    pattern = re.compile('<li class="resblock-list post_ulog_exposure_scroll has-results".*?>.*?title="('
                         + '.*?)".*?data-original="(.*?)".*?resblock-type".*?">(.*?)</span>.*?<span style="background.*?">(.*?)</span>.*?</div>.*?<i class="icon location-icon"></i>('
                         + '.*?)</a>.*?<a class="resblock-room".*?href="(.*?)".*?>(.*?)</a>.*?<div class="resblock-tag">.*?<span>(.*?)</span>.*?<span>(.*?)</span>.*?<span>('
                         + '.*?)</span>.*?<span>(.*?)</span>.*?</div>.*?number">(.*?)</span>.*?</li>', re.S)

    items = re.findall(pattern, html)
    for item in items:
        check = re.findall('area', item[6])
        if check:
            pattern = re.compile('area">(.*?)</span>', re.S)
            item6 = re.findall(pattern, item[6])
            item6 = item6[0]
        else:
            item6 = '暂无'
        yield {
            'page': 'pg'+ str(offset),
            'name': item[0],
            'image': item[1],
            'status': item[2],
            'type': item[3],
            'address': item[4].strip(),
            'link': item[5],
            'area': item6,
            'label': item[7] + ',' + item[8] + ',' + item[9] + ',' + item[10],
            'price': item[11]
        }

def extract_first(data):
    if data != []:
        return data[0]
    return ''

def trim(word):
    word=re.sub('\s+','',word)
    word=re.sub('[\u2002|\u3000|\xa0]+','',word)
    word=word.replace('&nbsp;','').replace('\\t','').replace('\\n','').replace('\\xa0','')
    return word

def getText(html,keyword):
    path = html.xpath(f'//span[@class="label" and contains(text(),"{keyword}")]/../span[@class="label-val"]')
    if path != []:
        return trim(path[0].xpath('string(.)'))
    else:
        return ''

def get(uri):
    n = 0
    while True:
        if n > 3:
            break
        try:
            res = session.get(uri)
            res.encoding = 'utf-8'
            html = etree.HTML(res.text)
            return html, res.text
        except Exception as e:
            print(e)
            print('error ',uri)
            n += 1
def gd_jsons(address,city,lat,lon,jingweidu):
    if  '上海周边' in address :
        address = address.replace('上海周边','')
    else:
        address = address
    # 输入API问号前固定不变的部分
    url='http://restapi.amap.com/v3/geocode/geo'
    # 将两个参数放入字典
    params = {'key': 'fb91b25523f85a7e5cadd0540c20071a',
              'address': address}
    res = requests.get(url, params)
    # 输出结果为json，将其转为字典格式
    jsons = json.loads(res.text)
    if jsons['geocodes'] == []:
        # 高德城市
        gd_city = city
        #高德区域
        gd_county = ''
        #高德经纬度
        gd_lnglat = jingweidu
        #高德经度
        gd_lng = lon
        #高德纬度
        gd_lat = lat
        # 城区编号
        gd_region_id = 0
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

into = "INSERT INTO odsdb.ori_newest_info_base_ls(uuid,platform,url,newest_name,city_name," +\
       "county_name,block_name,ori_lnglat,sale_status,address," +\
       "sale_address,sale_phone,alias,layout,total_price,unit_price,recent_opening_time," +\
       "recent_delivery_time,issue_date,issue_number,developer,investor,brander," +\
       "land_area,building_area,green_rate,volume_rate,building_type,property_type" +\
       ",household_num,right_term,property_comp,property_fee,park_rate,park_num," +\
       "building_num,avg_distance,ong_park_num,ung_park_num,floor_num," +\
       "ambitus,agent,decoration,layout_pic,estate_pic,heat_mode,power_mode,water_mode," +\
       "ring_locate,characters,deco_level,mate_equip,designer,builder,orientation,progress,provide_sche,land_time," +\
       "gd_city,gd_county,gd_lnglat,gd_lng,gd_lat,city_id,county_id)" +\
       "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

def main():
    db = pymysql.connect(host="172.28.36.77", user="mysql", password="egSQ7HhxajHZjvdX", database="temp_db", charset='utf8')
    cursor = db.cursor()
    sql = (" select city_name,floor_name,newest_id from dwd_db.dwd_data_log_customer_detail_full_floor_name where create_time is not null group by city_name,floor_name,newest_id limit 3562,1000 ")
    cursor.execute(sql)
    data = cursor.fetchall()
    for list in data:
        url = citys[list[0]]
        floor_name = list[1].replace('.','').replace('·','').replace('•','')
        print(floor_name)
        print(list[2])
        urls = ""+url+"/loupan/rs"+floor_name+"/"
        #print(urls)
        html = get_one_page(urls)
        for item in parse_one_page(html, 1):
            if "#" in item['link']:
                #uri = url + item['link'].split("#")[0]+ 'xiangqing/'
                uri = url + item['link'].split("#")[0]
                html, text = get(uri)
                html2, text2 = get(uri + 'xiangqing/')
                # 楼盘名称
                name = extract_first(html.xpath('//h2[@class="DATA-PROJECT-NAME"]/text()'))
                # 城市
                city = list[0]
                #uuid
                uuid = list[2]
                # 所在区域
                try:
                    region = html.xpath('//*[contains(@data-ulog-exposure, "xinfangpc_show=20027&location=2")]/text()')[0]
                except:
                    region = ''
                # 所在板块
                try:
                    bankuai = \
                    html.xpath('//*[contains(@data-ulog-exposure, "xinfangpc_show=20027&location=3")]/text()')[0]
                except:
                    bankuai = ''
                # 经纬度
                lat = extract_first(re.findall('"latitude":"([0-9\.]+)"', text2))
                lon = extract_first(re.findall('"longitude":"([0-9\.]+)"', text2))
                jingweidu = lat + ',' + lon
                # 楼盘地址
                address = getText(html2,'楼盘地址')
                # 售楼处地址
                slc_dz = getText(html2,'售楼处地址')
                # 售楼处电话
                slc_dh = getText(html2,'售楼处电话')
                pro = uri.split('_')[1].split('/')[0]
                url3 = 'https://ex.ke.com/sdk/recommend/html/100010998?hdicCityId=110000&id=100010998&mediumId=100000036&projectName=' + pro + '&projectType=&elementId=ke_agent_1&required400=true'
                text3 = trim(url3)
                m = re.findall(r'phone400":"(.*?)",', text3, re.S)
                if m:
                    slc_dh = trim(m[0])
                # 别名
                bm = extract_first(html.xpath('//div[@class="other-name"]/text()'))
                # 曾用名
                cym = extract_first(html.xpath('//div[@class="ceng-name"]/text()'))
                all_ids = html.xpath('//ul[@class="clear house-det"]/@data-id')
                all_ids = set(all_ids)

                hxs = html.xpath('//div[@class="common-frame-modal-wrap"]/div[2]/div/div[2]/div[1]/ul/li[1]/ul')
                # 楼盘户型
                a = []
                hxts = []
                for _id in all_ids:
                    h = html.xpath('//ul[@data-id="' + _id + '"]')[0]
                    house_type = trim(h.xpath('./div[@class="card-content"]/div[@class="content-title"]/text()')[0])
                    house_price = trim(
                        h.xpath('./div[@class="card-content"]/div[@class="content-price"]')[0].xpath('string(.)'))
                    house_area = trim(h.xpath('./div[@class="card-content"]/div[@class="content-area"]/text()')[0])
                    a.append('|'.join([house_type, house_price, '', house_area]))
                    hxts.append(extract_first(h.xpath('./div[@class="card-img"]/img/@src')))
                    inq = ''
                house_type = ''
                # 户型总价
                all_price = getText(html,'楼盘总价')
                # 套均总价（同户型平均价）
                ps = []
                unit_price = '||'.join(ps)
                # 户型面积
                areas = []
                for x in hxs:
                    try:
                        areas.append(trim(x.xpath('//div[@class="content-area"]/text()')[0]))
                    except:
                        pass
                area = '||'.join(areas)
                # 楼盘总价
                al_price = getText(html, '楼盘总价')
                # 楼盘单价
                danjia = getText(html2, '参考价格')
                # 最早开盘时间
                kaipan = extract_first(html.xpath('//span[contains(text(), "最新开盘")]/../span[@class="content"]/text()'))
                # 最早交房时间
                jiaofang = getText(html2, '最近交房')
                # 楼盘纪事
                lpjs = ''
                # 发证日期
                fazheng = extract_first(html2.xpath('//th[contains(text(), "发证时间")]/../../tr[2]/td[2]/text()'))
                # 预售证号
                yszs = html2.xpath('//h2[@id="xTable"]/following-sibling::table[1]/tr')
                lds = ','.join(html2.xpath('//span[@class="fq-fqbuild"]/span/text()'))
                yszss = []
                for ysz in yszs:
                    tds = ysz.xpath('./td')
                    if len(tds) == 0:
                        continue
                    yszss.append(tds[0].xpath('string(.)'))
                zhenghao = '|'.join(yszss)
                # 开发商
                kaifashang = getText(html2, '开发商')
                # 投资商
                touzishang = ''
                # 品牌商
                pinpaishang = ''
                # 总占地面积
                zzdmj = getText(html2, '占地面积')
                # 总建筑面积
                zjzmj = getText(html2, '建筑面积')
                # 建筑风格
                jzfg = ''
                # 绿化率
                lvl = getText(html2, '绿化率')
                # 容积率
                rjl = getText(html2, '容积率')
                # 建筑类型
                jzlx = getText(html2, '建筑类型')
                # 物业类型
                wylx = getText(html2, '物业类型')
                # 规划户数
                ghhs = getText(html2, '规划户数')
                # 产权年限
                cqnx = getText(html2, '产权年限')
                # 物业公司
                wygs = getText(html2, '物业公司')
                # 物业费
                wyf = getText(html2, '物业费')
                # 车位配比
                cwb = getText(html2, '车位配比')
                # 车位数量
                cws = getText(html2, '车位：')
                # 车位价格
                cwjg = ''
                # 朝向
                cx = ''
                # 楼栋数
                lds = getText(html2, '楼栋数') or int(len(set(lds.split(','))))
                # 平均楼间距
                pjljj = getText(html2, '平均楼间距')
                # 地上车位数
                dscws = re.findall('地上.+?(\d+)', cws)
                if dscws != []:
                    dscws = dscws[0]
                else:
                    dscws = ''
                # 地下车位数
                dxcws = re.findall('地下.+?(\d+)', cws)
                if dxcws != []:
                    dxcws = dxcws[0]
                else:
                    dxcws = ''
                # 建筑密度
                jzmd = getText(html, '建筑密度')
                # 建筑层数
                jzcs = getText(html, '楼层状况')
                # 周边配套（园区配套）
                try:
                    zbpt = html2.xpath('//span[@class="label"  and contains(text(), "周边规划")]/../*[@id="around_txt"]')[
                        0].xpath('string(.)')
                except:
                    pass
                # 非机动车停车位
                fjdctcw = getText(html, '非机动车停车位')
                # 代理商
                dls = getText(html, '代理商')
                # 装修情况
                zxqk = getText(html, '装修情况')
                # 户型图
                hxt = '|'.join(hxts)
                # 楼盘效果图
                lpxgt = ''.join(html2.xpath('//*[contains(text(), "效果图")]/../img/@src'))
                # 不利因素
                blyx = ''
                # 付款方式
                fkfs = ''
                xszt = extract_first(html.xpath('//*[@class="tag-item sell-type-tag"]/text()'))
                # 供暖方式
                gnfs = getText(html2, '供暖方式')
                # 供电方式
                gdfs = getText(html2, '供电方式')
                # 供水方式
                gsfs = getText(html2, '供水方式')
                # 环线位置
                hxwz = ''
                # 项目特色
                xmts = ''
                # 装修标准
                zxbz = ''
                # 建材设备
                jcsb = ''
                # 建筑设计单位
                jzsjdw = ''
                # 施工单位
                sgdw = ''
                # 楼盘朝向
                lpcx = ''
                # 工程进度
                gcjd = ''
                # 加推时间
                h3, t3 = get(uri + 'dongtai/')
                lis = h3.xpath('//*[@class="big-left fl"]/div[@class="dongtai-one for-dtpic"]')
                dts = []
                for li in lis:
                    tag = extract_first(li.xpath('./a/span[@class="a-tag"]/text()'))
                    title = extract_first(li.xpath('./a/span[@class="a-title"]/text()'))
                    ti = extract_first(li.xpath('./a/span[@class="a-time"]/text()'))
                    content = trim(li.xpath('./div')[0].xpath('string(.)'))
                    dts.append('|'.join([trim(x) for x in [tag, title, content, ti]]))
                jtsj = '^'.join(dts)
                # 拿地事件
                ndsj = ''

                # 获取高德信息
                print(item['address'],city)
                jsons = gd_jsons(item['address'],city,lat,lon,jingweidu)
                # jsons = gd_jsons(address, city, lat, lon, jingweidu)
                # print(jsons)
                # 高德城市
                gd_city = jsons[0]
                # 高德区域
                gd_county = jsons[1]
                # 高德经纬度
                gd_lnglat = jsons[2]
                # 高德经度
                gd_lng = jsons[3]
                # 高德纬度
                gd_lat = jsons[4]
                # 高德城区编号
                region_id = jsons[5]

                print(gd_city)
                city_id_sql = " select distinct city_id from dws_db.dws_area_detail  where city_name = '"+gd_city+"'  "
                # print(city_id_sql)
                cursor.execute(city_id_sql)
                gd_citys = cursor.fetchone()
                print(gd_citys)
                if gd_citys is None:
                    city_id = 0
                else:
                    city_id = gd_citys[0]
                data1 = [uuid,'贝壳', uri, name, city, region, bankuai, jingweidu,
                        xszt, address, slc_dz, slc_dh, bm, '^'.join(a), al_price,
                        danjia, kaipan, jiaofang, lpjs, fazheng, zhenghao,
                        kaifashang, touzishang, pinpaishang, zzdmj, zjzmj,
                        lvl, rjl, jzlx, wylx, ghhs, cqnx, wygs, wyf, cwb,
                        cws, lds, pjljj, dscws, dxcws, jzcs, zbpt, dls, zxqk,
                        hxt, lpxgt, gnfs, gdfs, gsfs, hxwz, xmts, zxbz,
                        jcsb, jzsjdw, sgdw, lpcx, gcjd, jtsj, ndsj]
                data  = {'uuid': uuid,'platform': '贝壳', 'url': uri, 'newest_name': name, 'city': city,
                        'county_name': region, 'block_name': bankuai, 'ori_lnglat': jingweidu,
                        'sale_status': xszt, 'address': address, 'sale_address': slc_dz,
                        'sale_phone': slc_dh, 'alias': bm, 'layout': '^'.join(a), 'total_price': al_price,
                        'unit_price': danjia, 'recent_opening_time': kaipan, 'recent_delivery_time': jiaofang,
                        'issue_date': fazheng, 'issue_number': zhenghao,
                        'developer': kaifashang, 'investor': touzishang, 'brander': pinpaishang,
                        'land_area': zzdmj, 'building_area': zjzmj,
                        'green_rate': lvl, 'volume_rate': rjl, 'building_type': jzlx, 'property_type': wylx,
                        'household_num': ghhs, 'right_term': cqnx, 'property_comp': wygs,
                        'property_fee': wyf, 'park_rate': cwb, 'park_num': cws, 'building_num': lds,
                        'avg_distance': pjljj, 'ong_park_num': dscws, 'ung_park_num': dxcws,
                        'floor_num': jzcs, 'ambitus': zbpt, 'agent': dls, 'decoration': zxqk,
                        'layout_pic': hxt, 'estate_pic': lpxgt, 'heat_mode': gnfs, 'power_mode': gdfs,
                        'water_mode': gsfs, 'ring_locate': hxwz, 'character': xmts, 'deco_level': zxbz,
                        'mate_equip': jcsb, 'designer': jzsjdw, 'builder': sgdw, 'orientation': lpcx,
                        'progress': gcjd, 'provide_sche': jtsj, 'land_time': ndsj,
                        'gd_city': gd_city, 'gd_county': gd_county, 'gd_lnglat': gd_lnglat,
                        'gd_lng': gd_lng, 'gd_lat': gd_lat, 'city_id': city_id,
                        'county_id': region_id
                         }
                ambitus = trim(data['ambitus'])
                alias = trim(data['alias'])
                values = (str(data['uuid']), str(data['platform']), str(data['url']),
                          str(data['newest_name']), str(data['city']),
                          str(data['county_name']), str(data['block_name']),
                          str(data['ori_lnglat']), str(data['sale_status']),
                          str(data['address']), str(data['sale_address']),
                          str(data['sale_phone']), alias,
                          str(data['layout']), str(data['total_price']),
                          str(data['unit_price']), str(data['recent_opening_time']),
                          str(data['recent_delivery_time']), str(data['issue_date']),
                          str(data['issue_number']),str(data['developer']), str(data['investor']),
                          str(data['brander']), str(data['land_area']),
                          str(data['building_area']), str(data['green_rate']),
                          str(data['volume_rate']), str(data['building_type']),
                          str(data['property_type']), str(data['household_num']),
                          str(data['right_term']), str(data['property_comp']),
                          str(data['property_fee']), str(data['park_rate']),str(data['park_num']),
                          str(data['building_num']), str(data['avg_distance']),
                          str(data['ong_park_num']), str(data['ung_park_num']),
                          str(data['floor_num']), ambitus,
                          str(data['agent']), str(data['decoration']),
                          str(data['layout_pic']), str(data['estate_pic']),
                          str(data['heat_mode']), str(data['power_mode']),
                          str(data['water_mode']), str(data['ring_locate']),
                          str(data['character']), str(data['deco_level']),
                          str(data['mate_equip']), str(data['designer']),
                          str(data['builder']), str(data['orientation']),
                          str(data['progress']),str(data['provide_sche']),
                          str(data['land_time']),str(data['gd_city']),
                          str(data['gd_county']),str(data['gd_lnglat']),
                          str(data['gd_lng']),str(data['gd_lat']),
                          str(data['city_id']),str(data['county_id']))
                #print(into)
                #print(values)
                cursor.execute(into, values)
                db.commit()
    db.close()
if __name__ == '__main__':
        main()
