import os
import random
import traceback
import  urllib3
urllib3.disable_warnings()
import requests
from logg import get_ua, strips
from lxml import etree
import re,math,time,datetime
from selenium import webdriver

#部分城市
citys = {
    '上海':'https://sh.fang.ke.com/loupan/',
    '广州':'https://gz.fang.ke.com/loupan/',
    '深圳':'https://sz.fang.ke.com/loupan/',
    '北京':'https://bj.fang.ke.com/loupan/',
    '东莞':'https://dg.fang.ke.com/loupan/',
    '佛山':'https://fs.fang.ke.com/loupan/',
    '苏州':'https://su.fang.ke.com/loupan/',
    '青岛':'https://qd.fang.ke.com/loupan/',
    '长沙':'https://cs.fang.ke.com/loupan/',
    '天津':'https://tj.fang.ke.com/loupan/',
    '郑州':'https://zz.fang.ke.com/loupan/',
    '西安':'https://xa.fang.ke.com/loupan/',
    '合肥':'https://hf.fang.ke.com/loupan/',
    '重庆':'https://cq.fang.ke.com/loupan/',
    '成都':'https://cd.fang.ke.com/loupan/',
    '沈阳':'https://sy.fang.ke.com/loupan/',
    '杭州':'https://hz.fang.ke.com/loupan/',
    '武汉':'https://wh.fang.ke.com/loupan/',
    '南京':'https://nj.fang.ke.com/loupan/',
    '惠州':'https://hui.fang.ke.com/loupan/',
    '中山':'https://zs.fang.ke.com/loupan/',
    '泉州':'https://quanzhou.fang.ke.com/loupan/',
    '无锡':'https://wx.fang.ke.com/loupan/',
    '石家庄':'https://sjz.fang.ke.com/loupan/',
    '南宁':'https://nn.fang.ke.com/loupan/',
    '昆明':'https://km.fang.ke.com/loupan/',
    '济南':'https://jn.fang.ke.com/loupan/',
    '大连':'https://dl.fang.ke.com/loupan/',
    '哈尔滨':'https://hrb.fang.ke.com/loupan/',
    '长春':'https://cc.fang.ke.com/loupan/',
    '福州':'https://fz.fang.ke.com/loupan/',
    '海口':'https://hk.fang.ke.com/loupan/',
    '宁波':'https://nb.fang.ke.com/loupan/',
    '厦门':'https://xm.fang.ke.com/loupan/',
    '唐山':'https://ts.fang.ke.com/loupan/',
    '保定':'https://bd.fang.ke.com/loupan/',
    '南通':'https://nt.fang.ke.com/loupan/',
    '嘉兴':'https://jx.fang.ke.com/loupan/',
    '肇庆':'https://zq.fang.ke.com/loupan',
    '珠海':'https://zh.fang.ke.com/loupan',
    '汕头':'https://st.fang.ke.com/loupan',
    '贵阳':'https://gy.fang.ke.com/loupan',
    '三亚':'https://san.fang.ke.com/loupan',
    '扬州':'https://yz.fang.ke.com/loupan',
    '徐州':'https://xz.fang.ke.com/loupan',
    '常州':'https://changzhou.fang.ke.com/loupan',
    '南昌':'https://nc.fang.ke.com/loupan',
    '九江':'https://jiujiang.fang.ke.com/loupan',
    '淄博':'https://zb.fang.ke.com/loupan',
    '宝鸡':'https://baoji.fang.ke.com/loupan',
    '咸阳':'https://xianyang.fang.ke.com/loupan',
    '温州':'https://wz.fang.ke.com/loupan',
    '湖州':'https://huzhou.fang.ke.com/loupan',
    '丽水':'https://lishui.fang.ke.com/loupan',
    '绍兴':'https://sx.fang.ke.com/loupan',
    '赣州':'https://ganzhou.fang.ke.com/loupan',
    '烟台':'https://yt.fang.ke.com/loupan',
    '济宁':'https://jining.fang.ke.com/loupan',
}
# cs = ['上海', '广州', '深圳', '北京', '东莞', '佛山', '苏州', '青岛', '长沙', '天津', '郑州', '西安', '合肥', '重庆', '成都', '沈阳', '杭州', '武汉', '南京', '惠州', '中山', '无锡', '石家庄', '南宁', '昆明', '济南', '长春', '福州', '海口', '宁波', '厦门', '唐山', '保定', '南通', '嘉兴',
cs = [
    #'肇庆'],
    # '珠海',
    # '汕头',
    # '贵阳', '北京','三亚', '扬州', '徐州', '常州', '南昌', '九江', '赣州', '烟台', '济宁', '淄博', '宝鸡', '咸阳', 
    #'温州', '湖州', '丽水', '绍兴'
    '北京']

# city_all=[
#     'sh.fang.ke.com/loupan','bj.fang.ke.com/loupan','tj.fang.ke.com/loupan','gz.fang.ke.com/loupan','sz.fang.ke.com/loupan','nj.fang.ke.com/loupan','xm.fang.ke.com/loupan','hz.fang.ke.com/loupan','cq.fang.ke.com/loupan','qd.fang.ke.com/loupan','cd.fang.ke.com/loupan','wh.fang.ke.com/loupan','sy.fang.ke.com/loupan',
# ]
session = requests.session()
session.headers = get_ua()
session.verify = False
# session.proxies = {
#     'http':'http://HA642I964639Z2KD:5080FAF01A15DFFE@http-dyn.abuyun.com:9020',
#     'https':'http://HA642I964639Z2KD:5080FAF01A15DFFE@http-dyn.abuyun.com:9020'
# }
region_url_set = set()
import glob

filepath = f'贝壳_url.txt'
if os.path.exists(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        for i in f:
            a = i.strip().split('\t')
            region_url_set.add(a[-1])
    f = open(filepath,'a+', encoding='utf-8')
else:
    f = open(filepath,'w', encoding='utf-8')

def getHtmlByWeb(link):
    source=''
    print(link)
    try: #使用try except方法进行各种异常处理
        browser = webdriver.Firefox()
        browser.get(link)
        time.sleep(3)
        #res = requests.get(link,headers=header,timeout=10,verify=True) #读取网页源码
        #res.encoding='UTF-8'
        source=browser.page_source
        browser.close()
    except Exception as e:
        print(e)
    finally:
        return source
def get_all_url(city_name,city_url):
    n = 1
    # if city_name == '长春':
    #     n = 47

    # time.sleep(random.randint(5,10))
    try:
        res = session.get(city_url)
        # 获取所有区
        # if city_url + f'/pg{n}' in region_url_set:
        #     break
        # print('3', city_url + f'/pg{n}')
        # f.write('3' + '\t' + city_url + f'/pg{n}' + '\n')

        f.flush()
        html = etree.HTML(res.text)

        # 获取所有去
        ###districts = html.xpath('//*[@class="bizcircle-item-name"]/../@data-bizcircle-spell')
        districts=html.xpath('//ul[@class="district-wrapper"]/li/@data-district-spell')
        print(districts)
        for district in districts:
            n = 1
            while True:
                uri2 = city_url +'/'+ district + f'/pg{n}/'

                if n > 100:
                    break
                print(uri2)
                res2 = session.get(uri2)
                # res2=getHtmlByWeb(uri2)
                if n != 1 and 'pg' not in res2.url:
                    break
                html = etree.HTML(res2.text)
                # html = etree.HTML(res2)
                # print(city_name,district,  html.xpath('//*[@class="resblock-have-find"]/span[@class="value"]/text()')[0])
                # break
                house_list = html.xpath('//li[@class="resblock-list post_ulog_exposure_scroll has-results"]')
                print(house_list)
                print('set now ', len(region_url_set))

                for house in house_list:
                    # if house not in region_url_set:
                    house_url = house.xpath('./a/@href')[0]
                    house.xpath('./div/a[1]')
                    bankuai = strips(''.join(house.xpath('./div/a[1]/text()'))).split('/')[1]
                    print(house_url)
                    f.write(city_name + '\t' + city_url.replace('/loupan', house_url)  + '\t' + bankuai+ '\n')
                    region_url_set.add(house)
                n += 1
                if house_list == [] or len(house_list) < 10 or len(house_list) > 10:
                    break
                # time.sleep(random.randint(10, 15))
    except:
        print(traceback.format_exc())
        # page_info = html.xpath('//*[@class="se-link-container"]/a/text()')
        # print(page_info)
        # try:
        #     num = int(page_info[-1])
        #     for i in range(1, num + 1):
        #         get_all_url(city_name, city_url + f'/pg{i}')
        # except:
        #     pass

        # time.sleep(random.randint(10, 15))


    # time.sleep(random.randint(10, 15))


def get_all_region(city_name, city_url):
    uri = city_url
    res = session.get(uri)
    html = etree.HTML(res.text)
    if '访问验证' in res.text:
        return
    # l1 = html.xpath('//span[contains(text(), "区域") and @class="item-title"]/../span[2]/a/@href')
    # if l1 != []:
    #     for l1_uri in l1:
    #         uri2 = l1_uri
    #         print(2, uri2)
    #         if uri2 not in region_url_set:
    #             # while True:
    #             #     try:
    #             #         res = session.get(uri2)
    #             #         if '没有找到相关房源' in res.text:
    #             #             break
    #             #         html = etree.HTML(res.text)
    #             #         l2 = html.xpath('//div[@data-role="ershoufang"]/div[2]/a/@href')
    #             #         print('一级', l2)
    #             #         if l2 == []:
    #             #             raise OSError("asd")
    #             #         for l2_uri in l2:
                        # break
                    # except Exception as e:
                    #     print(e)
if __name__ == '__main__':
    for c in cs:
        print(c, citys.get(c))
        get_all_url(c, citys.get(c))
    # for i, v in citys.items():
        # get_all_url(i, v)
    # for k in city_all:
    #     get_all_url('1', 'https://' + k)

