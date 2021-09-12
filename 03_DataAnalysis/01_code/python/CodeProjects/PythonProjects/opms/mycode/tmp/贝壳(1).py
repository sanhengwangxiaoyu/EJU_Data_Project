import datetime
import os
import re
import time
import traceback
import urllib3
urllib3.disable_warnings()
import requests
from logg import *
from lxml import etree
from queue import Queue

# from mongo_save_file import *
# gfs = GFS('spider', 'files')
def extract_first(data):
    if data != []:
        return data[0]
    return ''
urls = Queue()
region_url_set = set()

today = datetime.datetime.now().strftime('%Y%m%d')
filepath = f'贝壳_url.txt'
write_path = f'贝壳.txt'
if os.path.exists(write_path):
    with open(write_path, 'r', encoding='utf-8') as f:
        for i in f:
            a = i.strip().split('\t')
            region_url_set.add(a[1])
    mode = 'a+'
else:
    mode = 'w'
fres = open(write_path, mode, encoding='utf-8')

with open(filepath,'r', encoding='utf-8') as f:
    for i in f:
        a = i.strip().split('\t')
        print(a)
        if len(a) != 3:
            continue
        if a[1] not in region_url_set:
            if a[0] != '3':
                urls.put(tuple(a[:2]))

session = requests.session()
session.headers = get_ua()
tunnel = ""

# 用户名密码方式
username = ""
password = ""
# proxies = {
    # "http": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel},
    # "https": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel}
# }
# session.proxies = proxies
session.verify = False
# session.proxies ={
#                 'http':'http://H3W6P88SS196ED0D:C73DD41CE1F6BAB5@http-dyn.abuyun.com:9020',
#                 'https':'http://H3W6P88SS196ED0D:C73DD41CE1F6BAB5@http-dyn.abuyun.com:9020',
#             }

# @check(gfs)
def get(uri):
    n = 0
    while True:
        if n > 3:
            break
        try:
            res = session.get(uri)
            res.encoding = 'utf-8'
            html = etree.HTML(res.text)
            print(html.xpath('//title/text()'))
            return html, res.text
        except Exception as e:
            print(e)
            print('error ',uri)
            n += 1
        # time.sleep(random.randint(3, 5))
def trim(word):
    word=re.sub('\s+','',word)
    word=re.sub('[\u2002|\u3000|\xa0]+','',word)
    word=word.replace('&nbsp;','').replace('\\t','').replace('\\n','').replace('\\xa0','')
    return word
def getText(html, keyword):
    path = html.xpath(f'//span[@class="label"  and contains(text(), "{keyword}")]/../span[@class="label-val"]')
    if path != []:
        return strips(path[0].xpath('string(.)'))
    else:
        return ''
def getHtml(link):
    html=""
    try: #使用try except方法进行各种异常处理
        header = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/536.1 LBBROWSER'}
        

        res = requests.get(link,headers=header,timeout=8,verify=False) #读取网页源码
        #解码
        print(link)
        
        if res.encoding=='utf-8' or res.encoding=='UTF-8':
        		res.encoding='UTF-8'
        else:
            m = re.compile('<meta .*(http-equiv="?Content-Type"?.*)?charset="?([a-zA-Z0-9_-]+)"?', re.I).search(res.text)
            if m and m.lastindex == 2:
                charset = m.group(2).upper()
                if charset=='GB2312':
                    charset='GBK'
                res.encoding=charset
            else:
                res.encoding='UTF-8'
        html=res.text
    except Exception as e:
        print(traceback.format_exc())
    finally:
        return html
def parse(city, uri: str):
    if uri.endswith('loupan'):
    # if 'loupan' in uri:
        return
    html, text = get(uri)
    html2, text2 = get(uri + 'xiangqing/')
    # text = open('1.html', 'r', encoding='utf-8').read()
    # html = etree.HTML(text)
    if '未找到相关' in text:
        fres.write('\t'.join(['贝壳',uri,'无效']) + '\n')
        return
    if '要查看的页面丢失了' in text:
        fres.write('\t'.join(['贝壳',uri,'无效']) + '\n')
        return
    if '访问验证' in text:
        urls.put(uri)
        time.sleep(random.randint(15,20))
        return
    # 楼盘名称
    name = extract_first(html.xpath('//h2[@class="DATA-PROJECT-NAME"]/text()'))
    # 城市
    city = city
    # 所在区域
    try:
        region = html.xpath('//*[contains(@data-ulog-exposure, "xinfangpc_show=20027&location=2")]/text()')[0]
    except:
        region =''
    # 所在板块
    try:
        bankuai = html.xpath('//*[contains(@data-ulog-exposure, "xinfangpc_show=20027&location=3")]/text()')[0]
    except:
        bankuai = ''
    # 经纬度
    lat = extract_first(re.findall('"latitude":"([0-9\.]+)"', text2))
    lon = extract_first(re.findall('"longitude":"([0-9\.]+)"', text2))
    jingweidu = lat+','+lon
    # 楼盘地址
    address = getText(html2, '楼盘地址')
    # 售楼处地址
    slc_dz = getText(html2, '售楼处地址')
    # 售楼处电话
    slc_dh = getText(html2, '售楼处电话')
    pro=uri.split('_')[1].split('/')[0]
    url3='https://ex.ke.com/sdk/recommend/html/100010998?hdicCityId=110000&id=100010998&mediumId=100000036&projectName='+pro+'&projectType=&elementId=ke_agent_1&required400=true'
    text3 = getHtml(url3)
    m=re.findall(r'phone400":"(.*?)",',text3,re.S)
    print(m)
    if m :
        slc_dh=trim(m[0])
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
        print('//ul[@data-id="' + _id + '"]')
        h = html.xpath('//ul[@data-id="' + _id + '"]')[0]
        house_type = strips(h.xpath('./div[@class="card-content"]/div[@class="content-title"]/text()')[0])
        house_price = strips(
            h.xpath('./div[@class="card-content"]/div[@class="content-price"]')[0].xpath('string(.)'))
        house_area = strips(h.xpath('./div[@class="card-content"]/div[@class="content-area"]/text()')[0])
        a.append('|'.join([house_type, house_price, '', house_area]))
        hxts.append(extract_first(h.xpath('./div[@class="card-img"]/img/@src')))
        inq = ''
    house_type = ''
    # 户型总价
    all_price = getText(html, '楼盘总价')
    # 套均总价（同户型平均价）
    ps = []
    # for x in hxs:
    #     try:
    #         ps.append(strips(x.xpath('//div[@class="content-price"]/span/text()')[0]))
    #     except:
    #         pass
    unit_price = '||'.join(ps)
    # 户型面积
    areas = []
    for x in hxs:
        try:
            areas.append(strips(x.xpath('//div[@class="content-area"]/text()')[0]))
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
    #楼盘纪事
    lpjs=''
    
    # 发证日期
    fazheng = extract_first(html2.xpath('//th[contains(text(), "发证时间")]/../../tr[2]/td[2]/text()'))
    # 预售证号
    yszs = html2.xpath('//h2[@id="xTable"]/following-sibling::table[1]/tr')
    lds = ','.join(html2.xpath('//span[@class="fq-fqbuild"]/span/text()'))
    print(yszs)
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
    jzlx = getText(html2,'建筑类型')
    # 物业类型
    wylx = getText(html2,'物业类型')
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
    dscws = re.findall('地上.+?(\d+)',cws)
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
        zbpt = html2.xpath('//span[@class="label"  and contains(text(), "周边规划")]/../*[@id="around_txt"]')[0].xpath('string(.)')
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
    # 溢价点
    try:
        yjd = html2.xpath('//span[@class="label"  and contains(text(), "项目特色")]/../span[2]')[0].xpath('string(.)').replace(' ',',')
    except:
        return
    # 不利因素
    blyx = ''
    # 付款方式
    fkfs = ''
    xszt = extract_first(html.xpath('//*[@class="tag-item sell-type-tag"]/text()'))

    # 供暖方式
    gnfs = getText(html2,'供暖方式')
    # 供电方式
    gdfs = getText(html2,'供电方式')
    # 供水方式
    gsfs = getText(html2,'供水方式')
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
    h3,t3 = get(uri + 'dongtai/')
    lis = h3.xpath('//*[@class="big-left fl"]/div[@class="dongtai-one for-dtpic"]')
    dts = []
    for li in lis:
        tag = extract_first(li.xpath('./a/span[@class="a-tag"]/text()'))
        title = extract_first(li.xpath('./a/span[@class="a-title"]/text()'))
        ti = extract_first(li.xpath('./a/span[@class="a-time"]/text()'))
        content = strips(li.xpath('./div')[0].xpath('string(.)'))
        dts.append('|'.join([strips(x) for x in [tag, title, content, ti]]))
    print(dts)
    jtsj = '^'.join(dts)
    # 拿地事件
    ndsj = ''
    print(hxts)
    #uri,name, city, region, bankuai, jingweidu, address, slc_dz, slc_dh, bm, cym, '^'.join(a),al_price, danjia, kaipan, jiaofang,, fazheng, zhenghao, 
    #kaifashang,touzishang,pinpaishang, zzdmj, zjzmj, lvl, rjl, jzlx, wylx, ghhs, cqnx, wygs, wyf, cwb, cws, lds,pjljj,dscws, dxcws, jzcs, zbpt, dls, zxqk, 
    #hxt, lpxgt, yjd,gnfs, gdfs, gsfs, hxwz, xmts, zxbz, jcsb, jzsjdw, sgdw, lpcx, gcjd, jtsj, ndsj 

    data = ['贝壳', uri,name, city, region, bankuai, jingweidu, 
            xszt,address, slc_dz, slc_dh, bm, '^'.join(a),al_price,
             danjia, kaipan, jiaofang,lpjs,fazheng, zhenghao, 
             kaifashang,touzishang,pinpaishang, zzdmj, zjzmj, 
             lvl, rjl, jzlx, wylx, ghhs, cqnx, wygs, wyf, cwb, 
             cws, lds,pjljj,dscws, dxcws, jzcs, zbpt, dls, zxqk, 
             hxt, lpxgt, yjd, gnfs, gdfs, gsfs, hxwz, xmts, zxbz, 
             jcsb, jzsjdw, sgdw, lpcx, gcjd, jtsj, ndsj]
    print(data)
    fres.write('\t'.join([strips(str(i)) for i in data]) + '\n')
    fres.flush()

def run():
    while urls.qsize() != 0:
        city, uri = urls.get()
        print('fetching ',urls.qsize() , uri)
        try:
            parse(city, uri)
        except Exception as e:
            print(traceback.format_exc())
        # while True:
        #     try:
        #         parse(city, uri)
        #
        #         break
        #
        #     except Exception as e:
        #         print(traceback.format_exc())
        #
        #     time.sleep(random.randint(15,20))
        # time.sleep(random.randint(15, 20))
        # break

if __name__ == '__main__':
    from threading import Thread
    ths = []
    for i in range(3):
        t = Thread(target=run, args=())
        t.start()
        ths.append(t)
    for t in ths:
        t.join()
    # parse('重庆','https://sh.fang.ke.com/loupan/p_ltabnfp/')