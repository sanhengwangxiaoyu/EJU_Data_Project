# -*- coding:utf-8 -*-
import requests
import re
import pandas as pd
import urllib.parse as urlparse
from lxml import etree
from fake_useragent import UserAgent
from urllib.request import Request, urlopen
import time
import json
import urllib.parse
import random
from pandas import Series,DataFrame
import html
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from threading import Thread
from queue import Queue
import traceback,os

urls = Queue()
okurl = set()
listfile='list/xian_url.txt'
outfile='data/xian_new.txt'
oldfile='data/xian.txt'

class xa_spider():
    def parse_one(self):
        repeatlist=[]
        with open(listfile,'r', encoding='utf-8') as f:
            for i in f:
                a = i.strip().split('\t')
                repeatlist.append(a[0])
        
        areas=['00','70','80','76','75','79','78','77']
        for area in areas:
            pn=0
            total=0
            while True:
                pn+=1
                try:
                    print(area+",已抓取"+str(pn))
                    lburl = 'http://zjj.xa.gov.cn/ygsf/index.aspx?page='+str(pn)+'&key=&qdm='+area
                    h=self.getHtml(lburl,'')
                    if total==0:
                        total=int(re.findall('>(\d+)</span>个预售信息',h,re.S)[0])
                    print(total)
                    time.sleep(0.1)
                    table1 = etree.HTML(h)
                    # print(r.text)
                    # table1 = etree.HTML(r.content.decode('gb2312'))
                    table2 = table1.xpath('//tr[@class="listtr ysztr"]')
                    if len(table2)==0:
                        break
                    for kk in table2:
                        url1='http://zjj.xa.gov.cn/ygsf/'+''.join(kk.xpath(".//td[2]//a//@href"))
                        dict2 = {
                            'url':url1,
                            "城市": '西安',
                            "项目名称": ''.join(kk.xpath(".//td[2]//a//text()")),
                            "坐落位置":  ''.join(kk.xpath(".//td[3]//text()")),
                            "开发企业": ''.join(kk.xpath(".//td[4]//text()")),
                            "预售许可证编号": ''.join(kk.xpath(".//td[1]//a//text()")),
                            "发证日期": ''.join(kk.xpath(".//td[6]//text()")),
                            "开盘日期": '',
                            "预售证准许销售面积": '',
                            "销售状态": '',
                            "销售楼号": '',
                            "套数": '',
                            "面积": '',
                            "拟售价格": '',
                            "售楼电话": '',
                            "售楼地址": '',
                            "房号": '',
                            "房屋建筑面积": '',
                            "房屋销售状态": ''
                        }
                        if url1 in repeatlist:
                            continue
                        df = DataFrame(dict2, index=[0])
                        df.to_csv(listfile, sep='\t',encoding='utf-8', mode='a', index=False, header=None)
                        if total<=pn*10:
                            break
                        time.sleep(3)
                except Exception as e:
                    print(traceback.format_exc())
                    continue
        #2多线程爬项目
        if os.path.exists(oldfile):
            with open(oldfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.strip().split('\t')
                    okurl.add(a[0])
        if os.path.exists(outfile):
            with open(outfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.strip().split('\t')
                    okurl.add(a[0])
        
        with open(listfile,'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                if a[0] not in okurl:
                    urls.put(a)
                    okurl.add(a[0])
        print("qsize="+str(urls.qsize()))           
        time.sleep(5)
        ths = []
        for i in range(1):
            t = Thread(target=self.run, args=())
            t.start()
            ths.append(t)
        for t in ths:
            t.join()
            # time.sleep(1111)
    def run(self):
        while urls.qsize() != 0:
            print("qsize less="+str(urls.qsize()))   
            time.sleep(5)
            dicts=urls.get()
            url1=dicts[0]
            qdm=''.join(re.findall('qdm=(\d+)',url1))
            print(url1)
            try:
                dict2 = {
                    'url':url1,
                    "城市": '西安',
                    "项目名称": dicts[2],
                    "坐落位置": dicts[3],
                    "开发企业": dicts[4],
                    "预售许可证编号": dicts[5],
                    "发证日期": dicts[6].strip(),
                    "开盘日期": '',
                    "预售证准许销售面积": '',
                    "销售状态": '',
                    "销售楼号": '',
                    "套数": '',
                    "面积": '',
                    "拟售价格": '',
                    "售楼电话": '',
                    "售楼地址": '',
                    "房号": '',
                    "房屋建筑面积": '',
                    "房屋销售状态": ''
                }
                h1=self.getHtml(url1,'')
                time.sleep(0.1)
                table3 = etree.HTML(h1)
                table4 = table3.xpath('//div[@class="n_leftDiv"]//a')
                print(len(table4))
                if len(table4)==0:
                    df = DataFrame(dict2, index=[0])
                    df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                    continue
                rslist=[]
                for nn in table4:
                    dict3=dict2.copy()
                    dict3["销售楼号"] = ''.join(nn.xpath('.//text()'))
                    url2='http://zjj.xa.gov.cn/ygsf/Lpb.aspx'+''.join(nn.xpath(".//@href"))
                    print('url2=',url2)
                    h2=self.getHtml(url2,'')
                    time.sleep(0.1)
                    # print(r2.text)
                    table5 = etree.HTML(h2)
                    table6 = table5.xpath('//td[@class="tdfw"]//a')
                    print(len(table6))
                    if len(table6)==0:
                        rslist.append(dict3)
                        # df = DataFrame(dict2, index=[0])
                        # df.to_csv(outfile, sep='\t', mode='a',encoding = "utf-8", index=False, header=None)
                        continue
                    for mm in table6:
                        dict4=dict3.copy()
                        try:
                            dataId=''.join(mm.xpath(".//@data-id"))
                            color = ''.join(mm.xpath(".//@class"))
                            dict4["房屋销售状态"]=self.getstate(color)
                            url3='http://zjj.xa.gov.cn/ygsf/ashx/GetFwxx.ashx?fwbh=' + dataId + '&qdm='+qdm
                            # url3='http://zjj.xa.gov.cn/ygsf/ashx/GetFwxx.ashx?fwbh=A2BA0BC41E3E36319D2665258653F147&qdm=00'
                            # print('url3='+url3)
                            time.sleep(0.1)
                            h3=self.postHtml(url3,json.dumps(''),url2)
                            xqlist = h3.split(",")
                            dict4["房号"]=xqlist[0]
                            dict4["房屋建筑面积"] = xqlist[5]
                            # df = DataFrame(dict2, index=[0])
                            # df.to_csv(outfile, sep='\t', mode='a',encoding = "utf-8", index=False, header=None)
                        except Exception as e1:
                            print(traceback.format_exc())
                            continue
                        rslist.append(dict4)
                for dicti in rslist:
                    df = DataFrame(dicti, index=[0])
                    df.to_csv(outfile, sep='\t', mode='a',encoding = "utf-8", index=False, header=None)
            except Exception as e:
                print(traceback.format_exc())
                continue
    def postHtml(self,url,data,refer):
        # print(url)
        #使用try except方法进行各种异常处理
        header = {
                'Accept':'*/*',
                'Accept-Encoding':'gzip, deflate',
                'Accept-Language':'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
                'Cookie':'sto-id-37151=CDJDKIMAJBBP; __jsluid_h=140fb0b0004593f8760dba3cec859f53; _gscu_1682955347=26162163lqwzqy14; UM_distinctid=17a9ed16d4e138-09dd01b854506c-4c3f2d73-e1000-17a9ed16d5083d; CNZZDATA2851712=cnzz_eid%3D55587984-1626160786-%26ntime%3D1626317452; Hm_lvt_dba247c4516279e648c9ec128654c148=1626162163,1626224644,1626313005; _gscu_274604790=26245785u8fgkm32; yfx_c_g_u_id_10007920=_ck21071414562515033750475948214; yfx_f_l_v_t_10007920=f_t_1626245785502__r_t_1626245785502__v_t_1626245785502__r_c_0; Hm_lvt_f59e910dd13288381298e0b040added3=1626245786; ASP.NET_SessionId=yufzhgwan4m4bgtvbj5dcxov; SERVERID=5855a14cc67ddb7446605c622772da61|1626319881|1626319177; _gscbrs_1682955347=1; Hm_lpvt_dba247c4516279e648c9ec128654c148=1626319882; _gscs_1682955347=t26319178stj17f20|pv:3',
                'Host':'zjj.xa.gov.cn',
                'Origin':'http://zjj.xa.gov.cn',
                'Referer':refer,
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.02',
                'X-Requested-With':'XMLHttpRequest'
               }
        #data=json.loads(js)
        try:
            res=requests.post(url=url,data=data,headers=header,verify=False)
            res.encoding='UTF-8'
            return res.text
            
        except Exception as e:
            print(traceback.format_exc())
        return ''
    def getHtml(slef,link,refer):
        html=""
        # print(link)
        try: #使用try except方法进行各种异常处理
            header = {
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.02',
                'Host': 'zjj.xa.gov.cn',
                'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2' ,
                'Cookie':'sto-id-37151=CDJDKIMAJBBP; __jsluid_h=140fb0b0004593f8760dba3cec859f53; _gscu_1682955347=26162163lqwzqy14; UM_distinctid=17a9ed16d4e138-09dd01b854506c-4c3f2d73-e1000-17a9ed16d5083d; CNZZDATA2851712=cnzz_eid%3D55587984-1626160786-%26ntime%3D1626317452; Hm_lvt_dba247c4516279e648c9ec128654c148=1626162163,1626224644,1626313005; _gscu_274604790=26245785u8fgkm32; yfx_c_g_u_id_10007920=_ck21071414562515033750475948214; yfx_f_l_v_t_10007920=f_t_1626245785502__r_t_1626245785502__v_t_1626245785502__r_c_0; Hm_lvt_f59e910dd13288381298e0b040added3=1626245786; ASP.NET_SessionId=yufzhgwan4m4bgtvbj5dcxov; SERVERID=5855a14cc67ddb7446605c622772da61|1626319179|1626319177; _gscbrs_1682955347=1; Hm_lpvt_dba247c4516279e648c9ec128654c148=1626319179; _gscs_1682955347=t26319178stj17f20|pv:1',
                # 'Content-Type': 'application/json' ,
                'X-Requested-With': 'XMLHttpRequest' ,
                'Referer': 'http://zjj.xa.gov.cn' 
            }
            res = requests.get(link,headers=header,timeout=20,verify=False) #读取网页源码
            
            #解码
            # print(link)
            if res.encoding=='utf-8' or res.encoding=='UTF-8':
                    res.encoding='UTF-8'
            else:
                    m = re.compile('<meta .*(http-equiv="?Content-Type"?.*)?charset="?([a-zA-Z0-9_-]+)"?', re.I).search(res.text)
                    if m and m.lastindex == 2:
                        charset = m.group(2).upper()
                        res.encoding=charset
                    else:
                        res.encoding='GBK'
            html=res.text
        except Exception as e:
            print(traceback.format_exc())
        finally:
            return html
    def getstate(self, color):
        state = color
        if ('red' in color):
            state = '已网签备案'
        elif ('green' in color):
            state = '未网签备案'
        elif ('gray' in color):
            state = '不可售'
        elif ('jsf' in color):
            state = '经济适用房'
        elif ('xjf' in color):
            state = '限价商品房'
        return state
    def main(self):
        self.parse_one()
        time.sleep(10)

run = xa_spider()
run.main()