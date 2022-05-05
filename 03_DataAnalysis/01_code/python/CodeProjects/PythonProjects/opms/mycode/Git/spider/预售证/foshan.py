# -*- coding:utf-8 -*-
##每个链接是一个预售证，按全部链接做增量爬虫

import requests
import re
import pandas as pd
from lxml import etree
import time
import json
from pandas import Series,DataFrame
import html
from threading import Thread
from queue import Queue
import traceback,os,random

urls = Queue()
okurl = set()

listfile='list/foshan_url.txt'
okfile='data/foshan.txt'
outfile='data/foshan_new.txt'

class fs_spider():
    def getHtml(self,link):
        html=""
        # print(link)
        try: #使用try except方法进行各种异常处理
            header = {
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0'+str(random.random()),
            } 
            res = requests.get(link,headers=header,timeout=30,verify=False) #读取网页源码
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
            print(e)
            print('html err')
        finally:
            return html
    def parse_one(self):
        ##已爬URL列表
        if os.path.exists(outfile):
            with open(outfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.split('\t')
                    okurl.add(a[0])
        if os.path.exists(okfile):
            with open(okfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.split('\t')
                    okurl.add(a[0])
        #
        repeatlist=[]
        if os.path.exists(listfile):
            with open(listfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.split('\t')
                    repeatlist.append(a[0])
        #
        ##########
        of = open(listfile,'a+', encoding='utf-8') #保存结果文件
        #翻页获取项目列表range(1,185):
        page=0
        total=0
        while True:
            page+=1
            try:          
                lburl = 'https://fsfc.fszj.foshan.gov.cn/search/index.do?sw=&dn=&hx=&lx=&mj=&jg=&ys=0&od=-FZRQ&ad_check=1&p='+str(page)
                time.sleep(1)
                h1=self.getHtml(lburl)
                table1 = etree.HTML(h1)
                table2 = table1.xpath('//div[@class="col-2-1"]//dl//dd')
                if total==0:
                    total=int(re.findall('共(\d+)条记录',h1)[0])
                    print(total)
                print(page,len(table2))
                for kk in table2:
                    xmmc=''.join(kk.xpath(".//h3//a//text()")).strip()
                    id = ''.join(kk.xpath(".//h3//a//@onclick")).replace('testbarx(', '').replace(')', '')
                    url1 = 'https://fsfc.fszj.foshan.gov.cn/hpms_project/roomView.jhtml?id=' + id
                    dict2 = {
                        'url': url1,
                        "城市": '佛山',
                        "项目名称": xmmc,
                        "坐落位置": ''.join(kk.xpath(".//p[1]//text()")).replace('楼盘地址： ','').replace('\t','').strip(),
                        "开发企业": ''.join(kk.xpath(".//p[2]//text()")).replace('开  发  商：',''),
                        "预售许可证编号": '',
                        "发证日期": '',
                        "开盘日期": '',
                        "预售证准许销售面积": '',
                        "销售状态": '',
                        "销售楼号": '',
                        "套数": '',
                        "面积": '',
                        "拟售价格": ''.join(kk.xpath(".//h3//strong//text()")),
                        "售楼电话": ''.join(kk.xpath(".//span//text()")),
                        "售楼地址": '',
                        "房号": '',
                        "房屋建筑面积": '',
                        "房屋销售状态": ''
                    }
                    if url1 in okurl or url1 in repeatlist:
                        continue
                    df = DataFrame(dict2, index=[0])
                    df.to_csv(listfile, sep='\t', mode='a', index=False, header=None)
                if total<=page*10:
                    break
            except Exception as e:
                print(e)
                continue
        #2多线程爬项目
        repeatlist=[]
        
        with open(listfile,'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                if a[0] not in repeatlist and a[0] not in okurl:
                    urls.put(a)
                    repeatlist.append(a[0])
        print("qsize="+str(urls.qsize()))           
        time.sleep(5)
        ths = []
        for i in range(3):
            t = Thread(target=self.run, args=())
            t.start()
            ths.append(t)
        for t in ths:
            t.join()
            # time.sleep(1111)
    def run(self):
        while urls.qsize() != 0:
            print("qsize less="+str(urls.qsize()))   
            dicts=urls.get()
            url1=dicts[0]
            id=url1.split('=')[1]
            xmmc=dicts[2]
            try:                
                dict2 = {
                    'url': url1,
                    "城市": '佛山',
                    "项目名称": dicts[2],
                    "坐落位置": dicts[3],
                    "开发企业": dicts[4],
                    "预售许可证编号": '',
                    "发证日期": '',
                    "开盘日期": '',
                    "预售证准许销售面积": '',
                    "销售状态": '',
                    "销售楼号": '',
                    "套数": '',
                    "面积": '',
                    "拟售价格":  dicts[13],
                    "售楼电话": dicts[14],
                    "售楼地址": '',
                    "房号": '',
                    "房屋建筑面积": '',
                    "房屋销售状态": ''
                }
                print('url1='+url1)
                # r1 = self.pool.get(url1, timeout=60, headers=headers)
                h1=self.getHtml(url1)
                rr='<ahtml><td></td><ahtml/>'.join(re.findall(r'<p class="bot-a">([\s\S]*)<div class="tab-con js_trsTab">',h1, re.I))
                table3 = etree.HTML(rr)
                table4 = table3.xpath('//td')
                print(len(table4))
                # for pp in rr:
                #     id2=re.findall(r'id="(.*?)"', pp, re.I)
                #     print(id2)
                # print(rr)
                # time.sleep(0.1)
                url5='https://fsfc.fszj.foshan.gov.cn/hpms_project/ysxkz.jhtml?id='+str(id)+'&zmc='+xmmc
                print(url5)
                h5=self.getHtml(url5)
                table9 = etree.HTML(h5).xpath('//div[@class="js_con tab03"]//select//option')
                #销售号信息及楼号关系
                ysz_dict={}
                for hh in table9:
                    xkzid = ''.join(hh.xpath('.//@value'))
                    url99 = 'https://fsfc.fszj.foshan.gov.cn/hpms_project/ysxkzxx.jhtml?xkzh='+xkzid
                    h99=self.getHtml(url99)
                    r99 = json.loads(h99)
                    ysz_dict[r99.get('xkzh')]=r99
                print('ysz_list=',len(table9))
                time.sleep(0.1)
                #户室详情json
                for tt in table4:
                    dict2["销售楼号"]=''.join(tt.xpath('.//a//text()'))
                    id1=''.join(tt.xpath('.//a//@id'))
                    url2='https://fsfc.fszj.foshan.gov.cn/hpms_project/room.jhtml?id='+id1
                    print('url2-loudong=',url2)
                    r2=json.loads(self.getHtml(url2))
                    for item in r2:
                        dict4=dict2.copy()
                        # print(item)
                        xkzh=item["xkzh"]
                        dict4["预售许可证编号"] = xkzh
                        dict4["房屋销售状态"]=item["zt"]
                        dict4["房号"] = item["roomno"]
                        #判断销售号，取信息dict3
                        if xkzh in ysz_dict:
                            rjson = ysz_dict[xkzh]
                            dict4["预售证准许销售面积"] = rjson.get('yszmj')
                            timeStamp = rjson.get('fzrq')
                            if len(str(timeStamp))>10:
                                timeArray = time.localtime(timeStamp/1000)
                                otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
                                dict4["发证日期"] = otherStyleTime
                            dict4["套数"] = rjson.get('yszts')
                            dict4["面积"] = rjson.get('yszmj')
                        dict4["房屋建筑面积"] = item["jzmj"]
                        df = DataFrame(dict4, index=[0])
                        df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
            except Exception as e:
                print(traceback.format_exc())
                continue
    def main(self):
        self.parse_one()

run = fs_spider()
run.main()
