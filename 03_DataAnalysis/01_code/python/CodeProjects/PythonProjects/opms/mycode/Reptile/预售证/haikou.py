# -*- coding:utf-8 -*-
#按URL做增量

import requests
import re
import pandas as pd
from lxml import etree
import json
from pandas import Series,DataFrame


from threading import Thread
from queue import Queue
import traceback,os,random,time

urls = Queue()
okurl = set()
listfile='list/haikou_url.txt'
outfile='data/haikou_new.txt'
okfile='data/haikou.txt'

class hk_spider():
    def postHtml(self,url,data):
        #使用try except方法进行各种异常处理
        header = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' ,
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2' ,
            'Content-Type': 'application/x-www-form-urlencoded' ,
        }
        try:
            res=requests.post(url=url,data=data,headers=header,verify=False)
            res.encoding='UTF-8'
            return res.json()
        except Exception as e:
            print(traceback.format_exc())
        return {}
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
            print('html err')
        finally:
            return html
    def parse_one(self):
        #已爬列表
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
        
        repeatlist=[]
        if os.path.exists(listfile):
            with open(listfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.split('\t')
                    repeatlist.append(a[0])
        ##
        page=0
        end='no'
        while True:
            if end=='yes':
                break
            page+=1
            try:
                lburl = 'http://hkrealestate.haikou.gov.cn/hkweb/project/queryLatestSaleLicences?page='+str(page)+'&limit=100'
                h1=self.getHtml(lburl)
                time.sleep(1)
                r=json.loads(h1)
                table1=r.get("data").get("data")
                print("page="+str(page),len(table1))
                for kk in table1:
                    projectId = kk.get("projectId")
                    url1 = 'http://hkrealestate.haikou.gov.cn/hkweb/project/toDetail?uuid=' + projectId
                    dict2 = {
                        'URL': url1,
                        "城市": '海口',
                        "项目名称": kk.get("projectName"),
                        "坐落位置": kk.get("address"),
                        "开发企业": kk.get("orgName"),
                        "预售许可证编号": kk.get("number"),
                        "发证日期": kk.get('effectDate'),
                        "开盘日期": kk.get('sellStartDateYear')+"."+kk.get('sellStartDateMonthAndDay'),
                        "预售证准许销售面积":kk.get('sellArea'),
                        "销售状态": kk.get('type'),
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
                    print(dict2['发证日期'])
                    if dict2['发证日期'].find('2021-02')>-1:
                        end='yes'
                        break
                    if url1 in okurl or url1 in repeatlist:
                        continue
                    repeatlist.append(url1)
                    df = DataFrame(dict2, index=[0])
                    df.to_csv(listfile, sep='\t',encoding='utf-8', mode='a', index=False, header=None)
            except Exception as e:
                print(traceback.format_exc())
                continue
        #2多线程爬项目
        with open(listfile,'r', encoding='utf-8') as f:
            for i in f:
                a = i.replace('\n','').split('\t')
                if a[0] not in okurl:
                    urls.put(a)
                    okurl.add(a[0])
        print("qsize="+str(urls.qsize()))           
        time.sleep(5)
        ths = []
        for i in range(2):
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
            projectId=url1.split('=')[1]
            print(url1)
            dict2 = {
                'URL': url1,
                "城市": '海口',
                "项目名称": dicts[2],
                "坐落位置": dicts[3],
                "开发企业": dicts[4],
                "预售许可证编号": dicts[5],
                "发证日期": dicts[6],
                "开盘日期": dicts[7],
                "预售证准许销售面积":dicts[8],
                "销售状态": dicts[9],
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
            try:
                h2=self.getHtml(url1)
                time.sleep(1)
                table3 = etree.HTML(h2)
                dict2["套数"] = ''.join(table3.xpath('//div[@class="clearfix"]//div[3]//span[2]//text()'))
                dict2["售楼电话"] = ''.join(table3.xpath('//div[@class="clearfix"]//div[4]//span[2]//text()'))
                dict2["售楼地址"] = ''.join(table3.xpath('//div[@class="clearfix"]//div[5]//span[2]//text()'))
                url3 = 'http://hkrealestate.haikou.gov.cn/hkweb/project/queryBuildings'
                params3 = {
                    "projectId": projectId,
                }
                r3 = self.postHtml(url3, params3)
                time.sleep(1)
                rjson3 = r3.get("data").get("data")
                # print(rjson3)
                rslist=[]
                for ss in rjson3:
                    try:
                        buildId = ss.get("id")
                        units = ss.get("units",{})
                        if units==None or len(units)==0:
                            continue
                        for uu in units:
                            unitId = uu.get('id')
                            url4 = 'http://hkrealestate.haikou.gov.cn/hkweb/project/queryHousesGroupByFloor'
                            params4 = {
                                "projectId": projectId,
                                "buildingId": buildId,
                                "unitId": unitId,
                            }
                            r4 = self.postHtml(url4, params4)
                            time.sleep(1)
                            rjson4 = r4.get("data").get("data")
                            for pp in rjson4:
                                houses = pp.get("houses")
                                for mm in houses:
                                    dict3=dict3.copy()
                                    dict3["房屋建筑面积"] = mm.get("buildingArea")
                                    dict3["销售楼号"] = mm.get("buildingId")
                                    dict3["房号"] = mm.get("number")
                                    dict3["房屋销售状态"] = mm.get("sellStatus")
                                    rslist.append(dict3)
                    except Exception as e:
                        print(traceback.format_exc())
                        continue
                if len(rslist)==0:
                    df = DataFrame(dict2, index=[0])
                    df.to_csv(outfile, sep='\t',encoding='utf-8' ,mode='a', index=False, header=None)
                else:
                    for c in rslist:
                        df = DataFrame(c, index=[0])
                        df.to_csv(outfile, sep='\t',encoding='utf-8' ,mode='a', index=False, header=None)
            except Exception as e:
                print(traceback.format_exc())
                continue
   
    def main(self):
        self.parse_one()

run = hk_spider()
run.main()