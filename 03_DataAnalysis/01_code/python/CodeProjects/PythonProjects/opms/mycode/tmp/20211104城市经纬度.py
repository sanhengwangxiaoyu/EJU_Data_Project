#-*- coding: utf-8 -*-
import traceback

import requests,re
from fake_useragent import UserAgent
ua=UserAgent()
from queue import Queue
import threading


keylist=[
        
]
apikey=''
class Mythread(threading.Thread):
    
    def __init__(self,t):
        super().__init__()
        self.t=t
    def run(self):
        global apikey
        num=0
        while True:
            print(self.t.qsize())
            if self.t.empty():
                break
            else:
                num+=1
                if num>1900*10:
                    break
                apikey=keylist[int(num/1900)]
                self.url=self.t.get()
                self.jw(self.url)
    # url='https://restapi.amap.com/v3/geocode/geo?address=上海 铂翠廷&key=fd2412068345a6fbf8692dcb2d942b43'
    def jw(self,da):
        global apikey
        name = da[0]
        qu = da[2]
        city = da[1]
        key = f'{city} {qu} {name}'
        url=f"https://restapi.amap.com/v3/place/text?key={apikey}&keywords={key}&city={da[1]}"
        print(url)
        res=requests.get(url=url).json()
        data = da
        n = False
        for r in res['pois']:
            if r['name'] == name or name in r['name'] or r['name'] in name:
                data2 = [r['cityname'], r['adname'], r['location'], r['name'], '是']
                data.extend([str(x) for x in data2])
                print(data2)
                n = True
                break
        if n == False:
            try:
                r = res['pois'][0]
                data2 = [r['cityname'], r['adname'], r['location'], r['name'], '否']
                print(data2)
                data.extend([str(x) for x in data2])
            except:
                print(traceback.format_exc())
        f.write('\t'.join(data)  +'\n')
        f.flush()

if __name__ == '__main__':
    t = Queue()
    import csv
    # 输入文件，字段格式：楼盘名称\t区县\t城市
    with open('lplist.txt', 'r') as f:
        for i in f:
            a = i.replace('\n','').split('\t')
            t.put(a)
    # 输出文件
    f = open('lplist1.txt', 'a+', encoding='utf-8')

    crawl_tread = []
    for i in range(1):
        crawl = Mythread(t)
        crawl.start()
        crawl_tread.append(crawl)
    for thead in crawl_tread:
        thead.join()
