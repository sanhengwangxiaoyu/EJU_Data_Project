# -*- coding:utf-8 -*-

import requests
import re
import pandas as pd
from lxml import etree
import time
import json
import random
from pandas import Series,DataFrame
import html

from threading import Thread
from queue import Queue
import traceback,os

urls = Queue()
okurl = set()

listfile='list/lishui_url.txt'
outfile='data/lishui_new.txt'
okfile='data/lishui.txt'

def main():
    #
    has_new=0
    if os.path.exists(outfile):
        with open(outfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')                    
                okurl.add(a[0])
                has_new=1
    if os.path.exists(okfile):
        with open(okfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                okurl.add(a[0])
    repeatlist=[]
    if os.path.exists(listfile):
        with open(listfile, 'r', encoding='utf-8') as f:
            for i in f:
                a=i.split('\t')
                repeatlist.append(a[0])
    #如果outfile没数据，需要创建并写一个表头
    if has_new==0:
        of = open(outfile,'a', encoding='utf-8') #保存结果文件
        of.write('URL\t城市\t项目名称\t坐落位置\t开发企业\t预售许可证编号\t发证日期\t开盘日期\t预售证准许销售面积\t销售状态\t销售楼号\t套数\t面积\t拟售价格\t售楼电话\t售楼地址\t房号\t房屋建筑面积\t房屋销售状态（颜色区分）\n')
        of.flush()
    #
    of = open(outfile,'a', encoding='utf-8') #保存结果文件，按url增量
    page=0
    while True:
        page+=1
        # 'http://202.107.249.13:8098/api/Hourse/QueryTrading?pageSize=100&pageNumber=1'
        lburl = 'http://202.107.249.13:8098/api/Hourse/QueryTrading?pageSize=100&pageNumber='+str(page)
        json1 = getHtml(lburl)
        time.sleep(1)
        table1=json1.get("data").get("lpxx")
        print("已抓取",page,len(table1))
        for kk in table1:
            #{'id': 'b0cfdb3761064a7cb7319e5d09f1ef5a', 'ysxmmc': '松州华庭二区1幢、10幢', 'htqdsj': None, 'kprq': '2021-03-19 10:50:47', 'sfbatg': None, 'xzqh': '331124', 'lpmc': '松州华庭二区', 'slts': 117, 'qymj': 14083.0, 'jj': 9791.220833629199}
            projectId = kk.get("id")
            url1 ='http://jsj.lishui.gov.cn/col/col1229219555/index.html?id=' + projectId# 'http://202.107.249.13:8098/api/Hourse/QueryYSZ?id=' + projectId
            if url1 in okurl or url1 in repeatlist:
                continue
            repeatlist.append(url1)
            dict2 = {
                'url': url1,
                "城市": '丽水',
                "项目名称": kk.get("lpmc"),
                "坐落位置": '',
                "开发企业": kk.get("xmgs"),
                "预售许可证编号": kk.get("xkz"),
                "发证日期": kk.get('qfrq'),
                "开盘日期": kk.get('kprq'),
                "预售证准许销售面积": kk.get('sellArea'),
                "销售状态": kk.get('type'),
                "销售楼号": '',
                "套数": kk.get('kszzts'),
                "面积": kk.get('kszzmj'),
                "拟售价格": '',
                "售楼电话": kk.get('lxdh'),
                "售楼地址": kk.get('slcdz'),
                "房号": '',
                "房屋建筑面积": '',
                "房屋销售状态": ''
            }
            df = DataFrame(dict2, index=[0])
            df.to_csv(listfile, sep='\t', mode='a', index=False, header=None)
   
    #2多线程爬项目
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
def run():
    while urls.qsize() != 0:
        print("qsize less="+str(urls.qsize()))   
        dicts=urls.get()
        url1=dicts[0]
        pid=url1.split('=')[1]
        print(url1)
        dict2 = {
            'url': url1,
            "城市": '丽水',
            "项目名称": dicts[2],
            "坐落位置": dicts[3],
            "开发企业":  dicts[4],
            "预售许可证编号":  dicts[5],
            "发证日期":  dicts[6],
            "开盘日期":  dicts[7],
            "预售证准许销售面积":  dicts[8],
            "销售状态":  dicts[9],
            "销售楼号": '',
            "套数":  dicts[11],
            "面积":  dicts[12],
            "拟售价格": '',
            "售楼电话":  dicts[14],
            "售楼地址":  dicts[15],
            "房号": '',
            "房屋建筑面积": '',
            "房屋销售状态": ''
        }
        try:
            url1='http://202.107.249.13:8098/api/Hourse/QueryYSZ?id='+pid
            r1 = getHtml(url1)
            if len(r1)==0:
                r1 = self.pool.get('http://202.107.249.13:8098/api/Hourse/QueryXSXMXS?id='+pid, timeout=60, headers=headers).json()
                # print(r1)
                dict3=dict2.copy()
                dict3['套数']=r1['data'].get('allCount','0')
                dict3['预售证准许销售面积']=r1['data'].get('allMj','0')
                df = DataFrame(dict3, index=[0])
                df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                # time.sleep(12222)
                continue
            time.sleep(1)
            table3 = r1.get("data")
            if len(table3)==0:
                print(2222)
                r1 = self.pool.get('http://202.107.249.13:8098/api/Hourse/QueryXSXMXS?id='+pid, timeout=60, headers=headers).json()
                # print(r1)
                dict3=dict2.copy()
                dict3['套数']=r1['data'].get('allCount','0')
                dict3['预售证准许销售面积']=r1['data'].get('allMj','0')
                df = DataFrame(dict3, index=[0])
                df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                # time.sleep(12222)
                continue
            for mm in table3:
                try:
                    zrdid=mm.get("zrdid")
                    ysxmid = mm.get("ysxmid")
                    lp= mm.get("dmzh",'')
                    ts=mm.get('ksts',0)+mm.get('yszzts',0)
                    url2='http://202.107.249.13:8098/api/Hourse/QueryFw?id='+zrdid
                    print('url2='+url2)
                    try:
                        r2 = self.pool.get(url2, timeout=60, headers=headers).json()
                    except Exception as e1 :
                        print(3333)
                        dict3=dict2.copy()
                        dict3['销售楼号']=lp
                        dict3['套数']=str(ts)
                        df = DataFrame(dict3, index=[0])
                        df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                        # time.sleep(12222)
                        continue
                    time.sleep(1)
                    table5 = r2.get("data")
                    print('house='+str(len(table5)))
                    if len(table5)==0:
                        dict3=dict2.copy()
                        dict3['销售楼号']=lp
                        dict3['套数']=str(ts)
                        df = DataFrame(dict3, index=[0])
                        df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                        # time.sleep(12222)
                        continue
                    for nn in table5:
                        xq = nn.get("fw")
                        dict2['销售楼号']=lp
                        dict2['套数']=str(ts)
                        dict2["拟售价格"] = nn.get("ysfw").get("mpjg")
                        dict2["房号"] = xq.get("fwzl") #或fh
                        dict2["房屋建筑面积"] = xq.get("jzmj")
                        dict2["房屋销售状态"] = self.getstate(nn)
                        df = DataFrame(dict2, index=[0])
                        df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                        # time.sleep(1111)
                except Exception as e:
                    print(traceback.format_exc())
                    # time.sleep(1111)
                    continue
        except Exception as e:
            print(traceback.format_exc())
            # time.sleep(1111)
            continue
def getHtml(link):
    html={}
    # print(link)
    try: #使用try except方法进行各种异常处理
        header = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
            }
        res = requests.get(link,headers=header,timeout=20,verify=True) #读取网页源码
        #解码
        html=res.json()
    except Exception as e:
        print(e)
    finally:
        return html
def getstate(data):
    state=""
    if ((data.get("djmx") != None and data.get("djmx").get("djyxbz") == 1) or (data.get("fwgg") != None and data.get("fwgg").get("cfzt")!= 1300) or ( data.get("fwgg") != None and data.get("fwgg").get("djbz") != 000000000)):
        state= "暂停销售"
    if (data.get("htqd") != None):
        if (data.get("htqd").get("sfbatg")== 1):
            state="签约中"
        elif (data.get("htqd").get("sfbatg") == 2):
            state = "资金审核中"
        elif (data.get("htqd").get("sfbatg") == 0):
            state="已售"
        elif(data.get("htqd").get("sfbatg") == 5):
            state="资金审核未通过"
        else:
            state="不可售"
    elif(data.get("fwgg") != None):
        if (data.get("fwgg").get("pzkszt")== 1010 and data.get("fwgg").get("sjyxszt")== 1100):
            state="可售"
        elif (data.get("fwgg").get("pzkszt") == 1020):
            state="不可售"
        else:
            state="不可售"
    else:
        state="不可售"
    return state

if __name__ == '__main__':
		main()