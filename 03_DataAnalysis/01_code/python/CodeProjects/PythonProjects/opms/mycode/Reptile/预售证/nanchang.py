# -*- coding:utf-8 -*-
import requests
import re
import pandas as pd
import urllib.parse as urlparse
from lxml import etree
import time,os
import json
import urllib.parse
import random
from pandas import Series,DataFrame
import html
import base64
from urllib3.util.retry import Retry

ysz_ld_oklist=set()

outfile='data/nanchang_new.txt'
okfile='data/nanchang.txt'
listfile='list/nanchang.txt'


def getHtml(link):
    html=''
    print(link)
    while True:
        try: #使用try except方法进行各种异常处理
            header = {
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0 Win64 x64 rv:86.0) Gecko/20100101 Firefox/86.0',
                'Accept': '*/*',
                }
            res = requests.get(link,headers=header,timeout=60,verify=False) #读取网页源码
            #解码
            res.encoding='utf-8'
            html=res.text
            break
        except Exception as e:
            print(e)
            if str(e).find('Read timed out')>0:
                time.sleep(2)
    return html
   
def postHtml(url,data):
    header = {
            'Accept':'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding':'gzip, deflate',
            'Accept-Language':'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Connection':'keep-alive',
            'Content-Length':'83',
            'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8',
            'Host':'qjm.ncfdc.com.cn',
            'Origin':'http://qjm.ncfdc.com.cn',
            'Referer':'http://qjm.ncfdc.com.cn/inline/open/fcApi/yfyjIndex',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0',
            'X-Requested-With':'XMLHttpRequest',
            }
    try:
        res=requests.post(url=url,data=data,headers=header,verify=True)
        res.encoding='UTF-8'
        return res.text
        
    except Exception as e:
        print(traceback.format_exc())
    return ''
def main():
    has_new=0
    if os.path.exists(outfile):
        with open(outfile, 'r', encoding='utf-8') as f:
            for i in f:
                a=i.split('\t')
                if len(a)>10:
                    ysz_ld_oklist.add(a[5]+a[10])
                    has_new=1
    if os.path.exists(okfile):
        with open(okfile, 'r', encoding='utf-8') as f:
            for i in f:
                a=i.split('\t')
                if len(a)>10:
                    ysz_ld_oklist.add(a[5]+a[10])
    if has_new==0:
        of = open(outfile,'a', encoding='utf-8') #保存结果文件
        of.write('URL\t城市\t项目名称\t坐落位置\t开发企业\t预售许可证编号\t发证日期\t开盘日期\t预售证准许销售面积\t销售状态\t销售楼号\t套数\t面积\t拟售价格\t售楼电话\t售楼地址\t房号\t房屋建筑面积\t房屋销售状态（颜色区分）\n')
        of.flush()
    url0="http://qjm.ncfdc.com.cn/inline/open/sqjcx/querySsxq"
    #通过搜索面积获取所有展示楼盘
    url1 = 'http://qjm.ncfdc.com.cn/inline/open/fcApi/yfyjFirstSearch'
    print(url1)
    params2 = {
        "projectall_name":"",
        "xmjjmin":"",
        "xmjjmax":"",
        "mjmin":"0",
        "mjmax":"9999999",
        "kfqymc":"",
        "sfsxf":"",
        "xzqy":"",
        "xmzl":"",
    }
    h2 = postHtml(url1,params2)
    r2 = json.loads(h2)
    rjson1=r2.get("data")
    for js2 in rjson1:
        projectId = js2.get("PROJECTALL_ID","")
        pid1=base64.urlsafe_b64encode(projectId.encode("utf-8"))
        pid2 = base64.urlsafe_b64encode(pid1)
        pid3 = base64.urlsafe_b64encode(pid2)
        xmmc=js2.get("PROJECTALL_NAME","")
        url3 = 'http://qjm.ncfdc.com.cn/inline/open/fcApi/yfyjSecSearch'
        url33='http://qjm.ncfdc.com.cn/inline/open/fcApi/yfyjSecondIndex?projectall_id='+pid3.decode()+'&num=3&xmmc='#+urllib.parse.quote(xmmc)
        params3 = {
            "projectall_id":projectId,
            "presell_name":"",
            "mjmin":"",
            "mjmax":"",
            "spfw":"",
            "spfc":"",
            "spft":"",
            "spfs":"",
            "zh":"",
        }
        #获取此楼盘所有展示的楼号和对应的预售证
        h3=postHtml(url3,params3)
        r3=json.loads(h3).get("data")
        for js3 in r3:
            ysz=js3.get("PRESELL_NAME","")
            lh=js3.get("BUILDING_NUMBER","")
            if ysz+lh in ysz_ld_oklist:
                print('pass',ysz,lh)
                continue
            dict2 = {
                'URL':url33,
                "城市": '南昌',
                "项目名称": xmmc,
                "坐落位置": "",
                "开发企业": "",
                "预售许可证编号": ysz,
                "发证日期":js3.get("PRESELL_CREATEDATE",""),
                "开盘日期": "",
                "预售证准许销售面积": "",
                "销售状态": "",
                "销售楼号":lh ,
                "套数": "",
                "面积": "",
                "拟售价格": '',
                "售楼电话": "",
                "售楼地址": "",
                "房号": '',
                "房屋建筑面积": '',
                "房屋销售状态": '可售'
            }
            #
            #具体楼栋信息
            url3="http://qjm.ncfdc.com.cn/inline/open/fcApi/yfyjThiSearch"
            params3 = {
                "building_id": js3.get("BUILDING_ID",""),
                "djmin": "",
                "djmax": "",
                "mjmin": "",
                "mjmax": "",
                "szc": "",
                "dyh": "",
                "spfw": "",
                "spfc": "",
                "spft": "",
                "spfs": "",
            }
            h4 = postHtml(url3,params3)
            r4 = json.loads(h4)
            rjson4= r4.get("data")
            #户室
            print('lh=',lh,'room=',len(rjson4))
            for js4 in rjson4:
                dict2["房号"]=js4.get("HOUSE_ADDRESS","")
                dict2["房屋建筑面积"] = js4.get("JZMJ","")
                if js4.get("ONEPRICE")!=None:
                    dict2["拟售价格"] = js4.get("ONEPRICE","")
                df = DataFrame(dict2, index=[0])
                df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)

if __name__ == '__main__':
	main()
