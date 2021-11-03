# -*- coding:utf-8 -*-
#URL对应预售证，按URL过滤

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

listfile='list/chongqing_url.txt'
outfile='data/chongqing_new.txt'
okfile='data/chongqing.txt'

statusdicts={}

class cq_spider():
    def postHtml(self,url,data,refer):
        #使用try except方法进行各种异常处理
        header ={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
            'Connection': 'keep-alive',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'X-Requested-With': 'XMLHttpRequest',
            'Content-Type': 'application/json',
            'Origin': 'http://www.cq315house.com',
            'Referer': refer,
            'Accept-Language': 'en-US,en;q=0.9',
        }
        try:
            res=requests.post(url=url,data=data,headers=header,verify=False)
            res.encoding='UTF-8'
            return res.json()
        except Exception as e:
            print(traceback.format_exc())
        return {}
    def parse_one(self):
        #获取在售状态值
        rr = self.postHtml('http://www.cq315house.com/WebService/WebFormService.aspx/GetJsonStatus', '{"para":""}','http://www.cq315house.com/HtmlPage/PresaleDetail.html')
        statusdict=json.loads(rr.get("d"))
        for pp in statusdict:
            statusdicts[pp["val"]]=pp["ab"]
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
        ###
        page=-1
        while True:
            page+=1
            try:
                start = page*10+1
                end = page*10+1+10
                lburl = 'http://www.cq315house.com/WebService/WebFormService.aspx/getParamDatas2'
                params='{"siteid":"","useType":"","areaType":"","projectname":"","entName":"","location":"","minrow":'+str(start)+',"maxrow":'+str(end)+'}'
                r = self.postHtml(lburl,params,lburl)
                time.sleep(1)
                table1=json.loads(r.get("d"))
                print("page=",page,len(table1))
                #空页结束
                if len(table1)==0:
                    break
                for kk in table1:
                    dict2 = {
                        "url": '',
                        "城市": '重庆',
                        "项目名称": kk["projectname"],
                        "坐落位置": kk["location"],
                        "开发企业": kk["enterprisename"],
                        "预售许可证编号": kk["f_presale_cert"],
                        "发证日期": '',
                        "开盘日期": '',
                        "预售证准许销售面积": '',
                        "销售状态": '',
                        "销售楼号": kk["blockname"],
                        "套数": '',
                        "面积": '',
                        "拟售价格": '',
                        "售楼电话": '',
                        "售楼地址": '',
                        "房号": '',
                        "房屋建筑面积": '',
                        "房屋销售状态": ''
                    }
                    buildingids=kk["buildingid"].split(",")
                    blocknames=kk["blockname"].split(",")
                    for j in range(0,len(buildingids)):
                        try:
                            buildingid=buildingids[j]
                            blockname=blocknames[j]
                            dict2["销售楼号"] = blockname
                            url99 = 'http://www.cq315house.com/HtmlPage/ShowRooms.html?buildingid='+buildingid+'&block='+blockname.encode("utf-8").decode("latin1")
                            url2='http://www.cq315house.com/HtmlPage/ShowRooms.html?buildingid='+buildingid+'&block='+blockname
                            if url2 in okurl:
                                print('pass',buildingid,blockname)
                                continue
                            dict2["url"] = url2
                            url1='http://www.cq315house.com/WebService/WebFormService.aspx/GetRoomJson'
                            r3 = self.postHtml(url1,'{"buildingid":"'+buildingid+'"}',url99)
                            time.sleep(0.2)
                            table2 = json.loads(r3.get("d"))
                            print('{"buildingid":'+buildingid+'}',len(table2))
                            if len(table2)==0:
                                df = DataFrame(dict2, index=[0])
                                df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                                continue
                            for hh in table2:
                                rooms = hh["rooms"]
                                for tt in rooms:
                                    dict3=dict2.copy()
                                    dict3["房号"] = tt["unitnumber"]+'_'+tt["flr"]+'_'+tt["rn"]
                                    dict3["拟售价格"]=tt["nsjg"]
                                    dict3["房屋建筑面积"]=tt["bArea"]
                                    dict3["房屋销售状态"] = self.getstate(tt["status"])
                                    df = DataFrame(dict3, index=[0])
                                    df.to_csv(outfile, sep='\t', encoding='utf-8',mode='a', index=False, header=None)
                        except Exception as e1:
                            print(traceback.format_exc())
                            continue
            except Exception as e:
                print(traceback.format_exc())
                continue
    def getstate(self,status):
        if status in statusdicts:
            return statusdicts[status]
        else:
            return "未知"

    def main(self):
        self.parse_one()
        time.sleep(10)
run = cq_spider()
run.main()