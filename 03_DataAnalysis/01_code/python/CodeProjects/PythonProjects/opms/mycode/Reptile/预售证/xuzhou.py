# -*- coding:utf-8 -*-
#链接为项目地址，但预售证在项目里。需要根据预售证进行过滤，按日期限定重写待爬列表，通过预售证号来过滤。待爬列表中的url可能会重复，需要按预售证判断不重复


import time,random
from lxml import etree
import urllib.parse
import re,os
import requests
import json
import traceback

from threading import Thread
from queue import Queue

urls = Queue()
ysz_oklist=set()

dt=time.strftime('%Y%m%d',time.localtime(time.time()))
outfile="./data/xuzhou_new.txt"
outlistfile="./list/xuzhou.txt"
okfile="./data/xuzhou.txt"

url='https://www.xzhouse.com.cn/house/salePermit/getSalePermitListByPage.do'

def trim(word):
    return re.sub('\s+','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')

def get_data(contenturl):
    city='徐州'
    area=''
    rslist=[]
    cell={}
    detail=''
    newhtml=''
    SaleItemID=contenturl[8].strip()
    itemId=contenturl[0].split('=')[1]
    try:
        #pro
        cell = {
                "城市": city,
                "项目名称": contenturl[2],
                "坐落位置": contenturl[3],
                "开发企业": contenturl[4],
                "预售许可证编号":contenturl[5],
                "发证日期":contenturl[6],
                "开盘日期":'',
                "预售证准许销售面积":contenturl[7],
                "销售状态": '',
                "销售楼号": '',
                "套数": '',
                "面积": '',
                "拟售价格": '',
                "售楼电话": '',
                "售楼地址": '',
                "房号": '',
                "房屋建筑面积": '',
                "房屋销售状态": '',
        }
        #均价，可不用h1=postHtml('https://www.xzhouse.com.cn/house/sale/getSalePrice.do','itemId='+itemId)
        rslist=[]
        #ysz的楼栋列表
        h2=postHtml('https://www.xzhouse.com.cn/house/sale/getSaleBySaleItemId.do','saleItemId='+SaleItemID)
        if h2.find('查询结果为空')==-1:
            js=json.loads(h2)['obj']
            print('builds=',len(js))
            for j in js:
                cell['销售楼号']=j.get('buildingName','')
                cell['面积']=str(j.get('zmj',''))
                cell['套数']=str(j.get('rwzts',''))
                buildid=j.get('buildingId','')
                print('build=',buildid)
                #翻页户室
                page=1
                total=0
                data=f'xzqh=320300&pageSize=8&type=1&order=0&itemId={itemId}&currPageNo={page}&buildingId={buildid}'
                h3=postHtml('https://www.xzhouse.com.cn/house/houseApi/getHouseList.do',data)
                if h3.find('查询结果为空')>0:
                    continue
                s=re.findall('pageCount":"(\d+)',h3)
                if len(s)>0:
                    total=int(s[0])
                print('total=',''.join(re.findall('totalCount":"(.*?)"',h3)))
                while total>=page:
                    houselist=re.findall('houseId":"(.*?)"',h3)
                    #户室详情
                    for houseid in houselist:
                        h4=postHtml('https://www.xzhouse.com.cn/house//houseApi/getHouseInfoById.do','houseId='+houseid)
                        cell4=cell.copy()
                        cell4['房号']=''.join(re.findall('houseNo":"(.*?)"',h4))
                        cell4['房屋建筑面积']=''.join(re.findall('buArea":(.*?),',h4))
                        cell4['房屋销售状态']=''.join(re.findall('houseState":"(.*?)"',h4))
                        cell4['拟售价格']=''.join(re.findall('preparedPrice":(.*?),',h4))
                        rslist.append(cell4)
                    
                    page+=1
                    data=f'xzqh=320300&pageSize=8&type=1&order=0&itemId={itemId}&currPageNo={page}&buildingId={buildid}'
                    time.sleep(1) 
                    h3=postHtml('https://www.xzhouse.com.cn/house/houseApi/getHouseList.do',data)
                    if h3.find('查询结果为空')>0:
                        break
        if len(rslist)==0:
            rslist.append(cell)
    except Exception as e:
        print(traceback.format_exc()) 
    return rslist
def do_write(outfile,url,rsdict):
    of = open(outfile,'a+', encoding='utf-8') #保存结果文件
    for dicts in rsdict:
        of.write(url+"\t")
        of.write(dicts.get('城市','')+'\t')
        of.write(trim(dicts.get('项目名称',''))+'\t')
        of.write(trim(dicts.get('坐落位置',''))+'\t')
        of.write(trim(dicts.get('开发企业',''))+'\t')
        of.write(trim(dicts.get('预售许可证编号',''))+'\t')
        of.write(trim(dicts.get('发证日期',''))+'\t')
        of.write(trim(dicts.get('开盘日期',''))+'\t')
        of.write(trim(dicts.get('预售证准许销售面积',''))+'\t')
        of.write(trim(dicts.get('销售状态',''))+'\t')
        of.write(trim(dicts.get('销售楼号',''))+'\t')
        of.write(trim(dicts.get('套数',''))+'\t')
        of.write(trim(dicts.get('面积',''))+'\t')
        of.write(trim(dicts.get('拟售价格',''))+'\t')
        of.write(trim(dicts.get('售楼电话',''))+'\t')
        of.write(trim(dicts.get('售楼地址',''))+'\t')
        of.write(trim(dicts.get('房号',''))+'\t')
        of.write(trim(dicts.get('房屋建筑面积',''))+'\t')
        of.write(trim(dicts.get('房屋销售状态','')))
        of.write("\n")
        of.flush()
def postHtml(url,data):
    time.sleep(2)
    requests.adapters.DEFAULT_RETRIES = 5
    # print(url,data)
    #使用try except方法进行各种异常处理
    header = {
            'Accept':'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding':'gzip, deflate, br',
            'Accept-Language':'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Cache-Control':'max-age=0',
            'Connection':'keep-alive',
            'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8',
            'Cookie':'JSESSIONID=F7C49F7CD75A40F0BC1D1FFE566B8216; UM_distinctid=17b0aa60e022b9-0f8c06b8e65c1f8-4c3e257a-e1000-17b0aa60e03623; CNZZDATA1274037783=417185572-1627970534-%7C1628059651; JSESSIONID=B1E5E654AC5662D9FB6A1BFFF2B11519',
            'Host':'www.xzhouse.com.cn',
            'Origin':'https://www.xzhouse.com.cn',
            'Sec-Fetch-Dest':'document',
            'Sec-Fetch-Mode':'navigate',
            'Sec-Fetch-Site':'cross-site',
            'Upgrade-Insecure-Requests':'1',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.01111',
            }
    #data=json.loads(js)
    while True:
        try:
            s = requests.session()
            s.keep_alive = False
            res=requests.post(url=url,data=data,headers=header,verify=True)
            res.encoding='UTF-8'
            if res.text.find('您的操作过于频繁，请输入验证码后尝试')>0:
                print('您的操作过于频繁 wait 20分')
                time.sleep(20*60)
            else:
                return res.text
        except Exception as e:
            print(traceback.format_exc())
            time.sleep(20*60)
    return ''
def main():
    #预售证号列表
    repeatlist=[]
    if os.path.exists(outlistfile):
        with open(outlistfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                repeatlist.append(a[5])
    ##
    pn=0
    of = open(outlistfile,'a+', encoding='utf-8') #保存结果文件
    while True :
        break
        time.sleep(3)
        pn+=1
        data=f'pageSize=30&xzqh=320300&currPageNo={pn}'
        html=postHtml(url,data)
        js=json.loads(html)
        total=int(js['attributes']['pageCount'])
        #########
        hrefs=js['obj']
        print('total=',total,'thispage=',pn,len(hrefs))
        if len(hrefs)==0:
            break   
        for j in hrefs:
            SaleItemID=j['SaleItemID']
            ysz=j['CertificateNO']
            op=j['CorpName']
            fzrq=j['CertificateTime']
            pro=j['tgmc']#这里用的推广名称
            addr=j['xzqhStr']+j['ItemSite']
            area=str(j['TotalArea'])
            uid=j['id']
            # u=urllib.parse.quote((urllib.parse.quote(ysz)))
            contenturl='https://www.xzhouse.com.cn/page/generation/onlinePc/projectDetail.html?itemId='+uid
            # print(contenturl)
            # time.sleep(1222)
            s=contenturl+'\t徐州\t'+pro+'\t'+addr+'\t'+op+'\t'+ysz+'\t'+fzrq+'\t'+area+'\t'+SaleItemID
            if ysz in repeatlist:
                continue
            repeatlist.append(ysz)
            of.write(s+'\n')
            of.flush() 
        if total<=pn:
            break 
    #已爬列表
    if os.path.exists(outfile):
        with open(outfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')                    
                ysz_oklist.add(a[5])
                
    if os.path.exists(okfile):
        with open(okfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                ysz_oklist.add(a[5])
    #
    repeatlist=[]
    with open(outlistfile,'r',encoding='UTF-8') as txtData: 
        for line in txtData.readlines():
            contenturl=line.split('\t')
            if contenturl[5] in ysz_oklist:
                continue
            urls.put(contenturl)
            ysz_oklist.add(a[5])
            # outstr=get_data(contenturl)
            # do_write(outfile,contenturl,outstr)
            # time.sleep(2222)
        print("qsize="+str(urls.qsize()))           
        time.sleep(5)
        ths = []
        for i in range(1):
            t = Thread(target=run, args=())
            t.start()
            ths.append(t)
        for t in ths:
            t.join()
def run():
    while urls.qsize() != 0: 
        contenturl=urls.get()
        print("qsize less="+str(urls.qsize()))   
        print(contenturl)
        outstr=get_data(contenturl)
        do_write(outfile,contenturl[0],outstr)
        time.sleep(2)    
    
if __name__ == '__main__':
		main()