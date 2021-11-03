# coding=gbk
import queue,time,random
from lxml import etree

import re
import requests
import json
import tdzy
import xlrd
import traceback
from queue import Queue
from selenium import webdriver
from fake_useragent import UserAgent

dt=time.strftime('%Y%m%d',time.localtime(time.time()))
outfile="./data/sanya_"+ dt + ".txt"
outlistfile="./list/sanya.txt"
okfile="./data/sanya.txt"
outfile=okfile
#http://www.fcxx0898.com/syfcSiteWeb/Pages/Project/PresaleList.aspx
cookieid='x4xVjB2vFrnIjcc=KXZy8xczm0cNOm9VBxFKapEcVbWUvDcdzLd57db_PeEoZuxuRSwf__jjOTGedcHZmswX4i1IVXNd1fdx7wp_1Qdr9V5f61qxHv32Mn4X8WF5T'#时常变
url="http://www.fcxx0898.com/syfcSiteApi//Presale/ListPresale?"+cookieid

arealist=[]

def trim(word):
    return re.sub('\s+','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')
def getHtml(link,refer):
    html=None
    # print(link)
    try: #使用try except方法进行各种异常处理
        header = {
            'Host':'www.fcxx0898.com',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language':'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Accept-Encoding':'gzip, deflate',
            'Referer':refer,
            'Cookie':cookieid,
            'Upgrade-Insecure-Requests':'1',
            'Cache-Control':'max-age=0',
            'Connection':'keep-alive',
        } 
        res = requests.get(link,headers=header,timeout=20,verify=False) #读取网页源码
        html=res
    except Exception as e:
        print(traceback.format_exc()) 
    finally:
        return html
def postHtml(url,data):
    #使用try except方法进行各种异常处理
    global cookieid
    header = {
        'Host':'www.fcxx0898.com',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Accept':'*/*',
        'Accept-Language':'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Accept-Encoding':'gzip, deflate',
        'Content-Type':'application/json',
        'Authorization':'undefined',
        'X-Requested-With':'XMLHttpRequest',
        'Content-Length':'20',
        'Origin':'http://www.fcxx0898.com',
        'Referer':url,
        'Cookie':cookieid,
        'Cache-Control':'max-age=0',
        'Connection':'keep-alive',
    }
    #data=json.loads(js)
    try:
        res=requests.post(url=url,data=data,headers=header,verify=False)
        print(cookies_new)
        # #调用js函数
        # print(res.call('__doPostBack','AspNetPager1',2))
        res.encoding='UTF-8'
        return res.text
        
    except Exception as e:
        print(traceback.format_exc())
    return ''
def postbuildingHtml(url,data,refer):
    global cookieid
    #使用try except方法进行各种异常处理
    header = {
        'Host':'www.fcxx0898.com',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Accept':'*/*',
        'Accept-Language':'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Accept-Encoding':'gzip, deflate',
        'Content-Type':'application/json',
        'Authorization':'undefined',
        'X-Requested-With':'XMLHttpRequest',
        'Content-Length':'20',
        'Origin':'http://www.fcxx0898.com',
        'Referer':refer,
        'Cookie':cookieid,
        'Cache-Control':'max-age=0',
        'Connection':'keep-alive',
    }
    #data=json.loads(js)
    try:    
        res=requests.post(url=url,data=data,headers=header,verify=False)
        # #调用js函数
        # print(res.call('__doPostBack','AspNetPager1',2))
        res.encoding='UTF-8'
        while res.text.find('meta id=')>0:
            cookieidnew=input('需要输入新的cookie :')
            url=url.replace(cookieid,cookieidnew)
            cookieid=cookieidnew
            res=requests.post(url=url,data=data,headers=header,verify=False)
            
        return res.text
        
    except Exception as e:
        print(traceback.format_exc())
    return ''
def postroomHtml(url,data,refer,session):
    global cookieid
    #使用try except方法进行各种异常处理
    header = {
        'Host':'www.fcxx0898.com',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Accept':'*/*',
        'Accept-Language':'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Accept-Encoding':'gzip, deflate',
        'Content-Type':'application/json',
        'Authorization':'undefined',
        'X-Requested-With':'XMLHttpRequest',
        'Content-Length':'20',
        'Origin':'http://www.fcxx0898.com',
        'Referer':refer,
        'Cookie':cookieid+session,
        'Cache-Control':'max-age=0',
        'Connection':'keep-alive',
    }
    #data=json.loads(js)
    try:    
        res=requests.post(url=url,data=data,headers=header,verify=False)
        # #调用js函数
        # print(res.call('__doPostBack','AspNetPager1',2))
        res.encoding='UTF-8'
        while res.text.find('meta id=')>0:
            cookieidnew=input('需要输入新的cookie :')
            url=url.replace(cookieid,cookieidnew)
            cookieid=cookieidnew
            res=requests.post(url=url,data=data,headers=header,verify=False)
            
        return res.text
        
    except Exception as e:
        print(traceback.format_exc())
    return ''
def get_data(dicts):
    city='三亚'
    area=''
    rslist=[]
    ceil={}
    detail=''
    newhtml=''
    progect=''
    company=''
    addr=''
    ysz=''
    fzdt=''
    area=''
    try:
        proid=dicts[0].split('=')[1]
        ##
        data={"ID":proid}
        h1=postbuildingHtml('http://www.fcxx0898.com/syfcSiteApi//Presale/GetPerSale?'+cookieid,json.dumps(data),dicts[0])
        
        if h1.find('PresaleCert')==0:
            print('h1没有PresaleCert信息')
            return []
        js1=json.loads(h1)['Data']
        ceil = {
                "城市": city,
                "项目名称": js1.get('Name',''),
                "坐落位置": js1.get('Location',''), 
                "开发企业": js1.get('Owner',''),
                "预售许可证编号": js1.get('PresaleCert',''),
                "发证日期":'',
                "开盘日期":js1.get('StartDate',''),
                "预售证准许销售面积":str(js1.get('TotalArea','')),
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
        
        print(ceil)
        # time.sleep(1111)
        data={'PresaleID':proid}
        h2=postbuildingHtml('http://www.fcxx0898.com/syfcSiteApi//Project/ListBuildingByPresaleId?'+cookieid,json.dumps(data),dicts[0])
        # print(h2)
        j2=json.loads(h2).get('Data',[])
        
        if len(j2)==0:
            return [ceil]
        ######楼盘
        for loudong in j2:
            louhao=loudong.get('Block','')
            if louhao=='':
                continue
            buildId=str(loudong.get('BuildId',''))
            #http://www.fcxx0898.com/syfcSiteWeb/Pages/Project/ShowRoom.aspx?id=151716
            res3=getHtml(f'http://www.fcxx0898.com/syfcSiteWeb/Pages/Project/ShowRoom.aspx?id={buildId}',dicts[0]);
            token=''.join(re.findall("TokenCode: '(.*?)'",res3.text,re.S))
            print(louhao,token)
            data={
                "BuildId": buildId,
                "TokenCode": token
            }
            h4=postroomHtml('http://www.fcxx0898.com/syfcEstateDataApi//Bulid/ListBuildRoom?'+cookieid,json.dumps(data),'http://www.fcxx0898.com/syfcSiteWeb/Pages/Project/ShowRoom.aspx?id='+buildId,'; ASP.NET_SessionId=yqyd33cfxwpmueppabd033dn');
            # print(h4)
            j4=json.loads(h4)['Data'][0]['rooms']
            ts=str(len(j4))
            if ts==0:
                ceil2=ceil.copy()
                ceil2['销售楼号']=louhao
                rslist.append(ceil2)
                continue
            
            ######户室
            print('room len=',ts)
            for room in j4:
                ceil2=ceil.copy()
                ceil2['销售楼号']=louhao
                ceil2['套数']=ts
                ##
                ceil2['房号']=room.get('Floor')+'-'+room.get('RoomNo')
                ceil2['房屋销售状态']=room.get('HouseStatus','')
                for i in ceil2:
                    if ceil2[i]==None:
                        ceil2[i]=''
                rslist.append(ceil2)
           
    except Exception as e:
        print(traceback.format_exc()) 
    if len(rslist)==0:
        rslist.append(ceil)
    return rslist

def main():
    oklist=tdzy.openList(okfile)
    outlist=tdzy.openList(outfile)
    oklistlist=tdzy.openList(outlistfile)
    newlist=[]
    pn=0
    of = open(outlistfile,'a+', encoding='utf-8') 
    while False :
        pn+=1
        data={
            "KeyWord": "",
            "PageIndex": pn,
            "PageSize": 15
        }
        html=postHtml(url,json.dumps(data))
        ######### 
        js=json.loads(html)['Data']
        total=js.get('Total',0)
        print(pn)
        # print(js)
        js=js['pageList']
        if len(js)==0:
            break
        for j in js:
            contenturl='http://www.fcxx0898.com/syfcSiteWeb/Pages/Project/PresaleInfo.aspx?id='+str(j.get('ID'))
            xmmc=j.get('Name','')
            ysz=j.get('PresaleCert','')
            sp=j.get('Enterprise','')
            rs=[contenturl,xmmc,ysz,sp]
            print(rs)
            if contenturl in newlist:
                continue
            if contenturl not in oklistlist:
                newlist.append(contenturl)
                of.write('\t'.join(rs)+'\n')
                of.flush()
    
    # if len(newlist)>0:
        # tdzy.savefile(newlist,outlistfile)
    with open(outlistfile,'r',encoding='UTF-8') as txtData:
        datas=txtData.readlines()
        num=len(datas)
        for line in datas:
            num-=1
            print('less --------------',num)
            dicts=line.split('\t')
            if dicts[0] in oklist:
                continue
            print(dicts)
            outstr=get_data(dicts)
            # print(outstr)
            contenturl=dicts[0]
            tdzy.do_write(outfile,contenturl,outstr)
            time.sleep(1)
               
if __name__ == '__main__':
		main()