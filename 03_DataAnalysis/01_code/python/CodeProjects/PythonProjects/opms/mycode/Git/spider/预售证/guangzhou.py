# -*- coding=utf-8 -*-
import time,random
from lxml import etree
import base64
import execjs
from selenium import webdriver


import re,os
import requests
import json
import traceback

from Crypto.Cipher import PKCS1_v1_5 as Cipher_pksc1_v1_5
from Crypto.PublicKey import RSA

from threading import Thread
from queue import Queue

urls = Queue()
oklist=set()

outfile="./data/guangzhou_new.txt"
outlistfile="./list/guangzhou.txt"
okfile="./data/guangzhou.txt"

url='http://zfcj.gz.gov.cn/zfcj/fyxx/fdcxmxxRequest?sProjectName=&sProjectAddress=&sDeveloper=&sPresellNo=&ValidateCode=&page=$1&pageSize=15'

def trim(word):
    return re.sub('\s+','',word).replace('\\n','').replace('\\xa0','').replace('\\t','').replace('&nbsp;','')
def postHtml(url,data):
    #使用try except方法进行各种异常处理
    header = {
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
        'Host': 'zfcj.gz.gov.cn',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' ,
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2' ,
        'Content-Type': 'application/x-www-form-urlencoded' ,
        'Referer': 'http://zfcj.gz.gov.cn/zfcj/fyxx/xkb?sProjectId=100000022310&sPreSellNo=' ,
        'Cookie': 'JSESSIONID=74414D52F61A249250F56085102075A4; sto-id-20480=GLPNMCAKFAAA' ,
    }
    #data=json.loads(js)
    try:
        res=requests.post(url=url,data=data,headers=header,verify=False)
        
        # #调用js函数
        # print(res.call('__doPostBack','AspNetPager1',2))
        res.encoding='UTF-8'
        return res.text
        
    except Exception as e:
        print(traceback.format_exc())
    return ''
def get_data(contenturl,driver):
    #print(contenturl,pro,sp,ysz,addr)
    city='广州'
    area=''
    rslist=[]
    cell={}
    detail=''
    newhtml=''
    progect=contenturl[1]
    company=contenturl[2]
    ysz=contenturl[3]
    addr=contenturl[4].strip()
    
    sProjectId=re.findall('sProjectId=(\d+)',contenturl[0])[0]
    presell=contenturl[5].strip()
    try:
    
        #pro    http://zfcj.gz.gov.cn/zfcj/fyxx/projectdetail?sProjectId=45585&sDeveloperId=13844 http://zfcj.gz.gov.cn/zfcj/fyxx/jbxx?sProjectId=45585
        #ysz    http://zfcj.gz.gov.cn/zfcj/fyxx/jbxx?sProjectId=47140&sDeveloperId=15864  可能为空不用
        #lplist http://zfcj.gz.gov.cn/zfcj/fyxx/xkb?sProjectId=100000022310&sPreSellNo=
        #rooms POST http://zfcj.gz.gov.cn/zfcj/fyxx/xmxkbxxList
        cell = {
                "城市": city,
                "项目名称": progect,
                "坐落位置":addr, 
                "开发企业": company,
                "预售许可证编号":ysz,
                "发证日期":'',
                "开盘日期":'',
                "预售证准许销售面积":'',
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
        ##
        h1=getHtml(contenturl[0].replace('projectdetail','jbxx'))
        ysmj=''.join(re.findall('批准预售面积：</td>.*?<td class="tab_style01_td">(.*?)</td>',h1,re.S)).strip()
        cell['预售证准许销售面积']=ysmj
        ysz=''.join(re.findall('预售证：</td>.*?<td class="tab_style01_td">(.*?)</td>',h1,re.S)).strip()
        cell['预售许可证编号']=ysz
        ##fzrq
        presell_1=ysz.split(',')[0]
        h2=getHtml(f'http://zfcj.gz.gov.cn/zfcj/fyxx/ysz?sProjectId={sProjectId}&sPreSellNo={presell_1}')
        fzrq=''.join(re.findall('发证日期.*?align="left">(.*?)</p>',h2,re.S)).strip()
        cell['发证日期']=fzrq
        ##longdong list
        u=contenturl[0].replace('projectdetail','xkb')+'&sPreSellNo='+ysz
        driver.get(u)
        time.sleep(1)
        driver.execute_script('document.getElementsByName("modeID")[0].checked=false')
        time.sleep(1)
        driver.execute_script('document.getElementsByName("modeID")[1].checked=true')
        time.sleep(1)
        text=driver.page_source
        loudongs=re.findall('name="buildingId".*?value="(.*?)".*?>(.*?)</td>',text,re.S)
        stopnum=0
        while text.find('name="buildingId"')<0:
            driver.refresh()
            print('刷新')
            time.sleep(2)
            text=driver.page_source
            stopnum+=1
            if stopnum>5:
                break
        token=driver.execute_script('return document.getElementById("token").value')
        print('loudongs=',len(loudongs))
        
        if len(loudongs)==0:
            return [cell]
        ##############有户室
        parma={
                'sProjectId':sProjectId,
                'token':token,
                'modeID':'2',#2是列表，1是图
                'houseFunctionId':'0',
                'unitType':'',
                'houseStatusId':'0',
                'totalAreaId':'0',
                'inAreaId':'0',
                'buildingId':'',
        }
        num=0
        for loudong in loudongs:
            cell2=cell.copy()
            cell2['销售楼号']=loudong[1]
            #
            parma['buildingId']=loudong[0]
            # if (buildingIds[i].checked == true) {
                        # buildingIds[i].checked = false;
                        # var j =i + 1
                        # if (j<buildingIds.length)
                        # {
                            # buildingIds[j].checked = true;
                            # break
                        # }
                    # } 
            js='var buildingIds = document.getElementsByName("buildingId");for (var i = 0; i < buildingIds.length; i++) {buildingIds[i].checked = false;} buildingIds['+str(num)+'].checked = true;DoSearch();return document.getElementById("token").value;'
            
            # print(js)
            token=driver.execute_script(js)
            num+=1
            
            parma['token']=token
            time.sleep(2)
            h3=postHtml('http://zfcj.gz.gov.cn/zfcj/fyxx/xmxkbxxList',parma)
            stopnum=0
            while text.find('判断当前状态')<0:
                time.sleep(2)
                h3=postHtml('http://zfcj.gz.gov.cn/zfcj/fyxx/xmxkbxxList',parma)
                stopnum+=1
                if stopnum>5:
                    break
            tab33=etree.HTML(h3)
            rooms=tab33.xpath('//div[@class="content_tab"]/table/tr')
            # print('h3-token=',token)
            print('rooms=',len(rooms))
            if len(rooms)==0:
                rslist.append(cell2)
                continue
            cell2['套数']=str(len(rooms))
            for room in rooms:
                cell3=cell2.copy()
                cell3['房屋销售状态']=room.xpath('./td[5]')[0].xpath('string(.)').strip()
                cell3['房号']=room.xpath('./td[1]')[0].xpath('string(.)').strip()
                cell3['房屋建筑面积']=room.xpath('./td[3]')[0].xpath('string(.)').strip()
                rslist.append(cell3)
                # print(cell3)
        if len(rslist)==0:
            rslist.append(cell)
    except Exception as e:
        print(traceback.format_exc())
    print(len(rslist))
    # time.sleep(11111)
    return rslist
def openList(filename):
    datalist=[]
    try:
        if  os.path.exists(filename):
            with open(filename,'r',encoding='UTF-8') as txtData: 
                for line in txtData.readlines():
                    datalist.append(line.split('\t')[0])
    except Exception as e:
        print(traceback.format_exc())
    return datalist
def do_write(outfile,url,rsdict):
    of = open(outfile,'a+', encoding='utf-8') #保存结果文件
    for dicts in rsdict:
        of.write(url+"\t")
        of.write(dicts.get('城市','')+'\t')
        of.write(dicts.get('项目名称','')+'\t')
        of.write(dicts.get('坐落位置','')+'\t')
        of.write(dicts.get('开发企业','')+'\t')
        of.write(dicts.get('预售许可证编号','')+'\t')
        of.write(dicts.get('发证日期','')+'\t')
        of.write(dicts.get('开盘日期','')+'\t')
        of.write(dicts.get('预售证准许销售面积','')+'\t')
        of.write(dicts.get('销售状态','')+'\t')
        of.write(dicts.get('销售楼号','')+'\t')
        of.write(dicts.get('套数','')+'\t')
        of.write(dicts.get('面积','')+'\t')
        of.write(dicts.get('拟售价格','')+'\t')
        of.write(dicts.get('售楼电话','')+'\t')
        of.write(dicts.get('售楼地址','')+'\t')
        of.write(dicts.get('房号','')+'\t')
        of.write(dicts.get('房屋建筑面积','')+'\t')
        of.write(dicts.get('房屋销售状态',''))
        of.write("\n")
        of.flush()
def getHtml(link):
    html=""
    # print(link)
    try: #使用try except方法进行各种异常处理
        header = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0'+str(random.random()),
            'Host': 'zfcj.gz.gov.cn',
            'Accept': 'application/json, text/javascript, */*; q=0.01' ,
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2' ,
            # 'Content-Type': 'application/json' ,
            'X-Requested-With': 'XMLHttpRequest' ,
            'Referer': 'http://zfcj.gz.gov.cn/zfcj/fyxx/fdcxmxx' ,
            'Cookie': 'JSESSIONID=74414D52F61A249250F56085102075A4; sto-id-20480=GLPNMCAKFAAA' ,
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
def getHtml2(link,id):
    html=""
    # print(link)
    try: #使用try except方法进行各种异常处理
        header = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
            'Cookie': f'npsaleid={id}; wjbazt=; ASP.NET_SessionId=3b2mtoowny5c2dxpkwrvgidt;CNZZDATA915400=cnzz_eid%3D257811575-1619411532-%26ntime%3D1619411532; UM_distinctid=1790c9a3f071f0-077d05e1d885288-4b5f451b-144000-1790c9a3f08581',
            'Referer':'http://gs.czfdc.com.cn/newxgs/Pages/Lp_Show.aspx?a=0.38875284136369764',
           
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
        res.encoding='utf-8'
        html=res.text
    except Exception as e:
        print(e)
    finally:
        return html
def main():
    #已爬列表
    if os.path.exists(outfile):
        with open(outfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')                    
                oklist.add(a[0])
                
    if os.path.exists(okfile):
        with open(okfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                oklist.add(a[0])
    
    repeatlist=[]
    if os.path.exists(outlistfile):
        with open(outlistfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                repeatlist.append(a[0])
    ##
    of = open(outlistfile,'a+', encoding='utf-8') #保存结果文件
    pn=0
    total=0
    html=''
    while True :
        break
        pn+=1
        try:
            time.sleep(8)
            while True :
                html=getHtml(url.replace('$1',str(pn)))
                if html.find('接口签名错误或已超时')<0 and len(html)>100:
                    break
                print('sleep15')
                time.sleep(30)
            if total==0:
                total=int(re.findall('totalPage":(\d+),',html,re.S)[0])
            js=json.loads(html)['data']
            print('pn=',pn,len(js))
            #########
            if len(js)==0:
                break
            for j in js: 
                presell=str(j.get('presell',''))
                sProjectId=str(j.get('projectId',''))
                sDeveloperId=str(j.get('developerId',''))
                ysz=str(j.get('presell',''))
                pro=j.get('projectName','')
                addr=str(j.get('projectAddress',''))
                sp=j.get('developer','')  
                contenturl=f'http://zfcj.gz.gov.cn/zfcj/fyxx/projectdetail?sProjectId={sProjectId}&sDeveloperId={sDeveloperId}'
                time.sleep(0.1)
                if contenturl in oklist:
                    continue
                if contenturl not in repeatlist:
                    repeatlist.append(contenturl)
                    of.write(contenturl+'\t'+pro+'\t'+sp+'\t'+ysz+'\t'+addr+'\t'+presell+'\n')
                    of.flush()
        except Exception as e :
            print(traceback.format_exc())
            print('p',html)
            break
        #结束跳出while
        if total<=pn:
            break
        # break####当天只有一页
    repeatlist=[]
    if os.path.exists(outlistfile):
        with open(outlistfile,'r',encoding='UTF-8') as txtData: 
            for line in txtData.readlines():
                contenturl=line.split('\t')
                if contenturl[0] in oklist or contenturl[0] in repeatlist:
                    continue
                repeatlist.append(contenturl[0])
                oklist.add(contenturl[0])
                urls.put(contenturl)
                # driver = webdriver.PhantomJS(executable_path=r'D:\tools\phantomjs-2.1.1-windows\bin\phantomjs.exe')
                # outstr=get_data(contenturl,driver)
                # driver.close()
                # time.sleep(11111)
        print("qsize="+str(urls.qsize())) 
        ths = []
        for i in range(1):
            t = Thread(target=run, args=())
            t.start()
            ths.append(t)
        for t in ths:
            t.join()
def run():
    driver = webdriver.PhantomJS(executable_path=r'D:\tools\phantomjs-2.1.1-windows\bin\phantomjs.exe')
    while urls.qsize() != 0: 
        contenturl=urls.get()
        print("qsize less="+str(urls.qsize()))   
        print(contenturl)
        outstr=get_data(contenturl,driver)
        do_write(outfile,contenturl[0],outstr)
        # time.sleep(1)    
    driver.close()
    
def savefile(datalist,filename):
    old_datalist=[]
    try:
        with open(filename,'r') as txtData: 
            for line in txtData.readlines():
                old_datalist.append(line.strip())
    except Exception as e:
        print(traceback.format_exc())
    of = open(filename,'a+', encoding='utf-8') #保存文件
    for data in datalist:
        if datalist not in old_datalist:
            of.write(data+"\n")
            of.flush()              
if __name__ == '__main__':
		main()