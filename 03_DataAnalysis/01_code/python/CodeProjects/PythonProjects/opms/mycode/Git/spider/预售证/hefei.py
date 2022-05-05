# -*- coding:utf-8 -*-  
import time,random
from lxml import etree
import urllib.parse
import re,os
import requests
import json
import traceback
from selenium import webdriver

from threading import Thread
from queue import Queue
urls = Queue()


outfile="./data/hefei_new.txt"
outlistfile="./list/hefei_url.txt"
outprofile="./list/hefei_pro_url.txt"
okfile="./data/hefei.txt"

url='http://60.173.254.126:8888/Permit?p=$1&item=&use=&permitno='

ysz_oklist=set()

def trim(word):
    return re.sub('\s+','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')

def get_data(contenturl):
    city='合肥'
    area=''
    rslist=[]
    cell={}
    detail=''
    newhtml=''
    try:
        #rs=[contenturl,pro,sp,addr,ysz,loudong,ysmj,fzrq,prourl]
        cell = {
                "城市": city,
                "项目名称": contenturl[1],
                "坐落位置": contenturl[3],
                "开发企业": contenturl[2],
                "预售许可证编号":contenturl[4],
                "发证日期":contenturl[7],
                "开盘日期":'',
                "预售证准许销售面积":contenturl[6],
                "销售状态": '',
                "销售楼号": contenturl[5],
                "套数": '',
                "面积": '',
                "拟售价格": '',
                "售楼电话": '',
                "售楼地址": '',
                "房号": '',
                "房屋建筑面积": '',
                "房屋销售状态": '',
        }
        time.sleep(0.5)
        h2=getHtml(contenturl[0],'')
        cell["套数"] = ''.join(re.findall('套数：<span>(.*?)</span>',h2,re.S))
        cell["售楼电话"] = ''.join(re.findall('销售电话：<span>(.*?)</span>',h2,re.S))
        tab4 = etree.HTML(h2).xpath('//div[@class="bei2_t5"]//tr[position()>1]//td[position()>1]')
        if len(tab4)==0:
            print(tab4)
            return [cell]
        for kk in tab4:
            cell2=cell.copy()
            fh = ''.join(kk.xpath(".//text()"))
            if fh == "-":
                continue
            colorstyle = ''.join(kk.xpath(".//@style"))
            color = re.findall(r'background-color:(.*?);text-wrap:', colorstyle, re.I)[0]
            idroom=re.sub('[s\(\)\'\"]*','',''.join(kk.xpath('./@onclick')))
            time.sleep(0.5)
            h3=getHtml('http://60.173.254.126:8888/details/getrsa/'+nscaler(idroom),contenturl[0])
            hid=''.join(re.findall('"id":"(.*?)"}',h3))
            cell2["房号"] = fh
            cell2["房屋销售状态"] = getstate(color)
            if len(idroom)>1 and len(hid)>10:
                roomurl='http://60.173.254.126:8888/details/house/'+hid
                time.sleep(0.5)
                hroom=getHtml(roomurl,contenturl[0])
                # print(contenturl[0])
                # print(roomurl)
                # print(hroom)
                cell2['拟售价格']=''.join(re.findall('"iPrice":"(.*?)"',hroom,re.S)) 
                cell2['房屋建筑面积']=''.join(re.findall('"lbBuildArea":"(.*?)"',hroom,re.S))
            rslist.append(cell2)
    except Exception as e:
        print(traceback.format_exc()) 
    if len(rslist)==0:
        rslist.append(cell)
    # print(rslist)
    return rslist
def nscaler(a):
    ##6416506-3623103
    dictn={
        '0':'0',
        '1':'2',
        '2':'5',
        '3':'8',
        '4':'6',
        '5':'1',
        '6':'3',
        '7':'4',
        '8':'9',
        '9':'7'
    }
    b = "";
    for i in a:
        b+=dictn[i]
    return b

def getstate(color):
    state = color
    if (color == '#00ff00'):
        state = '可售'
    elif (color == '#0001fe'):
        state = '已签约'
    elif (color == '#fd0002'):
        state = '备案'
    elif (color == '#006500'):
        state = '已办产权'
    elif (color == '#febbff'):
        state = '抵押可售'
    elif (color == '#ffa500'):
        state = '摇号销售'
    elif (color == '#38a4ff'):
        state = '现房销售'
    elif (color == '#bebebe'):
        state = '已限制'
    elif (color == '#cc6600'):
        state = '限制销售'
    elif (color == '#33cc99'):
        state = '自建房'
    elif (color == '#01fffe'):
        state = '拆迁安置'
    elif (color == '#d0208f'):
        state = '抵押不可售'
    elif (color == '#0571a0'):
        state = '配套、物管、社区用房'
    elif (color == '#EEB422'):
        state = '附条件销售'
    return state
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
def getHtml(link,refer):
    html=""
    try: #使用try except方法进行各种异常处理
        header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.02' }
        res = requests.get(link,headers=header,timeout=20,verify=False) #读取网页源码
        #解码
        # print(link)
        res.encoding='UTF-8'
        html=res.text
    except Exception as e:
        print(e)
    finally:
        return html

def postHtml(url,data):
    #使用try except方法进行各种异常处理
    header = {
            'User-Agent':'%E6%88%BF%E7%AE%A1%E5%B1%80/1 CFNetwork/978.0.7 Darwin/18.7.0',
            'Content-Type':'application/x-www-form-urlencoded',
            'Cookie':'JSESSIONID=2420324B8617B7EACA7D596EE868CF02; JSESSIONID=5D1E7B5718E5C996ABC768E0C9A842AC',
            'Accept': '*/*'
            }
    #data=json.loads(js)
    try:
        res=requests.post(url=url,data=data,headers=header,verify=False)
        res.encoding='UTF-8'
        return res.text
    except Exception as e:
        print(traceback.format_exc())
    return ''
def main():
    if False:
        #proid urllist by id 
        driver =webdriver.Firefox()# webdriver.PhantomJS(executable_path=r'D:\tools\phantomjs-2.1.1-windows\bin\phantomjs.exe')
        driver.get('http://60.173.254.126:8888/index')
        time.sleep(5)
        taga=driver.find_elements_by_xpath('//div[@class="beian_se2_1_1"]/dl/dt/span')
        num=0
        for t in taga:
            num+=1
            if num==10:
                break
            t.click()
            time.sleep(1)
            
        html=driver.page_source
        tab=etree.HTML(html).xpath('//span[@class="nav1"]/a/@id')
        n=len(tab)
        lista = {}
        prolist=[]
        if os.path.exists(outprofile):
            with open(outprofile,'r',encoding='utf-8') as txtData: 
                for line in txtData.readlines():
                    prolist.append(line.split('\t')[0])
        #save pro url          
        of = open(outprofile,'a+', encoding='utf-8') #保存结果文件
        now_handle = driver.current_window_handle
        num=0
        print('pronum=',len(tab))
        for id in tab:
            num+=1
            try:
                if id in prolist:
                    continue
                prolist.append(id)
                driver.switch_to_window(now_handle)
                # JavascriptExecutor executor = (JavascriptExecutor)driver;
                # executor.executeScript("arguments[0].click();", ele);
                driver.find_element_by_id(str(id)).click()
                time.sleep(0.2)
                all_handles = driver.window_handles
                # print(len(all_handles))
                h2='--'
                ccurl=''
                for handle in all_handles:
                    if handle != now_handle:
                        driver.switch_to_window(handle)
                        h2=driver.page_source
                        time.sleep(0.2)
                        ccurl=driver.current_url
                        driver.close()
                        sp=''.join(re.findall('开发公司：</strong>(.*?)</dd>',h2,re.S))
                        addr=''.join(re.findall('项目地址：</strong>(.*?)</dd>',h2,re.S))
                        print(num,addr)
                        of.write(str(id)+'\t'+ccurl+'\t'+sp+'\t'+addr+'\n')
                        of.flush()
            except  Exception as e :
                time.sleep(3)
                print(traceback.format_exc())
        driver.switch_to_window(now_handle)
        driver.close()
        
    #按预售证过滤，爬取信息列表
    if os.path.exists(outfile):
        with open(outfile,'r',encoding='utf-8') as txtData: 
            for line in txtData.readlines():
                if len(line.split('\t'))>5:
                    ysz_oklist.add(line.split('\t')[5])
    if os.path.exists(okfile):
        with open(okfile,'r',encoding='utf-8') as txtData: 
            for line in txtData.readlines():
                if len(line.split('\t'))>5:
                    ysz_oklist.add(line.split('\t')[5])
    #读取预售证url列表
    listlisturl=[]
    if os.path.exists(outlistfile):
        with open(outlistfile,'r',encoding='utf-8') as txtData: 
            for line in txtData.readlines():
                listlisturl.append(line.split('\t')[4])
    #读取id对应的项目名称和地址
    lista={}
    if os.path.exists(outprofile):
        with open(outprofile,'r',encoding='utf-8') as txtData: 
            for line in txtData.readlines():
                s=line.split('\t')
                lista[s[0]]=(s[1],s[2],s[3].strip())
    #翻页爬取ysz URl列表
    of = open(outlistfile,'a+', encoding='utf-8') #保存结果文件
    pn=0
    end='no'
    while True :
        break
        #设置翻页中断
        if end=='yes':
            break
        time.sleep(1)
        pn+=1
        print('ysz pn=',pn)
        html=getHtml(url.replace('$1',str(pn)),'').replace('&nbsp;','')
        e1=etree.HTML(html)
        tab1=e1.xpath('//div[@class="right_table"]/table/tbody/tr[position()>1]')
        print('page=',pn,len(tab1))
        if len(tab1)==0:
            print('tab=0')
            break
        for tr in tab1:
            #设置翻页中断为2021/3
            fzrq=''.join(tr.xpath('./td[5]/text()'))
            print(fzrq,fzrq.find('2021/3/'))
            if fzrq.find('2021/3/')>-1:
                end='yes'
                break
            #过滤已爬预售证
            ysz=''.join(tr.xpath('./td[1]/a/text()'))
            if ysz in ysz_oklist or ysz in listlisturl:
                continue
            listlisturl.append(ysz)
            ##保存新地址
            id=''.join(tr.xpath('./td[2]/a/@id'))
            tmp=lista.get(id)
            if tmp==None:
                prourl=sp=addr=''
            else:
                prourl=tmp[0]
                sp=tmp[1]
                addr=tmp[2]
            contenturl='http://60.173.254.126:8888'+tr.xpath('./td[1]/a/@href')[0]
            pro=''.join(tr.xpath('./td[2]/a/text()'))
            loudong=''.join(tr.xpath('./td[3]/a/text()'))
            ysmj=''.join(tr.xpath('./td[4]/text()'))
            rs=[contenturl,pro,sp,addr,ysz,loudong,ysmj,fzrq,prourl]
            of.write('\t'.join(rs)+'\n')
            of.flush() 
    #爬虫房信息
    with open(outlistfile,'r',encoding='UTF-8') as txtData: 
        for line in txtData.readlines():
            contenturl=line.split('\t')
            if contenturl[4] in ysz_oklist:
                continue
            urls.put(contenturl)
            # outstr=get_data(contenturl)
            # print(outstr)
            # time.sleep(1111)
        print("qsize="+str(urls.qsize())) 
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
        do_write(outfile,contenturl[8].strip(),outstr)
        # time.sleep(111110)    

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