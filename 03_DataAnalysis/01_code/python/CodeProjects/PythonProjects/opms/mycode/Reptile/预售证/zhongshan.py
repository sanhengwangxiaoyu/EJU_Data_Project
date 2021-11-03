# -*- coding:utf-8 -*-

import queue,time,random
from lxml import etree
from selenium import webdriver
import re,urllib.parse
import requests
import json,os
import tdzy
import xlrd
import traceback
from threading import Thread
from queue import Queue

dt=time.strftime('%Y%m%d',time.localtime(time.time()))
outfile="./data/zhongshan_new.txt"
outlistfile="./list/zhongshan.txt"
okfile="./data/zhongshan.txt"
url="http://113.106.13.237:82/"

urls = Queue()
okurl = set()
arealist=[]
def trim(word):
    return re.sub('[\s]*','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')   
def get_data():
    while urls.qsize() != 0:
        contenturl=urls.get()
        print(contenturl)
        try:
            html=getHtml(contenturl[0])
            if len(html)<1000:
                return []
            lxmls = etree.HTML(html)
            trs =[]
            city='中山'
            area=''
            rslist=[]
            ceil={}
            ceil2={}
            ceil3={}
            detail=''
            newhtml=''
            company=''
            newurl=''
            table=lxmls.xpath('//table[@class="housedetail"]')[0]
            #项目名称
            progect=''
            try:
                progect=table.xpath('./tr[1]/td[2]/span/text()')[0]
            except Exception as e:
                print(traceback.format_exc())
            #坐落位置
            addr=''
            try:
                addr=table.xpath('./tr[2]/td[2]/span/text()')[0]
            except Exception as e:
                print(traceback.format_exc())
            #开发企业
            try:
                company=table.xpath('./tr[3]/td[2]/span/text()')[0]
            except Exception as e:
                print(traceback.format_exc())
            #预售许可证编号
            ysz=''
            try:
                ysz=table.xpath('./tr[4]/td[2]/span/text()')[0]
            except Exception as e:
                print(traceback.format_exc())
            #发证日期
            fzrq=''
            try:
                fzrq=table.xpath('./tr[4]/td[4]/span/text()')[0]
            except Exception as e:
                print(traceback.format_exc())
            #预售证准许销售面积
            ysmj=''
            try:
                ysmj=table.xpath('./tr[10]/td[4]/span/text()')[0]
            except Exception as e:
                print(traceback.format_exc())
           
            ceil['城市']=city
            ceil['项目名称']=progect
            ceil['开发企业']=company
            ceil['预售证准许销售面积']=ysmj
            ceil['坐落位置']=addr
            ceil['发证日期']=fzrq
            ceil['预售许可证编号']=ysz
            #
            print(ceil)
            
            lou=re.findall("<span for='build(.+?)'>(.*?)</span>",html,re.S)
            print(lou)
            for l in lou:
                ceil2={}
                #楼栋
                ld=l[1]
                ceil2['销售楼号']=ld
                dongid=l[0]
                #户列表
                newurl=f'http://113.106.13.237:82/HPMS/RoomList.aspx?code={dongid}&rsr=1000&rse=0&rhx=3000&jzmj=&tnmj='
                html3 = getHtml(newurl)
                lxml3 = etree.HTML(html3)
                trs=lxml3.xpath('//div[@id="divRoomList"]/table/tr')
                if len(trs)>0 :
                    #套数
                    ts=len(trs)-1
                    ceil2['套数']=str(ts)
                    print(ts)
                    for num in range(1,ts+1):
                        ceil3={}
                        try:
                            ceil3['房屋建筑面积']=trs[num].xpath('./td[4]/text()')[0]
                        except Exception as e:
                            # print(traceback.format_exc())
                            pass
                        try:
                            ceil3['房屋销售状态']=trs[num].xpath('./td[6]/text()')[0]
                        except Exception as e:
                            # print(traceback.format_exc())
                            pass
                        #单元
                        dy=''
                        try:
                            dy=trs[num].xpath('./td[2]/text()')[0]
                        except Exception as e:
                            # print(traceback.format_exc())
                            pass
                        try:
                            if len(dy)>1:
                                ceil3['房号']=dy+'-'+trs[num].xpath('./td[1]/a/u/text()')[0]
                            else:
                                ceil3['房号']=trs[num].xpath('./td[1]/a/u/text()')[0]
                        except Exception as e:
                            # print(traceback.format_exc())
                            pass
                        for c in ceil:
                            ceil3[c]=ceil[c]
                        for c in ceil2:
                            ceil3[c]=ceil2[c]
                        rslist.append(ceil3)
                       
                if len(rslist)==0:
                    for c in ceil:
                        ceil2[c]=ceil[c]
                    rslist.append(ceil2)
            if len(rslist)==0:
                rslist.append(ceil)
            # return rslist
        except Exception as e:
            print('error 1')
            print(traceback.format_exc()) 
        tdzy.do_write(outfile,contenturl[0],rslist)
        time.sleep(1)
def getHtml(link):
    num=0
    html="<html></html>"
    print(link)
    if link.find('code=X')>0:
        return html
    try: #使用try except方法进行各种异常处理
        header = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER'}
        res = requests.get(link,headers=header,timeout=50,verify=False) #读取网页源码
        #解码
        if res.encoding=='utf-8' or res.encoding=='UTF-8' or res.text.find('charset="utf-8"')>0:
        		res.encoding='utf-8'
        else:
        		m = re.compile('<meta .*(http-equiv="?Content-Type"?.*)?charset="?([a-zA-Z0-9_-]+)"?', re.I).search(res.text)
        		if m and m.lastindex == 2:
        		    charset = m.group(2).upper()
        		    res.encoding=charset
        		else:
        		    res.encoding='GBK'
        res.encoding='UTF-8'
        html=res.text
    except Exception as e:
        # num+=1
        # print('gethtml wrong num='+str(num))
        print(traceback.format_exc())
        # time.sleep(5)
        # html=getHtml(link)
    finally:
        return html

def main():
    #
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
    if True :
        source=''
        total=0
        of = open(outlistfile,'a', encoding='utf-8') #保存结果文件，按url+预售证增量添加
        try: #使用try except方法进行各种异常处理
            browser = webdriver.Firefox()
            browser.get(url)
            time.sleep(5)
            source=browser.page_source
            if total==0:
                m=re.findall('PageNavigator1_LblPageCount">(\d+)</span>页',source)
                total=int(m[0])
                print('total=',total)
            #翻页
            pn=1
            end='no'
            while pn<=total and end=='no':
                lxmls = etree.HTML(source)
                trs=lxmls.xpath('//div[@class="resultlist"]//tr[position()>1]')
                # trs=re.findall('(HPMS/PresellDetailsInfo.aspx\?id=.*?)"',source)
                print(pn,len(trs))
                if len(trs)==0:
                    break
                for tr in trs:
                    contenturl='http://113.106.13.237:82/'+tr.xpath('.//a/@href')[0]
                    date=tr.xpath('./td[6]/text()')[0]
                    if date.find('2021-03')>0:
                        end='yes'
                        print('pass')
                        break
                    if contenturl in okurl:
                        continue
                    of.write(contenturl+'\n')
                    of.flush();
                #       
                pn+=1
                browser.execute_script('javascript:__doPostBack(arguments[0],arguments[1])','PageNavigator1$LnkBtnNext','')
                time.sleep(3)
                source=browser.page_source
            browser.close()
        except Exception as e:
            print(traceback.format_exc())
   
    #################
    #2多线程爬项目
    with open(outlistfile,'r', encoding='utf-8') as f:
        for i in f:
            a = i.strip().split('\t')
            if a[0] in okurl:
                continue
            urls.put(a)
    ths = []
    for i in range(10):
        t = Thread(target=get_data, args=())
        t.start()
        ths.append(t)
    for t in ths:
        t.join()
     
if __name__ == '__main__':
		main()
        