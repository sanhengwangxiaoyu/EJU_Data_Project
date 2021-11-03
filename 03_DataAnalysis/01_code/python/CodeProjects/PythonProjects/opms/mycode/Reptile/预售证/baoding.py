# -*- coding:utf-8 -*-  
# 按URL增量，为预售证但没有预售证基本信息
#预售证基本信息单独爬取和保存，但无法和房源信息直接关联
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
okurl=set()

dt=time.strftime('%Y%m%d',time.localtime(time.time()))
outfile="./data/baoding_new.txt"
outlistfile="./list/baoding.txt"
okfile="./data/baoding.txt"
yszfile='./data/baoding_ysz.txt'

#用于获取房源信息
url='http://www.bdfdc.net/loadAllProjects.jspx?pageIndex=$1'
#用于获取全量的预售证信息
url_ysz='http://www.bdfdc.net/permitPageList.jspx?pageIndex=$1&orderType=1&orderFile=permit_start_date'
def trim(word):
    return re.sub('\s+','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')

def get_data(dicts):
    city='保定'
    area=''
    rslist=[]
    cell={}
    detail=''
    newhtml=''
    try:
        #dicts=[contenturl,pro,sp,addr,tel]
        #pro
        cell = {
                "城市": city,
                "项目名称": dicts[1],
                "坐落位置": dicts[3],
                "开发企业": dicts[2],
                "预售许可证编号":'',
                "发证日期":'',
                "开盘日期":'',
                "预售证准许销售面积":'',
                "销售状态": '',
                "销售楼号": '',
                "套数": '',
                "面积": '',
                "拟售价格": '',
                "售楼电话": dicts[4].strip(),
                "售楼地址": '',
                "房号": '',
                "房屋建筑面积": '',
                "房屋销售状态": '',
        }
        print(cell)
        h1=getHtml(dicts[0],'')
        tab1=re.findall('href="(/loadHouseSaleTable\.jspx\?str_item=.*?)">(.*?)</a>',h1,re.S) 
        if len(tab1)==0:
            print('tab1=0')
            return [cell]
        ###lou list 
        for href,loupan in tab1:
            cell2=cell.copy()
            h3=getHtml('http://www.bdfdc.net'+href,'')
            tab3 = etree.HTML(h3)
            rooms=tab3.xpath('//div[@class="pb_text"]/.//div/table/tr/.//table/tr/td')
            ts=str(len(rooms))
            print('rooms=',ts)
            if ts=='0':
                cell3=cell2.copy()
                cell3['销售楼号']=loupan
                rslist.append(cell3)
                continue
            for room in rooms:
                cell3=cell2.copy()
                jzmj=''.join(room.xpath('./a/@wf')).split('<br>')[0].replace('m<sup>2</sup>','m2').replace('建筑面积:','')
                fh=room.xpath('string(.)').strip()
                sale=''.join(room.xpath('./@bgcolor'))
                if '#ffffff' ==sale:
                    sale='可售'
                elif '#08F40C'==sale:
                    sale='备案'
                elif '#FDFF80'==sale:
                    sale='签约'
                elif '#848484'==sale:
                    sale='不可售'
                elif '#9849F0'==sale or '#FA9EEB'==sale or sale=='#FAA308':
                    sale='未知'
                elif '#FF0103'==sale:
                    sale='查封'
                cell3['销售楼号']=loupan
                cell3['套数']=ts
                cell3['房号']=fh
                cell3['房屋建筑面积']=jzmj
                cell3['房屋销售状态']=sale
                rslist.append(cell3)
                
    except Exception as e:
        print(traceback.format_exc()) 
    if len(rslist)==0:
        rslist.append(cell)
    # print(rslist)
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
def getHtml(link,refer):
    html=""
    try: #使用try except方法进行各种异常处理
        header = {
            }
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
    #获取已爬list
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
    if os.path.exists(outlistfile):
        with open(outlistfile, 'r', encoding='utf-8') as f:
            for i in f:
                repeatlist.append(i.split('\t')[0])
    #如果outfile没数据，需要创建并写一个表头
    if has_new==0:
        of = open(outfile,'a', encoding='utf-8') #保存结果文件
        of.write('URL\t城市\t项目名称\t坐落位置\t开发企业\t预售许可证编号\t发证日期\t开盘日期\t预售证准许销售面积\t销售状态\t销售楼号\t套数\t面积\t拟售价格\t售楼电话\t售楼地址\t房号\t房屋建筑面积\t房屋销售状态（颜色区分）\n')
        of.flush()
    ##爬取售证信息保存到ysz中
    of = open(yszfile,'a', encoding='utf-8') #保存结果文件
    pn=0
    #ysz
    while True:
        pn+=1
        print('ysz pn=',pn)
        html=getHtml(url_ysz.replace('$1',str(pn)),'')
        total=int(re.findall('共(\d+)页',html)[0])
        hrefs=re.findall('/searchPreSalePermit\.jspx\?id=(.*?)"',html,re.S)
        if len(hrefs)==0:
            print(0)
            break
        for href in hrefs:
            u2=f'http://www.bdfdc.net/searchPreSalePermit.jspx?id={href}'
            h2=getHtml(u2,'')
            e2=etree.HTML(h2)
            tab2=e2.xpath('//div[@class="pb_text"]/table')[0]
            ysz=tab2.xpath('./tr[1]/td[2]')[0].xpath('string(.)').strip()
            sp=tab2.xpath('./tr[2]/td[2]')[0].xpath('string(.)').strip()
            pro=tab2.xpath('./tr[3]/td[2]')[0].xpath('string(.)').strip()
            addr=tab2.xpath('./tr[4]/td[2]')[0].xpath('string(.)').strip()
            ysmj=tab2.xpath('./tr[5]/td[2]')[0].xpath('string(.)').strip()
            yt=tab2.xpath('./tr[6]/td[2]')[0].xpath('string(.)').strip()
            ts=tab2.xpath('./tr[7]/td[2]')[0].xpath('string(.)').strip()
            fzrj=tab2.xpath('./tr[8]/td[2]')[0].xpath('string(.)').strip()
            lp=tab2.xpath('./tr[9]/td[2]')[0].xpath('string(.)').strip()
            rs=[u2,pro,sp,ysz,addr,ysmj,fzrj,yt,ts,lp]
            # print(rs)
            of.write('\t'.join(rs)+'\n')
            of.flush()
        if pn>=total:
            break
    ##爬取售房信息列表
    of = open(outlistfile,'a', encoding='utf-8') #保存结果文件
    pn=0
    while True :
        pn+=1
        print('pro pn=',pn)
        html=getHtml(url.replace('$1',str(pn)),'').replace('&nbsp;','')
        total=int(re.findall('共(\d+)页',html)[0])
        e1=etree.HTML(html)
        tab1=e1.xpath('//div[@class="lie_right_center"]/ul/li/div')
        if len(tab1)==0:
            print('frs=0')
            break
        for div in tab1:
            contenturl='http://www.bdfdc.net'+div.xpath('./p[1]/a/@href')[0]
            pro=div.xpath('./p[1]/a')[0].xpath('string(.)').strip()
            tmp=div.xpath('./p[2]')[0].xpath('string(.)').replace('联系方式:','').split('开发商:')
            tel=tmp[0].strip()
            sp=tmp[1].strip()
            addr=div.xpath('./p[3]')[0].xpath('string(.)').replace('楼盘地址:','').strip()
            rs=[contenturl,pro,sp,addr,tel]
            if contenturl in okurl:
                continue
            if contenturl in repeatlist:
                continue
            repeatlist.append(contenturl)
            of.write('\t'.join(rs)+'\n')
            of.flush() 
        if total<=pn:
            break
    
    #爬虫房信息
    with open(outlistfile,'r',encoding='UTF-8') as txtData: 
        for line in txtData.readlines():
            contenturl=line.replace('\n','').split('\t')
            if contenturl[0] in okurl:
                continue
            urls.put(contenturl)
            okurl.add(contenturl[0])
            # print(contenturl)
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
        do_write(outfile,contenturl[0],outstr)
        # time.sleep(111110)    
  
if __name__ == '__main__':
		main()