# -*- coding=utf-8 -*-
#按url增量爬虫，url为预售证地址
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
outfile="./data/tangshan_new.txt"
outlistfile="./list/tangshan_url.txt"
okfile="./data/tangshan.txt"


url='http://60.2.165.74:8911/yushouxinxichaxun.aspx'

def trim(word):
    return re.sub('\s+','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')

def get_data(contenturl):
    city='唐山'
    area=''
    rslist=[]
    cell={}
    detail=''
    newhtml=''
    progect=contenturl[2]
    addr=contenturl[3]
    company=contenturl[4]
    ysz=contenturl[5].strip()
    try:
        #pro
        h0=getHtml(contenturl[0],contenturl[0])
        area=trim(''.join(re.findall('>预售面积</td>\s*<td class="even">(.*?)</td>',h0)))
        fzrq=trim(''.join(re.findall('>发证日期</td>\s*<td class="even">(.*?)</td>',h0)))
        cell = {
                "城市": city,
                "项目名称": progect,
                "坐落位置":addr,
                "开发企业": company,
                "预售许可证编号":ysz,
                "发证日期":fzrq,
                "开盘日期":'',
                "预售证准许销售面积":area,
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
      
        ###楼栋列表：http://60.2.165.74:8911/loupanliebiao.aspx?id=5n6PH%2feLJHU%3d
        u1=contenturl[0].replace('yushouxiangqing','loupanliebiao')
        h1=getHtml(u1,u1)
        __VIEWSTATE = re.findall(r'id="__VIEWSTATE" value="(.*?)" />', h1, re.I)[0]
        
        tab1=etree.HTML(h1).xpath('//div/table/tbody/tr')
        print('longdongs=',len(tab1))
        if len(tab1)==0:
            rslist.append(cell)
            return rslist
        for tr in tab1:
            cell2=cell.copy()
            cell2['销售楼号']=''.join(tr.xpath('./td[2]/text()'))
            cell2['套数']=''.join(tr.xpath('./td[3]/text()'))
            cell2['面积']=''.join(tr.xpath('./td[4]/text()'))
            button=''.join(tr.xpath('./td/input/@name'))
            print('销售楼号',cell2['销售楼号'])
            dict2={
                    "__VIEWSTATE":__VIEWSTATE,
                    button:'详情',
                }
            time.sleep(2)
            u2=postHtml_getURL(u1,dict2)
            time.sleep(2)
            h3=getHtml(u2,u2)
            tab3=etree.HTML(h3).xpath('//div/table/tbody/tr')
            if len(tab3)==0:
                rslist.append(cell2)
                continue
            for tr in tab3:
                cell3=cell2.copy()
                cell3['房号']=''.join(tr.xpath('./td[2]/text()'))
                cell3['房屋建筑面积']=''.join(tr.xpath('./td[3]/text()'))
                cell3['房屋销售状态']=''.join(tr.xpath('./td[7]/text()'))
                rslist.append(cell3)
    except Exception as e:
        print(traceback.format_exc()) 
    
    # print(rslist)
    return rslist

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
def postHtml(url,data):
    print(url)
    #使用try except方法进行各种异常处理
    header = {
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
        # 'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        # 'Accept-Language':'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        # 'Accept-Encoding':'gzip, deflate',
        'Content-Type':'application/x-www-form-urlencoded',
        'Referer':url,
        'Origin':'http://60.2.165.74:8911',
        'host':'60.2.165.74:8911',
        'Cookie':'safedog-flow-item='
    }
    #data=json.loads(js)
    try:
        res=requests.post(url=url,data=data,headers=header,verify=False)
        res.encoding='UTF-8'
        return res.text
        
    except Exception as e:
        print(traceback.format_exc())
    return ''
def postHtml_getURL(url,data):
    print('post',url)
    #使用try except方法进行各种异常处理
    header = {
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
        # 'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        # 'Accept-Language':'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        # 'Accept-Encoding':'gzip, deflate',
        'Content-Type':'application/x-www-form-urlencoded',
        'Referer':url,
        'Origin':'http://60.2.165.74:8911',
        'host':'60.2.165.74:8911',
        'Cookie':'safedog-flow-item='
    }
    #data=json.loads(js)
    try:
        res=requests.post(url=url,data=data,headers=header,verify=False)
        res.encoding='UTF-8'
        return res.url
        
    except Exception as e:
        print(traceback.format_exc())
    return ''
def getHtml(link,refer):
    html=""
    try: #使用try except方法进行各种异常处理
        header = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
            'Cookie':'Hm_lvt_b24ff3dd29c6727b010f2da3b8a746e8=1617843746,1618973782; ASP.NET_SessionId=qevt0ca2an0h1ktv0ihtf0uk; Hm_lpvt_b24ff3dd29c6727b010f2da3b8a746e8=1618983378',
            'Host':'60.2.165.74:8911',
            'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8',
            'Referer': refer
            }
        res = requests.get(link,headers=header,timeout=20,verify=False) #读取网页源码
        #解码
        print(link)
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
        print(e)
    finally:
        return html
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
    ##
    pn=1
    if True:
        text=postHtml(url,{})
        while True:
            break
            ##续爬房信息时，中断
            print('now=',pn)
            tab=etree.HTML(text)
            tab11=tab.xpath('//tbody/tr')
            print('tabs=',len(tab11))
            __VIEWSTATE=tab.xpath('//input[@name="__VIEWSTATE"]/@value')[0]
            for tr in tab11:
                button=tr.xpath('./td[6]/input/@name')[0]
                param={
                        '__EVENTTARGET':'',
                        '__EVENTARGUMENT':'',
                        '__VIEWSTATE':__VIEWSTATE,
                        'ddlSearchType':0,
                        'searchText':'',
                        button:'详情'
                    }
                contenturl=postHtml_getURL(url,param)
                outstr={
                    '城市':'唐山',
                    '项目名称':tr.xpath('./td[4]//text()')[0],
                    '坐落位置':tr.xpath('./td[5]//text()')[0],
                    '开发企业':tr.xpath('./td[3]//text()')[0],
                    '预售许可证编号':tr.xpath('./td[1]//text()')[0],
                }
                # print(outstr)
                if contenturl in okurl:
                    print('exists oklist')
                    continue
                if contenturl in repeatlist:
                    print('exists newlist')
                    continue
                repeatlist.append(contenturl)
                do_write(outlistfile,contenturl,[outstr])
                
            ##是否结束
            if text.find('btnNext" href=')<10:
                print('end')
                break
            ##下一页
            param={
                '__EVENTTARGET':'btnNext',
                '__EVENTARGUMENT':'',
                '__VIEWSTATE':__VIEWSTATE,
                'ddlSearchType':0,
                'searchText':'',
            }
            text=postHtml(url,param)
            pn+=1
  
    with open(outlistfile,'r',encoding='UTF-8') as txtData: 
        for line in txtData.readlines():
            contenturl=line.split('\t')
            print(contenturl[0])
            if contenturl[0] in okurl:
                continue
            okurl.add(contenturl[0])
            urls.put(contenturl)
            # outstr=get_data(contenturl)
            # do_write(outfile,contenturl[0],outstr)
            # time.sleep(10)
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
        time.sleep(10)    

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