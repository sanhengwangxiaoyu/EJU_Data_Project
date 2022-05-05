# -*- coding:utf-8 -*-
#许可证在项目中才能爬，但列表中的有许可证，通过许可证判断是否要有更新项目。每个项目只保留一条信息，读取所有许可证，过滤掉已爬的许可证。当没有无许可证时，通过url判断是否需要保存项目信息
#增量爬虫时，要覆盖重写待爬URL列表。
import queue,time,random
from lxml import etree

import re,os
import requests
import json
import traceback
from queue import Queue

from selenium import webdriver

urls= Queue()
ysz_oklist=set()
url_oklist=set()

dt=time.strftime('%Y%m%d',time.localtime(time.time()))
outfile="data/yangzhou_new.txt"
outlistfile="list/yangzhou.txt"
okfile="data/yangzhou.txt"

def trim(word):
    return re.sub('[\s]*','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')   
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
def get_data(contenturl,driver):
    trs =[]
    city='扬州'
    area=''
    rslist=[]
    cell={}
    #
    try:
        cell['项目名称']=contenturl[1].strip()
        #https://www.yzfdc.cn/zxlp/Loupan_View_Info.aspx?xmid=202004201013063923 楼栋对应的预售证\价格\开盘日期
        #跳入https://www.yzfdc.cn/Zxlp/Loupan_View.aspx?xmid=202004201013063923 项目，电话，楼栋，户室，无时时会跳入https://www.yzfdc.cn/zxlp/Loupan_View_s.aspx?xmid=202107121103452878 
        xmid=contenturl[0].split('=')[1]
        ######项目
        u1=f'https://www.yzfdc.cn/zxlp/Loupan_View_Info.aspx?xmid={xmid}'
        h1=getHtml(u1,contenturl[0],driver)
        e1=etree.HTML(h1)
        tab1=e1.xpath('//table[@id="gczhs"]//tr[position()>1]')
        lh_ysz_list={}
        ispass=0
        for tr in tab1:
            ysz=''.join(tr.xpath('./td[2]/text()'))
            if ysz in ysz_oklist:
                print('pass')
                ispass=1
                continue
            lh=''.join(tr.xpath('./td[1]/text()'))
            price=''.join(tr.xpath('./td[3]/text()'))
            kprq=''.join(tr.xpath('./td[4]/text()'))
            lh_ysz_list[lh]=(ysz,price,kprq)
        #无预售证时，输出空
        print(lh_ysz_list)
        if len(lh_ysz_list)==0 and ispass==1:
            print('pass,return []')
            return []
        time.sleep(2)
        #loudong
        #无楼栋时
        h2=getHtml(f'http://www.yzfdc.cn/BuildingDish_Project_View.Aspx?ProjectId={xmid}',contenturl[0],driver).replace('&nbsp;','')
        if h2.find('sub-nav-list')==-1:
            print('no ld')
            cell['坐落位置']=''.join(re.findall('具体地址：(.*?)<',h2,re.S)).strip()
            cell['开发企业']=''.join(re.findall('开 发 商：(.*?)<',h2,re.S)).strip()
            cell['拟售价格']=''.join(re.findall('起 售 价：(.*?)<',h2,re.S)).strip()
            cell['销售状态']=''.join(re.findall('销售状态：(.*?)<',h2,re.S)).strip()
            cell['售楼电话']=''.join(re.findall('售楼电话：(.*?)<',h2,re.S)).strip()
            h21=getHtml(f'https://www.yzfdc.cn/zxlp/Gczh_Xkzhm.aspx?xmid={xmid}',contenturl[0],driver).replace('&nbsp;','')
            tab21=etree.HTML(h21).xpath('//div[@class="itemContent mt20 mb20"]//tr[position()>1]')
            cell2={}
            #获取楼盘所有预售证和楼栋
            print('ysz-ld-list=',len(tab21))
            for tr in tab21:
                cell2=cell.copy()
                lh=''.join(tr.xpath('./td[1]/text()')).strip()
                ysz=''.join(tr.xpath('./td[2]/text()')).strip()
                if ysz in ysz_oklist:
                    print('pass ysz:',ysz)
                    continue
                ysz,price,kprq=lh_ysz_list.get(lh.strip(),('','',''))
                cell2['销售楼号']=lh
                cell2['预售许可证编号']=ysz
                if cell2.get('拟售价格')==None:
                    cell2['拟售价格']=price
                cell2['开盘日期']=kprq
                print(cell2)
                rslist.append(cell2)
            #如果没有楼栋，则按url增量保存项目信息，否则返回楼栋信息
            if len(rslist)==0:
                if contenturl[0] in url_oklist:
                    return []
                else:
                    return [cell2]
            else:
                return rslist
        else:
            print('have ld')
            #有楼栋时
            cell['坐落位置']=''.join(re.findall('项目地址：(.*?)<',h2,re.S)).strip()
            cell['开发企业']=''.join(re.findall('data-qydm=.*?>(.*?)</a>',h2,re.S)).strip()
        #多层结构
        #楼列表<div id="tab_pannel_*"，
        #楼信息<div id = "building_info_1" 
        #户型table/tbody/tr
        #户室/td[5]/ul/li,t[4]=面积
        e2=etree.HTML(h2)
        ld_divs=e2.xpath(f'//div[@class="tab-pannel" or @class="tab-pannel on"]')
        #楼栋组
        print('ld num=',len(ld_divs))
        for ld_div in ld_divs:
            ld_list=ld_div.xpath('./div[@class="sub-nav-list"]/span')
            #楼栋list
            for span in ld_list:
                #
                lhname=trim(span.xpath('./a/text()')[0])
                tmp=lh_ysz_list.get(lhname.replace('幢','').strip(),('','',''))
                if tmp==('','',''):
                    continue
                #
                dataid=span.xpath('./@data-id')[0]
                hxlist=ld_div.xpath(f'./div[@id="building_info_{dataid}"]')
                text=etree.tostring(hxlist[0]).decode()
                cell['套数']=''.join(re.findall('户数：(.*?)户',text,re.S))
                cell['销售楼号']=lhname
                cell['预售许可证编号']=tmp[0]
                cell['拟售价格']=tmp[1]
                cell['开盘日期']=tmp[2]
                # print(cell)
                #户型 room
                hxlist=ld_div.xpath(f'./div[@id="building_info_{dataid}"]/.//tr')
                for tr in hxlist:
                    mj=''.join(tr.xpath('./td[4]/text()'))
                    rooms=tr.xpath('.//li')
                    for room in rooms:
                        state=room.xpath('./@class')
                        lh=room.xpath('string(.)').strip()
                        cell2=cell.copy()
                        cell2['房号']=lh
                        cell2['房屋建筑面积']=mj
                        if state=='zzxs':
                            cell2['房屋销售状态']='可售'
                        else:
                            cell2['房屋销售状态']='不可售'
                        rslist.append(cell2)  
        if len(rslist)==0:
            if contenturl[0] in url_oklist:
                return []
            else:
                return [cell]
    except Exception as e:
        print(traceback.format_exc())
    return rslist
def getHtml(link,refer,driver):
    html=""
    print(link)
    try: #使用try except方法进行各种异常处理
        header = {
                'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Encoding':'gzip, deflate, br',
                'Accept-Language':'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
                'Cache-Control':'max-age=0',
                'Connection':'keep-alive',
                'Host':'www.yzfdc.cn',
                'User-Agent':'Mozilla/5.0 (Windows NT 6.2; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0',
                }
        while True:
            try:
                driver.get(link)
                time.sleep(3)
                # res = requests.get(link,headers=header,timeout=15,verify=True) #读取网页源码
                # res.encoding='UTF-8'
                if driver.page_source.find('抱歉，您访问的页面走丢')>0:
                # if res.text.find('抱歉，您访问的页面走丢')>0:
                    time.sleep(4)
                    print('页面走丢 sleep 4')
                    continue
                break
            except Exception as e:
                print(str(e))
                if str(e).find('Tried to run command without establishing a connection')>-1:
                    try:
                        driver.close()
                    except Exception as e2:
                        print(e2)
                    driver=webdriver.Firefox()
                print('sleep 15')
                time.sleep(15) 
        html=driver.page_source
    except Exception as e:
        print(e)
    finally:
        return html
def main():
    #
    has_new=1
    if os.path.exists(outfile):
        with open(outfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')                    
                ysz_oklist.add(a[5])
                url_oklist.add(a[0])
                has_new=1
    if os.path.exists(okfile):
        with open(okfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                ysz_oklist.add(a[5])
                url_oklist.add(a[0])
     #如果outfile没数据，需要创建并写一个表头
    if has_new==0:
        of = open(outfile,'a', encoding='utf-8') #保存结果文件
        of.write('URL\t城市\t项目名称\t坐落位置\t开发企业\t预售许可证编号\t发证日期\t开盘日期\t预售证准许销售面积\t销售状态\t销售楼号\t套数\t面积\t拟售价格\t售楼电话\t售楼地址\t房号\t房屋建筑面积\t房屋销售状态（颜色区分）\n')
        of.flush()
    ##翻页
    driver = webdriver.Firefox()# webdriver.PhantomJS(executable_path=r'D:\tools\phantomjs-2.1.1-windows\bin\phantomjs.exe')
    driver.get('https://zt.yzfdc.cn/yhgs/Default.aspx')
    of = open(outlistfile,'a', encoding='utf-8') #保存结果文件，按url+预售证增量添加
    repeatlist=[]
    total=0
    pn=1
    while True :
        break
        time.sleep(2)
        html=driver.page_source
        if total==0:
            total=int(re.findall('共(\d+)页',html)[0])
        time.sleep(2)
        ######### 
        tab=etree.HTML(html)
        hrefs=tab.xpath('//table[@class="zzlist"]//tr[position()>1]')
        print(total,'thispage=',pn,len(hrefs))
        if len(hrefs)==0:
            break
        for href in hrefs:
            contenturl=href.xpath('./td[1]/a/@href')[0]
            pro=href.xpath('./td[1]/a/text()')[0]
            ysz=href.xpath('./td[2]/text()')[0]
            ld=href.xpath('./td[3]/a/text()')[0]
            print(pro+'\t'+ysz+'\t'+ld)
            if ysz in ysz_oklist:
                continue
            if ysz in repeatlist:
                continue
            repeatlist.append(ysz)
            of.write(contenturl+"\t"+pro+'\t'+ysz+'\t\n')
            of.flush()
        pn+=1
        driver.execute_script(f"javascript:__doPostBack('AspNetPager1','{pn}')")
        if total<=pn:
            break
    
    ##爬详情
    repeatlist=[]
    if os.path.exists(outlistfile):
        with open(outlistfile,'r',encoding='UTF-8') as txtData: 
            for line in txtData.readlines():
                contenturl=line.split('\t')
                if contenturl[0] in repeatlist or contenturl[2] in ysz_oklist:
                    continue
                urls.put(contenturl)
                repeatlist.append(contenturl[0])
        print("qsize="+str(urls.qsize()))           
        time.sleep(5)
    while urls.qsize()!=0:
        contenturl=urls.get()
        print('urls less=',urls.qsize())
        outstr=get_data(contenturl,driver)
        print('allroom=',len(outstr))
        # time.sleep(1111)
        if len(outstr)==0:
            continue
        do_write(outfile,contenturl[0],outstr)
        
    driver.close()       
if __name__ == '__main__':
	main()