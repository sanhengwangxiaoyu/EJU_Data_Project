# -*- coding=utf-8 -*-
#按url id增量
import time,random
from lxml import etree
from selenium import webdriver

import re,os
import requests
import json
import traceback

from threading import Thread
from queue import Queue

urls=Queue()
oklist=set()

dt=time.strftime('%Y%m%d',time.localtime(time.time()))
outfile="./data/suzhou_new.txt"
outlistfile="./list/suzhou.txt"
okfile="./data/suzhou.txt"

url='http://58.210.252.234/szfcweb/(S(m1hiyih34f3xhmmgmw4ykuqv))/DataSerach/SaleInfoProListIndex.aspx'

states={
    'background-color:#66cc33;':'可售',
    'background-color:Yellow;':'签约中',
    'background-color:#cccccc;':'不可售',
    'background-color:#666600;':'限制中'
}
def action(driver,tag,act):
    if act=='click':
        tag.click()
    elif act=='js':
        driver.execute_script(tag)
    driver.implicitly_wait(10)
    time.sleep(10)
    while True:
        if driver.page_source.find('403 Forbidden')>-1:
            print('403 Forbidden————————sleep60*5')
            time.sleep(60*5)
            driver.refresh()
            time.sleep(2)
        else:
            break
        
def trim(word):
    return re.sub('\s+','',word).replace('\\n','').replace('\\xa0','').replace('\\t','').replace('&nbsp;','')
def get_data(driver,pronum):
    city='苏州'
    rslist=[]
    cell={}
    try:
        #点击项目，打开楼栋列表页
        action(driver,driver.find_element_by_xpath(f'//table[@id="MainContent_OraclePager1"]//tr[{pronum}]/td[1]/a'),'click')
        html_ld=driver.page_source.replace('&nbsp;','')
        tmp=re.findall('共([\d\s]+)页',html_ld,re.S)
        if tmp==None or len(tmp)==0:
            driver.back()
            return rslist
        #
        total=int(tmp[0].strip())
        cell = {
                "城市": city,
                "项目名称": ''.join(re.findall('id="MainContent_lb_Pro_NAME">(.*?)<',html_ld,re.S)),
                "坐落位置": ''.join(re.findall('id="MainContent_lb_Pro_Add">(.*?)<',html_ld,re.S)),
                "开发企业":  ''.join(re.findall('id="MainContent_lb_Com">(.*?)<',html_ld,re.S)),
                "预售许可证编号":'',
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
        #翻页获取更多楼栋
        page=0
        while True:
            page+=1
            tags2=re.findall('href="SaleInfoHouseShow[^>]+>(.*?)</a></td><td>(.*?)</td><td>(.*?)</td>',html_ld)
            ldnum=1
            print('ld totalpage=',total,',thispage=',page,',this_lds=',len(tags2))
            for lh,mj,ts in tags2:
                cell2=cell.copy()
                cell2['销售楼号']=lh
                cell2['套数']=ts
                cell2['面积']=mj
                ldnum+=1
                #点击楼栋，打开楼栋详情页面
                # driver.find_element_by_xpath(f'//table[@id="MainContent_OraclePager1"]//tr[{ldnum}]//a').click()
                # time.sleep(5)
                action(driver,driver.find_element_by_xpath(f'//table[@id="MainContent_OraclePager1"]//tr[{ldnum}]//a'),'click')
                html_room=driver.page_source
                e3=etree.HTML(html_room)
                rooms=e3.xpath('//table[@id="MainContent_gvxml"]//td[not(contains(@style,"White"))]')
                print('lh=',lh,',rooms=',len(rooms))
                if len(rooms)==0:
                    rslist.append(cell2)
                for room in rooms:
                    cell3=cell2.copy()
                    cell3["房号"]=''.join(room.xpath('./text()')).strip()
                    cell3["房屋销售状态"]=states.get(''.join(room.xpath('./@style')))
                    rslist.append(cell3)
                #退回到楼栋列表页
                driver.back()
                break
                time.sleep(1)
            #翻页
            if html_ld.find("javascript:__doPostBack('ctl00$MainContent$OraclePager1$ctl12$Next")>0:
                # driver.execute_script("javascript:__doPostBack('ctl00$MainContent$OraclePager1$ctl12$Next','')")
                # time.sleep(5)
                action(driver,"javascript:__doPostBack('ctl00$MainContent$OraclePager1$ctl12$Next','')",'js')
                html_ld=driver.page_source.replace('&nbsp;','')
            else:
                print('break to ldlist')
                break
        #退回到项目列表页
        while total>0:
            driver.back()
            time.sleep(1)
            total-=1
        print('back to prolist')
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
        of.write(dicts.get('发证日期','').strip()+'\t')
        of.write(dicts.get('开盘日期','').strip()+'\t')
        of.write(trim(dicts.get('预售证准许销售面积',''))+'\t')
        of.write(trim(dicts.get('销售状态',''))+'\t')
        of.write(trim(dicts.get('销售楼号',''))+'\t')
        of.write(trim(dicts.get('套数',''))+'\t')
        of.write(trim(dicts.get('面积',''))+'\t')
        of.write(trim(dicts.get('拟售价格',''))+'\t')
        of.write(re.sub('[\r\n]+','/',(dicts.get('售楼电话','').strip()))+'\t')
        of.write(trim(dicts.get('售楼地址',''))+'\t')
        of.write(trim(dicts.get('房号',''))+'\t')
        of.write(trim(dicts.get('房屋建筑面积',''))+'\t')
        of.write(trim(dicts.get('房屋销售状态','')))
        of.write("\n")
        of.flush()
def main():
    #已爬列表
    has_new=0
    if os.path.exists(outfile):
        with open(outfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')[0]
                oklist.add(a)
                has_new=1
    if os.path.exists(okfile):
        with open(okfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')[0]
                oklist.add(a)
    #如果outfile没数据，需要创建并写一个表头
    if has_new==0:
        of = open(outfile,'a', encoding='utf-8') #保存结果文件
        of.write('URL\t城市\t项目名称\t坐落位置\t开发企业\t预售许可证编号\t发证日期\t开盘日期\t预售证准许销售面积\t销售状态\t销售楼号\t套数\t面积\t拟售价格\t售楼电话\t售楼地址\t房号\t房屋建筑面积\t房屋销售状态（颜色区分）\n')
        of.flush()
    #
    driver = webdriver.Firefox()
    driver.get(url)
    time.sleep(3)
    
    #选择不同的区
    area_tag=driver.find_elements_by_xpath(f'//select[@id="MainContent_ddl_RD_CODE"]/option')
    for tag in area_tag:
        tag.click()
        # driver.find_element_by_id('MainContent_bt_select').click(); 
        # driver.implicitly_wait(10)     
        # time.sleep(8)
        action(driver,driver.find_element_by_id('MainContent_bt_select'),'click')
        text=driver.page_source.replace('&nbsp;','')
        total=int(re.findall('共([\d ]+)页',text,re.S)[0].strip())
        #翻页
        page=1
        while True:
            hrefs=re.findall('(SaleInfoBudingShow\.aspx.*?)"',text,re.S)
            ldnum=len(hrefs)
            print('pro list totalpage',total,'thispage=',page,ldnum)
            ldnum+=1
            while ldnum>1:
                tmp=ldnum-2
                contenturl=hrefs[tmp].split('=')[1]
                if contenturl in oklist:
                    print('pass url id=',contenturl)
                    ldnum=ldnum-1
                    continue
                print('contenturl id=',contenturl)
                outstr=get_data(driver,ldnum)
                if len(outstr)>0:
                    do_write(outfile,contenturl,outstr)
                ldnum=ldnum-1
            #翻页结束判断
            if page==total or text.find("ctl00$MainContent$OraclePager1$ctl12$Next")<0:
                break
            #javascript翻页
            page+=1
            # driver.execute_script("javascript:__doPostBack('ctl00$MainContent$OraclePager1$ctl12$Next','')")
            # driver.implicitly_wait(10)
            # time.sleep(8)
            action(driver,"javascript:__doPostBack('ctl00$MainContent$OraclePager1$ctl12$Next','')",'js')
            text=driver.page_source.replace('&nbsp;','')

if __name__ == '__main__':
		main()