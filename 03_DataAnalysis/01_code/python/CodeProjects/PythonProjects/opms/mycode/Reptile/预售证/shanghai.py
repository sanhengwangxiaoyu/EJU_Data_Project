# -*- coding=utf-8 -*-
#全项目列表，根据预售证过滤
import time,random
from lxml import etree
from selenium import webdriver

import re,os
import requests
import json
import traceback

from threading import Thread
from queue import Queue

urls = Queue()
ysz_oklist=set()

outfile="./data/shanghai_new.txt"
outlistfile="./list/shanghai.txt"
okfile="./data/shanghai.txt"

url='http://www.fangdi.com.cn/new_house/new_house_list.html'

sateDict={
    '_yellow_bg':'已签',
    '_red_bg':'已登记',
    '_green_bg':'可售',
    '_pinhong_bg':'已付定金',
    '_white_bg':'未纳入网上销售',
    'color666_bg':'动迁房',
    '_yellow_bg':'配套商品房',
    '_yellow_bg':'动迁安置房'
}
def roomJS(name,browser):
    js='''   
        var house_id = arguments[0];
        var plan_flarea = 0;
        var flarea = 0;
        url=""
        $.ajax({
            type : "POST",
            contentType : "application/x-www-form-urlencoded;charset=utf-8",
            url : path+"/service/freshHouse/queryHouseById.action",
            data:{'houseID':house_id},
            dataType : "json",
            async: false,
            success : function(data){
                var houseDetailList = data.houseDetailList;
                if (houseDetailList.length > 0){
                    var houseDetail = houseDetailList[0];
                    if (houseDetail['plan_flarea'])
                        plan_flarea = houseDetail['plan_flarea'];
                    if (houseDetail['flarea'])
                        flarea = houseDetail['flarea'];
                }
            },
            error : function(){
                
            }
        });
        if (plan_flarea=='0')
            return flarea
        else
            return plan_flarea
        '''
    return browser.execute_script(js,name)
def trim(word):
    return re.sub('\s+','',word).replace('\\n','').replace('\\xa0','').replace('\\t','').replace('&nbsp;','')
def get_data(contenturl,browser):
    city='上海'
    rslist=[]
    try:
        ##
        browser.get(contenturl)
        time.sleep(3)
        h1=browser.find_element_by_xpath('//html').get_attribute('innerHTML')
        e1=etree.HTML(h1)
        cell = {
                "城市": city,
                "项目名称":trim(''.join(re.findall('class="nameC" name="(.*?)">',h1,re.S))),
                "坐落位置":trim(''.join(re.findall('<span>项目地址：</span>(.*?)</',h1,re.S))),
                "开发企业":trim(''.join(re.findall('<span>企业名称：</span>.*?>(.*?)</',h1,re.S))),
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
        print(cell)
        text=browser.find_element_by_xpath('//html').get_attribute("outerHTML")
        e1=etree.HTML(text)
        ysz_list=e1.xpath('//div[@id="topBox"]/ul')
        lp_list=e1.xpath('//div[@id="BodyBox"]')
        ##预售证列表
        print('ysz_list=',len(ysz_list))
        for num in range(0,len(ysz_list)):
            ysz=ysz_list[num].xpath('./li[2]')[0].xpath('string(.)').strip()
            #按预售证增量爬
            if ysz in ysz_oklist:
                continue
            kprq=ysz_list[num].xpath('./li[3]')[0].xpath('string(.)').strip()
            ysmj=ysz_list[num].xpath('./li[7]')[0].xpath('string(.)').strip()
            yszt=ysz_list[num].xpath('./li[8]')[0].xpath('string(.)').strip()
            cell['预售许可证编号']=ysz
            cell['开盘日期']=kprq
            cell['预售证准许销售面积']=ysmj
            cell['销售状态']=yszt
            browser.find_element_by_id(f'top{num}').click()
            time.sleep(0.1)
            lps=lp_list[0].xpath(f'./div[@id="body{str(num)}"]')
            if len(lps)==0:
                rslist.append(cell)
                continue
            tmp=lps[0].xpath('./h4')[0].xpath('string(.)')
            cell['售楼地址']=''.join(re.findall('售楼地址：(.*?)(?:\s|$)',tmp,re.S))
            cell['售楼电话']=''.join(re.findall('售楼电话：(.*?)(?:\s|$)',tmp,re.S))
            # print(cell)
            lps2=lps[0].xpath('./div[@class="sale_table_items"]/div')
            if len(lps2)==0:
                rslist.append(cell)
                continue
            ###############楼栋
            print(ysz,':楼栋数=',len(lps2))
            lp_num=0
            for lp in lps2:
                cell2=cell.copy()
                cell2['销售楼号']=''.join(lp.xpath('./a/text()'))
                try:
                    ts=''.join(lp.xpath('./span[3]/text()'))
                    int(ts)
                except:
                    ts='0'
                cell2['套数']=ts
                cell2['面积']=''.join(lp.xpath('./span[4]/text()'))
                cell2['拟售价格']=''.join(lp.xpath('./span[1]/text()'))
                room_url='http://www.fangdi.com.cn/new_house/'+''.join(lp.xpath('./a/@href')).replace('./','')
                #####点击打开楼栋页面
                # print(browser.find_element_by_xpath(f'//div[@id="body{num}"]/div[1]/div[17]/a').get_attribute("outerHTML"))
                lp_num+=1
                browser.find_element_by_xpath(f'//div[@id="body{num}"]/div[1]/div[{lp_num}]/a').click()
                tt=int(ts)
                if tt>1000:
                    time.sleep(2.5)
                elif tt>300:
                    time.sleep(1.5)
                else:
                    time.sleep(0.7)
                text_room=browser.page_source
                tags=etree.HTML(text_room).xpath('//td[contains(@class,"mytest")]')
                print('rooms=',len(tags))
                if len(tags)==0:
                    rslist.append(cell2)
                    continue
                for room in tags:
                    time.sleep(0.2)
                    fh=room.xpath('string(.)').strip()
                    state_tmp=''.join(room.xpath('./@class')).split(' ')[0]
                    state=sateDict.get(state_tmp,state_tmp)
                    name=''.join(room.xpath('./@name'))
                    try:
                        mj=roomJS(name,browser)
                    except Exception as e :
                        mj=''
                    cell3=cell2.copy()
                    cell3['房号']=fh
                    cell3['房屋建筑面积']=str(mj)
                    cell3['房屋销售状态']=state
                    rslist.append(cell3)
                    # print(cell3)
                    # break
                #返回楼栋列表页
                browser.back()
                time.sleep(0.8)
                # break
            # break
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
                a = i.split('\t')                    
                ysz_oklist.add(a[5])
                has_new=1
    if os.path.exists(okfile):
        with open(okfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.strip()
                ysz_oklist.add(a[5])
    repeatlist=[]
    if os.path.exists(outlistfile):
        with open(outlistfile, 'r', encoding='utf-8') as f:
            for i in f:
                repeatlist.append(i.strip())
    #如果outfile没数据，需要创建并写一个表头
    if has_new==0:
        of = open(outfile,'a', encoding='utf-8') #保存结果文件
        of.write('URL\t城市\t项目名称\t坐落位置\t开发企业\t预售许可证编号\t发证日期\t开盘日期\t预售证准许销售面积\t销售状态\t销售楼号\t套数\t面积\t拟售价格\t售楼电话\t售楼地址\t房号\t房屋建筑面积\t房屋销售状态（颜色区分）\n')
        of.flush()
    ##
    of = open(outlistfile,'a+', encoding='utf-8')#保存结果文件
    pn=0
    total=0
    browser = webdriver.Firefox()
    browser.get(url)
    time.sleep(3)
    while True :
        break
        time.sleep(3)
        text=browser.execute_script("return document.getElementById('houseList').innerHTML")
        ids=re.findall("houseDetail\('(.+?)'\)",text,re.S)
        if total==0:
            total=int(re.findall('共<i>(\d+)</i>页',text,re.S)[0])
        print(total,'page=',pn,len(ids))
        for hid in ids:
            contenturl=f'http://www.fangdi.com.cn/new_house/new_house_detail.html?project_id={hid}'
            if contenturl in repeatlist:
                continue
            repeatlist.append(contenturl)
            of.write(contenturl+'\n')
            of.flush()
        #结束跳出while
        if total<=pn:
            break
        pn+=1
        browser.execute_script(f"nextPage({str(pn)})")
    repeatlist=[]
    if os.path.exists(outlistfile):
        with open(outlistfile,'r',encoding='UTF-8') as txtData: 
            for line in txtData.readlines():
                contenturl=line.strip()
                if contenturl in repeatlist:
                    continue
                repeatlist.append(contenturl)
                urls.put(contenturl)
        print("qsize=",urls.qsize())
        time.sleep(3)
        while urls.qsize()!=0:
            print("less=",urls.qsize())
            contenturl=urls.get()
            print(contenturl)
            outstr=get_data(contenturl,browser)
            if len(outstr)>0:
                do_write(outfile,contenturl,outstr)
            # time.sleep(111110.3)
    browser.close()
 
if __name__ == '__main__':
		main()