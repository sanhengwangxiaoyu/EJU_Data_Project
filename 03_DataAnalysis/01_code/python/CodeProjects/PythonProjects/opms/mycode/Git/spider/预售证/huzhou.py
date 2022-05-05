# -*- coding:utf-8 -*-
#按预售证年搜索获取，按项目保存URL，项目爬虫中通过预售证+楼栋过滤
import queue,time,random
from lxml import etree
from threading import Thread
import re,os
import requests
import json
import traceback
from selenium import webdriver
from queue import Queue

dt=time.strftime('%Y%m%d',time.localtime(time.time()))
urls = Queue()
okurl = set() 

outlistfile='list/huzhou_url.txt'
outfile='data/huzhou_new.txt'
okfile='data/huzhou.txt'

url = 'http://hufdc.jsj.huzhou.gov.cn/presell.jspx?pageno=$1&keyword='
driver=None

def getHtml(link):
    html=''
    print(link)
    while True:
        try: #使用try except方法进行各种异常处理
            header = {
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0 Win64 x64 rv:86.0) Gecko/20100101 Firefox/86.0',
                'Accept': '*/*',
                'Cookie':'Hm_lvt_bbb8b9db5fbc7576fd868d7931c80ee1=1626052730,1627002338; gr_user_id=55042e05-2d85-4d77-a0a2-10ea8a847f23; UM_distinctid=17a984bacf4471-05a516a46a4dd1-4c3f2d73-e1000-17a984bacf5305; BSFIT_EXPIRATION=1627058937296; BSFIT_DEVICEID=K9qh1VyKtcoMM8kfTviqmnTVxsUpLFc2NSsAADstQNa7aTeB9NBSEaCudILcpP7m2E62-Oh1UB-oOVAjVZR9aJRUS67kOUP23bDe3X6eQ2IPmBnuwM6BDGruKFND2OuJePmV3j2I6LWsr_vkFqewOmFccbe2OyYI; JSESSIONID=CFDA10667DBBA5FC1B3195A85FD2BF66; Hm_lvt_8db7cb1b4649ef76847e460b7e13171c=1629769635,1629769866,1629770544,1629771798; Hm_lpvt_8db7cb1b4649ef76847e460b7e13171c=1629771798; CNZZDATA1253675216=217668694-1629766042-http%253A%252F%252Fhufdc.jsj.huzhou.gov.cn%252F%7C1629771442; istiped=; __qc_wId=930; pgv_pvid=173463933'
                }
            res = requests.get(link,headers=header,timeout=60,verify=False) #读取网页源码
            #解码
            res.encoding='utf-8'
            html=res.text
            break
        except Exception as e:
            print(e)
            if str(e).find('Read timed out')>0:
                time.sleep(2)
    return html
def getHtml2(link):
    html='<html></html>'
    print(link)
    while True:
        try: #使用try except方法进行各种异常处理
            time.sleep(1)
            driver.get(link)
            #解码
            time.sleep(2)
            html=driver.page_source
            break
        except Exception as e:
            print(e)
            if str(e).find('Dismissed')>0:                
                return ''
    return html
def trim(word):
    return re.sub('[\s]*','',word).replace('\\n','').replace('\\xa0','').replace('\\t','').replace('&nbsp;','')   
def get_num(dicts):
    rs=''
    for d in dicts:
        if d=='numberone':
            rs+='1'
        if d=='numbertwo':
            rs+='2'
        if d=='numberthree':
            rs+='3'
        if d=='numberfour':
            rs+='4'
        if d=='numberfive':
            rs+='5'
        if d=='numbersix':
            rs+='6'
        if d=='numberseven':
            rs+='7'
        if d=='numbereight':
            rs+='8'
        if d=='numbernine':
            rs+='9'
        if d=='numberzero':
            rs+='0'
        if d=='numberdor':
            rs+='.'
    return rs
def run():
    while urls.qsize() != 0:
        print("qsize less="+str(urls.qsize()))   
        url1,ysz,pro,sp,fzrq=urls.get()
        rslist=[]
        cell = {
                'url': url1,
                "城市": '湖州',
                "项目名称": pro,
                "坐落位置":  '',
                "开发企业": sp,
                "预售许可证编号": ysz,
                "发证日期": fzrq.strip(),
                "开盘日期": '',
                "预售证准许销售面积": '',
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
        text =getHtml2(url1)
        if text.find('id="info" href="/newhouse/property')==-1:
            do_write(outfile,url1,[cell])
            continue
        
        #主页信息/newhouse/property_330500_123105240_info.htm"        
        u2='http://hu.tmsf.com'+re.findall('id="info" href="(/newhouse/property.*?)"',text,re.S)[0]
        h2=getHtml(u2)
        addr=trim(''.join(re.findall('楼盘地址：</strong>\s+<span title="(.*?)"',h2,re.S)))
        tel=re.sub('[\r\n]+','/',''.join(re.findall('售楼电话：</font>\s+<font class="colorred">(.*?)</font>',h2,re.S)).strip())
       
        cell['坐落位置']=addr
        cell['售楼电话']=tel
        #按楼栋列表获取，可以拿到栋栋的套数
        loudong_id_list=re.findall('javascript:doBuilding.*?building_(\d+?)">(.*?)</a>',text,re.S)
        if len(loudong_id_list)==0:
            do_write(outfile,url1,[cell])
            continue
        print('loudong_id_list=',len(loudong_id_list))
        for buildingid,buildname in loudong_id_list:
            u3=url1+f'&buildingid={buildingid}&area=&allprice=&housestate=&housetype=&page=&roomid'
            h3=getHtml2(u3)
            ts=int(''.join(re.findall('总数：(\d+)套',h3,re.S)))
            page=1
            #翻页获取rooms
            while True:
                rooms = etree.HTML(h3).xpath('//table[@class="sjtd"]//tr')
                # print(buildname,'rooms=',len(rooms))
                if len(rooms)==0:
                    break
                for room in rooms:
                    mj=get_num(room.xpath('./td[3]//span/@class'))+'㎡'
                    price=get_num(room.xpath('./td[6]//span/@class'))+'元/㎡'
                    
                    cell2=cell.copy()
                    cell2["销售楼号"]=trim(''.join(room.xpath("./td[1]//text()")))
                    cell2["套数"]=str(ts)
                    cell2["房号"] =trim(''.join(room.xpath("./td[2]//text()")))
                    cell2["拟售价格"] = price
                    cell2["房屋建筑面积"] = mj
                    cell2["房屋销售状态"] =trim(room.xpath("./td[9]")[0].xpath('string(.)'))
                    rslist.append(cell2)
                if page*14>=ts:
                    break
                #翻页
                page+=1
                time.sleep(0.5)
                u3=url1+f'&buildingid={buildingid}&area=&allprice=&housestate=&housetype=&page={str(page)}&roomid'
                h3=getHtml2(u3)
        if len(rslist)==0:
            do_write(outfile,url1,[cell])
        else:
            do_write(outfile,url1,rslist)
            
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
        of.write(trim(dicts.get('售楼电话',''))+'\t')
        of.write(trim(dicts.get('售楼地址',''))+'\t')
        of.write(trim(dicts.get('房号',''))+'\t')
        of.write(trim(dicts.get('房屋建筑面积',''))+'\t')
        of.write(trim(dicts.get('房屋销售状态','')))
        of.write("\n")
        of.flush()
def getstate(color):
    state = color
    if(color=='background:#0033FF'):
        state='已公示'
    elif(color=='background:#FFFFFF'):
        state = '不可售'
    elif (color == 'background:#00FF00'):
        state = '可售'
    elif (color == 'background:#0099FF'):
        state = '可现售'
    elif (color == 'background:#C4A93C'):
        state = '已销售上报'
    elif (color == 'background:#F3F394'):
        state = '已定'
    elif (color == 'background:#99CC00'):
        state = '已签,公租已签'
    elif (color == 'background:#CC99FF'):
        state = '公租备案(完全),备案,公租备案'
    elif (color == 'background:#FFCC99'):
        state = '在建工程抵押,工程最高额'
    elif (color == 'background:#AAAAAA'):
        state = '共管房'
    return state
def main():
    has_new=0
    if os.path.exists(outfile):
        with open(outfile, 'r', encoding='utf-8') as f:
            for i in f:
                a=i.split('\t')
                okurl.add(a[0])
                has_new=1
    if os.path.exists(okfile):
        with open(okfile, 'r', encoding='utf-8') as f:
            for i in f:
                a=i.split('\t')
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
    #按预售证的年号搜索
    of = open(outlistfile,'a', encoding='utf-8') #保存结果文件，按url增量
    page=0
    while True:
        break
        page+=1
        time.sleep(2)
        text=getHtml(url.replace('$1',str(page)))
        hrefs=etree.HTML(text).xpath('//div[@class="lbox"]/table/tr[position()>1]')
        print('page=',page,len(hrefs))
        
        if len(hrefs)==0:
            break
        for href in hrefs:
            ysz=href.xpath('./td[1]/a/@href')[0].replace("javascript:tolp('","").replace("');","")
            pro=href.xpath('./td[2]/text()')[0].strip()
            sp=href.xpath('./td[3]/text()')[0].strip()
            fzrq=href.xpath('./td[4]/text()')[0].strip()
            pname=json.dumps(ysz).replace('\\','%25').replace('"','')
            contenturl='http://hu.tmsf.com/newhouse/property_330500_0_price.htm?presellname='+pname
            if contenturl in repeatlist :
                continue
            repeatlist.append(contenturl)
            of.write(contenturl+'\t'+ysz+'\t'+pro+'\t'+sp+'\t'+fzrq+'\n')
            of.flush()
        
        #设置中断，目前增量只需要爬2021年的
        # if text.find('字（2021）第')>0:
            # break
        #翻页结束中断
        if text.find('下一页')==-1:
            break
    #2多线程爬项目，先登录账号
    global driver
    driver = webdriver.Firefox()
    driver.get('http://hu.tmsf.com/mem/login.jsp')
    driver.find_element_by_id("username").send_keys('15313517870')
    driver.find_element_by_id("showuserpwd").click()
    driver.switch_to.active_element.send_keys('l123456')
    driver.find_element_by_xpath('//div[@class="tijiao"]').click()
    time.sleep(0.5)
    #
    with open(outlistfile,'r', encoding='utf-8') as f:
        for i in f:
            a=i.split('\t')
            if a[0] in okurl:
                continue
            urls.put(a)
            okurl.add(a[0])
            # time.sleep(22222)
    print("qsize="+str(urls.qsize()))           
    time.sleep(1) 
    ths = []
    for i in range(1):
        t = Thread(target=run, args=())
        t.start()
        ths.append(t)
    for t in ths:
        t.join()
        # time.sleep(1111)
    driver.close()
if __name__ == '__main__':
	main()