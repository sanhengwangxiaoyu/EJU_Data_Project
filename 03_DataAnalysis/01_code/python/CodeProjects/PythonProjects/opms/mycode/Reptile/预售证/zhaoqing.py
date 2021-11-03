# -*- coding:utf-8 -*-
#url全爬获取项目，先按预售证做增量爬取列表。在项目里需求进一步匹配按预售证并爬取下面的楼号
import time,random
from lxml import etree

import re,os
import requests
import json
import traceback

from threading import Thread
from queue import Queue
urls = Queue()
ysz_oklist=set()

outfile="./data/zhaoqing_new.txt"
outlistfile="./list/zhaoqing.txt"
okfile="./data/zhaoqing.txt"

url='http://yueanju.zfcxjst.gd.gov.cn/zhfcgspt/posl?fn=%E9%A2%84%E7%8E%B0%E5%94%AE%E8%AE%B8%E5%8F%AF%E5%85%AC%E7%A4%BA&tid=441200'
url='http://yueanju.zfcxjst.gd.gov.cn/host/zhfc/trans/'

state={
    "#60B3AB":"冻结",
    "#83B183":"可售",
    "#202864":"抵押",
    "#D73027":"抵押",
    "#F36E43":"查封",
    "#18B4FF":"备案中",
    "#EE816C":"已备案",
    "#C51C7D":"已备案",
    "#6069BE":"已网签",
    "#EBCC00":"签约中",
    "#00EEFF":"内部限制",
    "#D73027":"限制销售",
    "#4D4D4D":"预告登记",
    "#5F50A2":"预告登记",
    "#4D4D4D":"首次登记",
    "#E08116":"超期未备案",
    "#0A554E":"销售审核中",
    "#93C4DE":"抵押权预告登记",
    "#DE77AE":"抵押权预告登记",
    "#EE816C":"在建工程抵押",
}
def trim(word):
    return re.sub('\s+','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')
def postHtml(url,data):
    time.sleep(1)
    #使用try except方法进行各种异常处理
    header = {
        'Accept':'application/json, text/plain, */*',
        'Accept-Encoding':'gzip, deflate',
        'Accept-Language':'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Cache-Control':'max-age=0',
        'Connection':'keep-alive',
        'Content-Length':'368',
        'Content-Type':'application/json',
        'Host':'yueanju.zfcxjst.gd.gov.cn',
        'Origin':'http://yueanju.zfcxjst.gd.gov.cn',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0',
    }
    #data=json.loads(js)
    try:
        res=requests.post(url=url,data=data,headers=header,verify=False)
        
        # #调用js函数
        # print(res.call('__doPostBack','AspNetPager1',2))
        res.encoding='utf-8'
        return res.json()
        
    except Exception as e:
        print(traceback.format_exc())
    return ''
def get_data(contenturl):
    city='肇庆'
    area=''
    rslist=[]
    cell={}
    detail=''
    newhtml=''
    progect=contenturl[9]
    progectName=contenturl[2]
    ysz=contenturl[3]
    try:
        #(contenturl+'\t'+sp+'\t'+pro+'\t'+ysz+'\t'+addr+'\t'+fzrq+'\t'+mj+'\t'+ts+'\t'+sale+'\t'+projectId+'\t\n')
        print(contenturl[0])
        cell={
            'url': contenturl[0],
            "城市": city,
            "项目名称": contenturl[2],
            "坐落位置":  contenturl[4],
            "开发企业": contenturl[1],
            "预售许可证编号": contenturl[3],
            "发证日期": contenturl[5],
            "开盘日期": '',
            "预售证准许销售面积": contenturl[6],
            "销售状态": '',
            "销售楼号": '',
            "套数": contenturl[7],
            "面积": '',
            "拟售价格": '',
            "售楼电话": '',
            "售楼地址": '',
            "房号": '',
            "房屋建筑面积": '',
            "房屋销售状态": '',
        }
        #获取预售证列表，并获取对对应的楼号编号，分页条设置成1w
        data={
            "chltype":"1000",
            "chlno":"400500",
            "version":"1.0.0",
            "temtype":"10",
            "loccoordinate":"loccoordinate",
            "msgid":"20200427wt161000000019406",
            "body":{"appCode":"",
            "invokeCode":"",
            "txCode":"A2020H1075",
            "tid":"441200",
            "entity":{"projectName":progectName,
            "projectId":progect,
            "fromDate":"",
            "endDate":"",
            "pageNo":1,
            "pageSize":10000}},
            "temip":"ip",
            "checksum":"checksum",
            "locaddr":"locaddr",
            "msgtype":"ZHFC00000001",
            "appcitycode":"1111"
        }
        json1=postHtml(url,json.dumps(data))['body']
        ysz_list=json1['blueprintInfoApps']
        #预售证列表，并判断是否为当前预售证#第一次时爬虫，获取所有项目，所有信息
        print('ysz_list=',len(ysz_list))
        if ysz_list==None:
            return [cell]
        for ysz_js in ysz_list:
            this_ysz=ysz_js['licenseid']
            #全量爬时，要去掉this_ysz!=ysz判断
            if this_ysz!=ysz:
                continue
            #判断预售证是否已爬
            if this_ysz in ysz_oklist :
                print('pass ysz',this_ysz)
                continue
            #
            ysz_mj=ysz_js.get('houArea','')
            ysz_frzq=ysz_js.get('certificationDate','')[0:10]
            if len(ysz_frzq)<10:
                ysz_frzq=ysz_js.get('fromDate','')[0:10]
            ysz_ts=str(ysz_js.get('houNum',''))
            lh_list=ysz_js['listNatu']
            print(this_ysz,'lh_list=',len(lh_list))
            if lh_list==None:
                continue
            for lhjs in lh_list:
                # natuUuid=lhjs['natuUuid']#有时是natuId呢？
                natuUuid =lhjs['natuId'] 
                lhname=lhjs.get('natuName','')
                if natuUuid==None or len(natuUuid)<4:
                    continue
                #按楼号获取户室，使用图例模式
                data={"chltype":"1000",
                    "chlno":"400500",
                    "version":"1.0.0",
                    "temtype":"10",
                    "loccoordinate":"loccoordinate",
                    "msgid":"20200427wt161000000019406",
                    "body":{"appCode":"",
                    "invokeCode":"",
                    "txCode":"A2020H969",
                    "tid":"441200",
                    "entity":{"ldbm":natuUuid}},
                    "temip":"ip",
                    "checksum":"checksum",
                    "locaddr":"locaddr",
                    "msgtype":"ZHFC00000001",
                    "appcitycode":"1111"
                }
                try:
                    json2=postHtml(url,json.dumps(data))['body']['lpbMhResult']['lpbData']['unitList']
                except:
                    continue
                    
                for j22 in json2:
                    dy=j22.get('name','')
                    houseList=j22.get('houseList',[])
                    for room in houseList:
                        cell2=cell.copy()
                        cell2['预售许可证编号']=this_ysz
                        cell2['套数']=ysz_ts
                        cell2['发证日期']=ysz_frzq
                        #
                        cell2['销售楼号']=lhname
                        cell2['拟售价格']=str(room.get('fwzj',''))
                        cell2['房号']=dy+room.get('fjh','')
                        cell2['房屋建筑面积']=str(room.get('jzmj',''))
                        cell2['房屋销售状态']=state.get(room.get('tldsdm',''),'')
                        rslist.append(cell2)
                        # time.sleep(22222)
        if len(rslist)==0:
            rslist.append(cell)
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
def getHtml(link,refer):
    html=""
    # print(link)
    try: #使用try except方法进行各种异常处理
        header = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Accept-Encoding': 'gzip, deflate',
            'Referer': refer,
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
            }
        res = requests.get(link,headers=header,timeout=20,verify=True) #读取网页源码
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
    #
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
                a = i.split('\t')
                ysz_oklist.add(a[5])
    repeatlist=[]
    if os.path.exists(outlistfile):
        with open(outlistfile, 'r', encoding='utf-8') as f:
            for i in f:
                a=i.split('\t')
                repeatlist.append(a[3])
    #如果outfile没数据，需要创建并写一个表头
    if has_new==0:
        of = open(outfile,'a', encoding='utf-8') #保存结果文件
        of.write('URL\t城市\t项目名称\t坐落位置\t开发企业\t预售许可证编号\t发证日期\t开盘日期\t预售证准许销售面积\t销售状态\t销售楼号\t套数\t面积\t拟售价格\t售楼电话\t售楼地址\t房号\t房屋建筑面积\t房屋销售状态（颜色区分）\n')
        of.flush()
    
    #按URl保存#按1w条每页翻页
    of = open(outlistfile,'a', encoding='utf-8') #保存结果文件，按url增量
    pn=0
    total=0
    while True :
        break
        pn+=1
        data={"chltype":"1000",
            "chlno":"400500",
            "version":"1.0.0",
            "temtype":"10",
            "loccoordinate":"loccoordinate",
            "msgid":"20200427wt161000000019406",
            "body":{"appCode":"",
            "invokeCode":"",
            "txCode":"A2020H1075",
            "tid":"441200",
            "entity":{"keyword":"",
            "pageNo":pn,
            "pageSize":10000}},
            "temip":"ip",
            "checksum":"checksum",
            "locaddr":"locaddr",
            "msgtype":"ZHFC00000001",
            "appcitycode":"1111"}
        js=postHtml(url,json.dumps(data))
        if total==0:
            total=js['body']['total']
            print(total)
        
        data=js['body']['blueprintInfoApps']
        print('page=',pn,len(data))
        if len(data)==0:
            break
        for d in data:
            # print(d)
            sp=d.get('orgName','')
            pro=d.get('projectName','')
            ysz=d.get('licenseid','')
            addr=d.get('projectAddress','')
            fzrq=d.get('certificationDate','')[0:10]
            if len(fzrq)<10:
                Datefzrq=d.get('from','')[0:10]
            mj=d.get('houArea','')
            ts=d.get('houNum','')
            sale=d.get('saleType','')
            tid=d.get('tid','441200')
            projectId=d.get('projectId','')
           
            contenturl=f'http://yueanju.zfcxjst.gd.gov.cn/zhfcgspt/project?pid={projectId}&tid={tid}&on='
            
            if ysz in repeatlist:
                # print('pass repeat',contenturl)
                continue
            if ysz in ysz_oklist:
                # print('pass ysz ',contenturl)
                continue
            repeatlist.append(ysz)
            #
            of.write(contenturl+'\t'+sp+'\t'+pro+'\t'+ysz+'\t'+addr+'\t'+fzrq+'\t'+mj+'\t'+ts+'\t'+sale+'\t'+projectId+'\t\n')
            of.flush()
            # print(contenturl+'\t'+sp+'\t'+pro+'\t'+ysz+'\t'+addr+'\t'+fzrq+'\t'+mj+'\t'+ts+'\t'+sale+'\t'+projectId+'\t\n')
            # # time.sleep(22222)
        if total<=pn*10000:
            break
    #首次按url去重爬所有预售证。以后按预售证增量爬虫
    repeatlist=[] 
    with open(outlistfile,'r',encoding='UTF-8') as txtData: 
        for line in txtData.readlines():
            a=line.split('\t')
            if a[3] in ysz_oklist:
                continue
            # #首次保留此判断
            # if a[0] in repeatlist:
                # # print('pass')
                # continue
            # repeatlist.append(a[0])
            urls.put(a)
            # outstr=get_data(a)
            # do_write(outfile,a[0],outstr)
            # time.sleep(2222)
        print("qsize="+str(urls.qsize()))           
        time.sleep(5)
        ths = []
        for i in range(5):
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
        
if __name__ == '__main__':
		main()
