# coding=gbk
import queue,time,random
from lxml import etree
from selenium import webdriver
import re,urllib.parse
import requests
import json
import xlrd
import traceback

dt=time.strftime('%Y%m%d',time.localtime(time.time()))
outfile="./data/guiyang_"+ dt + ".txt"
outlistfile="./list/guiyang.txt"
okfile="./data/guiyang.txt"
url="https://www.gyfc.net.cn/2_proInfo/search.aspx?type=1&box_pro_qu=-1&box_pro_xz=undefined&box_pro_mc=&box_pro_dj="

arealist=[]
def trim(word):
    return re.sub('[\s]*','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')   
def get_data(contenturl):
    s=contenturl.split('\t')
    uid=s[0].split('?')[1].split('&')
    xkzh=uid[0].replace('yszh=','')
    uid[0]=urllib.parse.quote(uid[0].replace('yszh=','').encode('gb2312'))
    html=getHtml(f'https://www.gyfc.net.cn/pro_query/index.aspx?yszh={uid[0]}&{uid[1]}')
    lxmls = etree.HTML(html)
    trs =[]
    city='贵阳'
    area=''
    rslist=[]
    ceil={}
    ceil2={}
    ceil3={}
    detail=''
    newhtml=''
    progect=s[1]
    addr=s[2]
    
    company=''
    table=lxmls.xpath('//table[@id="Table2"]/tr')
    newurl=''
    #开发商
    try:
        company=trim(table[5].xpath('./td[2]/text()')[0])
    except Exception as e:
        print(traceback.format_exc())
    #phone
    xsdh=''
    try:
        xsdh=trim(table[6].xpath('./td[2]/text()')[0])
        newurl=table[1].xpath('./td[2]/a/@href')[0]
    except Exception as e:
        print(traceback.format_exc())
    #ysmj
    ysmj=''
    if len(newurl)>0:
        tmp=getHtml('https://www.gyfc.net.cn/'+newurl)
        m=re.findall('预售面积：(.*?)<',tmp)
        if m :
            ysmj=trim(m[0])
    #addr
    ceil['城市']=city
    ceil['项目名称']=progect
    ceil['坐落位置']=addr
    ceil['开发企业']=company
    ceil['预售许可证编号']=xkzh
    ceil['预售证准许销售面积']=ysmj
    ceil['售楼电话']=xsdh
    print(ceil)
    #4
    newurl=f'https://www.gyfc.net.cn/pro_query/donginfo.aspx?yszh={uid[0]}&{uid[1]}'
    tmp=getHtml(newurl)
    lxmls = etree.HTML(tmp)
    trs=lxmls.xpath('//table[@id="ContentPlaceHolder1_dong_info1_dg1"]/tr')
    ceil_ld={}
    for num in range(1,len(trs)):
        ldh=trs[num].xpath('./td[1]/text()')[0]
        ts=trs[num].xpath('./td[3]/text()')[0]
        ceil_ld[ldh]=ts
    print(ceil_ld)
    #
    newurl=f'https://www.gyfc.net.cn/pro_query/FloorList.aspx?yszh={uid[0]}&{uid[1]}'
    html2=getHtml(newurl)
    lxmls2 = etree.HTML(html2)
    lou=lxmls2.xpath('//select[@id="ContentPlaceHolder1_floor_list1_ddlDong"]/option')
    ##楼栋信息
    for l in lou:
        ceil2={}
        ld=l.xpath('string(.)').replace('&#183;','.')
        ceil2['销售楼号']=ld
        dongid=l.xpath('./@value')[0]
        ######## 楼栋
        newurl='https://www.gyfc.net.cn/pro_query/index/floorView.aspx?dongID='+dongid+'&danyuan=%C8%AB%B2%BF&qu=%B9%F3%D1%F4&yszh='+uid[0]
        html3=getHtml(newurl)
        lxml3 = etree.HTML(html3)
        ms=re.findall("div class='P0.*?title='(.*?)'.*?>([^>]*?)</span>",html3,re.S)
        if ms :
            for m in ms:
                ceil3={}
                title=m[0].split('\n')
                sale=title[0]
                if len(title)<2:
                    continue
                mj=title[1]
                fh=m[1]
                ceil3['房屋建筑面积']=mj
                ceil3['房屋销售状态']=sale
                ceil3['房号']=fh
                ceil3['套数']=ceil_ld.get(dongid,'')
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
    return rslist
def postHtml(url,data):
    #使用try except方法进行各种异常处理
    header = {
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
        'Host':'www.gyfc.net.cn',
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language':'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Accept-Encoding':'gzip, deflate',
        'Content-Type':'application/x-www-form-urlencoded',
        'Origin':'www.gyfc.net.cn',
        'Connection':'keep-alive',
        'Referer':url,
        'Upgrade-Insecure-Requests':'1',
        'Pragma':'no-cache',
        'Cache-Control':'no-cache'
    }
    #data=json.loads(js)
    try:    
        res=requests.post(url=url,data=json.dumps(data),headers=header,verify=False)
        print(res)
        
        #调用js函数
        print(res.call('__doPostBack','AspNetPager1',2))
        
        res.encoding='GBK'
        return res.text
        
    except Exception as e:
        print(e)
    return ''
     
def getHtml(link):
    html=""
    try: #使用try except方法进行各种异常处理
        header = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER'}
        res = requests.get(link,headers=header,timeout=10,verify=False) #读取网页源码
        #解码
        
        print(link)
        if res.encoding=='utf-8' or res.encoding=='UTF-8' or res.text.find('charset="utf-8"')>0:
        		res.encoding='utf-8'
        else:
        		m = re.compile('<meta .*(http-equiv="?Content-Type"?.*)?charset="?([a-zA-Z0-9_-]+)"?', re.I).search(res.text)
        		if m and m.lastindex == 2:
        		    charset = m.group(2).upper()
        		    res.encoding=charset
        		else:
        		    res.encoding='GBK'
        res.encoding='GBK'
        html=res.text
    except Exception as e:
        print(traceback.format_exc())
    finally:
        return html
def getHtmlByWeb(link):
    source=''
    print(link)
    newlist=[]
    try: #使用try except方法进行各种异常处理
        browser = webdriver.Firefox()
        browser.get(link)
        time.sleep(5)
        #res = requests.get(link,headers=header,timeout=10,verify=True) #读取网页源码
        #res.encoding='UTF-8'
        source=browser.page_source
     
        m=re.findall('总页数：.*?(\d+)</b>',source)
        total=int(m[0])
        print(total)
        pn=1
        while pn<total+1:
            lxmls = etree.HTML(source)
            tables=lxmls.xpath('//div[@style="padding: 5px; width: 750px; border-style: dashed; border-width: 1px; border-color: skyblue;"]/table/tbody/tr/td/table/tbody')
            
            if len(tables)==0:
                break
            print(pn)
            for tb in tables:
                xsz=tb.xpath('./tr[1]/td[1]/a/@href')[0]#tb.xpath('./tr[1]/td[3]/text()')[0].split('】')[1]
                pro=tb.xpath('./tr[2]/td[2]/text()')[0]
                addr=tb.xpath('./tr[3]/td[2]/text()')[0]
                newlist.append(xsz+'\t'+pro+'\t'+addr)
            pn+=1
            browser.execute_script('javascript:__doPostBack(arguments[0],arguments[1])','AspNetPager1',str(pn))
            time.sleep(5)
            source=browser.page_source
        browser.close()
    except Exception as e:
        print(traceback.format_exc())
    finally:
        return newlist
def openFile2(filename):
    datalist=[]
    try:
        if  os.path.exists(filename):
            with open(filename,'r',encoding='UTF-8') as txtData: 
                for line in txtData.readlines():
                    datalist.append(line.strip())
    except Exception as e:
        print(traceback.format_exc())
    return datalist
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
def main():
    outlist=openFile2(outlistfile)
    oklist1=openList(okfile)
    oklist=[]
    for i in oklist1:
        if i in oklist:
            continue
        oklist.append(i)
    newlist=[]
    while True :
        newlist=getHtmlByWeb(url)
        #########
        break
    if len(newlist)>0:
        savefile(newlist,outlistfile)
    # print(oklist)
    
    for contenturl in outlist:
        print(contenturl.split('\t')[0] )
        if contenturl.split('\t')[0] in oklist:
            continue
        outstr=get_data(contenturl)
        # print(outstr)
        do_write(outfile,contenturl.split('\t')[0],outstr)
        time.sleep(1)   
        
if __name__ == '__main__':
		main()