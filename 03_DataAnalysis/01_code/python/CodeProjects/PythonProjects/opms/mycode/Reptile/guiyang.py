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
    city='����'
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
    #������
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
        m=re.findall('Ԥ�������(.*?)<',tmp)
        if m :
            ysmj=trim(m[0])
    #addr
    ceil['����']=city
    ceil['��Ŀ����']=progect
    ceil['����λ��']=addr
    ceil['������ҵ']=company
    ceil['Ԥ������֤���']=xkzh
    ceil['Ԥ��֤׼���������']=ysmj
    ceil['��¥�绰']=xsdh
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
    ##¥����Ϣ
    for l in lou:
        ceil2={}
        ld=l.xpath('string(.)').replace('&#183;','.')
        ceil2['����¥��']=ld
        dongid=l.xpath('./@value')[0]
        ######## ¥��
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
                ceil3['���ݽ������']=mj
                ceil3['��������״̬']=sale
                ceil3['����']=fh
                ceil3['����']=ceil_ld.get(dongid,'')
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
    #ʹ��try except�������и����쳣����
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
        
        #����js����
        print(res.call('__doPostBack','AspNetPager1',2))
        
        res.encoding='GBK'
        return res.text
        
    except Exception as e:
        print(e)
    return ''
     
def getHtml(link):
    html=""
    try: #ʹ��try except�������и����쳣����
        header = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER'}
        res = requests.get(link,headers=header,timeout=10,verify=False) #��ȡ��ҳԴ��
        #����
        
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
    try: #ʹ��try except�������и����쳣����
        browser = webdriver.Firefox()
        browser.get(link)
        time.sleep(5)
        #res = requests.get(link,headers=header,timeout=10,verify=True) #��ȡ��ҳԴ��
        #res.encoding='UTF-8'
        source=browser.page_source
     
        m=re.findall('��ҳ����.*?(\d+)</b>',source)
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
                xsz=tb.xpath('./tr[1]/td[1]/a/@href')[0]#tb.xpath('./tr[1]/td[3]/text()')[0].split('��')[1]
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
    of = open(filename,'a+', encoding='utf-8') #�����ļ�
    for data in datalist:
        if datalist not in old_datalist:
            of.write(data+"\n")
            of.flush()
def do_write(outfile,url,rsdict):
    of = open(outfile,'a+', encoding='utf-8') #�������ļ�
    for dicts in rsdict:
        of.write(url+"\t")
        of.write(dicts.get('����','')+'\t')
        of.write(dicts.get('��Ŀ����','')+'\t')
        of.write(dicts.get('����λ��','')+'\t')
        of.write(dicts.get('������ҵ','')+'\t')
        of.write(dicts.get('Ԥ������֤���','')+'\t')
        of.write(dicts.get('��֤����','')+'\t')
        of.write(dicts.get('��������','')+'\t')
        of.write(dicts.get('Ԥ��֤׼���������','')+'\t')
        of.write(dicts.get('����״̬','')+'\t')
        of.write(dicts.get('����¥��','')+'\t')
        of.write(dicts.get('����','')+'\t')
        of.write(dicts.get('���','')+'\t')
        of.write(dicts.get('���ۼ۸�','')+'\t')
        of.write(dicts.get('��¥�绰','')+'\t')
        of.write(dicts.get('��¥��ַ','')+'\t')
        of.write(dicts.get('����','')+'\t')
        of.write(dicts.get('���ݽ������','')+'\t')
        of.write(dicts.get('��������״̬',''))
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







# 爬取一个html并保存

import requests

url = "http://www.baidu.com"

response = requests.get( url )

response.encoding = "utf-8" #设置接收编码格式

print("\nr的类型" + str( type(response) ) )

print("\n状态码是:" + str( response.status_code ) )

print("\n头部信息:" + str( response.headers ) )

print( "\n响应内容:" )

print( response.text )

#保存文件
file = open("C:\\爬虫\\baidu.html","w",encoding="utf")  #打开一个文件，w是文件不存在则新建一个文件，这里不用wb是因为不用保存成二进制

file.write( response.text )

file.close()