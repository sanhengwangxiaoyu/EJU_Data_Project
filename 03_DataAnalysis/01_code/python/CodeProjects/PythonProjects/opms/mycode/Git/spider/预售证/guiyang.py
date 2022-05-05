# -*- coding:utf-8 -*-
#按URL增量爬虫，为预售证url
import queue,time,random
from lxml import etree
from selenium import webdriver
import re,urllib.parse
import requests
import json
import xlrd
import traceback,os
from queue import Queue

urls = Queue()
okurl = set()

outfile="./data/guiyang_new.txt"
outlistfile="./list/guiyang.txt"
okfile="./data/guiyang.txt"
url="https://www.gyfc.net.cn/2_proInfo/search.aspx?type=1&box_pro_qu=-1&box_pro_xz=undefined&box_pro_mc=&box_pro_dj="

arealist=[]
def trim(word):
    return re.sub('(\s+)','',word).replace('\\n','').replace('\\xa0','').replace('\\t','').replace('&nbsp;','')
def get_data(contenturl):
    uid=contenturl[0].split('?')[1].split('&')
    xkzh=uid[0].replace('yszh=','')
    uid[0]=urllib.parse.quote(uid[0].replace('yszh=','').encode('gb2312'))
    html=getHtml(f'https://www.gyfc.net.cn/pro_query/index.aspx?yszh={uid[0]}&{uid[1]}')
    lxmls = etree.HTML(html)
    trs =[]
    city='贵阳'
    area=''
    rslist=[]
    cell={}
    cell2={}
    cell3={}
    detail=''
    newhtml=''
    progect=contenturl[1]
    addr=contenturl[2].strip()
    
    table=lxmls.xpath('//table[@id="Table2"]/tr')
    #开发商
    company=trim(''.join(re.findall('开发商：.*?</td>.*?>(.*?)</td>',html,re.S)))
    #sale
    saleaddr=trim(''.join(re.findall('售楼处地址：.*?</td>.*>(.*?)</td>',html,re.S)))
    xsdh=''.join(re.findall('销售电话：.*?</td>.*?>(.*?)</td>',html,re.S)).strip()
    print(xsdh)
    xsdh=re.sub('([\s、，,\.])+','/',xsdh)
    newurl=''.join(re.findall('/pro_query/index/proAffiche\.aspx\?gonggaoId=(.*?)yszh=',html,re.S))
    newurl=f'https://www.gyfc.net.cn/pro_query/index/proAffiche.aspx?gonggaoId={newurl}&yszh={uid[0]}'
    #ysmj
    ysmj=''
    fzrq=''
    if len(newurl)>0:
        tmp=getHtml(newurl)
        ysmj=trim(''.join(re.findall('预售面积(?:：|\:)(.*?)<',tmp,re.S)))
        if len(ysmj)==0:
            ysmj=trim(''.join(re.findall('建筑面积(?:：|\:)(.*?)<',tmp,re.S)))
        fzrq=trim(''.join(re.findall('有效期(?:：|\:)(.*?)(?:至|<)',tmp,re.S)))
        if len(fzrq)==0:
            fzrq=trim(''.join(re.findall('预售许可延期情况.*>([\d\- ]*)</td><td.*?>[\d\- ]*</td>\s*</tr>\s*</table>',tmp,re.S)))
    #addr
    cell['城市']=city
    cell['项目名称']=progect
    cell['坐落位置']=addr
    cell['开发企业']=company
    cell['预售许可证编号']=xkzh
    cell['发证日期']=fzrq
    cell['预售证准许销售面积']=ysmj
    cell['售楼地址']=saleaddr
    cell['售楼电话']=xsdh
    print(cell)
    #4
    newurl=f'https://www.gyfc.net.cn/pro_query/donginfo.aspx?yszh={uid[0]}&{uid[1]}'
    tmp=getHtml(newurl)
    lxmls = etree.HTML(tmp)
    trs=lxmls.xpath('//table[@id="ContentPlaceHolder1_dong_info1_dg1"]/tr')
    cell_ld={}
    for num in range(1,len(trs)):
        ldh=trs[num].xpath('./td[1]/text()')[0]
        ts=trs[num].xpath('./td[3]/text()')[0]
        cell_ld[ldh]=ts
    print(cell_ld)
    #
    newurl=f'https://www.gyfc.net.cn/pro_query/FloorList.aspx?yszh={uid[0]}&{uid[1]}'
    html2=getHtml(newurl)
    lxmls2 = etree.HTML(html2)
    lou=lxmls2.xpath('//select[@id="ContentPlaceHolder1_floor_list1_ddlDong"]/option')
    ##楼栋信息
    for l in lou:
        cell2={}
        ld=l.xpath('string(.)').replace('&#183;','.')
        cell2['销售楼号']=ld
        dongid=l.xpath('./@value')[0]
        ######## 楼栋
        newurl='https://www.gyfc.net.cn/pro_query/index/floorView.aspx?dongID='+dongid+'&danyuan=%C8%AB%B2%BF&qu=%B9%F3%D1%F4&yszh='+uid[0]
        html3=getHtml(newurl)
        lxml3 = etree.HTML(html3)
        ms=re.findall("div class='P0.*?title='(.*?)'.*?>([^>]*?)</span>",html3,re.S)
        if ms :
            for m in ms:
                cell3={}
                title=m[0].split('\n')
                sale=title[0]
                if len(title)<2:
                    continue
                mj=title[1]
                fh=m[1]
                cell3['房屋建筑面积']=mj
                cell3['房屋销售状态']=sale
                cell3['房号']=fh
                cell3['套数']=cell_ld.get(dongid,'')
                for c in cell:
                    cell3[c]=cell[c]
                for c in cell2:
                    cell3[c]=cell2[c]
                rslist.append(cell3)
        if len(rslist)==0:
            for c in cell:
                cell2[c]=cell[c]
            rslist.append(cell2)
    if len(rslist)==0:
        rslist.append(cell)
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
    print(link)
    try: #使用try except方法进行各种异常处理
        header = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER'}
        res = requests.get(link,headers=header,timeout=60,verify=True) #读取网页源码
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
        res.encoding='GBK'
        html=res.text
    except Exception as e:
        print(traceback.format_exc())
    finally:
        return html
def getHtmlByWeb(link,of,repeatlist):
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
        print('pages=',total)
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
                if xsz in repeatlist or xsz in okurl:
                    continue
                of.write(xsz+'\t'+pro+'\t'+addr+'\n')
                of.flush()
            pn+=1
            #时间中断判断为2020年
            if source.find('SellInfo.aspx?yszh=2020')>0:
                break
            browser.execute_script('javascript:__doPostBack(arguments[0],arguments[1])','AspNetPager1',str(pn))
            time.sleep(5)
            source=browser.page_source
            
    except Exception as e:
        print(traceback.format_exc())
    finally:
        browser.close()
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
def main():
    #if has new
    has_new=0
    if os.path.exists(outfile):
        with open(outfile, 'r', encoding='utf-8') as f:
            for i in f:                  
                okurl.add(i.split('\t')[0])
                has_new=1
    if os.path.exists(okfile):
        with open(okfile, 'r', encoding='utf-8') as f:
            for i in f:
                okurl.add(i.split('\t')[0])
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
    
    #爬列表，中断时间设置为2020年
    of = open(outlistfile,'a', encoding='utf-8') #保存结果文件
    while True :
        break
        getHtmlByWeb(url,of,repeatlist)
        #########
        break
    #爬详细
    repeatlist=[]    
    with open(outlistfile, 'r', encoding='utf-8') as f:
         for i in f:
            s=i.split('\t')
            if s[0] in okurl:
                continue
            if s[0] in repeatlist:
                continue
            repeatlist.append(s[0])
            urls.put(s)
    #
    print('qsize=',urls.qsize())
    while urls.qsize()!=0:
        print('qsize less=',urls.qsize())
        contenturl=urls.get()
        # contenturl=['https://www.gyfc.net.cn/pro_query/index.aspx?yszh=2019058&qu=3','','','','','']
        outstr=get_data(contenturl)
        # print(outstr)
        do_write(outfile,contenturl[0],outstr)
           
        
if __name__ == '__main__':
		main()