# -*- coding:utf-8 -*-  

import requests
import re
from lxml import etree
import time
import json
import random
import html


from threading import Thread
from queue import Queue
import traceback,os

urls = Queue()
okurl = set()
ok_yszlist=set()

listfile='list/wuxi_url.txt'
outfile='data/wuxi_new.txt'
okfile='data/wuxi.txt'

class wx_spider():
    def getHtml(self,link):
        num=0
        html="<html></html>"
        print(link)
        if link.find('code=X')>0:
            return html
        try: #使用try except方法进行各种异常处理
            header = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER'}
            res = requests.get(link,headers=header,timeout=120,verify=True) #读取网页源码
            #解码
            if res.encoding=='utf-8' or res.encoding=='UTF-8' or res.text.find('charset="utf-8"')>0:
                res.encoding='utf-8'
            else:
                res.encoding='GBK'
            html=res.text
        except Exception as e:
            print(traceback.format_exc())
        finally:
            return html
    def trim(self,word):
        return re.sub('[\s]*','',word).replace('\\n','').replace('\\xa0','').replace('\\t','') 
    def parse_one(self):
        ####历史已爬虫
        has_new=0
        if os.path.exists(outfile):
            with open(outfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.strip().split('\t')
                    okurl.add(a[0])
                    ok_yszlist.add(a[5])
                    has_new=1
        if os.path.exists(okfile):
            with open(okfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.strip().split('\t')
                    okurl.add(a[0])
                    ok_yszlist.add(a[5])
        if has_new==0:
            of = open(outfile,'a', encoding='utf-8') #保存结果文件
            of.write('URL\t城市\t项目名称\t坐落位置\t开发企业\t预售许可证编号\t发证日期\t开盘日期\t预售证准许销售面积\t销售状态\t销售楼号\t套数\t面积\t拟售价格\t售楼电话\t售楼地址\t房号\t房屋建筑面积\t房屋销售状态（颜色区分）\n')
            of.flush()
        #####
        repeatlist=[]
        of = open(listfile,'a', encoding='utf-8') #保存结果文件80
        page=0
        
        #按预售证爬增量
        while True:
            break
            page+=1
            text=self.getHtml(f'http://www.wxhouse.com/listprelicence?pager.currentPage={page}')
            tabs=etree.HTML(text).xpath('//div[@class="yxsxkz"]/a')
            print('page=',page,len(tabs))
            if len(tabs)==0:
                break
            for tr in tabs:
                ysz=''.join(tr.xpath('./li/text()')).strip()
                if ysz in ok_yszlist:
                    continue
                yszurl='http://www.wxhouse.com/'+tr.xpath('./@href')[0]
                ok_yszlist.add(ysz)
                of.write(yszurl+'\t'+ysz+'\t\n')
           
        #2多线程爬ysz
        with open(listfile,'r', encoding='utf-8') as f:
            for i in f:
                urls.put(i.split('\t'))
                # self.run()
                # time.sleep(1111)
        print("qsize="+str(urls.qsize()))  
        if urls.qsize()==0:
            return ''
        time.sleep(3)
        ths = []
        for i in range(1):
            t = Thread(target=self.run, args=())
            t.start()
            ths.append(t)
        for t in ths:
            t.join()
    def run(self):
        while urls.qsize() != 0:
            print("qsize less="+str(urls.qsize()))  
            us=urls.get()
            url2=us[0] #http://www.wxhouse.com/showprelicencemain?preLicence.icon=63d2982b4f871f011e9f91207cd30cc9
            ysz=us[1]
            print('url2='+url2)
            rslist=[]
            try:
                text=self.getHtml(url2)
                proid=''.join(re.findall('/showpro\?pro\.icon=(.*?)"',text,re.S))
                if len(proid)>0:
                    text2=self.getHtml(f'http://www.wxhouse.com/showprodetail?pro.icon={proid}')
                    text3=self.getHtml(f'http://www.wxhouse.com/showpro?pro.icon={proid}')
                else:
                    text2=''
                dict2 = {
                    "URL": url2,
                    "城市": '无锡',
                    "项目名称": self.trim(''.join(re.findall('楼盘名称：</strong>.*?>(.*?)</a>',text,re.S))),
                    "坐落位置": self.trim(''.join(re.findall('商品房屋坐落：</strong>(.*?)<',text,re.S))),
                    "开发企业": self.trim(''.join(re.findall('单位名称：</strong>(.*?)<',text,re.S))),
                    "预售许可证编号": ysz,
                    "发证日期": self.trim(''.join(re.findall('发证时间：</strong>(.*?)<',text,re.S))),
                    "开盘日期": '',
                    "预售证准许销售面积": self.trim(''.join(re.findall('建筑面积：</strong>(.*?平方米)',text,re.S))),
                    "销售状态": '',
                    "销售楼号": self.trim(''.join(re.findall('商品房屋名称：</strong>(.*?)<',text,re.S))),
                    "套数": '',
                    "面积": '',
                    "拟售价格":'' ,
                    "售楼电话": ''.join(re.findall('售楼处地址：</li>\s+<li class="info_title_r">(.*?)</li>',text3,re.S)),
                    "售楼地址": ''.join(re.findall('售楼处地址：</li>\s+<li class="info_title_r">(.*?)</li>',text2,re.S)),
                    "房号": '',
                    "房屋建筑面积": '',
                    "房屋销售状态": '',
                }
                print(dict2)
                ##非在售、售完的，没有预售证，直接保存
                url3 =f'http://www.wxhouse.com/listsaleinfo?pro.icon={proid}'
                print('url3='+url3)
                time.sleep(0.1)
                text5=self.getHtml(url3)
                table5 = etree.HTML(text5)
                #预售证列表及楼盘列表
                text5=re.sub('\s+','',text5)
                #1无列表预售证
                ul1 = table5.xpath('//ul[1]/li/a/@href')
                for u in ul1:
                    # ru1 = self.pool.get('http://www.wxhouse.com'+u, timeout=60, headers=headers)
                    # textu =  ru1.content.decode('utf-8')
                    textu=self.getHtml('http://www.wxhouse.com'+u)
                    this_ysz=self.trim(''.join(re.findall('<h3>(.*?)</h3>',textu)))
                    #非当前ysz，跳过
                    print(this_ysz)
                    # if this_ysz in ysz:
                        # continue
                    if this_ysz in ok_yszlist:
                        continue
                    dictu = dict2.copy()
                    dictu['预售许可证编号']=self.trim(''.join(re.findall('<h3>(.*?)</h3>',textu)))
                    dictu['预售证准许销售面积']=self.trim(''.join(re.findall('上市预（销）售建筑面积：</strong>(.*?)<',textu)))
                    dictu['发证日期']=self.trim(''.join(re.findall('发证时间：</strong>(.*?)<',textu)))
                    dictu['销售楼号']=self.trim(''.join(re.findall('商品房屋名称：</strong>(.*?)<',textu)))
                    dictu['套数']=''
                    rslist.append(dictu)
                    # df = DataFrame(dictu, index=[0])
                    # df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                #2有楼栋的或有 销售证地址的[销售证，楼栋，预售面积，发证时间，楼栋list]
                ul2text=''.join(re.findall('<ul>.*?</ul>.*?(<ul>.*?</ul>)',text5))
                # ul2 = re.findall('<liclass="yszbg">([^>]*?)</li>.*?(<ahref=.*?)<',ul2text)
                #类型 680f975640fe095dfe0cda9ecd8341c6
                list_ysz=[]
                ul2=ul2text.split('<divclass="clear"></div>')
                for utext in ul2:
                    list_lp={}
                    print('u2-<li>')
                    murl=re.findall('<ahref="(/showprelicencemain.*?)"',utext)
                    if murl:
                        testa=self.getHtml("http://www.wxhouse.com"+murl[0])
                        list_lp={
                            '预售许可证编号':self.trim(''.join(re.findall('<h3>(.*?)</h3>',testa))),
                            '预售证准许销售面积':self.trim(''.join(re.findall('上市预（销）售建筑面积：</strong>(.*?)<',testa))),
                            '发证日期':self.trim(''.join(re.findall('发证时间：</strong>(.*?)<',testa)))
                            }
                    else:
                        m=re.findall('class="yszbg">(.*?)</li>',utext,re.S)
                        if m:
                            list_lp['预售许可证编号']=m[0].replace('+','')
                    lurl=re.findall('<ahref="(/listsaleinfo.*?)".*?>([^>]*?)</li></a>',utext)
                    if lurl:
                        list_lp['list']=lurl
                    if len(list_lp)>0:
                        list_ysz.append(list_lp)
                print(list_ysz)
                ###多组预售证
                for key in list_ysz:
                    #非当前ysz，跳过
                    thisysz=key.get('预售许可证编号','')
                    # if thisysz!=ysz or thisysz=='':
                        # continue
                    if thisysz in ok_yszlist:
                        print('ysz pass',thisysz)
                        continue
                    ok_yszlist.add(thisysz)
                    #
                    dict4 = dict2.copy()
                    dict4['预售许可证编号']=thisysz
                    dict4['预售证准许销售面积']=key.get('预售证准许销售面积','')
                    dict4['发证日期']=key.get('发证日期','')
                    #无楼栋时
                    if len(key.get('list',''))==0:
                        print('continue,key.get null',key)
                        # df = DataFrame(dict4, index=[0])
                        # df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                        rslist.append(dict4)
                        continue
                    #有楼时
                    print('lou----')
                    for up in key['list']:
                        urlfang='http://www.wxhouse.com'+up[0]
                        time.sleep(0.1)
                        table55 = etree.HTML(self.getHtml(urlfang))
                        table66 = table55.xpath('//table[@class="h30 dgray16px6"]')
                        table77 = table55.xpath('//td[@class=" black20px"]/text()')
                        table999 = list(zip(table66, table77))
                        print('tab999='+str(len(table999)))
                        if len(table999)==0:
                            # df = DataFrame(dict4, index=[0])
                            # df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                            rslist.append(dict4)
                            continue
                        for qq in table999:
                            table888 = qq[0].xpath('.//tr[position()>1]')
                            dy=self.trim(qq[1])
                            print(dy)
                            if len(table888)==0:
                                # df = DataFrame(dict4, index=[0])
                                # df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                                rslist.append(dict4)
                                continue
                            for hh in table888:
                                dict5=dict4.copy()
                                dict5['销售楼号']=up[1]
                                dict5["房号"] = dy+'-'+''.join(hh.xpath('.//td[1]//text()'))
                                dict5["房屋建筑面积"] = ''.join(hh.xpath('.//td[2]//text()'))
                                state=self.getstate(''.join(hh.xpath('.//td[4]//td//@class')))
                                if state=='':
                                    state=self.getstate(''.join(hh.xpath('.//td[4]//td//@style')))
                                if state=='':
                                    state=''.join(hh.xpath('.//td[4]//td//@class'))+''.join(hh.xpath('.//td[4]//td//@style'))
                                dict5["房屋销售状态"] = state
                                rslist.append(dict5)
                                # df = DataFrame(dict5, index=[0])
                                # df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                    if len(rslist)==0:
                        rslist.append(dict2)
                    self.do_write(rslist)
                    # time.sleep(1220)
            except Exception as e:
                print(traceback.format_exc())
                continue
            
    def do_write(self,rsdict):
        of = open(outfile,'a+', encoding='utf-8') #保存结果文件
        for dicts in rsdict:
            of.write(dicts.get('URL','')+'\t')
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
    def getSale(self,us):
        if us[2]=='/wxhproject/images/zaishou.png':
            sale='在售'
            ok=True
        elif us[2]=='/wxhproject/images/weifang.png':
            sale='尾盘'
            ok=True
        elif us[2]=='/wxhproject/images/shouwan.png':
            sale='售完'
            ok=True
        elif  us[2]=='/wxhproject/images/weishou.png':
            sale='未售'
            ok=False
        elif  us[2]=='/wxhproject/images/jingshi.png':
            sale='警示'
            ok=False
        else:
            sale=''
            ok=False
        return ok,sale
    def getstate(self,color):
        state = color
        if(color=='bg_sale'):
            state='待售'
        elif(color=='bg_sold'):
            state = '已售'
        elif (color=='background:#0cc'):
            state = '登记'
        elif (color=='background:#3f0'):
            state = '待售'
        elif (color=='background:#930'):
            state = '查封'
        elif (color=='background:#0cc'):
            state = '登记'
        elif (color=='background:#c93'):
            state = '保留'
        return state

    def main(self):
        self.parse_one()
run = wx_spider()
run.main()