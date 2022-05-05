# -*- coding:utf-8 -*-  
#按url增量，对应的是预售证链接
__date__ = '2021/4/1 21:16'
import requests
from lxml import etree
import time,os
import json
import re
from threading import Thread
from queue import Queue
import traceback

urls = Queue()
okurl = set()
outfile='data/beijing_new.txt'
okfile='data/beijing.txt'
listfile='list/beijing_url.txt'

#设置：列表的中断翻页，预售证时间
end_YYYYMM='2021-06'

class bj_spider():
    def get(self,link,header):
        html=""
        while True:
            try: #使用try except方法进行各种异常处理
                # header = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER'}
                res = requests.get(link,headers=header,timeout=20,verify=False) #读取网页源码
                #解码
                res.encoding='utf-8'
                html=res.text
                break
            except Exception as e:
                print(e)
                if str(e).find('RemoteDisconnected')>0:
                    print(1,link)
                    time.sleep(1)
                elif str(e).find('RemoteDisconnected')>0:
                    print(2,link)
                    time.sleep(1)
                else:
                    break
        return html
    def post(self,url,data):
        #使用try except方法进行各种异常处理
        header = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER'}
        try:
            res=requests.post(url=url,data=data,headers=header,verify=False)
            res.encoding='UTF-8'
            return res.text
        except Exception as e:
            print(traceback.format_exc())
        return ''
    def trim(self,word):
        return re.sub('[\s]*','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')   
    def parse_detail(self):
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
        if os.path.exists(listfile):
            with open(listfile, 'r', encoding='utf-8') as f:
                for i in f:
                    repeatlist.append(i.strip())
        #如果outfile没数据，需要创建并写一个表头
        if has_new==0:
            of = open(outfile,'a', encoding='utf-8') #保存结果文件
            of.write('URL\t城市\t项目名称\t坐落位置\t开发企业\t预售许可证编号\t发证日期\t开盘日期\t预售证准许销售面积\t销售状态\t销售楼号\t套数\t面积\t拟售价格\t售楼电话\t售楼地址\t房号\t房屋建筑面积\t房屋销售状态（颜色区分）\n')
            of.flush()
        #####翻页获取全部链接
        of = open(listfile,'a+', encoding='utf-8') #保存list文件
        #设置：是否获取新链接
        ifgeturl=False
        
        #'rblFWType':'q','rblFWType':'x'  期房，现房
        for rblFWType in ['q','x']:
            #初始翻页
            total=0
            page=0
            end='no'
            while ifgeturl and end=='no':
                page+=1
                dict33 = {
                    'rblFWType':rblFWType,
                    "currentPage": str(page),
                    "pageSize": "15",
                }
                #http://bjjs.zjw.beijing.gov.cn/eportal/ui?pageId=308452&isTrue=1
                html8 = self.post("http://bjjs.zjw.beijing.gov.cn/eportal/ui?pageId=307670", dict33)
                table8 = etree.HTML(html8)
                table11 = table8.xpath('//form//table[2]//tr[2]//td//table//tr[position()>1]')
                if total==0:
                    total=int(re.findall('总记录数:(\d+),',html8,re.S)[0])
                print('totalpage=',total,'thispage='+str(page),len(table11))
                for mm in table11:
                    url11 = 'http://bjjs.zjw.beijing.gov.cn'+''.join(mm.xpath(".//td[1]/a/@href"))
                    #中断设置
                    if mm.xpath('string(.)').find(end_YYYYMM)>0:
                        end='yes'
                        break
                    #已存在，跳过   
                    if url11 in repeatlist:
                        continue
                    repeatlist.append(url11)
                    of.write(url11+'\n')
                    of.flush()
                #结束跳出
                if total<=page:
                    break
        ###2多线程爬项目
        #待爬url
        if  os.path.exists(listfile):
            with open(listfile,'r', encoding='utf-8') as f:
                for i in f:
                    a = i.strip()
                    if len(a)>7 and a in okurl:
                        continue
                    urls.put(a)
                    # self.run()
                    # time.sleep(111111)
        print(urls.qsize())
        time.sleep(5)
        ths = []
        for i in range(1):
            t = Thread(target=self.run, args=())
            t.start()
            ths.append(t)
        for t in ths:
            t.join()
            # time.sleep(1111)
    def run(self):
        while urls.qsize() != 0:
            dict = {
                "URL": '',
                "城市": '北京',
                "项目名称": '',
                "坐落位置": '',
                "开发企业": '',
                "预售许可证编号": '',
                "发证日期": '',
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
            try:
                url11=urls.get()
                print('less=',urls.qsize())
                # url11='http://bjjs.zjw.beijing.gov.cn/eportal/ui?pageId=391154&projectID=6519873&systemID=2&srcId=1'
                print(url11)
                headers = {
                    'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Encoding':'gzip, deflate',
                    'Accept-Language':'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
                    'Cache-Control':'max-age=0',
                    'Connection':'keep-alive',
                    'Host':'bjjs.zjw.beijing.gov.cn',
                    'Upgrade-Insecure-Requests':'1',
                    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
                }
                content = self.get(url11,headers)
                # print(dict)
                tablea = etree.HTML(content)
                xmmc =self.trim(''.join(re.findall('id="项目名称".*?>(.*?)</td>',content,re.S)))
                kfqy =self.trim(''.join(re.findall('(?:id="开发企业|id="房屋所有权人名称")".*?>(.*?)</td>',content,re.S)))
                zlwz =self.trim(''.join(re.findall('id="坐落位置".*?>(.*?)</td>',content,re.S)))
                ysxk =self.trim(''.join(re.findall('(?:id="预售许可证编号|id="房屋所有权证号")".*?>(.*?)</td>',content,re.S)))
                xsmj =self.trim(''.join(re.findall('id="准许销售面积".*?>(.*?)</td>',content,re.S)))
                fzrq =self.trim(''.join(re.findall('id="发证日期".*?>(.*?)</td>',content,re.S)))
                dict['URL'] = url11
                dict['项目名称'] = xmmc.strip()
                dict['坐落位置'] = zlwz.strip()
                dict['开发企业'] = kfqy.strip()
                dict['预售许可证编号'] = ysxk.strip()
                dict['预售证准许销售面积'] = xsmj
                dict['发证日期'] = fzrq
                ##为现房，转到现房地址，例：http://bjjs.zjw.beijing.gov.cn/eportal/ui?pageId=320794&projectID=3634586&systemID=2&srcId=1
                newurl=''.join(tablea.xpath('//tr[@class="现房销售"]/td[2]/a/@href'))
                rslist=[]
                if len(newurl)>0:
                    h11 = self.get('http://bjjs.zjw.beijing.gov.cn'+newurl,headers)
                    m=re.findall('<td style="height:28px;"><a href="(.*?)">',h11,re.S)
                    if len(m)==0:
                        rslist.append(dict)
                    else:
                        ulist={}
                        #有重复URL，通过{}达至去重
                        for u in m :
                            ulist[u]=''
                        for u in ulist:
                            contentu= self.get('http://bjjs.zjw.beijing.gov.cn'+u,headers)
                            tablea=etree.HTML(contentu)
                            rstmp=self.getHouse(dict,contentu,tablea,url11)
                            for r in rstmp:
                                rslist.append(r)
                else:
                    rslist=self.getHouse(dict,content,tablea,url11)
                print('rslist=',len(rslist))
                if len(rslist)>0:
                    self.do_write(url11,rslist)
                time.sleep(0.1)
            except  Exception as e:
                print(traceback.format_exc())
            # time.sleep(1111)
    def getHouse(self,dict,content,tablea,url11):
        dict['售楼地址']=''.join(re.findall('>售楼地址.*?<td.*?>(.*?)</td>',content,re.S)).strip()
        tel=''.join(re.findall('>售楼电话.*?<td.*?>(.*?)</td>',content,re.S)).strip()
        tel=re.sub('[\r\n ]+','/',tel.strip())
        dict['售楼电话']=tel
        headers = {
                'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Encoding':'gzip, deflate',
                'Accept-Language':'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
                'Connection':'keep-alive',
                'Cookie':'JSESSIONID=E79A1C0162BB68C7396E5542B3A92473; _va_id=28491113b11d231f.1625460758.1.1625467187.1625460758.; _va_ses=*',
                'Host':'bjjs.zjw.beijing.gov.cn',
                'Referer':url11,
                'Upgrade-Insecure-Requests':'1',
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            }
        #数据解析,无楼盘表直接保存，有楼盘表且数量大于8时，需要用更多，大于20个时，需要翻页。结果临时保存成List中
        loupan_list=[]
        m=re.findall('共有 <span id="">(\d+)</span> 个楼栋信息',content,re.S)
        if len(m)==1:
            total=int(m[0])
        else:
            total=0
        time.sleep(0.5)
        if total<9:
            trs = tablea.xpath('//span[@id="Span1"]//tr')
            for tr in trs:
                cell=[]
                for i in range(1,6):
                    tmp=''
                    try:
                        tmp=tr.xpath('./td['+str(i)+']')[0].xpath('string(.)').strip()
                    except Exception as e:
                        tmp=''
                    cell.append(tmp)
                try:
                    tmp=tr.xpath('./td//a/@href')[0].strip()
                except Exception as e:
                    tmp=''
                tmp=re.sub('(\s+)','',tmp)
                cell.append(tmp)
                loupan_list.append(cell)
        else:
            pn=0
            tab_href = tablea.xpath('//div[@id=""]/table[2]//a/@href')  # 判断是否有更多楼盘
            if len(tab_href)==1:
                u2='http://bjjs.zjw.beijing.gov.cn'+tab_href[0]
                while True:
                    pn+=1
                    h2 = self.get(u2+'&currentPage='+str(pn)+'&pageSize=20',headers)
                    tab2=etree.HTML(h2)
                    trs = tab2.xpath('//span[@id="Span1"]//tr[position()>1]')
                    for tr in trs:
                        cell=[]
                        for i in range(1,6):
                            tmp=''
                            try:
                                tmp=tr.xpath('./td['+str(i)+']')[0].xpath('string(.)').strip()
                            except Exception as e:
                                tmp=''
                            cell.append(tmp)
                        try:
                            tmp=tr.xpath('./td//a/@href')[0]
                        except Exception as e:
                            tmp=''
                        tmp=re.sub('(\s+)','',tmp)
                        cell.append(tmp)
                        loupan_list.append(cell)
                    if total<=pn*20:
                        break
            else:
                return [dict]
                
            # time.sleep(200000)
        rslist=[]
        #lp=0
        if len(loupan_list)==0:
            print('loupan=0')
            rslist.append(dict)
        #遍历所有楼栋，所有楼栋都爬完后，一起保存。
        print('楼数=',len(loupan_list))
        for lp in loupan_list: 
            #lp=销售楼号,批准销售套数,批准销售,面积(m2),销售状态,住宅拟售,价格(元/m2),楼盘表href                    
            dict2={
                '销售楼号':lp[0],
                '套数':lp[1],
                '面积':lp[2],
                '开盘日期':lp[3],
                '拟售价格':lp[4]
            }
            dict3=dict.copy()
            dict3.update(dict2)
            if lp[5]=='#' or lp[5]=='':
                rslist.append(dict3)
                continue
            
            time.sleep(0.5)
            u3='http://bjjs.zjw.beijing.gov.cn'+lp[5]
            print('u3=',u3)
            h3 = self.get(u3,headers)
            tab3 = etree.HTML(h3)
            rooms = tab3.xpath('//table[@id="table_Buileing"]//td//div')
            #
            print('louhao='+lp[0]+',单元户室='+str(len(rooms)))
            if len(rooms)==0:
                rslist.append(dict3)
                continue
            #有户室
            for room in rooms:
                dict4=dict3.copy()
                mianji=''
                hprice=''
                xszt = ''.join(room.xpath('./@style'))
                fh = ''.join(room.xpath('.//a/text()'))
                room_url = ''.join(room.xpath('.//a/@href'))
                xszt = re.findall(r'width:130px;background:(.*?);float', xszt, re.I)[0]
                xszt=self.state(xszt)
                sss=''
                if len(room_url)>3:
                    sss= "http://bjjs.zjw.beijing.gov.cn" + room_url
                    h4=self.get(sss,headers)
                    sss=self.trim(''.join(re.findall('(?:>建筑面积</td>|建筑面积\(m2\)</td>).*?>(.*?)</td>',h4,re.S)))
                dict4['房屋销售状态']=xszt
                dict4['房号']=self.trim(fh)
                dict4['房屋建筑面积']=sss
                if len(hprice)>0:
                    dict4['拟售价格']=hprice
                rslist.append(dict4)
        return rslist
    def state(self,xszt):
        if(xszt=='#CCCCCC'):
            xszt='不可售'
        elif(xszt=='#33CC00'):
            xszt = '可售'
        elif (xszt == '#FFCC99'):
            xszt = '已预订'
        elif (xszt == '#FF0000'):
            xszt = '已签约'
        elif (xszt == '#ffff00'):
            xszt = '已办理预售项目抵押'
        elif (xszt == '#d2691e'):
            xszt = '网上联机备案'
        elif (xszt == '#00FFFF'):
            xszt = '资格核验中'
        elif (xszt == '#0099CC'):
            xszt = '资格核验中'
        return xszt
    def do_write(self,url,rsdict):
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
    def main(self):
        self.parse_detail()
        
run = bj_spider()
run.main()