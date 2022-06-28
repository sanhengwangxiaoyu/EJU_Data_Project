#In[]
from ast import Pass
from sqlalchemy import create_engine
from lxml import etree
from requests.adapters import HTTPAdapter
from queue import Queue
from difflib import SequenceMatcher
from threading import Thread
from selenium import webdriver
import requests,json,pandas as pd,time,numpy as np,pymysql,os,configparser,datetime,re,random,traceback


class MysqlClient:
    def __init__(self, db_host,database,user,password):
        """
        create connection to hive server
        """
        self.conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
    def query(self, sql):
        """
        query
        """
        cur = self.conn.cursor()
        cur.execute(sql)
        res = cur.fetchall()
        columnDes = cur.description #获取连接对象的描述信息
        columnNames = [columnDes[i][0] for i in range(len(columnDes))]
        data = pd.DataFrame([list(i) for i in res],columns=columnNames)
        cur.close()
        return data
    # 更新SQL
    def updata_one(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()
    def close(self):
        self.conn.close() 

#####创建父类
class spider():
    def get(self,link,header):
        html=""
        while True:
            try: 
                header = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER'}
                res = requests.get(link,headers=header,timeout=20,verify=False) 
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
    def is_nullto_value(self,value_str):
        if value_str is None :
            return '暂无'
        elif len(value_str) == 0:
            return '暂无'
        else:
            return value_str
    def repsPost(self,url,data) :
        header = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER'}
        response = requests.post(url, headers=header, data=data)
        response.encoding='UTF-8'
        response.close()
        return response.json()
    def respGet(self,url,data) :
        header = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER'}
        response = requests.get(url,headers=header,data=data,timeout=15,verify=False) #读取网页源码
        response.encoding='UTF-8'
        response.json()
    def similarity(self,a, b):
        return SequenceMatcher(None, b, a).ratio()#引用ratio方法，返回序列相似性的度量
    #对一个字符串内容，进行去重的方法
    def str_drop_duplicates(self,str):
        listl_newest_name = list(str)
        lists_newest_name = list(set(str))
        lists_newest_name.sort(key=listl_newest_name.index)
        str_result = "".join(lists_newest_name)
        return str_result
    def do_write(self,outfile,url,rsdict):
        of = open(outfile,'a+', encoding='utf-8') #保存结果文件
        for dicts in rsdict:
            of.write(url+"\t")
            of.write(dicts.get('城市','')+'\t')
            of.write(self.trim(dicts.get('项目名称',''))+'\t')
            of.write(self.trim(dicts.get('坐落位置',''))+'\t')
            of.write(self.trim(dicts.get('开发企业',''))+'\t')
            of.write(self.trim(dicts.get('预售许可证编号',''))+'\t')
            of.write(dicts.get('发证日期','').strip()+'\t')
            of.write(dicts.get('开盘日期','').strip()+'\t')
            of.write(self.trim(dicts.get('预售证准许销售面积',''))+'\t')
            of.write(self.trim(dicts.get('销售状态',''))+'\t')
            of.write(self.trim(dicts.get('销售楼号',''))+'\t')
            of.write(self.trim(dicts.get('套数',''))+'\t')
            of.write(self.trim(dicts.get('面积',''))+'\t')
            of.write(self.trim(dicts.get('拟售价格',''))+'\t')
            of.write(re.sub('[\r\n]+','/',(dicts.get('售楼电话','').strip()))+'\t')
            of.write(self.trim(dicts.get('售楼地址',''))+'\t')
            of.write(self.trim(dicts.get('房号',''))+'\t')
            of.write(self.trim(dicts.get('房屋建筑面积',''))+'\t')
            of.write(self.trim(dicts.get('房屋销售状态','')))
            of.write("\n")
            of.flush()
    def run():
        Pass

class fs_spider(spider):
    urls = Queue()
    okurl = set()

    listfile='list/foshan_url.txt'
    okfile='data/foshan.txt'
    outfile='data/foshan_new.txt'
    def getHtml(self,link):
        html=""
        # print(link)
        try: #使用try except方法进行各种异常处理
            header = {
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0'+str(random.random()),
            } 
            res = requests.get(link,headers=header,timeout=30,verify=False) #读取网页源码
            #解码
            # print(link)
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
            print('html err')
        finally:
            return html
    def parse_one(self):
        ##已爬URL列表
        if os.path.exists(self.outfile):
            with open(self.outfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.split('\t')
                    self.okurl.add(a[0])
        if os.path.exists(self.okfile):
            with open(self.okfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.split('\t')
                    self.okurl.add(a[0])
        #
        repeatlist=[]
        if os.path.exists(self.listfile):
            with open(self.listfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.split('\t')
                    repeatlist.append(a[0])
        #
        ##########
        of = open(self.listfile,'a+', encoding='utf-8') #保存结果文件
        #翻页获取项目列表range(1,185):
        page=0
        total=0
        while True:
            page+=1
            try:          
                lburl = 'https://fsfc.fszj.foshan.gov.cn/search/index.do?sw=&dn=&hx=&lx=&mj=&jg=&ys=0&od=-FZRQ&ad_check=1&p='+str(page)
                time.sleep(1)
                h1=self.getHtml(lburl)
                table1 = etree.HTML(h1)
                table2 = table1.xpath('//div[@class="col-2-1"]//dl//dd')
                if total==0:
                    total=int(re.findall('共(\d+)条记录',h1)[0])
                    print(total)
                print(page,len(table2))
                for kk in table2:
                    xmmc=''.join(kk.xpath(".//h3//a//text()")).strip()
                    id = ''.join(kk.xpath(".//h3//a//@onclick")).replace('testbarx(', '').replace(')', '')
                    url1 = 'https://fsfc.fszj.foshan.gov.cn/hpms_project/roomView.jhtml?id=' + id
                    dict2 = {
                        'url': url1,
                        "城市": '佛山',
                        "项目名称": xmmc,
                        "坐落位置": ''.join(kk.xpath(".//p[1]//text()")).replace('楼盘地址： ','').replace('\t','').strip(),
                        "开发企业": ''.join(kk.xpath(".//p[2]//text()")).replace('开  发  商：',''),
                        "预售许可证编号": '',
                        "发证日期": '',
                        "开盘日期": '',
                        "预售证准许销售面积": '',
                        "销售状态": '',
                        "销售楼号": '',
                        "套数": '',
                        "面积": '',
                        "拟售价格": ''.join(kk.xpath(".//h3//strong//text()")),
                        "售楼电话": ''.join(kk.xpath(".//span//text()")),
                        "售楼地址": '',
                        "房号": '',
                        "房屋建筑面积": '',
                        "房屋销售状态": ''
                    }
                    if url1 in self.okurl or url1 in repeatlist:
                        continue
                    df = pd.DataFrame(dict2, index=[0])
                    df.to_csv(self.listfile, sep='\t', mode='a', index=False, header=None)
                if total<=page*10:
                    break
            except Exception as e:
                print(e)
                continue
        #2多线程爬项目
        repeatlist=[]
        
        with open(self.listfile,'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                if a[0] not in repeatlist and a[0] not in self.okurl:
                    self.urls.put(a)
                    repeatlist.append(a[0])
        print("qsize="+str(self.urls.qsize()))           
        time.sleep(5)
        ths = []
        for i in range(3):
            t = Thread(target=self.run, args=())
            t.start()
            ths.append(t)
        for t in ths:
            t.join()
            # time.sleep(1111)
    def run(self):
        while self.urls.qsize() != 0:
            print("qsize less="+str(self.urls.qsize()))   
            dicts=self.urls.get()
            url1=dicts[0]
            id=url1.split('=')[1]
            xmmc=dicts[2]
            try:                
                dict2 = {
                    'url': url1,
                    "城市": '佛山',
                    "项目名称": dicts[2],
                    "坐落位置": dicts[3],
                    "开发企业": dicts[4],
                    "预售许可证编号": '',
                    "发证日期": '',
                    "开盘日期": '',
                    "预售证准许销售面积": '',
                    "销售状态": '',
                    "销售楼号": '',
                    "套数": '',
                    "面积": '',
                    "拟售价格":  dicts[13],
                    "售楼电话": dicts[14],
                    "售楼地址": '',
                    "房号": '',
                    "房屋建筑面积": '',
                    "房屋销售状态": ''
                }
                print('url1='+url1)
                # r1 = self.pool.get(url1, timeout=60, headers=headers)
                h1=self.getHtml(url1)
                rr='<ahtml><td></td><ahtml/>'.join(re.findall(r'<p class="bot-a">([\s\S]*)<div class="tab-con js_trsTab">',h1, re.I))
                table3 = etree.HTML(rr)
                table4 = table3.xpath('//td')
                print(len(table4))
                # for pp in rr:
                #     id2=re.findall(r'id="(.*?)"', pp, re.I)
                #     print(id2)
                # print(rr)
                # time.sleep(0.1)
                url5='https://fsfc.fszj.foshan.gov.cn/hpms_project/ysxkz.jhtml?id='+str(id)+'&zmc='+xmmc
                print(url5)
                h5=self.getHtml(url5)
                table9 = etree.HTML(h5).xpath('//div[@class="js_con tab03"]//select//option')
                #销售号信息及楼号关系
                ysz_dict={}
                for hh in table9:
                    xkzid = ''.join(hh.xpath('.//@value'))
                    url99 = 'https://fsfc.fszj.foshan.gov.cn/hpms_project/ysxkzxx.jhtml?xkzh='+xkzid
                    h99=self.getHtml(url99)
                    r99 = json.loads(h99)
                    ysz_dict[r99.get('xkzh')]=r99
                print('ysz_list=',len(table9))
                time.sleep(0.1)
                #户室详情json
                for tt in table4:
                    dict2["销售楼号"]=''.join(tt.xpath('.//a//text()'))
                    id1=''.join(tt.xpath('.//a//@id'))
                    url2='https://fsfc.fszj.foshan.gov.cn/hpms_project/room.jhtml?id='+id1
                    print('url2-loudong=',url2)
                    r2=json.loads(self.getHtml(url2))
                    for item in r2:
                        dict4=dict2.copy()
                        # print(item)
                        xkzh=item["xkzh"]
                        dict4["预售许可证编号"] = xkzh
                        dict4["房屋销售状态"]=item["zt"]
                        dict4["房号"] = item["roomno"]
                        #判断销售号，取信息dict3
                        if xkzh in ysz_dict:
                            rjson = ysz_dict[xkzh]
                            dict4["预售证准许销售面积"] = rjson.get('yszmj')
                            timeStamp = rjson.get('fzrq')
                            if len(str(timeStamp))>10:
                                timeArray = time.localtime(timeStamp/1000)
                                otherStyleTime = time.strftime("%Y-%m-%d", timeArray)
                                dict4["发证日期"] = otherStyleTime
                            dict4["套数"] = rjson.get('yszts')
                            dict4["面积"] = rjson.get('yszmj')
                        dict4["房屋建筑面积"] = item["jzmj"]
                        df = pd.DataFrame(dict4, index=[0])
                        df.to_csv(self.outfile, sep='\t', mode='a', index=False, header=None)
            except Exception as e:
                print(traceback.format_exc())
                continue
    def main(self):
        self.parse_one()

class sh_spider(spider):
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
    def roomJS(self,name,browser):
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
    def trim(self,word):
        return re.sub('\s+','',word).replace('\\n','').replace('\\xa0','').replace('\\t','').replace('&nbsp;','')
    def get_data(self,contenturl,browser):
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
                    "项目名称":self.trim(''.join(re.findall('class="nameC" name="(.*?)">',h1,re.S))),
                    "坐落位置":self.trim(''.join(re.findall('<span>项目地址：</span>(.*?)</',h1,re.S))),
                    "开发企业":self.trim(''.join(re.findall('<span>企业名称：</span>.*?>(.*?)</',h1,re.S))),
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
                if ysz in self.ysz_oklist:
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
                        state=self.sateDict.get(state_tmp,state_tmp)
                        name=''.join(room.xpath('./@name'))
                        try:
                            mj=self.roomJS(name,browser)
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
    def do_write(self,outfile,url,rsdict):
        of = open(outfile,'a+', encoding='utf-8') #保存结果文件
        for dicts in rsdict:
            of.write(url+"\t")
            of.write(dicts.get('城市','')+'\t')
            of.write(self.trim(dicts.get('项目名称',''))+'\t')
            of.write(self.trim(dicts.get('坐落位置',''))+'\t')
            of.write(self.trim(dicts.get('开发企业',''))+'\t')
            of.write(self.trim(dicts.get('预售许可证编号',''))+'\t')
            of.write(dicts.get('发证日期','').strip()+'\t')
            of.write(dicts.get('开盘日期','').strip()+'\t')
            of.write(self.trim(dicts.get('预售证准许销售面积',''))+'\t')
            of.write(self.trim(dicts.get('销售状态',''))+'\t')
            of.write(self.trim(dicts.get('销售楼号',''))+'\t')
            of.write(self.trim(dicts.get('套数',''))+'\t')
            of.write(self.trim(dicts.get('面积',''))+'\t')
            of.write(self.trim(dicts.get('拟售价格',''))+'\t')
            of.write(re.sub('[\r\n]+','/',(dicts.get('售楼电话','').strip()))+'\t')
            of.write(self.trim(dicts.get('售楼地址',''))+'\t')
            of.write(self.trim(dicts.get('房号',''))+'\t')
            of.write(self.trim(dicts.get('房屋建筑面积',''))+'\t')
            of.write(self.trim(dicts.get('房屋销售状态','')))
            of.write("\n")
            of.flush()
    def main(self):
        #已爬列表
        has_new=0
        if os.path.exists(self.outfile):
            with open(self.outfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.split('\t')                    
                    self.ysz_oklist.add(a[5])
                    has_new=1
        if os.path.exists(self.okfile):
            with open(self.okfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.strip()
                    self.ysz_oklist.add(a[5])
        repeatlist=[]
        if os.path.exists(self.outlistfile):
            with open(self.outlistfile, 'r', encoding='utf-8') as f:
                for i in f:
                    repeatlist.append(i.strip())
        #如果outfile没数据，需要创建并写一个表头
        if has_new==0:
            of = open(self.outfile,'a', encoding='utf-8') #保存结果文件
            of.write('URL\t城市\t项目名称\t坐落位置\t开发企业\t预售许可证编号\t发证日期\t开盘日期\t预售证准许销售面积\t销售状态\t销售楼号\t套数\t面积\t拟售价格\t售楼电话\t售楼地址\t房号\t房屋建筑面积\t房屋销售状态（颜色区分）\n')
            of.flush()
        ##
        of = open(self.outlistfile,'a+', encoding='utf-8')#保存结果文件
        pn=0
        total=0
        browser = webdriver.Firefox()
        browser.get(self.url)
        time.sleep(3)
        while True :
            # break
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
        if os.path.exists(self.outlistfile):
            with open(self.outlistfile,'r',encoding='UTF-8') as txtData: 
                for line in txtData.readlines():
                    contenturl=line.strip()
                    if contenturl in repeatlist:
                        continue
                    repeatlist.append(contenturl)
                    self.urls.put(contenturl)
            print("qsize=",self.urls.qsize())
            time.sleep(3)
            while self.urls.qsize()!=0:
                print("less=",self.urls.qsize())
                contenturl=self.urls.get()
                print(contenturl)
                outstr=self.get_data(contenturl,browser)
                if len(outstr)>0:
                    self.do_write(self.outfile,contenturl,outstr)
                # time.sleep(111110.3)
        browser.close()

class gz_spider(spider):
    url='http://zfcj.gz.gov.cn/zfcj/fyxx/fdcxmxxRequest?sProjectName=&sProjectAddress=&sDeveloper=&sPresellNo=&ValidateCode=&page=$1&pageSize=15'
    def trim(self,word):
        return re.sub('\s+','',word).replace('\\n','').replace('\\xa0','').replace('\\t','').replace('&nbsp;','')
    def postHtml(self,url,data):
        #使用try except方法进行各种异常处理
        header = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
            'Host': 'zfcj.gz.gov.cn',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' ,
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2' ,
            'Content-Type': 'application/x-www-form-urlencoded' ,
            'Referer': 'http://zfcj.gz.gov.cn/zfcj/fyxx/xkb?sProjectId=100000022310&sPreSellNo=' ,
            'Cookie': 'JSESSIONID=74414D52F61A249250F56085102075A4; sto-id-20480=GLPNMCAKFAAA' ,
        }
        #data=json.loads(js)
        try:
            res=requests.post(url=url,data=data,headers=header,verify=False)
            
            # #调用js函数
            # print(res.call('__doPostBack','AspNetPager1',2))
            res.encoding='UTF-8'
            return res.text
            
        except Exception as e:
            print(traceback.format_exc())
        return ''
    def getHtml(self,link):
        html=""
        # print(link)
        try: #使用try except方法进行各种异常处理
            header = {
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0'+str(random.random()),
                'Host': 'zfcj.gz.gov.cn',
                'Accept': 'application/json, text/javascript, */*; q=0.01' ,
                'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2' ,
                # 'Content-Type': 'application/json' ,
                'X-Requested-With': 'XMLHttpRequest' ,
                'Referer': 'http://zfcj.gz.gov.cn/zfcj/fyxx/fdcxmxx' ,
                'Cookie': 'JSESSIONID=74414D52F61A249250F56085102075A4; sto-id-20480=GLPNMCAKFAAA' ,
            } 
            res = requests.get(link,headers=header,timeout=30,verify=False) #读取网页源码
            #解码
            # print(link)
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
            print('html err')
        finally:
            return html
    def getHtml2(self,link,id):
        html=""
        # print(link)
        try: #使用try except方法进行各种异常处理
            header = {
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
                'Cookie': f'npsaleid={id}; wjbazt=; ASP.NET_SessionId=3b2mtoowny5c2dxpkwrvgidt;CNZZDATA915400=cnzz_eid%3D257811575-1619411532-%26ntime%3D1619411532; UM_distinctid=1790c9a3f071f0-077d05e1d885288-4b5f451b-144000-1790c9a3f08581',
                'Referer':'http://gs.czfdc.com.cn/newxgs/Pages/Lp_Show.aspx?a=0.38875284136369764',
            
                }
            res = requests.get(link,headers=header,timeout=20,verify=False) #读取网页源码
            #解码
            # print(link)
            if res.encoding=='utf-8' or res.encoding=='UTF-8':
                    res.encoding='UTF-8'
            else:
                    m = re.compile('<meta .*(http-equiv="?Content-Type"?.*)?charset="?([a-zA-Z0-9_-]+)"?', re.I).search(res.text)
                    if m and m.lastindex == 2:
                        charset = m.group(2).upper()
                        res.encoding=charset
                    else:
                        res.encoding='GBK'
            res.encoding='utf-8'
            html=res.text
        except Exception as e:
            print(e)
        finally:
            return html
    def get_data(self,contenturl,driver):
        #print(contenturl,pro,sp,ysz,addr)
        city='广州'
        area=''
        rslist=[]
        cell={}
        detail=''
        newhtml=''
        progect=contenturl[1]
        company=contenturl[2]
        ysz=contenturl[3]
        addr=contenturl[4].strip()
        
        sProjectId=re.findall('sProjectId=([\da-zA-Z]+)',contenturl[0])[0]
        presell=contenturl[5].strip()
        try:
        
            #pro    http://zfcj.gz.gov.cn/zfcj/fyxx/projectdetail?sProjectId=45585&sDeveloperId=13844 http://zfcj.gz.gov.cn/zfcj/fyxx/jbxx?sProjectId=45585
            #ysz    http://zfcj.gz.gov.cn/zfcj/fyxx/jbxx?sProjectId=47140&sDeveloperId=15864  可能为空不用
            #lplist http://zfcj.gz.gov.cn/zfcj/fyxx/xkb?sProjectId=100000022310&sPreSellNo=
            #rooms POST http://zfcj.gz.gov.cn/zfcj/fyxx/xmxkbxxList
            cell = {
                    "城市": city,
                    "项目名称": progect,
                    "坐落位置":addr, 
                    "开发企业": company,
                    "预售许可证编号":ysz,
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
            ##
            h1=self.getHtml(contenturl[0].replace('projectdetail','jbxx'))
            ysmj=''.join(re.findall('批准预售面积：</td>.*?<td class="tab_style01_td">(.*?)</td>',h1,re.S)).strip()
            cell['预售证准许销售面积']=ysmj
            ysz=''.join(re.findall('预售证：</td>.*?<td class="tab_style01_td">(.*?)</td>',h1,re.S)).strip()
            cell['预售许可证编号']=ysz
            ##fzrq
            presell_1=ysz.split(',')[0]
            h2=self.getHtml(f'http://zfcj.gz.gov.cn/zfcj/fyxx/ysz?sProjectId={sProjectId}&sPreSellNo={presell_1}')
            fzrq=''.join(re.findall('发证日期.*?align="left">(.*?)</p>',h2,re.S)).strip()
            cell['发证日期']=fzrq
            ##longdong list
            u=contenturl[0].replace('projectdetail','xkb')+'&sPreSellNo='+ysz
            driver.get(u)
            time.sleep(1)
            driver.execute_script('document.getElementsByName("modeID")[0].checked=false')
            time.sleep(1)
            driver.execute_script('document.getElementsByName("modeID")[1].checked=true')
            time.sleep(1)
            text=driver.page_source
            loudongs=re.findall('name="buildingId".*?value="(.*?)".*?>(.*?)</td>',text,re.S)
            stopnum=0
            while text.find('name="buildingId"')<0:
                driver.refresh()
                print('刷新')
                time.sleep(2)
                text=driver.page_source
                stopnum+=1
                if stopnum>5:
                    break
            token=driver.execute_script('return document.getElementById("token").value')
            print('loudongs=',len(loudongs))
            
            if len(loudongs)==0:
                return [cell]
            ##############有户室
            parma={
                    'sProjectId':sProjectId,
                    'token':token,
                    'modeID':'2',#2是列表，1是图
                    'houseFunctionId':'0',
                    'unitType':'',
                    'houseStatusId':'0',
                    'totalAreaId':'0',
                    'inAreaId':'0',
                    'buildingId':'',
            }
            num=0
            for loudong in loudongs:
                cell2=cell.copy()
                cell2['销售楼号']=loudong[1]
                #
                parma['buildingId']=loudong[0]
                # if (buildingIds[i].checked == true) {
                            # buildingIds[i].checked = false;
                            # var j =i + 1
                            # if (j<buildingIds.length)
                            # {
                                # buildingIds[j].checked = true;
                                # break
                            # }
                        # } 
                js='var buildingIds = document.getElementsByName("buildingId");for (var i = 0; i < buildingIds.length; i++) {buildingIds[i].checked = false;} buildingIds['+str(num)+'].checked = true;DoSearch();return document.getElementById("token").value;'
                
                # print(js)
                token=driver.execute_script(js)
                num+=1
                
                parma['token']=token
                time.sleep(2)
                h3=self.postHtml('http://zfcj.gz.gov.cn/zfcj/fyxx/xmxkbxxList',parma)
                stopnum=0
                while text.find('判断当前状态')<0:
                    time.sleep(2)
                    h3=self.postHtml('http://zfcj.gz.gov.cn/zfcj/fyxx/xmxkbxxList',parma)
                    stopnum+=1
                    if stopnum>5:
                        break
                tab33=etree.HTML(h3)
                rooms=tab33.xpath('//div[@class="content_tab"]/table/tr')
                # print('h3-token=',token)
                print('rooms=',len(rooms))
                if len(rooms)==0:
                    rslist.append(cell2)
                    continue
                cell2['套数']=str(len(rooms))
                for room in rooms:
                    cell3=cell2.copy()
                    cell3['房屋销售状态']=room.xpath('./td[5]')[0].xpath('string(.)').strip()
                    cell3['房号']=room.xpath('./td[1]')[0].xpath('string(.)').strip()
                    cell3['房屋建筑面积']=room.xpath('./td[3]')[0].xpath('string(.)').strip()
                    rslist.append(cell3)
                    # print(cell3)
            if len(rslist)==0:
                rslist.append(cell)
        except Exception as e:
            print(traceback.format_exc())
        print(len(rslist))
        # time.sleep(11111)
        return rslist
    def openList(self,filename):
        datalist=[]
        try:
            if  os.path.exists(filename):
                with open(filename,'r',encoding='UTF-8') as txtData: 
                    for line in txtData.readlines():
                        datalist.append(line.split('\t')[0])
        except Exception as e:
            print(traceback.format_exc())
        return datalist
    def main(self):
        end_date = '9999-99-99'
        #已爬列表
        if os.path.exists(gz_outfile):
            with open(gz_outfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.split('\t')                    
                    gz_oklist.add(a[0])
                    
        if os.path.exists(gz_okfile):
            with open(gz_okfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.split('\t')
                    gz_oklist.add(a[0])
        
        repeatlist=[]
        if os.path.exists(gz_outlistfile):
            with open(gz_outlistfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.split('\t')
                    repeatlist.append(a[0])
        ##
        of = open(gz_outlistfile,'a+', encoding='utf-8') #保存结果文件
        pn=0
        total=0
        html=''
        while True :
            # break
            pn+=1
            try:
                time.sleep(8)
                while True :
                    html=self.getHtml(self.url.replace('$1',str(pn)))
                    if html.find('接口签名错误或已超时')<0 and len(html)>100:
                        break
                    print('sleep15')
                    time.sleep(30)
                if total==0:
                    total=int(re.findall('totalPage":(\d+),',html,re.S)[0])
                js=json.loads(html)['data']
                print('pn=',pn,len(js))
                #########
                if len(js)==0:
                    break
                for j in js: 
                    presell=str(j.get('presell',''))
                    sProjectId=str(j.get('projectId',''))
                    sDeveloperId=str(j.get('developerId',''))
                    ysz=str(j.get('presell',''))
                    pro=j.get('projectName','')
                    addr=str(j.get('projectAddress',''))
                    sp=j.get('developer','')  
                    ysz_date=j.get('awardedDate','')  
                    contenturl=f'http://zfcj.gz.gov.cn/zfcj/fyxx/projectdetail?sProjectId={sProjectId}&sDeveloperId={sDeveloperId}'
                    time.sleep(0.1)
                    if contenturl in gz_oklist:
                        continue
                    if contenturl not in repeatlist:
                        repeatlist.append(contenturl)
                        of.write(contenturl+'\t'+pro+'\t'+sp+'\t'+ysz+'\t'+addr+'\t'+presell+'\n')
                        of.flush()
                    if ysz_date <= end_date.replace('-',''):
                        break
                if ysz_date <= end_date.replace('-',''):
                    break
            except Exception as e :
                print(traceback.format_exc())
                print('p',html)
                break
            #结束跳出while
            if total<=pn:
                break
            # break####当天只有一页
        repeatlist=[]
        if os.path.exists(gz_outlistfile):
            with open(gz_outlistfile,'r',encoding='UTF-8') as txtData: 
                for line in txtData.readlines():
                    contenturl=line.split('\t')
                    if contenturl[0] in gz_oklist or contenturl[0] in repeatlist:
                        continue
                    repeatlist.append(contenturl[0])
                    gz_oklist.add(contenturl[0])
                    gz_urls.put(contenturl)

            print("qsize="+str(gz_urls.qsize())) 
            ths = []
            for i in range(1):
                t = Thread(target=self.run, args=())
                t.start()
                ths.append(t)
            for t in ths:
                t.join()
    def run(self):
        driver = webdriver.PhantomJS(executable_path=r'D:\EJU\after_20210520\10_Tools\phantomjs\phantomjs-2.1.1-windows\phantomjs-2.1.1-windows\bin\phantomjs.exe')
        while gz_urls.qsize() != 0: 
            contenturl=gz_urls.get()
            print("qsize less="+str(gz_urls.qsize()))   
            print(contenturl)
            outstr=self.get_data(contenturl,driver)
            self.do_write(gz_outfile,contenturl[0],outstr)
            # time.sleep(1)    
        driver.close()
    
    def savefile(self,datalist,filename):
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

class sanya_spider(spider):
    ###三亚
    def run(self):
        df = pd.DataFrame(columns=['url','region','gd_city','floor_name','address','business','issue_code','issue_date', \
                 'issue_area','building_code','room_sum'])
        url = "http://www.fcxx0898.com/syfcSiteApi//Presale/ListPresale"
        for i in range(1,2):
            try:
                dat = {
                    "KeyWord": "", 
                    "PageIndex": i, 
                    "PageSize": 15
                }
                # resq = self.post(url,headers=headers,data=dat)
                # json_data = json.loads(resq.text)
                json_data = self.repsPost(url,dat)
                pageList = json_data['Data']['pageList']
                for item in range(0,len(pageList)):
                    ID = json_data['Data']['pageList'][item]['ID']
                    chil_url = 'http://www.fcxx0898.com/syfcSiteApi//Presale/GetPerSale'
                    chil_dat = {"ID": ID}
                    # chil_resq = requests.post(chil_url,headers=headers,data=chil_dat)
                    # chil_json_data = json.loads(chil_resq.text)
                    chil_json_data = self.repsPost(chil_url,chil_dat)
                    chil_json_data = chil_json_data['Data']
                    if chil_json_data['RegisterDate'][0:10] >= date[0:10] :  ## 获取指定时间之前的数据
                        newest_name = json_data['Data']['pageList'][item]['Name']
                        busniss = json_data['Data']['pageList'][item]['Enterprise']
                        issue_code = chil_json_data['PresaleCert']
                        issue_location = chil_json_data['Location']
                        issue_region = self.is_nullto_value(chil_json_data['Location']).replace('海南省三亚市','').replace('三亚市','').replace('海南省','')[0:3]
                        issue_area = str(chil_json_data['TotalArea'])
                        issue_room_num = str(chil_json_data['RoomCount'])
                        issue_start_date = chil_json_data['StartDate'][0:10]
                        issue_building_code = chil_json_data['Content']
                        city_name = "三亚市"
                        # print(chil_json_data)
                        new=pd.DataFrame({'url':"http://www.fcxx0898.com/syfcSiteWeb/Pages/Project/PresaleInfo.aspx?id="+str(ID),'region':issue_region,'gd_city':city_name,'floor_name':newest_name,'address':issue_location,'business':busniss,'issue_code':issue_code,'issue_date':issue_start_date,'issue_area':issue_area,'building_code':issue_building_code,'room_sum':issue_room_num},index=[0])
                        df=df.append(new,ignore_index=True) 
                    else:
                        print("从网址的第"+str(i)+"页获取 :" + (datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d') +" 登记的数据失败")
                        break 
                    # chil_resq.close()
            except requests.exceptions.RequestException as e:
                print(e)
                print("报异常:跳过")
                pass
        # resq.close()
        df.at[df['region'].isin(['三亚崖','海榆西','红塘湾']),'region'] = '崖州区'
        df.at[df['region'].isin(['三亚海','南田温','国营南','崖州湾','海棠北','海棠湾']),'region'] = '海棠区'
        df.at[df['region'].isin(['三亚中','三亚凤','南新横','吉阳镇','新风街','榆亚路','河东区','田独镇','荔枝沟','迎宾路','龙塘路','东岸湿','凤凰路','河东路']),'region'] = '吉阳区'
        df.at[df['region'].isin(['三亚湾','园林路','海南三','海坡度','育秀路','金鸡岭']),'region'] = '天涯区'
        df.at[df['region'].isin(['三亚湾','园林路','海南三','海坡度','育秀路','金鸡岭']),'region'] = '天涯区'
        print(df)

class bj_spider(spider):
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
        ifgeturl=True
        #'rblFWType':'q','rblFWType':'x'  期房，现房
        for rblFWType in ['q','x']:
            #初始翻页
            total=0
            page=0
            end='no'
            while ifgeturl and end=='no':
                issue_date = ""
                page+=1
                dict33 = {
                    'rblFWType':rblFWType,
                    "currentPage": str(page),
                    "pageSize": "15",
                }
                #http://bjjs.zjw.beijing.gov.cn/eportal/ui?pageId=308452&isTrue=1
                html8 = self.post("http://bjjs.zjw.beijing.gov.cn/eportal/ui?pageId=307670", dict33)
                # print(html8  + '\n' + "===============================================")
                table8 = etree.HTML(html8)
                table11 = table8.xpath('//form//table[3]//tr[2]//td//table//tr[position()>1]')
                if total==0:
                    total=int(re.findall('总记录数:(\d+),',html8,re.S)[0])
                print('totalpage=',total,'thispage='+str(page),len(table11))
                for mm in table11:
                    url11 = 'http://bjjs.zjw.beijing.gov.cn'+''.join(mm.xpath(".//td[2]/a/@href"))
                    issue_date = mm.xpath("./td[3]/text()")[0].strip()
                    #已存在，跳过   
                    if url11 in repeatlist:
                        continue
                    if total<=page or issue_date <= end_date:
                        break
                    repeatlist.append(url11)
                    of.write(url11+'\n')
                    of.flush()
                #结束跳出
                if total<=page or issue_date <= end_date:
                    break
            # print(issue_date,end_date)
            if issue_date <= end_date:
                continue

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
                    self.do_write(outfile,url11,rslist)
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
    def main(self):
        self.parse_detail()





# ##读取配置文件##
# pymysql.install_as_MySQLdb()
# cf = configparser.ConfigParser()
# path = os.path.abspath(os.curdir)
# confpath = path + "/conf/config4.ini"
# cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块
# ##设置变量初始值##
# user = cf.get("Mysql", "user")  # 获取user对应的值
# password = cf.get("Mysql", "password")  # 获取password对应的值
# db_host = cf.get("Mysql", "host")  # 获取host对应的值
# database = cf.get("Mysql", "database")  # 获取dbname对应的值
# date = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d')+" T00:00:00"
# end_date='2022-06-06'
# urls = Queue()
# okurl = set()
# outfile='data/beijing_new.txt'
# okfile='data/beijing.txt'
# listfile='list/beijing_url.txt'     
# #创建数据库连接
# con = MysqlClient(db_host,database,user,password)
# issue_ccode_maxdate = con.query("select gd_city,max(issue_date) issue_date from odsdb.ods_city_issue_code where dr = 0 and issue_date != '9999-09-09' group by gd_city")  #指定已经爬取的预售证信息
# issue_ccode = con.query("select gd_city,issue_date,issue_code from odsdb.ods_city_issue_code where dr = 0 and issue_date != '9999-09-09' and region != '通州区' group by issue_code,issue_date,gd_city")  #指定已经爬取的预售证信息
# issue_ccode = pd.merge(issue_ccode,issue_ccode_maxdate,how='inner',on=['issue_date','gd_city']) #筛选出最大时间的所有预售证
# issue_ccode = issue_ccode.groupby(by=['issue_date','gd_city']).agg({'issue_code':list}).reset_index()

# # ##北京
# # run = bj_spider()
# # run.main()
# # ##三亚
# # result = sanya_spider()
# # result.run()
# ##广州
# gz_urls = Queue()
# gz_oklist=set()
# gz_outfile="./data/guangzhou_new.txt"
# gz_outlistfile="./list/guangzhou.txt"
# gz_okfile="./data/guangzhou.txt"
# gz_run = gz_spider()
# gz_run.main()



#In[]

# run = fs_spider()
# run.main()


#In[]
run = sh_spider()
run.main()

        