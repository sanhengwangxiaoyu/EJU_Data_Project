import scrapy, time, os, traceback, re, requests, json
from scrapy import Selector, Request
from tutorial.items import RoomItem
from queue import Queue
from lxml import etree

class HzRoomsSpider(scrapy.Spider):
    name = 'hz_rooms'
    allowed_domains = ['www.tmsf.com']
    custom_settings = {
        'ITEM_PIPELINES': {'tutorial.pipelines.RoomsPipeline': 300,}
        # 'ITEM_PIPELINES': {'tutorial.pipelines.TutorialPipeline': 300,}
    }
    urls = Queue()
    okurl = set()
    outfile='data/hz_rooms.csv'
    listfile='data/hz_urlsList.txt'
    cookies = open('hz_cookie.txt', 'r').read()

    
    headers = {
            'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER',
            "cookie":cookies
    }
 
#=========================================================

    if os.path.exists(outfile):
        with open(outfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split(',')
                # print(a[1])
                okurl.add(re.sub(r'\n','',a[12]))
    repeatlist=[]
    if  os.path.exists(listfile):
       
        with open(listfile,'r', encoding='utf-8') as f:
            for i in f:
                a = i.strip()
                repeatlist.append(i.strip())
                if len(a)>7 and a in okurl:                    
                    repeatlist.remove(a)
                    continue
                urls.put(a)
    if urls.qsize() != 0:
        url = urls.get()

#=========================================================
    #判断空值方法
    def isEmpty(self,s):
        if s is None:
            return 0
        if len(s) < 1: 
            return 0

    def get_text(self,link):
        text=""
        while True:
            try: #使用try except方法进行各种异常处理
                # header = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER'}
                res = requests.get(link,headers=self.headers,timeout=20,verify=False) #读取网页源码
                #解码
                res.encoding='utf-8'
                text=res.text
                res.close()
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
        return text
        
   # 重载start_requests方法
    def start_requests(self):
        print(self.url ,'    less=',self.urls.qsize())
        print('待爬链接数： ',len(self.repeatlist))
        yield Request(url=self.url, callback=self.parse, headers=self.headers)

    def get_rooms(self,objec,issue_area,sale_state,room_sum,sale_telephone,region):
        room_item = RoomItem()
        chuild_url2 = 'http://www.tmsf.com' + objec
        data = re.sub(r'\t|\n|\r','',self.get_text(chuild_url2))
        obj = re.compile(r"<div class=\"house_children_name\">(?P<room_name>.*?)<div class=\"house_children_name_tips\">.*"
                        "单价.*house_children_information_pirce\">(?P<sale_price>\d+)<.*元/㎡.*"
                        "建筑面积.*<div class=\"house_children_information_right\">(?P<area>.*?)<.*"
                        "套内面积.*<div class=\"house_children_information_right\">(?P<sale_area>.*?)<.*"
                        "当前状态.*<div class=\"house_children_information_right\">(?P<sale_state>.*?)<.*"
                        "户型.*<div class=\"house_children_information_right\">(?P<sale_layout>.*?)<.*")
        ret = obj.finditer(data)
        time.sleep(10)
        for i in ret:
            room_item['city_name'] = '杭州市'
            room_item['url'] = self.url
            room_item['issue_area'] = issue_area
            room_item['sale_state'] = sale_state
            room_item['room_sum'] = room_sum
            room_item['area'] = re.sub(r' |㎡','',i.group("area"))
            room_item['sumulation_price'] = '-' if re.sub(r' ','',i.group("sale_price")) == '0' else re.sub(r' ','',i.group("sale_price"))
            room_item['sale_telephone'] = '-' if self.isEmpty(sale_telephone) == 0 else sale_telephone
            room_item['room_code'] = re.sub(r'&middot;','·',re.sub(r' ','',i.group("room_name")))
            room_item['room_sale_area'] = re.sub(r'  |㎡','',i.group("sale_area"))
            room_item['room_sale_state'] = re.sub(r' |\u3000','',i.group("sale_state"))
            room_item['region'] = region
            room_item['layout'] = re.sub(r' ','',i.group("sale_layout"))
            return room_item


    def parse(self, response):
        sel = Selector(response) 
        issue_args = re.findall(r'\d{1,30}',self.url)
        child_url = 'http://www.tmsf.com/newhouse/NewPropertyHz_createPresellInfo.jspx?sid='+str(issue_args[0])+'&presellid='+str(issue_args[2])+'&propertyid='+str(issue_args[1])+'&_='+str(int(time.time()))
        json_data = json.loads(self.get_text(child_url))
        issue_area = json_data['pre']['area']
        sale_state = json_data['property']['propertystatename']
        room_sum = json_data['pre']['avanum']
        sale_telephone = json_data['property']['selltel']
        region = json_data['pre']['districtname']
        max_page = int(re.findall(r'\d+/\d+',sel.xpath('//div[9]/div/div/div[3]/span/text()').get())[0].split('/')[1].strip())
        for i in range(1,max_page+1):
            if i == 1:
                for tr in sel.xpath('//table/tbody/tr'):
                    yield self.get_rooms(str(tr.xpath('./td[1]/a/@href').get()),issue_area,sale_state,room_sum,sale_telephone,region)
            else :
                nextpage_url = 'http://www.tmsf.com/newhouse/property_'+str(issue_args[0])+'_'+str(issue_args[1])+'_price.htm?isopen=&presellid='+str(issue_args[2])+'&buildingid=&area=&allprice=&housestate=&housetype=&page='+str(i)
                nextpage_htmldata = etree.HTML(self.get_text(nextpage_url))
                for tr in nextpage_htmldata.xpath('//table/tbody/tr'):
                    yield self.get_rooms(str(tr.xpath('./td[1]/a/@href')[0]),issue_area,sale_state,room_sum,sale_telephone,region)

        if self.urls.qsize() != 0:
            try:
                self.url=self.urls.get()
                print(self.url ,'    less=',self.urls.qsize())
                yield Request(url=self.url, callback=self.parse, headers=self.headers)
            except  Exception as e:
                print(traceback.format_exc())

# if __name__ == '__main__':
#     execute(['scrapy', 'crawl', 'httpbin'])


