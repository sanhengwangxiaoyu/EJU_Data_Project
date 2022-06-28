from cv2 import pencilSketch
import scrapy, time, re
from scrapy import Selector, FormRequest
from tutorial.items import IssueItem


class HzSpider(scrapy.Spider):
    name = 'hz'
    start_urls = ['http://www.tmsf.com/newhouse/OpenReport_shownew.htm']
    custom_settings = {
        'ITEM_PIPELINES': {'tutorial.pipelines.RoomsPipeline': 300,}
        # 'ITEM_PIPELINES': {'tutorial.pipelines.TutorialPipeline': 300,}
    }
    page = 1
    end_tag = False
    listfile='data/hz_urlsList.txt'
    cookies = open('hz_cookie.txt', 'r').read()
    # print(cookies)

#==============
    end_date = time.strftime("%Y-%m-%d", time.localtime()) 
    arg_adte = '2022-06-26'
    if len(arg_adte) != 0:
        end_date = arg_adte
#==============

    #判断空值方法
    def isEmpty(self,s):
        if s is None:
            return 0
        if len(s) < 1: 
            return 0


    # 重载start_requests方法
    def start_requests(self):
        headers = {
            'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER',
            "cookie":self.cookies
        }
        dat = {
            'searchtype': '1',
            'page': str(self.page),
        }
        print('正在爬取第 '+str(self.page)+' 页')
        yield FormRequest(url=self.start_urls[0], callback=self.parse, headers=headers, formdata = dat)
    # 爬取内容的主体方法
    def parse(self, response):
        sel = Selector(response) 
        issue_item = IssueItem()
        i = 2
        for div in sel.xpath('//dd/div[1]/div'):
            if str(div.xpath('./div[2]/table/tr[3]/td/ul/li[1]/text()').get()) < self.end_date:
                self.end_tag=True
                continue
            if self.isEmpty(div.xpath('./div[2]/table/tr[1]/td/text()').get()) != 0: 
                issue_item['city_name'] = '杭州市'
                issue_item['open_date'] = div.xpath('./div[2]/table/tr[3]/td/ul/li[1]/text()').get()
                issue_item['building_code'] = re.sub('预售幢号：','',str(div.xpath('./div[2]/table/tr[4]/td/ul/li[1]/font/text()').get()))
                issue_item['address'] = div.xpath('./div[2]/table/tr[5]/td/text()').get()
                issue_item['sale_address'] = div.xpath('./div[2]/table/tr[6]/td/text()').get()
                issue_xpath = '//dd/div[2]/table/tr['+str(i)+']'
                issue_item['newest_name'] = sel.xpath(issue_xpath+'/td[1]/text()').get()
                issue_item['issue_code'] = sel.xpath(issue_xpath+'/td[2]/text()').get()
                issue_item['issue_date'] = sel.xpath(issue_xpath+'/td[3]/text()').get()
                issue_item['developers_name'] = sel.xpath(issue_xpath+'/td[4]/div/text()').get()
                issue_item['property_type'] = sel.xpath(issue_xpath+'/td[5]/text()').get()
                issue_item['rooms_url'] = 'http://www.tmsf.com' + str(sel.xpath(issue_xpath+'/td[6]/a/@href').get())
                if self.isEmpty(sel.xpath(issue_xpath+'/td[6]/a/@href').get()) != 0  :
                    of = open(self.listfile,'a',encoding='utf-8')
                    of.write('http://www.tmsf.com' + str(sel.xpath(issue_xpath+'/td[6]/a/@href').get()) + '\n')
                    of.flush()
                    i+=1
                    yield issue_item
        time.sleep(2)
        # 爬取下一页数据
        self.page = self.page + 1
        all_page = sel.xpath('//dl/dd/div[1]/div[12]/a/text()').extract()
        if str(self.page) in all_page and self.end_tag == False:
            headers = {
                ####谷歌
                # 'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER',
                # "cookie":"SECKEY_ABVK=W17e7gimMSmNNOk75+lqL0zb5e8jb3JfP/kiNDbB2Zk%3D; BMAP_SECKEY=W17e7gimMSmNNOk75-lqLzTK0X9t9ZorOiZmjjLzseo_olh7kU2Vcw--wiWDN4wYFgWXICk624gQumEuK-AyBPdqbQm4Ghqo_5OAACcHNqRIZVzVDYNNuQQAK3EjN04U5kACqKxwfN565yU9Jbf2BrSG5CbMun6HBqXShOpHdT6Ky1k4-EBkCCo8GUMoawuv; UM_distinctid=17f539bd91328e-00c45aa16759e4-a3e3164-146d15-17f539bd9142f1; JSESSIONID=44B980EF082EC5AB9E55796F71B1C7E0; Hm_lvt_e2f064987fcc6204fb690aa68ec76ac3=1655262816; Hm_lpvt_e2f064987fcc6204fb690aa68ec76ac3=1655262816; BSFIT_EXPIRATION=1655335904817; BSFIT_DEVICEID=ZjJ5l6p1Kjo99-N9MjVr2jBTbKMsYox-VbamDTqnViFNUKWegqVjZwsCMNL6A-izdBEPhLCsq_TyMagCkauqLBm1HtYQ6sRZ0TUWhUjfg8IOqoVhTkMiYhZSZjiILx7nCwpiOl06Y6ytLJK7KQj5vbwgxpkMawNV; Hm_lvt_bbb8b9db5fbc7576fd868d7931c80ee1=1655262835; gr_user_id=32733172-d624-4812-9eab-0abd5c7b85d3; b61f24991053b634_gr_session_id=5c109ecf-b033-4b63-8f83-37a00fa7ef14; b61f24991053b634_gr_session_id_5c109ecf-b033-4b63-8f83-37a00fa7ef14=true; new_index_guide=0; Hm_lpvt_bbb8b9db5fbc7576fd868d7931c80ee1=1655263125; BSFIT_yupC6="
                ####edge
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.124 Safari/537.36 Edg/102.0.1245.44',
                'cookie':'gr_user_id=8b2b86e8-b91d-47e6-b809-880d6e373158; Hm_lvt_bbb8b9db5fbc7576fd868d7931c80ee1=1655710568,1656035445; BSFIT_EXPIRATION=1656115771509; BSFIT_DEVICEID=DL8y3AmQYm9fMGzsPkTvuKLiGCfBay3oKN1Q3i3FfqrOLIsEqaaQuf2J3JGwml9QIO7PKHDROlVub_pf8QW4owIyJpZv1T4UIztNGkL_2qJI-VGXxOU-vFPOBaz4kRKYtfLHnzsQaeMMoxn3FVuvfCnjitkcF-Um; JSESSIONID=F89B1AFFA7C3C1651A40D4723E9C70E7; b61f24991053b634_gr_session_id=bfa18ec7-51c7-4962-aca3-8fa3af3e4da7; b61f24991053b634_gr_session_id_bfa18ec7-51c7-4962-aca3-8fa3af3e4da7=true; Hm_lpvt_bbb8b9db5fbc7576fd868d7931c80ee1=1656040343; BSFIT_ylApw=ozuL/cy0opo5/pmGog,opyLopjJoTyL/y,opyP/cqJocjC/y,opyT9zoJocj5oy,opy5/ckJockC9v,opyT/TqJocjT/J,opwJ/zvJocj2oy'
            }
            dat = {
                'searchtype': '1',
                'page': str(self.page),
            }
            print('正在爬取第 '+str(self.page)+' 页')
            yield scrapy.FormRequest(url=self.start_urls[0], formdata=dat, headers=headers, callback=self.parse)    







