import scrapy
from scrapy.http import Request,FormRequest
import urllib.request


class DoubanSpider(scrapy.Spider):
    name = "douban"
    allowed_domains = ["douban.com"]

    UserAgent = {"User-Agent:":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.104 Safari/537.36 Core/1.53.2050.400 QQBrowser/9.5.10169.400"}

    def Login(self, response):
        pass
