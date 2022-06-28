import scrapy


class TestSpider(scrapy.Spider):
    name = 'test'
    allowed_domains = ['www.tmsf.com']
    start_urls = ['http://www.tmsf.com/']

    def parse(self, response):
        pass
