# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from turtle import title
import scrapy

class MovieItem(scrapy.Item):
    title = scrapy.Field()

class IssueItem(scrapy.Item):
    city_name = scrapy.Field()
    newest_name = scrapy.Field()
    issue_code = scrapy.Field()
    issue_date = scrapy.Field()
    developers_name = scrapy.Field()
    property_type = scrapy.Field()
    rooms_url = scrapy.Field()
    open_date = scrapy.Field()
    building_code = scrapy.Field()
    address = scrapy.Field()
    sale_address = scrapy.Field()


class RoomItem(scrapy.Item):
    city_name = scrapy.Field()
    url = scrapy.Field()
    issue_area = scrapy.Field()   ##预售证面积
    sale_state = scrapy.Field()
    room_sum = scrapy.Field()
    area = scrapy.Field()    ##建筑面积
    sumulation_price = scrapy.Field()
    sale_telephone = scrapy.Field()
    room_code = scrapy.Field()
    room_sale_area = scrapy.Field()  ##房间面积
    room_sale_state = scrapy.Field()
    region = scrapy.Field()
    layout = scrapy.Field()

