# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

import pymysql

class RoomsPipeline:
    def __init__(self):
        self.conn = pymysql.connect(host='172.28.36.77', user='wanganming', password='NDR_AhfzXT3MSxfh', database='dwd_db',charset='utf8')
        self.cur = self.conn.cursor()
    
    def process_item(self, item, spider):
        for j in range(0, len(item["city_name"])):
            city_name = item["city_name"]
            url = item["url"]
            issue_area = item["issue_area"]
            sale_state = item["sale_state"]
            room_sum = item["room_sum"]
            area = item["area"]
            sumulation_price = item["sumulation_price"]
            sale_telephone = item["sale_telephone"]
            room_code = item["room_code"]
            room_sale_area = item["room_sale_area"]
            room_sale_state = item["room_sale_state"]
            region = item["region"]
            layout = item["layout"]
            sql = "insert into dwd_spider_newest_room(city_name,url,issue_area,sale_state,room_sum,area,sumulation_price,sale_telephone,room_code,room_sale_area,room_sale_state,region,layout) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            self.cur.execute(sql, (city_name,url,issue_area,sale_state,room_sum,area,sumulation_price,sale_telephone,room_code,room_sale_area,room_sale_state,region,layout))
            self.conn.commit()
            return item
            
    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()


class IssuePipeline:
    def __init__(self):
        self.conn = pymysql.connect(host='172.28.36.77', user='wanganming', password='NDR_AhfzXT3MSxfh', database='dwd_db',charset='utf8')
        self.cur = self.conn.cursor()
    
    def process_item(self, item, spider):
        for j in range(0, len(item["city_name"])):
            city_name = item["city_name"]
            newest_name = item["newest_name"]
            issue_code = item["issue_code"]
            issue_date = item["issue_date"]
            developers_name = item["developers_name"]
            property_type = item["property_type"]
            rooms_url = item["rooms_url"]
            open_date = item["open_date"]
            building_code = item["building_code"]
            address = item["address"]
            sale_address = item["sale_address"]
            sql = "insert into dwd_spider_newest_issue(city_name,newest_name,issue_code,issue_date,developers_name,property_type,rooms_url,open_date,building_code,address,sale_address) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            self.cur.execute(sql, (city_name,newest_name,issue_code,issue_date,developers_name,property_type,rooms_url,open_date,building_code,address,sale_address))
            self.conn.commit()
            return item
            
    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()


class TutorialPipeline:
    pass