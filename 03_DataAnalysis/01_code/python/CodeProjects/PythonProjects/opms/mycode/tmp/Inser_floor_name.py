# -*- codeing = utf-8 -*-
# @Time : 2021/5/27 15:32
# @Author :SunHZ
# @File : Inser_floor_name.py
# @Software: PyCharm


#打开数据库连接
import pymysql


into = "INSERT INTO temp_db.tmp_city_newest_deal_ls3(floor_name,clean_floor_name,gd_city,newest_name) VALUES (%s,%s,%s,%s)"

db = pymysql.connect(host="172.28.36.77",user="mysql",password="egSQ7HhxajHZjvdX",database="temp_db",charset='utf8')
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()
# 使用 execute()  方法执行 SQL 查询
sql = ("select a.floor_name,a.clean_floor_name,a.gd_city,case when b.newest_name = '' or b.newest_name is null then a.clean_floor_name else b.newest_name end newest_name from (select distinct floor_name,case when clean_floor_name is null or clean_floor_name = '' then floor_name else clean_floor_name end clean_floor_name,gd_city from temp_db.tmp_city_newest_deal) a left join (select distinct clean_floor_name,floor_name_sub,newest_name,gd_city from temp_db.tmp_city_newest_deal_ls ) b on a.clean_floor_name=b.clean_floor_name and a.gd_city=b.gd_city")
cursor.execute(sql)
# 使用 fetchall() 方法获取多条数据.
data = cursor.fetchall()
i = 0
for floor_name in data:
    print(floor_name)
    values = (str(floor_name[0]), str(floor_name[1]), str(floor_name[2]), str(floor_name[3]))
    cursor.execute(into, values)
    db.commit()
    print(i)
    i = i + 1
# 关闭数据库连接
db.close()