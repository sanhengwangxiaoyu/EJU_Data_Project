# -*- codeing = utf-8 -*-
# @Time : 2021/5/31 9:33
# @Author :SunHZ
# @File : GetName.py
# @Software: PyCharm


into = "INSERT INTO temp_db.tmp_city_newest_deal_ls(floor_name,clean_floor_name,newest_name,floor_name_sub,gd_city) VALUES (%s,%s,%s,%s,%s)"

def getFloorNameNew(floor_name,clean_floor_name,newest_name):
    newest_name_sql = ("select a.newest_name,b.city_name from (select distinct city_id,newest_name from dwb_db.dwb_newest_info where newest_name LIKE '%" + newest_name[0:4] + "%') a left join dws_db.dim_geography b on b.city_id=a.city_id and b.grade=3 ")
    cursor.execute(newest_name_sql)
    newest_name_list = cursor.fetchall()
    for floor_name_new in newest_name_list:
        print(floor_name_new[0])
        values = (str(floor_name),str(clean_floor_name),str(floor_name_new[0]), str(newest_name[0:4]), str(floor_name_new[1]))
        cursor.execute(into, values)
        db.commit()


#打开数据库连接
import pymysql

db = pymysql.connect(host="172.28.36.77",user="mysql",password="egSQ7HhxajHZjvdX",database="temp_db",charset='utf8')
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()
# 使用 execute()  方法执行 SQL 查询
sql = ("select a.*,b.* from (select distinct floor_name,clean_floor_name,gd_city from temp_db.tmp_city_newest_deal) a left join (select distinct a0.newest_name,a1.city_name  from dwb_db.dwb_newest_info a0 left join dws_db.dim_geography a1 on a1.city_id=a0.city_id and a1.grade=3) b on a.clean_floor_name=b.newest_name and a.gd_city=b.city_name having newest_name is null ")
cursor.execute(sql)
# 使用 fetchall() 方法获取多条数据.
data = cursor.fetchall()
for floor_name in data:
    print(floor_name[0])
    if floor_name[0] != "":
        if "." in floor_name[0]:
            newest_name_list = floor_name[0].split(".")
            if newest_name_list[1] == "":
                newest_name = newest_name_list[0]
                getFloorNameNew(floor_name[0],floor_name[1],newest_name)
            else:
                newest_name = newest_name_list[1]
                getFloorNameNew(floor_name[0],floor_name[1],newest_name)
        else:
            getFloorNameNew(floor_name[0],floor_name[1],floor_name[0])
db.close()

