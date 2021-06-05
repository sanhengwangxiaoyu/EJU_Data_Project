import pymysql
 
mydb = pymysql.connect(
  host="172.28.36.77",       # 数据库主机地址
  user="mysql",    # 数据库用户名
  passwd="egSQ7HhxajHZjvdX",   # 数据库密码
  database="temp_db"       #数据库名字
)
 
#print(mydb)
mycursor = mydb.cursor()
for x in mycursor: print(x)

mycursor.execute("sql")
#for x in mycursor: print(x)