import requests,json

url = 'https://restapi.amap.com/v5/geocode/geo'   # 输入API问号前固定不变的部分
params = { 'key': '61d7e8cf769016c6904b3cea7b719e3d',
           'address': '北京市丰台区盛坊路1号院',
           'city': '北京市' }                # 将两个参数放入字典
res = requests.get(url, params)
jd = json.loads(res.text)      # 将json数据转化为Python字典格式
jd
