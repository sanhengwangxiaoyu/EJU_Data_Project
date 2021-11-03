import requests,json,time,pandas as pd,datetime

print("开始时间 >>>>>>>>  :" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) +'\n\n\n' +'===============================================================================')
# 设置参数: 评论类型，最大页数，排序类型，产品id
page = '0'
scoreType = '0'   #  score=0  全部评论    score=1  差评   score=2  中评    "score=3  好评    score=4  未知    score=5  追评
soType = '6'      #  sortType=6  时间排序     sortType=5 默认排序
productId='100004474324'   #  100004474324   美的（Midea）喆物典雅金麦饭石色涂层不粘炒锅家用电磁炉煤气灶适用炒菜锅平底煎锅送竹铲 香槟金28WOK302
headers = {
  'Cookie':'__jdu=1607998142947127793161;'
          'shshshfpa=1b3f61eb-e9e8-b13d-31e8-fcabe5f951e0-1607998143;' 
          'shshshfpb=bz3qqahewxupE5Oj3yWK54w%3D%3D;' 
          'unpl=V2_ZzNtbUBVEBYiXRFQK01bV2JUGl5KXhdCfQATAHlMDwYzUxENclRCFnUURlVnGlUUZgQZXkZcQhdFCEdk'
          'eBBVAWMDE1VGZxBFLV0CFSNGF1wjU00zQwBBQHcJFF0uSgwDYgcaDhFTQEJ2XBVQL0oMDDdRFAhyZ0AVRQhHZHse'
          'XQNvCxtdR1RLEHMIRVJ9EV4FYAMibUVncyV2DkdReBxsBFcCIh8WC0URcg1EGXseXQNvCxtdR1RLEHMIRVJ9EV4F'
          'YAMiXHJU; __jdv=76161171|baidu-pinzhuan|t_288551095_baidupinzhuan|cpc|'
          '0f3d30c8dba7459bb52f2eb5eba8ac7d_0_33b2fdf5ae6c4f9298ef99de3db2ea2a|1635373504753; '
          'jwotest_product=99; areaId=2; ipLoc-djd=2-2826-51941-0; __jda=122270672.'
          '1607998142947127793161.1607998143.1635373505.1635661226.18; '
          'shshshfp=e5603078c79e9fa0edb05b6b13b9ac0f; '
          '3AB9D23F7A4B3C9B=A2XNTD4HHTLHNJSEF22NEFO3ZZ3CEYOCYYZ3LN3L5KCA2AXUTNV7X6QKIWDP4MVL7BXRN6L'
          'IIM6EINKGZIE44AFABA; JSESSIONID=194037639E91DCDA02FC98E5C19932F2.s1',
  'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
          '(KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36',
}

# 请求数据
url = "https://club.jd.com/comment/productPageComments.action?callback=fetchJSON_comment98&productId="+productId+"&score="+scoreType+"&sortType="+soType+"&page="+page+"&pageSize=10&isShadowSku=0&fold=1"
r = requests.get(url, headers = headers)
# r.text
# 替换数据中的不必要部分
json_data = (json.loads(r.text.replace(');','').replace('fetchJSON_comment98(','')))
# 获取最大页数
page = json_data['maxPage']+1

#######下边是所有的优值的字段，但是不一定用到，我当时也是浪费时间了
productCommentSummary = json_data['productCommentSummary']
skuId = productCommentSummary['skuId']
averageScore = productCommentSummary['averageScore']
commentCountStr = productCommentSummary['commentCountStr']
goodCountStr = productCommentSummary['goodCountStr']
goodRate = productCommentSummary['goodRate']
generalCountStr = productCommentSummary['generalCountStr']
generalRate = productCommentSummary['generalRate']
poorCountStr = productCommentSummary['poorCountStr']
poorRate = productCommentSummary['poorRate']
videoCountStr = productCommentSummary['videoCountStr']
afterCountStr = productCommentSummary['afterCountStr']
showCountStr = productCommentSummary['showCountStr']
poorRateStyle = productCommentSummary['poorRateStyle']
generalRateStyle = productCommentSummary['generalRateStyle']
goodRateStyle = productCommentSummary['goodRateStyle']
score1Count = productCommentSummary['score1Count']
score2Count = productCommentSummary['score2Count']
score3Count = productCommentSummary['score3Count']
score4Count = productCommentSummary['score4Count']
score5Count = productCommentSummary['score5Count']

# # 创建sheet
# wk = openpyxl.Workbook()
# sheet = wk.create_sheet()

print("此次爬取的产品id：" + productId)
print("此次爬取的评论类型： " + scoreType)
print("此次爬取的页数一共有： " + str(json_data['maxPage']) +'页'+'\n'+'===============================================================================')
# 创建dataframe
df = pd.DataFrame(columns=['referenceName','goodRate','generalRate','poorRate','productId','scoreType','score1Count','score2Count','score3Count','score4Count','score5Count','id','guid','content','creatTime','productColor','url'])
# 循环遍历所有评论，并加载到本地文件中
for i in range(page):
# for i in range(1):
  # 请求数据
  url = "https://club.jd.com/comment/productPageComments.action?callback=fetchJSON_comment98&productId="+productId+"&score="+scoreType+"&sortType="+soType+"&page="+str(i)+"&pageSize=10&isShadowSku=0&fold=1"
  r = requests.get(url, headers = headers)
  # 替换数据中的不必要部分i
  json_datas = (json.loads(r.text.replace(');','').replace('fetchJSON_comment98(','')))['comments'] 
  # 解析数据，并落地
  for item in json_datas:
    # print(str(item['creationTime']))
    # print(time.strftime("%Y-%m-%d", time.localtime()))
    if datetime.datetime.strptime(item['creationTime'],"%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d') == time.strftime("%Y-%m-%d", time.localtime()) :
      id = item['id']
      guid = item['guid']
      content = item['content'].replace('\n','')
      creatTime = item['creationTime']
      productColor = item['productColor']
      referenceName = item['referenceName']
      # data =id.astype(str)+"     "+guid.astype(str)
      print(str(id)+'|||^|||'+guid+'|||^|||'+referenceName+'|||^|||'+content+'|||^|||'+creatTime+'|||^|||'+productColor)
      # sheet.append([str(id),guid,content,creatTime,productColor,referenceName])
      # wk.save('C:\\Users\\Damon\\Desktop\\test_2.xlsx')
      new=pd.DataFrame({'referenceName':referenceName,'goodRate':goodRate,'generalRate':generalRate,'poorRate':poorRate,'productId':productId,'scoreType':scoreType,'score1Count':score1Count,'score2Count':score2Count,'score3Count':score3Count,'score4Count':score4Count,'score5Count':score5Count,'id':id,'guid':guid,'content':content,'creatTime':creatTime,'productColor':productColor,'url':url},index=[0])
      df=df.append(new,ignore_index=True)   # ignore_index=True,表示不按原来的索引，从0开始自动递增
    else :
      break
df.to_csv("C:\\Users\\Damon\\Desktop\\tmp\\"+productId+"_"+time.strftime("%Y-%m-%d", time.localtime())+".txt",encoding='utf_8_sig',sep='\t')
