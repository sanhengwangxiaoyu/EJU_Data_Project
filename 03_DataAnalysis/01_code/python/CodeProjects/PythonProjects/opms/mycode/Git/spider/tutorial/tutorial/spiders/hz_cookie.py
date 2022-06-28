import scrapy, re, pytesseract, time
from selenium import webdriver
from scrapy import Request
from PIL import Image


class HzCookieSpider(scrapy.Spider):
    name = 'hz_cookie'
    allowed_domains = ['www.tmsf.com']
    start_urls = ['http://www.tmsf.com/']
    driver = webdriver.Chrome()
    driver.maximize_window()
    driver.get('http://www.tmsf.com/mem/main.htm')
    driver.implicitly_wait(10)
    driver.find_element_by_xpath('//*[@id="memForm"]')
    # driver.find_element_by_css_selector("p#login_checkimg").click()
    driver.save_screenshot('code.png')
    element = driver.find_element_by_xpath('//*[@id="codeimg"]')
    captchapicfile = "aucthcode.png"
    left = element.location['x'] +670
    top = element.location['y'] +260
    right = left + element.size['width'] + int(80)
    bottom = top + element.size['height'] + int(35)
    img = Image.open('code.png')
    imgcod = img.crop((left,top,right,bottom))          # 根据 div的长宽截图
    imgcod.save('aucthcode.png')
    file = Image.open(captchapicfile)
    testdata_dir_config = '--tessdata-dir "D:\\EJU\\after_20210520\\10_Tools\\tesseract"'
    text = pytesseract.image_to_string(file,config=testdata_dir_config,lang="eng")
    xieyi=driver.find_element_by_xpath('//*[@id="yesbox"]').click() #协议
    time.sleep(2)
    zhanghao=driver.find_element_by_xpath('//*[@id="username"]').send_keys('15064847527')  #输入账号
    dianjimima=driver.find_element_by_xpath('//*[@id="showuserpwd"]').click() #点击密码
    time.sleep(3)
    mima=driver.find_element_by_xpath('//*[@id="userpwd"]').send_keys('413727') #密码
    time.sleep(2)
    dianjiyazhengma=driver.find_element_by_xpath('//*[@id="imagecode"]').click() #点击验证码
    time.sleep(2)
    yanzhengma=driver.find_element_by_xpath('//*[@id="imagecode"]').send_keys(str(re.findall('\d+',text)[0])) #验证码
    time.sleep(2)
    denglvs= driver.find_element_by_xpath('//*[@id="memForm"]/div[2]').click()  #点击登录按钮
    time.sleep(3)
    driver.implicitly_wait(10)
    driver.refresh()   #刷新网页
    time.sleep(1)
    driver.get('http://www.tmsf.com/')
    driver.implicitly_wait(10)
    time.sleep(5)
    driver.get('http://www.tmsf.com/newhouse/NewProperty_searchallUpgrade.jspx')
    cookies=driver.get_cookies()  #获取cookie信息
    cookiestr = ""
    for cookie in cookies:
        if len(cookiestr) != 0:
            cookiestr = cookiestr + ";" +"%s=%s" % (cookie['name'], cookie['value'])
        else :
            cookiestr = cookiestr +"%s=%s" % (cookie['name'], cookie['value'])
    time.sleep(2)
    driver.quit()
    print(cookiestr)
    of = open('hz_cookie.txt','w',encoding='utf-8')
    of.write(cookiestr)
    of.flush()
    of.close()
 

