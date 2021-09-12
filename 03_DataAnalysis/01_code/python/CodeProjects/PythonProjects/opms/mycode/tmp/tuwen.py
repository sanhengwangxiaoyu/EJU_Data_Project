import time
import pandas as pd
import json
import random
from aip import AipOcr
import os

# 连接api
APP_ID = "22000766"
API_KEY = "jySdOj1iQyAUGi0ZFiGqSM39"
SECRET_KEY = "UfF0QnNdi5WzWl9tF5V3WThI3gtA6hfi"
client = AipOcr(APP_ID, API_KEY, SECRET_KEY)

# 返回值设置
options = {}
options["language_type"] = "CHN_ENG"
options["detect_direction"] = "False"
options["detect_language"] = "False"
options["probability"] = "False"

# 读取图片函数
def get_file_content(filePath):
    with open(filePath, 'rb') as fp:
        return fp.read()


# 读取评论信息下面所有文件夹的名称
# mer_folds = os.listdir(r'C://Users//86133//Desktop//城市截图')
# print('文件夹下面共有 %d 个图片' % len(mer_folds))
# mer_folds[:5]

# get_file_content('./GDP/test1.jpg')

# from PIL import Image
# im = Image.open('GDP/test3.jpg')
# im.show() # show完之后会新打开一个窗口 显示图片的内容

image = get_file_content(r'C://Users//86133//Desktop//上海2021Q2.png')
tag1 = client.basicGeneral(image, options)
tag1

