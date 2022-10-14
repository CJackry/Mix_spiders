import hashlib
from ctypes import *
from urllib.parse import urlencode

import pytesseract
from PIL import Image
import _json
import execjs
import re
import hashlib
import time
import hmac


def get_md5(url):
    # python3是使用Unicode编码，直接使用update函数会报错，因此要判断编码（以便python2与python3通用），对必要的URL进行编码
    if isinstance(url, str):
        url = url.encode("utf-8")  # python3的str即是Unicode，python3没有Unicode这个关键词
    m = hashlib.md5()
    m.update(url)
    return m.hexdigest()


def _get_logindata():
    """
    通过 Hmac 算法计算返回签名
    实际是几个固定字符串加时间戳
    :param timestamp: 时间戳
    :return: 签名
    """

    login_data = {
        'client_id': 'c3cef7c66a1843f8b3a9e6a1e3160e20',
        'grant_type': 'password',
        'source': 'com.zhihu.web',
        'username': 'cjf1998@foxmail.com',
        'password': 'wasd.123456.cjf',
        'lang': 'en',
        'ref_source': 'other_https://www.zhihu.com/signin?next=%2F',
        'utm_source': '',
        'captcha': 'en',
        'signature': ''
    }
    timestamp = str(int(time.time() * 1000))
    ha = hmac.new(b'd1b964811afb40118a12068ff74a12f4', digestmod=hashlib.sha1)
    grant_type = login_data['grant_type']
    client_id = login_data['client_id']
    source = login_data['source']
    ha.update(bytes((grant_type + client_id + source + str(timestamp)), 'utf-8'))
    login_data.update({
        'signature': ha.hexdigest(),
        'timestamp': timestamp
    })

    return login_data


def getformdata_zhihu():
    # 读取js文件
    with open('Mix_spiders/utils/encrypt.js') as f:
        js = execjs.compile(f.read())
        return js.call('b', urlencode(_get_logindata()))


def YDM_parse():
    # 调用云打码平台API进行图片验证码识别
    Ydmapi = windll.LoadLibrary("Mix_spiders/utils/yundamaAPI-x64.dll")
    appID = 10146
    appKey = b'6c7f7fe40f27328e014b5fb74b1b7426'
    username = b'CJack'
    password = b'chen666666'
    codetype = 1004
    result = c_char_p(b"                              ")
    timeout = 60
    filename = b'captcha.png'
    captchaId = Ydmapi.YDM_EasyDecodeByPath(username, password, appID, appKey, filename, codetype, timeout, result)
    print("一键识别：验证码ID：%d，识别结果：%s" % (captchaId, str(result.value)))
    return str(result.value)


def pytesseract_parse(img):
    # 采用pytesseract图片识别（需自学习较为麻烦便舍弃）
    result = pytesseract.image_to_string(img)
    print("未处理：", result)
    # 图片灰度处理
    img = img.convert('L')

    # 二值化处理
    threshold = 128
    t_list = []
    for i in range(256):
        if i < threshold:
            t_list.append(0)
        else:
            t_list.append(1)
    img = img.point(t_list, '1')
    img.save("captcha_after.png")
    result = pytesseract.image_to_string(img)
    print("处理后：", result)


def extract_num(text):
    # 从字符串中提取出数字
    match_re = re.match(r".*?(\d+).*", text)
    if match_re:
        nums = int(match_re.group(1))
    else:
        nums = 0

    return nums
