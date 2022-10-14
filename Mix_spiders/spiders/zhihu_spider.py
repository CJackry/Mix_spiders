# -*- coding: utf-8 -*-
from urllib import request
import requests
from io import BytesIO
from urllib import parse
import scrapy
import json
import base64
from PIL import Image
from Mix_spiders.utils.common import YDM_parse
from Mix_spiders.utils.common import getformdata_zhihu
from Mix_spiders.items import ZhihuAnswerItem
from Mix_spiders.items import ZhihuQuestionItem
import datetime
from scrapy.loader import ItemLoader
import re
from scrapy.http.cookies import CookieJar
from scrapy_redis.spiders import RedisSpider
from scrapy.http import Request


class ZhihuSpiderSpider(RedisSpider):
    name = 'zhihu_spider'
    allowed_domains = ['www.zhihu.com']
    redis_key = 'zhihu:start_urls'
    start_urls = ['https://www.zhihu.com/']
    login_url = "https://www.zhihu.com/api/v3/oauth/sign_in"
    captcha_url_en = "https://www.zhihu.com/api/v3/oauth/captcha?lang=en"
    cookie_jar = CookieJar()

    start_answer_url = "https://www.zhihu.com/api/v4/questions/{0}/answers?include=data%5B*%5D.is_normal%2Cadmin_closed_comment%2Creward_info%2Cis_collapsed%2Cannotation_action%2Cannotation_detail%2Ccollapse_reason%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Ccreated_time%2Cupdated_time%2Creview_info%2Crelevant_info%2Cquestion%2Cexcerpt%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cis_labeled%2Cis_recognized%2Cpaid_info%2Cpaid_info_content%3Bdata%5B*%5D.mark_infos%5B*%5D.url%3Bdata%5B*%5D.author.follower_count%2Cbadge%5B*%5D.topics&offset={1}&limit={2}&sort_by=default&platform=desktop"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/80.0.3987.122 Safari/537.36",
        "content-type": "application/x-www-form-urlencoded",
        "X-Zse-83": "3_2.0"
    }

    def start_requests(self):
        # 重写start_requests这个方法，不去遍历start_urls这个列表了，而是先去请求验证码。
        # 起始请求是向captcha_url发送get请求，先知道是否有验证码。
        with open('zhihu_cookie.txt') as f:
            cookies = f.read()
        if cookies:
            p = re.compile(r'<Cookie (.*?) for .*?>')
            cookies = re.findall(p, cookies)
            cookies = (cookie.split('=', 1) for cookie in cookies)
            cookies = dict(cookies)
            print('cookies: ')
            print(cookies)
            yield Request(url=self.start_urls[0],
                          cookies=cookies,
                          callback=self.parse,
                          headers=self.headers
                          )
        else:
            yield Request(url=self.captcha_url_en,
                          callback=self.parse_get_captcha,
                          headers=self.headers,
                          method='GET',
                          )
            return self.next_requests()

    def parse_get_captcha(self, response):
        """
        解析 验证码的get请求，获取show_captcha
        :return:
        """
        print(response.text)
        is_captcha = json.loads(response.text).get("show_captcha")
        # Cookie1 = response.headers.getlist('Set-Cookie')
        # print("Cookies: " + Cookie1)
        if is_captcha:
            print("有验证码")
            yield scrapy.Request(url=self.captcha_url_en, method='PUT',
                                 callback=self.parse_image_url,
                                 headers=self.headers)

    def parse_image_url(self, response):
        """
        解析验证码put请求，获取图片的加密地址
        :param response:
        :return:
        """
        img_url = json.loads(response.text).get("img_base64")
        # 对加密图片进行解密，获取原始地址
        img_data = base64.b64decode(img_url)
        # 根据得到的Bytes-like对象，创建一个字节码对象（bytes对象）
        img_real_url = BytesIO(img_data)
        # 利用Image请求该图片，获得图片对象
        img = Image.open(img_real_url)
        img.save("Mix_spiders/utils/captcha.png")
        # captcha_result = str(YDM_parse())
        captcha_result = input('键入验证码： ')
        yield scrapy.FormRequest(
            url=self.captcha_url_en,
            callback=self.parse_post_captcha,
            formdata={
                'input_text': str(captcha_result)
            },
        )

    def parse_post_captcha(self, response):
        """
        解析验证码post请求，获取验证码的识别结果，输入的验证码正确与否
        :param response:
        :return:
        """
        result = json.loads(response.text).get("success", '')
        if result:
            print('验证码正确')
            # 访问此sign_in的URL进行登录
            post_data = getformdata_zhihu()
            # post_data = json.dumps(post_data).encode('utf-8')
            yield scrapy.FormRequest(
                url=self.login_url,
                body=post_data,
                callback=self.check_login,
                headers=self.headers,
                method='POST',
            )

    def check_login(self, response):
        # 验证是否登录成功
        text_json = json.loads(response.text)
        print(text_json)
        if text_json['cookie']:
            cookie_jar = CookieJar()
            cookie_jar.extract_cookies(response, response.request)
            with open('zhihu_cookie.txt', 'w') as f:
                for cookie in cookie_jar:
                    f.write(str(cookie) + '\n')
            yield scrapy.Request(url=self.start_urls[0], headers=self.headers, callback=self.parse)

    def parse(self, response):
        """
        提取出html页面中的所有url 并跟踪这些url进行一步爬取
        如果提取的url中格式为 /question/xxx 就下载之后直接进入解析函数
        """
        all_urls = response.css("a::attr(href)").extract()
        all_urls = [parse.urljoin(response.url, url) for url in all_urls]
        all_urls = filter(lambda x: True if x.startswith("https") else False, all_urls)
        for url in all_urls:
            match_obj = re.match(r"(.*zhihu.com/question/(\d+))(/|$).*", url)
            if match_obj:
                # 如果提取到question相关的页面则下载后交由提取函数进行提取
                request_url = match_obj.group(1)
                yield scrapy.Request(request_url, headers=self.headers, callback=self.parse_question, errback=self.start_requests)
                question_id = int(match_obj.group(2))
                print('请求问题答案json')
                yield scrapy.Request(url=self.start_answer_url.format(question_id, 0, 20),
                                     callback=self.parse_answer,
                                     headers=self.headers,
                                     dont_filter=True,
                                     priority=1)
            else:
                # 如果不是question页面则直接进一步跟踪
                yield scrapy.Request(url, headers=self.headers, callback=self.parse)
                pass

    def parse_question(self, response):
        # 处理question页面， 从页面中提取出具体的question item
        print(response.url)
        if "QuestionHeader-title" in response.text:
            # 处理新版本
            match_obj = re.match(r"(.*zhihu.com/question/(\d+))(/|$).*", response.url)
            question_id = int(match_obj.group(2))

            itemloader = ItemLoader(item=ZhihuQuestionItem(), response=response)
            itemloader.add_value('zhihu_id', question_id)
            itemloader.add_css('title', 'h1.QuestionHeader-title::text')
            itemloader.add_css('topics', 'a.TopicLink div.Popover div::text')
            itemloader.add_value('url', response.url)
            itemloader.add_css('content', 'div.QuestionHeader-detail span.RichText.ztext::text')
            itemloader.add_css('answer_nums', 'h4.List-headerText span::text')
            itemloader.add_css('comment_nums', '.QuestionHeader-Comment button::text')
            itemloader.add_css('watch_user_nums', '.QuestionFollowStatus div.NumberBoard-item strong::attr(title)')
            itemloader.add_css('click_nums', '.QuestionFollowStatus div.NumberBoard-item strong::attr(title)')
            itemloader.add_value('crawl_time', datetime.datetime.now())

            question_item = itemloader.load_item()

            if 'content' not in question_item:
                question_item['content'] = 'NULL'

            yield question_item

            # print('请求问题答案json')
            # yield scrapy.Request(url=self.start_answer_url.format(question_id, 0, 20),
            #                      callback=self.parse_answer,
            #                      headers=self.headers,
            #                      dont_filter=True)

            pass

    def parse_answer(self, response):
        # 处理question的answer
        answer_json = json.loads(response.text)
        is_end = answer_json['paging']['is_end']
        next_url = answer_json['paging']['next']
        print('问题答案json请求成功')

        # 提取信息
        for answer in answer_json['data']:
            answer_item = ZhihuAnswerItem()
            answer_item['zhihu_id'] = answer['id']
            answer_item['question_id'] = answer['question']['id']
            answer_item['question_title'] = answer['question']['title']

            answer_url = 'https://www.zhihu.com/question/' + str(answer_item['question_id']) + '/answer/' \
                         + str(answer_item['zhihu_id'])
            answer_item['url'] = answer_url

            answer_item['author_id'] = answer['author']['id'] if 'id' in answer['author'] else None
            answer_item['content'] = answer['excerpt']
            answer_item['praise_nums'] = answer['voteup_count']
            answer_item['comment_nums'] = answer['comment_count']
            answer_item['create_time'] = answer['created_time']
            answer_item['update_time'] = answer['updated_time']
            answer_item['crawl_time'] = datetime.datetime.now()
            answer_item['author_name'] = answer['author']['name'] if 'name' in answer['author'] else 'null'

            yield answer_item

        if not is_end:
            yield scrapy.Request(next_url, callback=self.parse_answer, headers=self.headers)
