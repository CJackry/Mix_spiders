# -*- coding: utf-8 -*-
import scrapy
import re
from urllib import parse
from Mix_spiders.items import CSDNItem
from Mix_spiders.utils.common import get_md5
from scrapy.loader import ItemLoader
import datetime
from scrapy_redis.spiders import RedisSpider


class CsdnDfsSpider(scrapy.Spider):
    name = 'csdn_DFS'
    allowed_domains = ['blog.csdn.net']
    # redis_key = 'csdndfs:start_urls'
    start_urls = ['http://blog.csdn.net/']

    def parse(self, response):
        all_urls = response.css("a::attr(href)").extract()  # 提取该网页上所有a标签的href属性值
        all_urls = [parse.urljoin(response.url, url) for url in all_urls]  # 补全URL
        all_urls = filter(lambda x: True if x.startswith("https://blog.csdn.net/") else False, all_urls)  # 筛选出属于博客的URL
        for url in all_urls:
            # print(url)
            match_obj = re.match("(.*csdn.net/(.*?)/article/details.*)", url)   # 筛选出博文URL
            if match_obj:
                request_url = match_obj.group(1)
                yield scrapy.Request(request_url, callback=self.parse_detail)   # 如果是博文URL则进行下一步分析爬取
            else:
                yield scrapy.Request(url, callback=self.parse)  # 如果不是博文链接则从此网站进行重复分析筛选
            pass
        pass
        # yield scrapy.Request(url='https://blog.csdn.net/qq_37653449/category_7971677.html', callback=self.parse_detail, dont_filter=True)

    def parse_detail(self, response):
        print("Start parse_detail, url = " + response.url)
        # 实例化item
        article_item = CSDNItem()

        # 获取文章信息
        title = response.css(".article-title-box h1::text").extract()[0].strip()  # CSS选择器来进行提取
        author = response.css(".follow-nickName::text").extract()[0].strip()
        push_time = response.css(".time::text").extract()[0].strip().replace("最后发布于", "")
        content = response.css("#content_views").extract()[0]
        if len(content) >= 15000:
            content = content[0: 14499] + '......'

        read = response.css(".read-count::text").extract()[0].replace('阅读数 ', '')
        # read_nums = re.match(r'[1-9]\d*', read)
        if read == "":
            read_nums = 0
        else:
            read_nums = int(read)

        # create_article = response.css(".text span::text").extract()

        # item
        article_item["title"] = title
        try:
            push_time = datetime.datetime.strptime(push_time, "%Y-%m-%d %H:%M:%S")
        except Exception as e:
            push_time = datetime.datetime.now()
        article_item["push_time"] = push_time
        article_item["author"] = author
        article_item["url"] = response.url
        article_item["read_nums"] = int(read_nums)
        article_item["url_id"] = get_md5(response.url)
        article_item["article_content"] = content
        # article_item["create_article_nums"] = create_article_nums

        yield article_item


