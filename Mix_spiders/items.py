# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
import datetime
import redis

import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, TakeFirst, Join

from Mix_spiders.utils.common import extract_num
from Mix_spiders.settings import SQL_DATETIME_FORMAT, SQL_DATE_FORMAT

from Mix_spiders.models.es_types import CSDNType, zhihu_answerType, zhihu_questionType
from w3lib.html import remove_tags

from elasticsearch_dsl.connections import connections
es = connections.create_connection(hosts=['localhost'])
redis_cli = redis.StrictRedis()


class MixSpidersItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


def gen_suggests(index, info_tuple):
    # 生成suggest
    uesd_words = set()
    suggests = []
    for text, weight in info_tuple:
        if text:
            # 调用es analysis接口分析字符串
            words = es.indices.analyze(index=index, analyzer='ik_max_word', params={'filter':['lowercase']}, body=text)
            analyzed_words = set([r['token'] for r in words['tokens'] if len(r['token']) > 1])
            new_words = analyzed_words - uesd_words
        else:
            new_words = set()
        if new_words:
            suggests.append({'input': list(new_words), 'weight': weight})

    return suggests


class CSDNItem(scrapy.Item):
    author = scrapy.Field()
    title = scrapy.Field()
    push_time = scrapy.Field()
    url = scrapy.Field()
    url_id = scrapy.Field()
    read_nums = scrapy.Field()
    article_content = scrapy.Field()

    # create_article_nums = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
                        insert into csdn_spider(title, push_time, author, url, url_id, content, read_nums)
                            values (%s, %s, %s, %s, %s, %s, %s)
                            on DUPLICATE KEY UPDATE content = values (content)
                    """
        read_nums = extract_num(''.join((str(self['read_nums']))))
        params = (
            self["title"], self['push_time'], self['author'], self['url'],
            self['url_id'], self['article_content'], read_nums)
        return insert_sql, params

    def save_to_es(self):
        item = CSDNType()
        item.title = self['title']
        item.author = self['author']
        item.url = self['url']
        item.article_content = remove_tags(self['article_content'])
        item.push_time = self['push_time']
        item.read_nums = self['read_nums']
        item.meta.id = self['url_id']

        item.suggest = gen_suggests(CSDNType._doc_type.index, ((item.title, 10), (item.author, 7)))

        item.save()

        redis_cli.incr("csdn_count")

        return

    pass


class ZhihuQuestionItem(scrapy.Item):
    # 知乎的问题 item
    zhihu_id = scrapy.Field()
    topics = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    answer_nums = scrapy.Field()
    comment_nums = scrapy.Field()
    watch_user_nums = scrapy.Field()
    click_nums = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
                    insert into zhihu_question(zhihu_id, topics, url, title, content,  
                                answer_nums, comment_nums, watch_user_nums, click_nums, crawl_time, crawl_update_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    on DUPLICATE KEY UPDATE content = values (content), answer_nums = values (answer_nums), 
                                    comment_nums = values (comment_nums), watch_user_nums = values (watch_user_nums), 
                                    click_nums = values (click_nums), crawl_update_time = values (crawl_update_time)
        """

        zhihu_id = int(''.join(str(self['zhihu_id'][0])))
        topics = ",".join(self['topics'])
        url = self['url']
        title = self['title']
        content = self['content']
        answer_nums = extract_num(''.join(self['answer_nums'][0]))
        if self['comment_nums'] == '添加评论':
            comment_nums = 0
        else:
            comment_nums = extract_num(''.join(self['comment_nums'][0]))
        watch_user_nums = extract_num(''.join(self['watch_user_nums'][0]))
        click_nums = extract_num(''.join(self['click_nums'][0]))
        crawl_time = self['crawl_time']
        crawl_update_time = datetime.datetime.now()

        params = (zhihu_id, topics, url, title, content,
                  answer_nums, comment_nums, watch_user_nums, click_nums, crawl_time, crawl_update_time)

        return insert_sql, params

    def save_to_es(self):
        item = zhihu_questionType()
        item.meta.id = int(''.join(str(self['zhihu_id'][0])))
        item.topics = ",".join(self['topics'])
        item.url = self['url']
        item.title = self['title']
        item.content = self['content']
        item.answer_nums = extract_num(''.join(self['answer_nums'][0]))
        if self['comment_nums'] == '添加评论':
            item.comment_nums = 0
        else:
            item.comment_nums = extract_num(''.join(self['comment_nums'][0]))
        item.watch_user_nums = extract_num(''.join(self['watch_user_nums'][0]))
        item.click_nums = extract_num(''.join(self['click_nums'][0]))
        item.crawl_time = self['crawl_time']
        item.crawl_update_time = datetime.datetime.now()

        item.suggest = gen_suggests(zhihu_questionType._doc_type.index, ((item.title, 10), (item.topics, 7)))

        item.save()

        redis_cli.incr('zhihu_question_count')

        return


class ZhihuAnswerItem(scrapy.Item):
    # 知乎的问题回答item
    zhihu_id = scrapy.Field()
    url = scrapy.Field()
    question_id = scrapy.Field()
    author_id = scrapy.Field()
    content = scrapy.Field()
    praise_nums = scrapy.Field()
    comment_nums = scrapy.Field()
    create_time = scrapy.Field()
    update_time = scrapy.Field()
    crawl_time = scrapy.Field()
    question_title = scrapy.Field()
    author_name = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
                    insert into zhihu_answer(zhihu_id, url, question_id, author_id, content, praise_nums, comment_nums, 
                                            create_time, update_time, crawl_time, crawl_update_time, question_title, author_name)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                     on DUPLICATE KEY UPDATE content = values (content), praise_nums = values (praise_nums), 
                                    comment_nums = values (comment_nums), update_time = values (update_time), 
                                    crawl_update_time = values (crawl_update_time)
        """
        create_time = datetime.datetime.fromtimestamp(self['create_time']).strftime(SQL_DATETIME_FORMAT)
        zhihu_id = int(''.join(str(self['zhihu_id'])))
        url = self['url']
        question_id = self['question_id']
        author_id = self['author_id']
        content = self['content']
        praise_nums = self['praise_nums']
        comment_nums = self['comment_nums']
        update_time = datetime.datetime.fromtimestamp(self['update_time']).strftime(SQL_DATETIME_FORMAT)
        crawl_time = self['crawl_time']
        crawl_update_time = datetime.datetime.now()
        question_title = self['question_title']
        author_name = self['author_name']

        params = (zhihu_id, url, question_id, author_id, content,
                  praise_nums, comment_nums, create_time, update_time, crawl_time, crawl_update_time, question_title, author_name)

        return insert_sql, params

    def save_to_es(self):
        item = zhihu_answerType()
        item.meta.id = int(''.join(str(self['zhihu_id'])))
        item.url = self['url']
        item.question_id = self['question_id']
        item.author_id = self['author_id']
        item.content = self['content']
        item.praise_nums = self['praise_nums']
        item.comment_nums = self['comment_nums']
        item.create_time = datetime.datetime.fromtimestamp(self['create_time']).strftime(SQL_DATETIME_FORMAT)
        item.update_time = datetime.datetime.fromtimestamp(self['update_time']).strftime(SQL_DATETIME_FORMAT)
        item.crawl_time = self['crawl_time']
        item.crawl_update_time = datetime.datetime.now()
        item.question_title = self['question_title']
        item.author_name = self['author_name']

        item.suggest = gen_suggests(zhihu_answerType._doc_type.index, ((item.question_title, 10), (item.author_name, 7)))

        item.save()

        redis_cli.incr('zhihu_answer_count')

        return
