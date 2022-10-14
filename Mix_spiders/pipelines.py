# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import codecs
import json
from scrapy.exporters import JsonItemExporter
import MySQLdb
from twisted.enterprise import adbapi
import MySQLdb.cursors
from models.es_types import CSDNType
from w3lib.html import remove_tags


class MixSpidersPipeline(object):
    def process_item(self, item, spider):
        return item


# 采用同步机制插入数据
class MysqlPipeline(object):
    def __init__(self):
        self.conn = MySQLdb.connect(
            'localhost',
            'root',
            'password',
            'Mix_spider',
            charset="utf8",
            use_unicode=True)
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        insert_sql = """
            insert into csdn_spider(title, push_time, author, url, url_id, read_nums)
                values (%s, %s, %s, %s, %s, %s)
        """
        item_dict = dict()

        self.cursor.execute(insert_sql, (item["title"], item["push_time"], item["author"], item["url"], item["url_id"], item["read_nums"]))
        self.conn.commit()


# 利用twisted采用异步机制插入数据
class MysqlTwistedPipeline(object):
    def __init__(self, dbppol):
        self.dbpool = dbppol

    @classmethod
    def from_settings(cls, settings):
        dbparms = dict(
            host=settings["MYSQL_HOST"],
            db=settings["MYSQL_DBNAME"],
            user=settings["MYSQL_USER"],
            passwd=settings["MYSQL_PASSWORD"],
            charset='utf8',
            cursorclass=MySQLdb.cursors.DictCursor,
            use_unicode=True
        )
        dbpool = adbapi.ConnectionPool("MySQLdb", **dbparms)

        return cls(dbpool)
        pass

    def process_item(self, item, spider):
        # 使用twisted将mysql插入变成异步执行
        query = self.dbpool.runInteraction(self.do_insert, item)    # 可以将内部的逻辑变成异步操作
        query.addErrback(self.handle_error)

        item.save_to_es()
        return item

    def handle_error(self, failure):
        # 处理异步插入异常
        print(failure)

    def do_insert(self, cursor, item):
        # 执行具体的插入
        # 根据不同的item 构建不同的sql语句并插入到mysql中
        insert_sql, params = item.get_insert_sql()
        print(insert_sql, params)
        cursor.execute(insert_sql, params)

    # def do_insert(self, cursor, item):
    #     # 具体插入逻辑
    #     insert_sql = """
    #                     insert into csdn_spider(title, push_time, author, url, url_id, read_nums)
    #                         values (%s, %s, %s, %s, %s, %s)
    #                 """
    #     cursor.execute(insert_sql, (item["title"], item["push_time"], item["author"], item["url"], item["url_id"], item["read_nums"]))
    #     # 此cursor是函数传递进来的cursor


# 自定义写入json文件
class JsonWithEncodingPipeline(object):
    def __init__(self):
        self.file = codecs.open("CSDN_article.json", "w", encoding="utf-8")
    
    # 处理item的函数
    def process_item(self, item, spider):
        lines = json.dumps(dict(item), ensure_ascii=False) + '\n'   #ensure_ascii是为了防止写入中文或者其他编码的字符时出错乱码
        self.file.write(lines)
        return item
    
    def spider_closed(self, spider):
        self.file.close()


# 调用scrapy的json export导出json文件
class JsonExporterPipeline(object):
    def __init__(self):
        self.file = open('article_export.json', 'wb')
        self.exporter = JsonItemExporter(self.file, encoding='utf-8', ensure_ascii=False)
        self.exporter.start_exporting()

    def close_spider(self):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item


class ElasticsearchPipeline(object):

    def process_item(self, item, spider):
        item.save_to_es()

        return item
