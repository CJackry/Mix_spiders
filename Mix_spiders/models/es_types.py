from datetime import datetime
from elasticsearch_dsl import DocType, Date, Nested, Boolean, \
    analyzer, InnerObjectWrapper, Completion, Keyword, Text, Integer

from elasticsearch_dsl.analysis import CustomAnalyzer as _CustomAnalyzer

from elasticsearch_dsl.connections import connections
connections.create_connection(hosts=['localhost'])


# 防止suggest报错
class CustomAnalyzer(_CustomAnalyzer):
    def get_analysis_definition(self):
        return {}


ik_analyzer = CustomAnalyzer("ik_max_word", filter=['lowercase'])


# CSDN导入
class CSDNType(DocType):
    suggest = Completion(analyzer=ik_analyzer)
    author = Keyword()
    title = Text(analyzer='ik_max_word')
    push_time = Date()
    url = Keyword()
    url_id = Keyword()
    read_nums = Integer()
    article_content = Text(analyzer='ik_max_word')

    class Meta:
        index = 'csdn'
        doc_type = 'posts'


# 知乎问题导入
class zhihu_questionType(DocType):
    suggest = Completion(analyzer=ik_analyzer)
    zhihu_id = Keyword()
    topics = Text(analyzer='ik_max_word')
    url = Keyword()
    title = Text(analyzer='ik_max_word')
    content = Text(analyzer='ik_max_word')
    answer_nums = Integer()
    comment_nums = Integer()
    watch_user_nums = Integer()
    click_nums = Integer()
    crawl_time = Date()
    crawl_update_time = Date()

    class Meta:
        index = 'zhihu_question'
        doc_type = 'question'


# 知乎回答导入
class zhihu_answerType(DocType):
    suggest = Completion(analyzer=ik_analyzer)
    zhihu_id = Keyword()
    url = Keyword()
    question_id = Keyword()
    author_id = Keyword()
    content = Text(analyzer='ik_max_word')
    praise_nums = Integer()
    comment_nums = Integer()
    create_time = Date()
    update_time = Date()
    crawl_time = Date()
    crawl_update_time = Date()
    question_title = Text(analyzer='ik_max_word')

    class Meta:
        index = 'zhihu_answer'
        doc_type = 'answer'


if __name__ == '__main__':
    CSDNType.init()
    zhihu_questionType.init()
    zhihu_answerType.init()
