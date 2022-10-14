from scrapy.cmdline import execute
from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerProcess
import os
import sys

settings = get_project_settings()
crawler = CrawlerProcess(settings)

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
choice = input("1 for csdn, 2 for zhihu_spider, 3 for zhihu_login, 4 for mix_spider(no login):")
if choice == "1":
    print('csdn_dfs')
    execute(["scrapy", "crawl", "csdn_DFS"])
elif choice == "2":
    print('zhihu_spider')
    execute(["scrapy", "crawl", "zhihu_spider"])
elif choice == "3":
    print("zhihu_login")
    execute(["scrapy", "crawl", "zhihu_login"])
elif choice == "4":
    print("mix_spider")
    crawler.crawl("csdn_DFS")
    crawler.crawl("zhihu_spider")
    crawler.start()
    crawler.start()