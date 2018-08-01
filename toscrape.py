#!/usr/bin/env python
# # -*- coding: utf-8 -*-
        
import pandas as pd
import numpy as np
import multiprocessing as multi
import csv
from datetime import datetime
from urlparse import urlparse
import articleDateExtractor
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.crawler import CrawlerProcess
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from twisted.internet import reactor
import sqlite3
from sqlite3 import Error

## Extracting Dates
from date_retrieval_manually import *
from date_retrieval_manually import _extractFromURL

## Extracting Text
from article_text_retrieval import extract_text

DB_NAME = 'news_db.db'    # name of the sqlite database file
TABLE_NAME = 'NEWS_DATA'   # name of the table to be created
TEMP_TABLE_NAME = 'NEWS_TEMP'
NEW_COLUMN1 = 'LINK' # name of the link (PRIMARY KEY) column
NEW_COLUMN2 = 'ARTICLE_TEXT'  
NEW_COLUMN3 = 'ARTICLE_DATE'  
NEW_COLUMN4 = 'INSERTION_DATE' # E.g., INTEGER, TEXT, NULL, REAL, BLOB
NEW_COLUMN5 = 'ARTICLE_TYPE'
COLUMN1_field_type = 'TEXT'
COLUMN2_field_type = 'TEXT'
COLUMN3_field_type = 'TEXT'
COLUMN4_field_type = 'TEXT'
COLUMN5_field_type = 'TEXT'


# Connecting to the database file
conn = sqlite3.connect(DB_NAME)
cur = conn.cursor()

# Creating a new SQLite table with 1 column
# cur.execute('''CREATE TABLE {tn} ({nf1} {ft1} PRIMARY KEY,
#                                   {nf2} {ft2},
#                                   {nf3} {ft3},
#                                   {nf4} {ft4},
#                                   {nf5} {ft5})'''
#         .format(tn=TABLE_NAME, nf1=NEW_COLUMN1, ft1=COLUMN1_field_type,
#                 nf2=NEW_COLUMN2, ft2=COLUMN2_field_type,
#                 nf3=NEW_COLUMN3, ft3=COLUMN3_field_type,
#                 nf4=NEW_COLUMN4, ft4=COLUMN4_field_type,
#                 nf5=NEW_COLUMN5, ft5=COLUMN5_field_type))


DATE = str(datetime.now().date())

def find_date(url):
    try:
        if find_date_time(url) != None:
            return find_date_time(url)
        elif find_chedot_dates(url) != None:
            return find_chedot_dates(url)
        elif find_date_id(url) != None:
            return find_date_id(url)
        elif find_date_meta(url) != None:
            return find_date_meta(url)
        elif _extractFromURL(url) != None:
            return _extractFromURL(url)
        elif find_date_in_class(url) != None:
            return find_date_in_class(url)

        else:
            return None
    except:
        print url, ':::BadStatusLine|Connection aborted:::'

## Data Preparation for Parellism
def chunks(n, page_list):
    """Splits the list into n chunks"""
    return np.array_split(page_list, n)

cpus = multi.cpu_count() # return cpus count
print "DATE ", str(DATE)
# websites_df = pd.read_csv("website_nonRSS_Dates.csv", encoding='utf-8')
websites_df = pd.read_excel("Egyptian_news_sites .xlsx", 'Non_RSS_websites', encoding='utf-8')
websites_df.fillna(np.nan, inplace=True)

start_urls = websites_df.loc[((websites_df.Dates != 'None') & (websites_df.Dates.notnull()))]['Name of news sites and newswires'].map(lambda x: "http://"+ x).tolist() # add condition that have date != None
allowed_domains = websites_df.loc[ ((websites_df.Dates != 'None') & (websites_df.Dates.notnull()))]['Name of news sites and newswires'].tolist()

start_urls_allowed_domains = np.stack((start_urls, allowed_domains), axis=-1) # to match every website with its domain

page_bins = chunks(cpus, start_urls_allowed_domains)
start_urlss = []
allowed_domainss = []

for i in page_bins:
    for j in i:
        start_urlss.append(j[0])
        allowed_domainss.append(j[1])

start_urlss_chunks = chunks(cpus, start_urlss)
allowed_domainss_chunks = chunks(cpus, allowed_domainss)

# print start_urlss
# print len(start_urlss)
# print allowed_domainss
# print len(allowed_domainss)


class MySpider1(CrawlSpider):

    name = 'News_websites1'

    start_urls = list(start_urlss_chunks[0])
    allowed_domains  = list(allowed_domainss_chunks[0])

    
    rules = (
        Rule(LinkExtractor(), callback='parse_item', follow=False),
    )


    def parse_item(self, response):

        # parsed_uri = urlparse(response.url)
        date = articleDateExtractor.extractArticlePublishedDate(response.url)
        if date == None:
            date = find_date(response.url)
        print "date ", str(date)[0:10]
        if str(date)[0:10] == DATE:
            new_text = extract_text(response.url)
            try:
                cur.execute("INSERT INTO NEWS_DATA (LINK, ARTICLE_TEXT, ARTICLE_DATE, INSERTION_DATE, ARTICLE_TYPE) values (?,?,?,?,?)",(response.url, new_text, str(date)[0:10], datetime.now().strftime("%Y-%m-%d"),'NON-RSS'))
                conn.commit()
                print "Record Added"
            except sqlite3.Error as e:
                print ("Error Writing in the DB.. " + e.args[0])
            # with open("Parallel_Extracted_Urls/urls_0.txt", "a") as myfile:    
            #     print "LINK ADDED_0"
            #     myfile.write('{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri) + "," + response.url + "," + str(date)[0:10]+ "\r\n")



class MySpider2(CrawlSpider):

    name = 'News_websites2'

    start_urls = list(start_urlss_chunks[1])
    allowed_domains  = list(allowed_domainss_chunks[1])

    rules = (
        Rule(LinkExtractor(), callback='parse_item', follow=False),
    )



    def parse_item(self, response):

        # parsed_uri = urlparse(response.url)
        date = articleDateExtractor.extractArticlePublishedDate(response.url)
        if date == None:
            date = find_date(response.url)
        print "date ", str(date)[0:10]
        if str(date)[0:10] == DATE:
            new_text = extract_text(response.url)
            try:
                cur.execute("INSERT INTO NEWS_DATA (LINK, ARTICLE_TEXT, ARTICLE_DATE, INSERTION_DATE, ARTICLE_TYPE) values (?,?,?,?,?)",(response.url, new_text, str(date)[0:10], datetime.now().strftime("%Y-%m-%d"),'NON-RSS'))
                conn.commit()
                print "Record Added"
            except sqlite3.Error as e:
                print ("Error Writing in the DB.. " + e.args[0])
            # with open("Parallel_Extracted_Urls/urls_1.txt", "a") as myfile:    
            #     print "LINK ADDED_1"
            #     myfile.write('{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri) + "," + response.url + "," + str(date)[0:10]+ "\r\n")



class MySpider3(CrawlSpider):

    name = 'News_websites3'

    start_urls = list(start_urlss_chunks[2])
    allowed_domains  = list(allowed_domainss_chunks[2])
    
    rules = (
        Rule(LinkExtractor(), callback='parse_item', follow=False),
    )



    def parse_item(self, response):

        # parsed_uri = urlparse(response.url)
        date = articleDateExtractor.extractArticlePublishedDate(response.url)
        if date == None:
            date = find_date(response.url)
        print "date ", str(date)[0:10]
        if str(date)[0:10] == DATE:
            new_text = extract_text(response.url)
            try:
                cur.execute("INSERT INTO NEWS_DATA (LINK, ARTICLE_TEXT, ARTICLE_DATE, INSERTION_DATE, ARTICLE_TYPE) values (?,?,?,?,?)",(response.url, new_text, str(date)[0:10], datetime.now().strftime("%Y-%m-%d"),'NON-RSS'))
                conn.commit()
                print "Record Added"
            except sqlite3.Error as e:
                print ("Error Writing in the DB.. " + e.args[0])
            # with open("Parallel_Extracted_Urls/urls_2.txt", "a") as myfile:    
            #     print "LINK ADDED_2"
            #     myfile.write('{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri) + "," + response.url + "," + str(date)[0:10]+ "\r\n")



class MySpider4(CrawlSpider):

    name = 'News_websites4'

    start_urls = list(start_urlss_chunks[3])
    allowed_domains  = list(allowed_domainss_chunks[3])

    
    rules = (
        Rule(LinkExtractor(), callback='parse_item', follow=False),
    )


    def parse_item(self, response):

        # parsed_uri = urlparse(response.url)
        date = articleDateExtractor.extractArticlePublishedDate(response.url)
        if date == None:
            date = find_date(response.url)
        print "date ", str(date)[0:10]
        if str(date)[0:10] == DATE:
            new_text = extract_text(response.url)
            try:
                cur.execute("INSERT INTO NEWS_DATA (LINK, ARTICLE_TEXT, ARTICLE_DATE, INSERTION_DATE, ARTICLE_TYPE) values (?,?,?,?,?)",(response.url, new_text, str(date)[0:10], datetime.now().strftime("%Y-%m-%d"),'NON-RSS'))
                conn.commit()
                print "Record Added"
            except sqlite3.Error as e:
                print ("Error Writing in the DB.. " + e.args[0])
            # with open("Parallel_Extracted_Urls/urls_3.txt", "a") as myfile:    
            #     print "LINK ADDED_3"
            #     myfile.write('{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri) + "," + response.url + "," + str(date)[0:10]+ "\r\n")



class MySpider5(CrawlSpider):

    name = 'News_websites5'

    start_urls = list(start_urlss_chunks[4])
    allowed_domains  = list(allowed_domainss_chunks[4])

    
    rules = (
        Rule(LinkExtractor(), callback='parse_item', follow=False),
    )



    def parse_item(self, response):

        # parsed_uri = urlparse(response.url)
        date = articleDateExtractor.extractArticlePublishedDate(response.url)
        if date == None:
            date = find_date(response.url)
        print "date ", str(date)[0:10]
        if str(date)[0:10] == DATE:
            new_text = extract_text(response.url)
            try:
                cur.execute("INSERT INTO NEWS_DATA (LINK, ARTICLE_TEXT, ARTICLE_DATE, INSERTION_DATE, ARTICLE_TYPE) values (?,?,?,?,?)",(response.url, new_text, str(date)[0:10], datetime.now().strftime("%Y-%m-%d"),'NON-RSS'))
                conn.commit()
                print "Record Added"
            except sqlite3.Error as e:
                print ("Error Writing in the DB.. " + e.args[0])
            # with open("Parallel_Extracted_Urls/urls_4.txt", "a") as myfile:    
            #     print "LINK ADDED_4"
            #     myfile.write('{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri) + "," + response.url + "," + str(date)[0:10]+ "\r\n")



class MySpider6(CrawlSpider):

    name = 'News_websites6'

    start_urls = list(start_urlss_chunks[5])
    allowed_domains  = list(allowed_domainss_chunks[5])

    
    rules = (
        Rule(LinkExtractor(), callback='parse_item', follow=False),
    )



    def parse_item(self, response):

        # parsed_uri = urlparse(response.url)
        date = articleDateExtractor.extractArticlePublishedDate(response.url)
        if date == None:
            date = find_date(response.url)
        print "date ", str(date)[0:10]
        if str(date)[0:10] == DATE:
            new_text = extract_text(response.url)
            try:
                cur.execute("INSERT INTO NEWS_DATA (LINK, ARTICLE_TEXT, ARTICLE_DATE, INSERTION_DATE, ARTICLE_TYPE) values (?,?,?,?,?)",(response.url, new_text, str(date)[0:10], datetime.now().strftime("%Y-%m-%d"),'NON-RSS'))
                conn.commit()
                print "Record Added"
            except sqlite3.Error as e:
                print ("Error Writing in the DB.. " + e.args[0])
            # with open("Parallel_Extracted_Urls/urls_5.txt", "a") as myfile:    
            #     print "LINK ADDED_5"
            #     myfile.write('{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri) + "," + response.url + "," + str(date)[0:10]+ "\r\n")
    


class MySpider7(CrawlSpider):

    name = 'News_websites7'

    start_urls = list(start_urlss_chunks[6])
    allowed_domains  = list(allowed_domainss_chunks[6])

    
    rules = (
        Rule(LinkExtractor(), callback='parse_item', follow=False),
    )



    def parse_item(self, response):

        # parsed_uri = urlparse(response.url)
        date = articleDateExtractor.extractArticlePublishedDate(response.url)
        if date == None:
            date = find_date(response.url)
        print "date ", str(date)[0:10]
        if str(date)[0:10] == DATE:
            new_text = extract_text(response.url)
            try:
                cur.execute("INSERT INTO NEWS_DATA (LINK, ARTICLE_TEXT, ARTICLE_DATE, INSERTION_DATE, ARTICLE_TYPE) values (?,?,?,?,?)",(response.url, new_text, str(date)[0:10], datetime.now().strftime("%Y-%m-%d"),'NON-RSS'))
                conn.commit()
                print "Record Added"
            except sqlite3.Error as e:
                print ("Error Writing in the DB.. " + e.args[0])
            # with open("Parallel_Extracted_Urls/urls_6.txt", "a") as myfile:    
            #     print "LINK ADDED_6"
            #     myfile.write('{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri) + "," + response.url + "," + str(date)[0:10]+ "\r\n")
    


class MySpider8(CrawlSpider):

    name = 'News_websites8'

    start_urls = list(start_urlss_chunks[7])
    allowed_domains  = list(allowed_domainss_chunks[7])

    
    rules = (
        Rule(LinkExtractor(), callback='parse_item', follow=False),
    )



    def parse_item(self, response):

        # parsed_uri = urlparse(response.url)
        date = articleDateExtractor.extractArticlePublishedDate(response.url)
        if date == None:
            date = find_date(response.url)
        print "date ", str(date)[0:10]
        if str(date)[0:10] == DATE:
            new_text = extract_text(response.url)
            try:
                cur.execute("INSERT INTO NEWS_DATA (LINK, ARTICLE_TEXT, ARTICLE_DATE, INSERTION_DATE, ARTICLE_TYPE) values (?,?,?,?,?)",(response.url, new_text, str(date)[0:10], datetime.now().strftime("%Y-%m-%d"),'NON-RSS'))
                conn.commit()
                print "Record Added"
            except sqlite3.Error as e:
                print ("Error Writing in the DB.. " + e.args[0])
            # with open("Parallel_Extracted_Urls/urls_7.txt", "a") as myfile:    
            #     print "LINK ADDED_7"
            #     myfile.write('{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri) + "," + response.url + "," + str(date)[0:10]+ "\r\n")
    



# Call the spiders
process = CrawlerProcess()
print '============================= Spider No. 1 is working ============================='
process.crawl(MySpider1)
print '============================= Spider No. 2 is working ============================='
process.crawl(MySpider2)
print '============================= Spider No. 3 is working ============================='
process.crawl(MySpider3)
print '============================= Spider No. 4 is working ============================='
process.crawl(MySpider4)
print '============================= Spider No. 5 is working ============================='
process.crawl(MySpider5)
print '============================= Spider No. 6 is working ============================='
process.crawl(MySpider6)
print '============================= Spider No. 7 is working ============================='
process.crawl(MySpider7)
print '============================= Spider No. 8 is working ============================='
process.crawl(MySpider8)

# Start the spidersd
# process.start(stop_after_crawl=False) 
process.start(stop_after_crawl=True) # default setting


cur.close()
conn.close()

# blocks process; so always keep as the last statement
reactor.run()

# # configure_logging()
# # runner = CrawlerRunner()
# # print 'Spider No. 1 is working .......'
# # runner.crawl(MySpider1)
# # print 'Spider No. 2 is working .......'
# # runner.crawl(MySpider2)
# # print 'Spider No. 3 is working .......'
# # runner.crawl(MySpider3)
# # print 'Spider No. 4 is working .......'
# # runner.crawl(MySpider4)
# # print 'Spider No. 5 is working .......'
# # runner.crawl(MySpider5)
# # print 'Spider No. 6 is working .......'
# # runner.crawl(MySpider6)
# # print 'Spider No. 7 is working .......'
# # runner.crawl(MySpider7)
# # print 'Spider No. 8 is working .......'
# # runner.crawl(MySpider8)

# # d = runner.join()
# # d.addBoth(lambda _: reactor.stop())

# # reactor.run() # the script will block here until all crawling jobs are finished













# def find_date(url):
#     try:
#         if find_date_time(url) != None:
#             return find_date_time(url)
#         elif find_date_id(url) != None:
#             return find_date_id(url)
#         elif find_date_meta(url) != None:
#             return find_date_meta(url)
#         elif _extractFromURL(url) != None:
#             return _extractFromURL(url)
#         elif find_date_in_class(url) != None:
#             return find_date_in_class(url)
#         else:
#             return None
#     except:
#         print url, ':::BadStatusLine|Connection aborted:::'



# def chunks(n, page_list):
#     """Splits the list into n chunks"""
#     return np.array_split(page_list, n)
 
# cpus = multi.cpu_count()
# workers = []
# page_list = ['www.website.com/page1.html', 'www.website.com/page2.html'
#              'www.website.com/page3.html', 'www.website.com/page4.html']

# page_bins = chunks(cpus, page_list)

# for cpu in range(cpus):
#     sys.stdout.write("CPU " + str(cpu) + "\n")
#     # Process that will send corresponding list of pages 
#     # to the function perform_extraction
#     worker = multi.Process(name=str(cpu), 
#                            target=perform_extraction, 
#                            args=(page_bins[cpu],))
#     worker.start()
#     workers.append(worker)

# for worker in workers:
#     worker.join()
    
# def perform_extraction(page_ranges):
#     """Extracts data, does preprocessing, writes the data"""
#     # do requests and BeautifulSoup
#     # preprocess the data
#     file_name = multi.current_process().name+'.txt'
#     # write into current process file


# def chunks(n, page_list):
#     """Splits the list into n chunks"""
#     return np.array_split(page_list, n)

# cpus = multi.cpu_count() # return cpus count

# print "DATE ", str(DATE)
# name = 'News websites'

# # allowed_domains = ['botler.io']


# websites_df = pd.read_csv("website_nonRSS_Dates.csv", encoding='utf-8')
# start_urls = websites_df.loc[websites_df.dates != 'None']['Name of news sites and newswires'].map(lambda x: "https://"+ x).tolist() # add condition that have date != None
# allowed_domains = websites_df.loc[websites_df.dates != 'None']['Name of news sites and newswires'].tolist()

# # xx = []
# # for i in start_urls:
# #     for j in allowed_domains:
# #         xx.append((i,j))
# xx = np.stack((start_urls, allowed_domains), axis=-1)
# print xx[0]
# page_bins = chunks(cpus, xx)
# print page_bins
# print page_bins[0]
# print page_bins[0][1]

# import scrapy


# class ToscrapeSpider(scrapy.Spider):
#     name = "toscrape"
#     allowed_domains = ["books.toscrape.com"]
#     start_urls = [
#         'http://books.toscrape.com/',
#     ]

#     def parse(self, response):
#         print "URL >>>>>>>>>>>>>>>>>>> ", response.url
#         # for book_url in response.css("article.product_pod > h3 > a ::attr(href)").extract():
#         #     yield scrapy.Request(response.urljoin(book_url), callback=self.parse_book_page)
#         # next_page = response.css("li.next > a ::attr(href)").extract_first()
#         # if next_page:
#         for url in response:
#             scrapy.Request(response.urljoin(url), callback=self.parse)


#     # def parse_book_page(self, response):
#     #     item = {}
#     #     product = response.css("div.product_main")
#     #     item["title"] = product.css("h1 ::text").extract_first()
#     #     item['category'] = response.xpath(
#     #         "//ul[@class='breadcrumb']/li[@class='active']/preceding-sibling::li[1]/a/text()"
#     #     ).extract_first()
#     #     item['description'] = response.xpath(
#     #         "//div[@id='product_description']/following-sibling::p/text()"
#     #     ).extract_first()
#     #     yield item



# class SjsuSpider(CrawlSpider):

#     name = 'sjsu'
#     allowed_domains = ['sjsu.edu']
#     start_urls = ['http://cs.sjsu.edu/']
#     # allow=() is used to match all links
#     rules = [Rule(SgmlLinkExtractor(allow=()), callback='parse_item')]

#     def parse_item(self, response):
#         x = HtmlXPathSelector(response)

#         filename = "sjsupages"
#         # open a file to append binary data
#         open(filename, 'ab').write(response.body)