#!/usr/bin/env python
# -*- coding: utf-8 -*-
# !/usr/bin/python3.5

from newspaper import Article
import pandas as pd
import numpy as np
import time
# import datetime
try:
    from urlparse import urlparse
except:
    from urllib.parse import urlparse
import sqlite3

#url = 'http://almalnews.com/Story/363713/12/1-4-%D9%85%D9%84%D9%8A%D8%A7%D8%B1-%D8%AF%D9%88%D9%84%D8%A7%D8%B1-%D8%B2%D9%8A%D8%A7%D8%AF%D8%A9-%D9%81%D9%89-%D8%B5%D8%A7%D9%81%D9%89-%D8%A7%D9%84%D8%A3%D8%B5%D9%88%D9%84-%D8%A7%D9%84%D8%A3%D8%AC%D9%86%D8%A8%D9%8A%D8%A9'
#url = 'http://parlmany.youm7.com/News/10/214729/%D8%A7%D9%84%D8%AD%D9%83%D9%88%D9%85%D8%A9-%D8%AA%D9%88%D8%A7%D9%81%D9%82-%D8%B9%D9%84%D9%89-%D8%B9%D8%AF%D8%AF-%D9%85%D9%86-%D8%A7%D9%84%D8%A5%D8%AC%D8%B1%D8%A7%D8%A1%D8%A7%D8%AA-%D8%A7%D9%84%D8%AE%D8%A7%D8%B5%D8%A9-%D8%A8%D8%AA%D8%B3%D9%87%D9%8A%D9%84-%D8%A5%D8%AE%D9%84%D8%A7%D8%A1-%D9%85%D8%AB%D9%84%D8%AB'
#url = 'http://www.videoyoum7.com/News/6/250666/%D9%81%D9%8A%D8%AF%D9%8A%D9%88-%D8%A7%D9%84%D8%AF%D8%A7%D8%AE%D9%84%D9%8A%D8%A9-%D8%AA%D8%B4%D8%B1%D8%AD-%D8%A7%D9%84%D9%82%D9%8A%D8%A7%D8%AF%D8%A9-%D8%A7%D9%84%D8%A2%D9%85%D9%86%D8%A9-%D9%84%D9%84%D8%B3%D8%A7%D8%A6%D9%82%D9%8A%D9%86-%D8%AA%D8%B2%D8%A7%D9%85%D9%86%D8%A7-%D9%85%D8%B9-%D8%A7%D9%84%D8%B4%D8%A8%D9%88%D8%B1%D8%A9-%D9%88%D8%A7%D9%84%D8%A3%D8%AA%D8%B1%D8%A8%D8%A9'

table_name = 'NEWS_DATA'
NEW_COLUMN1 = 'LINK' # name of the link (PRIMARY KEY) column
NEW_COLUMN2 = 'ARTICLE_TEXT'  
NEW_COLUMN3 = 'ARTICLE_DATE'  
NEW_COLUMN4 = 'INSERTION_DATE' # E.g., INTEGER, TEXT, NULL, REAL, BLOB
NEW_COLUMN5 = 'ARTICLE_TYPE'

# df = pd.read_csv('All_pipeline_POS_version_ar_and_en.csv', encoding='utf-8')
conn = sqlite3.connect("news_db.db")
c = conn.cursor()

df = pd.read_sql_query("select * from NEWS_DATA where ARTICLE_TEXT='';", conn) 
print(df.shape)
# df.fillna(np.nan, inplace=True)
# empty_text_links = df[df.isnull().any(axis=1)]['Article_Link'].tolist() # to capture nan text columns
empty_text_links = df['LINK'].tolist()
# print(len(empty_text_links))

def extract_text(urls):
    print('Number of articles with Empty text --> ', len(urls))
    # text = []
    count_links = 0
    for url in urls:
        count_links += 1
        print('-------------------',count_links,'-------------------')
        print(url)
        try:
            article = Article(url)
            article.download()
            article.parse()
        except:
            print('Not Properly parsed!!')
        time.sleep(1)
        try:
            c.execute('UPDATE {dn} SET {cn1}={v1} WHERE {cn2}={v2}'.\
                format(dn=table_name, cn1=NEW_COLUMN2, cn2=NEW_COLUMN1, v2=url, v1=article.text))
            conn.commit()
            print("Record Added")
        except sqlite3.Error as e:
            print ("Error Writing in the DB.. " + e.args[0])
        
        # text.append(article.text)
    #     returned_txt_count = 0
    #     for txt in text:
    #         if txt != u'':
    #             returned_txt_count += 1
    # print('Number of sucessfully Returned text --> ', returned_txt_count)
        #print(article.authors)
        #print(article.publish_date)
        #print(article.movies)
    # return text

# text = extract_text(empty_text_links)
extract_text(empty_text_links)
conn.close()
# print(len(text)) 

# B.1) Updating specific rows that meet a certain criterion
# here: update column_1 with value_1 if row has value_2 in column_2
# try:
#     c.execute('UPDATE {dn} SET {cn1}={v1} WHERE {cn1}={v2}'.format(dn=table_name, cn1=NEW_COLUMN2, v2='', v1=text))
#     conn.commit()
#     print("Record Added")
# except:
#     print("Can not Write in Database")
# conn.close()

# df = pd.DataFrame({'Article_Link':empty_text_links,'Text':text})
# print (df.shape)
# df.to_csv('All_pipeline_POS_version_newspaper_3k.csv', encoding='utf-8', index=False)



