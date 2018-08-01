#!/usr/bin/env python
# import pandas as pd 
# import numpy as np 

import requests
# import os
# from os import listdir
# from os.path import isfile, join

from goose import Goose 
from goose.text import StopWordsArabic, StopWords

from urlparse import urlparse


def extract_text(link):


	english_websites = [] # list of English Written websites 
	goose = Goose({'stopwords_class': StopWordsArabic})
	goose_soup = Goose({'stopwords_class': StopWordsArabic,  'parser_class':'soup'})

	goose_en = Goose({'stopwords_class': StopWords})
	goose_soup_en = Goose({'stopwords_class': StopWords,  'parser_class':'soup'})

	parsed_link = urlparse(link)
	domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_link)
	if domain in english_websites:
		try:
			article = goose_en.extract(link).cleaned_text
			if article == u'':
				article = goose_soup_en.extract(link).cleaned_text
			return article
			# if goose_en.extract(link).cleaned_text == u'':
			# 	article = goose_soup_en.extract(link)
			# else:
			# 	article = goose_en.extract(link)
		except:
			print 'NO ENGLISH PARSER FOUND!'
	else:
		try:
			article = goose.extract(link).cleaned_text
			if article == u'':
				article = goose_soup.extract(link).cleaned_text
			return article
			# if goose.extract(link).cleaned_text == u'':
			# 	article = goose_soup.extract(link)
			# else:
			# 	article = goose.extract(link)
		except:
			print 'NO ARABIC PARSER FOUND!'

		

## Some Examples to try


# print(extract_text('http://shabab.ahram.org.eg/News/74367.aspx'))
# print(extract_text('http://shabab.ahram.org.eg/News/74375.aspx'))
# print(extract_text('http://www.innfrad.com/News/32/1001861/%D8%A7%D9%84%D8%B3%D9%8A%D8%B3%D9%89-%D9%8A%D8%AC%D8%B1%D9%89-%D8%B2%D9%8A%D8%A7%D8%B1%D8%A9-%D9%85%D9%81%D8%A7%D8%AC%D8%A6%D8%A9-%D9%81%D8%AC%D8%B1%D8%A7-%D9%84%D9%84%D9%83%D9%84%D9%8A%D8%A9-%D8%A7%D9%84%D8%AD%D8%B1%D8%A8%D9%8A%D8%A9-%D9%88%D9%8A%D8%AA%D9%81%D9%82%D8%AF-%D8%A7%D9%84%D9%86%D8%B4%D8%A7%D8%B7-%D8%A7%D9%84%D8%AA%D8%AF%D8%B1%D9%8A%D8%A8%D9%89'))
# print(extract_text('http://www.el7dath.com/'))
