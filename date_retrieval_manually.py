#!/usr/bin/env python
from bs4 import BeautifulSoup
import requests
import re
import dateparser
from dateutil.parser import parse
from urlparse import urlparse
from user_agent import generate_user_agent # library to generate user agent


# hdr = { 'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
# 		'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
# 		'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
# 		'Accept-Encoding': 'none',
# 		'Accept-Language': 'en-US,en,ar;q=0.8',
# 		'Connection': 'keep-alive'
# 		}

# generate a user agent
hdr = {'User-Agent': generate_user_agent(device_type="desktop", os=('mac', 'linux'))}

# proxies = {'http' : 'http://10.10.0.0:0000',  
#            'https': 'http://120.10.0.0:0000'}

def parseStrDate(dateString):

    if '|' in dateString:
        return dateparser.parse(dateString.split('|')[0], settings={'DATE_ORDER': 'DMY'}, languages=['ar','en'])

    if ',' in dateString:
    	# print dateString
    	if dateparser.parse(dateString.split(',')[1], settings={'DATE_ORDER': 'DMY'}, languages=['ar','en']) == None:
    		return dateparser.parse(dateString, settings={'DATE_ORDER': 'DMY'}, languages=['ar','en'])
        return dateparser.parse(dateString.split(',')[1], settings={'DATE_ORDER': 'DMY'}, languages=['ar','en'])

    if '-' in dateString:
    	if dateparser.parse(dateString.split('-')[0], settings={'DATE_ORDER': 'DMY'}, languages=['ar','en']) == None: 
	    	return dateparser.parse(dateString.split('-')[2], settings={'DATE_ORDER': 'YMD'}, languages=['ar','en'])

    	elif dateparser.parse(dateString.split('-')[0], settings={'DATE_ORDER': 'DMY'}, languages=['ar','en']):
	    	try:
		        return dateparser.parse(dateString.split('-')[0], settings={'DATE_ORDER': 'DMY'}, languages=['ar','en'])
	    	except:
		    	return dateparser.parse(dateString.encode('ISO-8859-1').split('-')[0], settings={'DATE_ORDER': 'DMY'}, languages=['ar','en'])


    if len(dateString) > 0:
    	return str(dateparser.parse(dateString, settings={'DATE_ORDER': 'YMD'}, languages=['ar','en']))[:10]

    else:
        return None



def find_date_time(url):
	"""This function is used to extract text 
	   from datetime parameter in the time tag
	"""
	try:
		page_response  = requests.get(url, headers=hdr)
		if page_response.status_code == 200:
			data = page_response.text
		else:
			print('---------------------------- ERROR STATUS NUMBER ---------------------------- ', page_response.status_code)
	except requests.Timeout as e:
		print("IT IS TIME TO TIMEOUT")
		print(str(e))

	soup = BeautifulSoup(data, 'lxml')
	for time in soup.find_all("time"):
		if time != None:
			return str(dateparser.parse(time.get('datetime', '')))[:10]



def find_date_id(url):
	"""This function is used to extract date
	   from span, div, p tags given the tag id"""

	try:
		page_response  = requests.get(url,  headers=hdr)
		if page_response.status_code == 200:
			data = page_response.text
			
		else:
			print('---------------------------- ERROR STATUS NUMBER ---------------------------- ', page_response.status_code)
	except requests.Timeout as e:
		print("IT IS TIME TO TIMEOUT")
		print(str(e))
	soup = BeautifulSoup(data, 'lxml')
	parsedHTML = soup
	for tag in parsedHTML.find_all(['span', 'p', 'div'], id = re.compile('ContentPlaceHolder1_divDate|ContentPlaceHolder1_source|PressRDateAdded|newsWriter', re.IGNORECASE)):
	    if tag != None:
		    dateText = tag.text
		    possibleDate = parseStrDate(dateText)
		    return str(possibleDate)[:10]




def find_date_meta(url):
	"""
	A function used to extract date ffrom the following tags
	'span','p','div', 'h4','h6','i','li' 
	"""
	try:
		page_response  = requests.get(url, headers=hdr)
		if page_response.status_code == 200:
			data = page_response.text
			
		else:
			print('---------------------------- ERROR STATUS NUMBER ---------------------------- ', page_response.status_code)
	except requests.Timeout as e:
		print("IT IS TIME TO TIMEOUT")
		print(str(e))
	soup = BeautifulSoup(data, 'lxml')
	parsedHTML = soup
	# Find tags that have specific class
	for tag in parsedHTML.find_all(['span','p','div', 'h4','h6','i','li'], class_ = re.compile('text_seham_date|date date_inner col-lg-12 col-md-12 col-sm-12 col-xs-12|Date|updated|articletime|large-4 medium-5 small-12 column inDate|date txtCenter|entry-date|fa fa-calendar-check-o|thetime|date date04|Space|news-date|show-date|date meta-item|large-4 medium-4 small-6 column noPadR date|time|post-published-date|redtxtcolor|i-panel-footer-p|p-0|byline|field-item even|pad-r-15 x_s pad-b-10|post-date|date')):
		if tag != None:
			dataa = tag.text #.replace(',', '').replace('|', '').replace('-', '')
	
			if tag.find('span'):
				dataa = tag.text.replace(',', '')
			if tag.find('p'):
				dataa = tag.text
			if tag.find('i', class_='icon-time mi'):
				dataa = tag.text
			if tag.has_attr('style'):
				if tag['style'] in ['float:right; line-height: 30px;', 'font-family:Tahoma;font-size:12px;display:block;padding:4px;']:
					dataa = tag.text
			return str(parseStrDate(dataa))[:10]

	# Find tags that have no class
	for tag in parsedHTML.find_all(['p','span','div']):
		# print tag
		if tag != None:
			if tag.has_attr('style'):
				if tag['style'] in ['font: 13px arial;','float:right']:#,"font-family: 'Droid Arabic Kufi',sans-serif;"]:
					dataa = tag.text
					return str(dateparser.parse(dataa, settings={'DATE_ORDER': 'YMD'}))[:10]




def _extractFromURL(url):
    """A function to extract date from article link"""
    #Regex by Newspaper3k  - https://github.com/codelucas/newspaper/blob/master/newspaper/urls.py
    m = re.search(r'([\./\-_]{0,1}(19|20)\d{2})[\./\-_]{0,1}(([0-3]{0,1}[0-9][\./\-_])|(\w{3,5}[\./\-_]))([0-3]{0,1}[0-9][\./\-]{0,1})?', url)
    if m:
        return str(dateparser.parse(m.group(0), settings={'DATE_ORDER': 'YMD'}))[:10]

    return  None


def find_date_in_class(url):
	try:
		page_response  = requests.get(url, headers=hdr)
		if page_response.status_code == 200:
			data = page_response.text
			
		else:
			print('---------------------------- ERROR STATUS NUMBER ---------------------------- ', page_response.status_code)
	except requests.Timeout as e:
		print("IT IS TIME TO TIMEOUT")
		print(str(e))
	soup = BeautifulSoup(data, 'lxml')
	parsedHTML = soup
	for tag in parsedHTML.find_all(['span','p','div','body'], class_= re.compile('date', re.IGNORECASE)):
		if tag != None and tag.find('a'):  # | tag != 'None':
		    dataa = tag.a.text
		    return str(parseStrDate(dataa))[:10]


def find_chedot_dates(url):
	parsed_link = urlparse(url)
	domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_link)
	if domain in ['http://chedot.com/']:
		try:
			page_response  = requests.get(url, headers=hdr)
			if page_response.status_code == 200:
				data = page_response.text
			else:
				print('---------------------------- ERROR STATUS NUMBER ---------------------------- ', page_response.status_code)
		except requests.Timeout as e:
			print("IT IS TIME TO TIMEOUT")
			print(str(e))

		soup = BeautifulSoup(data, 'lxml')
		# To get dates of chedot.com
		for tag in soup.find_all(['div'],  class_=re.compile('source', re.IGNORECASE)):
			if tag != None:
				if tag.find('a'):
					dataa = tag.text
					return str(parseStrDate(dataa))[:10] #str(dateparser.parse(dataa.split(',')[1]))[:10]
				else:
					return None



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



# print(find_date('https://www.soutmisr.com/3591/%d8%a7%d9%84%d9%85%d8%aa%d8%ad%d8%af%d8%ab-%d8%a7%d9%84%d8%b9%d8%b3%d9%83%d8%b1%d9%89-%d9%82%d9%88%d8%a7%d8%aa-%d8%a7%d9%84%d8%ac%d9%8a%d8%b4-%d9%88%d8%a7%d9%84%d8%b4%d8%b1%d8%b7%d8%a9-%d8%aa%d9%86/'))
# print(find_date('http://chedot.com/eg/news/business/7302178/'))
# print(find_date('http://egynewstoday.com/?p=24829'))
# print(find_date('http://egynewstoday.com/?p=24855'))
# print(find_date('http://egynewstoday.com/?p=24842'))
# print(find_date('http://chedot.com/eg/news/business/7303160/'))
# print(find_date('http://chedot.com/eg/news/business/7303160/'))
# print(find_date('http://chedot.com/eg/news/culture/7292130/'))
# print(find_date('https://ellmada.com/%d8%a7%d9%84%d8%ac%d8%a7%d8%ad%d8%b1-%d8%b2%d9%8a%d8%a7%d8%af%d8%a9-%d8%b9%d8%af%d8%af-%d8%a7%d9%84%d8%b1%d8%ad%d9%84%d8%a7%d8%aa-%d8%a8%d9%8a%d9%86-%d9%85%d8%af%d9%86-%d9%84%d9%85%d8%af%d9%86-%d8%a7/'))

