# WebCrawler_NewspaperWebsites
### An implementation to a Scrapy Web Crawler, that crawl article links from Newspapers websites on a daily basis. Mainly Arabic Written Websites. __PYTHON 2.7__ project
- the project is developed for crawling news websites that have RSS feeds.
- Store the data into a database.

>***NON_RSS_Websites*** = 88

>***NON_RSS_Websites that return dates*** = 62

The directory structure is divided into those files:

### Prerequisites: 
1. [article-date-extractor](https://github.com/Webhose/article-date-extractor): library to return article dates
2. [python-goose](https://github.com/grangier/python-goose): library to extract text
3. [scrapy](https://github.com/scrapy/scrapy): to install it `pip install scrapy`
4. news_db.db: A sqlite database where article_links, articel_text,article_date, and insertion_date of each crawled article is stored.
	Database DDL Query:
	- type into the linux terminal sqlite3 `news_db.db`
	- into sqlite3 CLI run the following command:
	```
	CREATE TABLE NEWS_DATA(LINK TEXT PRIMARY KEY NOT NULL,ARTICLE_TEXT TEXT,ARTICLE_DATE TEXT,INSERTION_DATE TEXT);
	```
5. dateparser: python library to normalized article dates
6. urlparse: python libraray that can parse urls and return its componens e.g. base websites url,..etc.
7. user_agent: python library to randomize the user agents of ypur requests so the crawler don't get blocked or something, to install it `pip intstall user_agent`
8. multiprocessing: python library that returns the number of cores in your machine
9. newspaper_package_test: A python3 library that we are planning to use in future as it proved to be able to extract text from some articles that python-goose failed to retrieve but note that it is currently NOT USED: https://github.com/codelucas/newspaper/

### Project Directory files:
1. `toscrape.py`
- Core file of the NON-RSS-SITES crawler where the spiders are created and called 
- To run the crawler, write the following command in the terminal [scrapy runspider toscrape.py]
- To disable the automatic logs of the spider add option `--nolog`
- For more investigation regarding scrapy options use ```scrapy --help``` or `scrapy runspider --help`
2. `article_text_retrieval.py`: Contains a function that uses python-goose to extract arabic and english text using 'html parser' which is specified by default or 'soup' parser.
3. `date_retrieval_manually.py`: A file that contains some functions to try extract article dates specifically for the non-RSS websites list, it was developed using Beautifulsoup4 library.
4. `aricele_3k_extractor.py`: Contains code that runs the newspaper package [python3], it is ***NOT YET USED*** and need integration with `sqlite3` database

### Description:
- The part for the websites that don't have RSS feed support, An 8 scrapy spiders that crawl equal subsets of the NON_RSS websites that work concurrrently to make use of the #processrs of the machine. Note that you may adjust the crawler for your machine by inspecting the #cores in your machine then specify Number of spiders = Number of cores. Just comment the rest of Spider Classes and their calling. the crawler only scrape one level of the websites in other words it only scraps the home page for each website and only follow one level of links.

### Final Note:
- You can comment the code that writes in the news_db.db, and just write data in files by uncommenting the following section in each of the spiders class in toscrape.py file:
```python
with open("Parallel_Extracted_Urls/urls_0.txt", "a") as myfile:    
	print "LINK ADDED_0"
	myfile.write('{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri) + "," + response.url + "," + str(date)[0:10]+ "\r\n") 
```
