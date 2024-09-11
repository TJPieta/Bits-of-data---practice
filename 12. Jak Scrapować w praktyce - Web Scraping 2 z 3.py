import scrapy
import scrapy.crawler as crawler
from bs4 import BeautifulSoup
from scrapy.crawler import CrawlerProcess
import logging

def text_to_float(txt):
    return float(''.join(c for c in txt if c in [str(i) for i in range(20)]))

def flat_to_dict(soup):
    node = [flat for flat in soup.find_all('div', id = 'basic-info-price-row')][0]
    spans = node.find_all('span')
    no_spans = (len(spans)<2)
    next_data = node.next_sibling()
    locations = values = [flat for flat in soup.find_all('h2')][1]
    location = [n.text.strip().strip(',') for n in locations.find_all('span')]
    
    node_data = [flat for flat in soup.find_all('div', {'class': 'vZJg9t'}) if flat.span.text == 'Data dodania'][0]
    size = [flat for flat in soup.find_all('div', {'class': 'vZJg9t'}) if flat.span.text == 'Pow. całkowita'][0].find_all('div')[-1].text
    
    return{
        'price': '' if no_spans else text_to_float(spans[0].text),
        'price_per_metr': '' if no_spans else text_to_float(spans[1].text),
        'rooms': next_data[0].span.text,
        'floor': next_data[6].span.text,
        'location': ", ".join(location),
        'title': [flat for flat in soup.find_all('h1')][0].text,
        'text': "".join([i.text for i in [flat for flat in soup.find_all('h1')][0].next_sibling()]),
        'Date_add': node_data.find_all('div')[-1].text,
        "Date_update": node_data.next_sibling()[-1].text,
        'Size (m2)': size[:-3] if size[-2] == 'm' else size,
        
    }

results = []

class MySpider(scrapy.Spider):
    name = 'my_morizon_spider'
    start_urls = [
        'https://www.morizon.pl/mieszkania/krakow/?page=%d' %i for i in range(1,20)
    ]

    custom_settings = {
        'DOWNLOAD_DELAY': '4.0',
        'ROBOTSTXT_OBEY': True,
        'AUTOTHROTTLE_ENABLE': True,
        'USER_AGENT': 'My Morizon Demo Bot (qwerty@niepodam.pl)'
        }

    def __init__(self, *args, **kwargs):
        logger = logging.getLogger("scrapy.spidermiddlewares.httperror")
        logger.setLevel(logging.WARNING)
        logger = logging.getLogger("scrapy.score.engine")
        logger.setLevel(logging.WARNING)
        super().__init__(*args, **kwargs)

    def parse(self, response):
        self.logger.setLevel(logging.INFO)
        soup = BeautifulSoup(response.body, 'lxml')

        links = [link.get('href') for link in soup.find_all('a')]

        links = [ 'https://www.morizon.pl'+link for link in links if link.startswith('/oferta')]
        
        for item_url in links:
            yield scrapy.Request(item_url, self.parse_item)

    def parse_item(self, response):
        self.logger.info('Got successful response from {}'.format(response.url))
        soup = BeautifulSoup(response.body, 'lxml')
        data = flat_to_dict(soup)
        data['url'] = response.url
        results.append(data)


    def spider_closed(self, spider):
        try:
            from datetime import datetime
            
            spider.logger.info('Spider closed: %s', spider.name)
            now = datetime.now().strftime("%Y-%m-%d")
            spider.logger.info('Spider closed: %s', spider.name)
            df = pd.DataFrame(results)
            fname = f"data/morizon-{now}.csv"
            print(fname)
            spider.logger.info('Spider writing to: %s', fname)
            df.to_csv(fname)
        except Exception as e:
            print(e)



#%%

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

process = CrawlerProcess()
process.crawl(MySpider)
process.start()

#%%

import pandas as pd

df = pd.DataFrame.from_records(results)

from datetime import datetime

now = datetime.now().strftime("%Y-%m-%d")
fname = f"data/morizon-{now}.csv"
fname

df.to_csv(fname)