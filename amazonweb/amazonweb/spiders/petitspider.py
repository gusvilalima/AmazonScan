#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 24 16:19:13 2021

@author: Gustavo
"""

import scrapy 
import pandas as pd
import numpy as np
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner

import os
from multiprocessing import Process, Queue

PATH = 'amazonweb/amazonweb/CSV/'


class AmazonSpider(scrapy.Spider): 
    
    name = 'amazon_spider'
    custom_settings = {'FEEDS':{PATH + 'amazonresultsweb.csv':{'format':'csv'}}}
    headers = {
           'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36',
           'accept': '*/*',
           'accept-encoding': 'gzip, deflate, br',
           'accept-language': 'en-US,en;q=0.9',
           'referer': 'https://www.amazon.com'
        }
    try:
        os.remove(PATH + 'amazonresultsweb.csv')
    except OSError:
        pass
    
    def parse(self,response):
        
        
        products = response.css('div.a-section.a-spacing-medium')
        for product in products:
            try:
                name = product.css('img').attrib['alt']
            except:
                name = 'Could not get name'
            try:
                link = product.css('img').attrib['src']
            except:
                link = 'Could not get link'
            finally:
                yield {
                    'name': name,
                    'link': link,
                    'url': response.request.meta['redirect_urls'][0]
                    }
        
class GoogleSpider(scrapy.Spider): 
    
    name = 'google_spider'
    custom_settings = {'FEEDS':{PATH + 'googleresultsweb.csv':{'format':'csv'}}}
    header = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
    'Accept-Encoding': 'none',
    'Accept-Language': 'en-US,en;q=0.8',
    'Connection': 'keep-alive',
    } 
    try:
        os.remove(PATH + 'googleresultsweb.csv')
    except OSError:
        pass
    
    def parse(self,response): 
        
        images = response.css('div.RAyV4b')
        for image in images:
            try:
                link = image.css('img').attrib['src']
            except:
                link = 'Could not get link'
            finally:
                yield {
                    'link': link,
                    'url': response.url,
                    }


def run_spider(spider_amazon, spider_google, start_amazon_urls, start_google_urls):

    q = Queue()
    p = Process(target=f, args=(q,spider_amazon, spider_google, start_amazon_urls, start_google_urls))
    p.start()
    result = q.get()
    p.join()

    if result is not None:
        raise result

def f(q, spider_amazon, spider_google, start_amazon_urls, start_google_urls):
    try:
        runner = CrawlerRunner()
        runner.crawl(spider_amazon, start_urls = start_amazon_urls)
        runner.crawl(spider_google, start_urls = start_google_urls)
        deferred = runner.join()
        deferred.addBoth(lambda _: reactor.stop())
        reactor.run()
        q.put(None)
    except Exception as e:
        q.put(e)
        
def string_to_amzonUrl(names):
    if isinstance(names, list) or isinstance(names, np.ndarray):
        return [string_to_amzonUrl(name) for name in names]
    #in_b = names.replace(' ', '-')
    names = names.replace(' ', '+').replace(',', '%2C')
    url = ''.join(['https://www.amazon.com/s?k=', names])
    return url

def string_to_googleUrl(names):
    if isinstance(names, list) or isinstance(names, np.ndarray):
        return [string_to_googleUrl(name) for name in names]
    Google_Image = 'https://www.google.com/search?site=&tbm=isch&source=hp&biw=1873&bih=990&q='
    names = names.replace(' ', '+').replace(',', '%2C')
    url = ''.join([Google_Image, names])
    return url

def main(filtered):
    
    amazon_urls = string_to_amzonUrl(filtered['keywordname'].values)
    google_urls = string_to_googleUrl(filtered['keywordname'].values)
    print('urls updated')
    run_spider(AmazonSpider, GoogleSpider, start_amazon_urls = amazon_urls, start_google_urls = google_urls)
    print('finished scraping images')
    try:
        images_amazon = pd.read_csv(PATH + 'amazonresultsweb.csv')
    except:
        raise SystemExit('Could not scrape amazon images')
    images_google = pd.read_csv(PATH + 'googleresultsweb.csv')
    url_dict_amazon = images_amazon.groupby(by = ['url'])['link'].groups
    url_dict_google = images_google.groupby(by = ['url'])['link'].groups
    image1 = []
    image2 = []
    image3 = []
    image4 = []
    image5 = []
    image6 = []
    for amazonurl, googleurl in zip(amazon_urls, google_urls):
        try:
            image1.append(images_amazon.iloc[url_dict_amazon[amazonurl][0], 1])
        except:
            image1.append('none')
        try:
            image2.append(images_amazon.iloc[url_dict_amazon[amazonurl][1], 1])
        except:
            image2.append('none')
        try:
            image3.append(images_amazon.iloc[url_dict_amazon[amazonurl][2], 1])
        except:
            image3.append('none')
        try:
            image4.append(images_google.iloc[url_dict_google[googleurl][0], 0])
        except:
            image4.append('none')
        try:
            image5.append(images_google.iloc[url_dict_google[googleurl][1], 0])
        except:
            image5.append('none')
        try:
            image6.append(images_google.iloc[url_dict_google[googleurl][2], 0])
        except:
            image6.append('none')
    #filtered['realimage'] = filtered['imageurl']
    #filtered.drop(columns=['imageurl'])
    filtered['im1'] = [''.join(['=Image(','"', x, '"', ',4,100,122)']) for x in image1]
    filtered['im2'] = [''.join(['=Image(','"', x, '"', ',4,100,122)']) for x in image2]
    filtered['im3'] = [''.join(['=Image(','"', x, '"', ',4,100,122)']) for x in image3]
    filtered['im4'] = [''.join(['=Image(','"', x, '"', ',4,100,122)']) for x in image4]
    filtered['im5'] = [''.join(['=Image(','"', x, '"', ',4,100,122)']) for x in image5]
    filtered['im6'] = [''.join(['=Image(','"', x, '"', ',4,100,122)']) for x in image6]
    filtered.to_csv(PATH + 'keyword_table_with_images.csv', index=False, sep= '\t')
        
