#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 16 16:44:34 2021

@author: Gustavo
"""

from amazonweb.amazonweb.spiders import petitspider
from GoogleCreds import GoogleSheets as gs
from app import AWSConnection as aws
import pandas as pd
from app import r, q
from rq.job import Job



COLUMNS = ['totalnopforkeyword', 'keywordname', 'score', 'matches', 'keywordid',
       'exactsuggestedbidmedian', 'avggiveaway', 'exactavgcpc',
       'exactsearchvolume', 'estimatedbroadsearchvolume', 'keywordcountry',
       'quarterlytrend', 'estimatedavggiveaway', 'easeofrankingscore',
       'broadsearchvolume', 'broadsuggestedbidmedian', 'keywordcategory',
       'monthlytrend', 'broadavgcpc', 'estimatedexactsearchvolume',
       'keyword_url', 'hasupdatedsearchvolume', 'hasupdatedcpc',
       'organicproductcount', 'sponsoredproductcount']

def scrapy_task(job_database_id):
    
    job_database = Job.fetch(job_database_id, connection=r)
    df = job_database.result

    
    print("Task in queue for {} new items".format(df.shape[0]))
    petitspider.main(df)
    gs.main()
    
def load_database(int_features):
    
    database = aws.DataBaseConnection()
    sql = '''SELECT products_database_table.totalNOPforKeyword, keyword_table.* FROM keyword_table 
                JOIN products_database_table ON products_database_table.parentkeyword = keyword_table.keywordname 
                WHERE easeOfRankingScore > {} AND exactSearchVolume > {} AND totalNOPforKeyword < {} AND totalNOPforKeyword > 9
                ORDER BY exactSearchVolume ASC;'''.format(int_features[0], int_features[1], int_features[2])
    
    database.cursor.execute(sql)
    x = database.cursor.fetchall()
    df = pd.DataFrame(data = x, columns=COLUMNS).drop_duplicates(subset=['keywordid'])
    print(f'Shape: {df.shape[0]}')
    return df