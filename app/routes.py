#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 16 16:44:34 2021

@author: Gustavo
"""

import numpy as np
import pandas as pd
from flask import render_template, request, flash, redirect, session, url_for, session
from app import app
from app.forms import FilterForm, PageForm
from app import AWSConnection as aws
import psycopg2
from amazonweb.amazonweb.spiders import petitspider
from GoogleCreds import GoogleSheets as gs
from app import r
from app import q
from app.tasks import scrapy_task, load_database
from rq.job import Job
import time


COLUMNS = ['totalnopforkeyword', 'keywordname', 'score', 'matches', 'keywordid',
       'exactsuggestedbidmedian', 'avggiveaway', 'exactavgcpc',
       'exactsearchvolume', 'estimatedbroadsearchvolume', 'keywordcountry',
       'quarterlytrend', 'estimatedavggiveaway', 'easeofrankingscore',
       'broadsearchvolume', 'broadsuggestedbidmedian', 'keywordcategory',
       'monthlytrend', 'broadavgcpc', 'estimatedexactsearchvolume',
       'keyword_url', 'hasupdatedsearchvolume', 'hasupdatedcpc',
       'organicproductcount', 'sponsoredproductcount']


@app.route('/', methods=['GET', 'POST'])
def productresearch():
    output_text = None
    form = FilterForm()
    if form.validate_on_submit():
        int_features = [request.form[field] for field in filter(lambda a: a.startswith('filter'), dir(form))]
        job_database = Job.create(load_database, id = 'loading_database', result_ttl = 600, args=(int_features,), connection = r)
        q.enqueue_job(job_database)
        job_image = Job.create(scrapy_task, id = 'scrape_images', depends_on = job_database.id, args=(job_database.id,), connection = r)
        q.enqueue_job(job_image)
        print(f'number of tasks in queue: {len(q)}')
        output_text = 'Google sheet is being updated with new items'
        
   
    return render_template('indexes.html', output_text = output_text, form=form)

# @app.route('/table', methods=['GET', 'POST'])
# def tableproduct():
#     output_text = None
#     pageform = PageForm()
#     # database = aws.DataBaseConnection()
#     if pageform.validate_on_submit():
#         page = int(request.form['pagination']) 
#         int_features = session['filters']
        
#         #sql = '''SELECT products_database_table.totalNOPforKeyword, keyword_table.* FROM keyword_table JOIN products_database_table ON products_database_table.parentkeyword = keyword_table.keywordname WHERE easeOfRankingScore > {} AND exactSearchVolume > {} AND totalNOPforKeyword < {} ORDER BY exactSearchVolume DESC FETCH NEXT 500 ROWS ONLY;'''.format(int_features[0], int_features[1], int_features[2])
        
        
#         #sql = '''SELECT products_database_table.totalNOPforKeyword, keyword_table.* FROM keyword_table JOIN products_database_table ON products_database_table.parentkeyword = keyword_table.keywordname WHERE easeOfRankingScore > {} AND exactSearchVolume > {} AND totalNOPforKeyword < {} ORDER BY exactSearchVolume DESC OFFSET {} ROWS FETCH NEXT 200 ROWS ONLY;'''.format(int_features[0], int_features[1], int_features[2], 200*(page-1))
#         # database.cursor.execute(sql)
#         # x = database.cursor.fetchall()
#         # df = pd.DataFrame(data = x, columns=COLUMNS).drop_duplicates(subset=['keywordid'])
#         job_database = Job.create(load_database, id = 'loading_database', result_ttl = 600, args=(int_features,), connection = r)
#         q.enqueue_job(job_database)
#         #print(job_database.result.shape[0])
#         #while True:
#         #    if (job_database.get_status() == 'finished'):
#         #        break
#         job_image = Job.create(scrapy_task, id = 'scrape_images', depends_on = job_database.id, args=(job_database.id,), connection = r)
#         # while job_image.args[0] is None:
#         #     #print('The arg for the job is null')
#         #     time.sleep(0.1)
#         #     job_image = Job.create(scrapy_task, id = 'scrape_images', depends_on = job_database.id, args=(job_database.result,), connection = r)
#         q.enqueue_job(job_image)
#         # for i in range(0, df.shape[0], 500):
#         #     end = min(df.shape[0], i+500)
#         #     job = q.enqueue(, df[i:end])
#         print(len(q))
#         output_text = 'Google sheet is being updated with new items'
#         # try:
#         #     petitspider.main(df)
#         #     gs.main()
#         # except ValueError:
#         #     return render_template('tableofproducts.html', output_text = 'Google sheet could not be updated. Try again later', form=pageform)

    
#     return render_template('tableofproducts.html', output_text = output_text, form=pageform)






