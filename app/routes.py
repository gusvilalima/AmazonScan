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
    form = FilterForm()
    if form.validate_on_submit():
        int_features = [request.form[field] for field in filter(lambda a: a.startswith('filter'), dir(form))]
        #database = aws.DataBaseConnection()
        #sql = '''SELECT products_database_table.totalNOPforKeyword, keyword_table.* FROM keyword_table JOIN products_database_table ON products_database_table.parentkeyword = keyword_table.keywordname WHERE easeOfRankingScore > {} AND  exactSearchVolume > {} AND totalNOPforKeyword < {};'''.format(int_features[0], int_features[1], int_features[2])
        #database.cursor.execute(sql)
        #x = database.cursor.fetchall()
        #x = pd.read_sql_query(sql, database.connection).drop_duplicates(subset=['keywordid']).sort_values(by='exactsearchvolume', ascending=False)
        session['filters'] = int_features
        return redirect(url_for('tableproduct'))
        # petitspider.main(x[:100])
        # gs.main()
        #return render_template('indexes.html', output_text = 'Google sheet has been updated', form=form)
    else:
        flash('Inputs are not valid')
        return render_template('indexes.html', output_text = None, form=form)

@app.route('/table', methods=['GET', 'POST'])
def tableproduct():
    pageform = PageForm()
    database = aws.DataBaseConnection()
    if pageform.validate_on_submit():
        page = int(request.form['pagination'])
        int_features = session['filters']
        sql = '''SELECT products_database_table.totalNOPforKeyword, keyword_table.* FROM keyword_table JOIN products_database_table ON products_database_table.parentkeyword = keyword_table.keywordname WHERE easeOfRankingScore > {} AND exactSearchVolume > {} AND totalNOPforKeyword < {} ORDER BY exactSearchVolume DESC OFFSET {} ROWS FETCH NEXT 100 ROWS ONLY;'''.format(int_features[0], int_features[1], int_features[2], 100*(page-1))
        database.cursor.execute(sql)
        x = database.cursor.fetchall()
        df = pd.DataFrame(data = x, columns=COLUMNS).drop_duplicates(subset=['keywordid'])
        print('starting spider')
        try:
            petitspider.main(df)
            gs.main()
        except ValueError:
            return render_template('tableofproducts.html', output_text = 'Google sheet could not be updated. Try again later', form=pageform)
        return render_template('tableofproducts.html', output_text = 'Google sheet has been updated', form=pageform)
    else:
        return render_template('tableofproducts.html', output_text = None, form=pageform)

