#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 16 16:44:34 2021

@author: Gustavo
"""

import numpy as np
import pandas as pd
from flask import render_template, request, flash, redirect, session, url_for
from app import app
from app.forms import FilterForm
from app import AWSConnection as aws
import psycopg2
from amazonweb.amazonweb.spiders import petitspider
from GoogleCreds import GoogleSheets as gs


@app.route('/', methods=['GET', 'POST'])
def productresearch():
    form = FilterForm()
    if form.validate_on_submit():
        int_features = [request.form[field] for field in filter(lambda a: a.startswith('filter'), dir(form))]
        database = aws.DataBaseConnection()
        sql = '''SELECT products_database_table.totalNOPforKeyword, keyword_table.* FROM keyword_table JOIN products_database_table ON products_database_table.parentkeyword = keyword_table.keywordname WHERE easeOfRankingScore > {} AND  exactSearchVolume > {} AND totalNOPforKeyword < {};'''.format(int_features[0], int_features[1], int_features[2])
        x = pd.read_sql_query(sql, database.connection).drop_duplicates(subset=['keywordid'])
        petitspider.main(x[:100])
        gs.main()
        return render_template('indexes.html', output_text = 'Google sheet has been updated', form=form)
    else:
        flash('Inputs are not valid')
        return render_template('indexes.html', output_text = None, form=form)



