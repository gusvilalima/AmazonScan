#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 16 19:27:14 2021

@author: Gustavo
"""

from flask_wtf import FlaskForm
from wtforms import IntegerField, SubmitField
from wtforms.validators import DataRequired

class FilterForm(FlaskForm):
    filter_1 = IntegerField('Ease to Rank', validators=[DataRequired()])
    filter_2 = IntegerField('Ease to Rank', validators=[DataRequired()])
    filter_2 = IntegerField('30-Day Search Volume', validators=[DataRequired()])
    filter_3 = IntegerField('Proxy Supply', validators=[DataRequired()])
    submit = SubmitField('Find Products')
    
class PageForm(FlaskForm):
    pagination = IntegerField('Page', validators=[DataRequired()])
    submit = SubmitField('Save to Google Sheets')
    
class FilterForm2(FlaskForm):
    _filter = {}
    _filter['Ease of Ranking Score'] = {'from': IntegerField('ERS_start', validators=[DataRequired()]),
                                        'to': IntegerField('ERS_end')}
    _filter['30-Day Search Volume'] = {'from': IntegerField('SV_start', validators=[DataRequired()]),
                                        'to': IntegerField('SV_end')}
    _filter['Proxy Supply'] = {'from': IntegerField('PS_start', validators=[DataRequired()]),
                                        'to': IntegerField('PS_end')}
    _filter['Categories'] = {'from': IntegerField('PS_start', validators=[DataRequired()]),
                                        'to': IntegerField('PS_end')} 