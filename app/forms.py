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
    filter_2 = IntegerField('30-Day Search Volume', validators=[DataRequired()])
    filter_3 = IntegerField('Proxy Demand', validators=[DataRequired()])
    submit = SubmitField('Find Products')