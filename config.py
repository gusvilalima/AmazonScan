#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 16 18:29:03 2021

@author: Gustavo
"""

import os

class Config(object):
    SECRET_KEY = os.environ.get('SECRET_KEY') or b'_5#y2L"F4Q8z\n\xec]/'