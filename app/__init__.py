#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 16 16:42:42 2021

@author: Gustavo
"""

from flask import Flask
from config import Config

app = Flask(__name__)
app.config.from_object(Config)

from app import routes