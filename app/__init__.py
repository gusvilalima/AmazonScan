#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 16 16:42:42 2021

@author: Gustavo
"""

from flask import Flask
from config import Config
import redis
from rq import Queue
import os


app = Flask(__name__)
app.config.from_object(Config)

r = redis.Redis()
#r = redis.from_url(os.environ['REDIS_URL'])
q = Queue(connection=r)


from app import routes
from app import tasks