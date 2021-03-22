#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 27 14:59:42 2021

@author: Gustavo
"""



import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

scopes = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

credentials = Credentials.from_service_account_file(
    'GoogleCreds/googlecreds.json',
    scopes=scopes
)

WORKSHEET_ID = 682330983

def main():
    try:
        gc = gspread.authorize(credentials)
        print('Accessed Google Sheet')
    except:
        print('Access denied')

    request_body = {
        'requests': [
            {
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': WORKSHEET_ID,
                        'dimension': 'COLUMNS',
                        'startIndex': 25,
                        'endIndex': 30
                        },
                    'properties': {
                        'pixelSize': 122
                        },
                    'fields': 'pixelSize'
                    }    
                },
            {
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': WORKSHEET_ID,
                        'dimension': 'ROWS',
                        'startIndex': 1,
                        },
                    'properties': {
                        'pixelSize': 100
                        },
                    'fields': 'pixelSize'
                    }    
                }
            ]
     
     }
    spreadsheet_key = '1oC4U8EKnL0g2EBAVT9UV8SUvqaTHUlrjxPcY1R8RSzg'
    sh = gc.open_by_key(spreadsheet_key)
    worksheet = sh.get_worksheet(5)
    df = pd.read_csv('amazonweb/amazonweb/CSV/keyword_table_with_images.csv', sep = '\t')
    set_with_dataframe(worksheet, df)
    service = build('sheets', 'v4', credentials=credentials)
    response = service.spreadsheets().batchUpdate(
        spreadsheetId = spreadsheet_key,
        body = request_body
        ).execute()
    print('Google Sheet was updated {}, {}'.format(df.shape[0], df.shape[1]))
