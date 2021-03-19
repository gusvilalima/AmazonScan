#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 18:36:13 2021

@author: Gustavo
"""

import psycopg2
import pandas as pd
import numpy as np
import ujson

class DataBaseConnection:
    def __init__(self):
        try:
            self.connection =  psycopg2.connect(
                host = 'junglescout.cusvdvuwccev.eu-west-3.rds.amazonaws.com',
                port = 5432,
                user = 'gusvila',
                password = 'Panther1.',
                database='PostGresJSS'
                )
            self.cursor= self.connection.cursor()  
            print('Successfully connected to the database!')
        except:
            print('Couldnt connect to the database')
    
    def create_keyword_table(self):
        try:
            create_command =  """CREATE TABLE keyword_table(
            keywordName TEXT NOT NULL PRIMARY KEY,
            score FLOAT8,
            matches FLOAT8,
            keywordId TEXT NOT NULL,
            exactSuggestedBidMedian FLOAT8,
            avgGiveaway FLOAT8,
            exactAvgCpc FLOAT8,
            exactSearchVolume FLOAT8,
            estimatedBroadSearchVolume FLOAT8,
            keywordCountry VARCHAR(10),
            quarterlyTrend FLOAT8,
            estimatedAvgGiveaway FLOAT8,
            easeOfRankingScore FLOAT8,
            broadSearchVolume FLOAT8,
            broadSuggestedBidMedian FLOAT8,
            keywordCategory TEXT,
            monthlyTrend FLOAT8,
            broadAvgCpc FLOAT8,
            estimatedExactSearchVolume FLOAT8,
            keyword_url TEXT,
            hasUpdatedSearchVolume BOOLEAN,
            hasUpdatedCpc BOOLEAN,
            organicProductCount FLOAT8,
            sponsoredProductCount FLOAT8 );"""
            
            self.cursor.execute(create_command)
            self.connection.commit()
            print('Table created succesfully!')
        except Exception as error:
            print('Error trying to create table: {}'.format(error))

        #self.connection.close()

        #self.cursor.close()
    #   cur.executemany(
    # """INSERT INTO "%s" (data) VALUES (%%s)""" % (args.tableName),rows)  
        
    
    
    
    def create_products_database_table(self):
        try:
            create_command =  """CREATE TABLE products_database_table(
                 productName TEXT NOT NULL, 
                 productId TEXT NOT NULL PRIMARY KEY,
                 nReviews FLOAT8, 
                 estimatedSales FLOAT8, 
                 productCountry TEXT, 
                 weight FLOAT8, 
                 weightUnit TEXT,
                 state TEXT, 
                 apiUpdatedAt FLOAT8, 
                 imageUrl TEXT, 
                 fees FLOAT8, 
                 productSubCategory TEXT,
                 productSubCategories TEXT,
                 width FLOAT8, 
                 dimensions TEXT,
                 dimensionUnit TEXT,
                 categoryNullifiedAt FLOAT8,
                 estRevenue FLOAT8,
                 scrapedAt TEXT, 
                 productRating FLOAT8,
                 tier TEXT, 
                 hasVariants FLOAT8,
                 rawCategory TEXT,
                 sellerName TEXT, 
                 nSellers FLOAT8,
                 dimensionValuesDisplayData TEXT,
                 productCategory TEXT, 
                 isUnavailable FLOAT8, 
                 listingQualityScore FLOAT8, 
                 sellerType TEXT,
                 listedAt FLOAT8,
                 estimatedListedAt TEXT,
                 length FLOAT8,
                 noParentCategory FLOAT8,
                 isSharedBSR FLOAT8, 
                 color TEXT, 
                 calculatedCategory TEXT, 
                 asin TEXT,
                 brand TEXT, 
                 scrapedParentAsin TEXT,
                 multipleSellers FLOAT8, 
                 productRank FLOAT8, 
                 pageAsin TEXT,
                 height FLOAT8,  
                 price FLOAT8,
                 apiCategory TEXT,
                 net FLOAT8,
                 feeBreakdown TEXT, 
                 variantAsinsCount FLOAT8,
                 sampleVariants TEXT, 
                 product_url TEXT, 
                 bsr_product FLOAT8,
                 hasRankFromApi FLOAT8,
                 currency_code TEXT,  
                 parentKeyword TEXT NOT NULL,
                 totalNOPforKeyword INT NOT NULL,
                 FOREIGN KEY (parentKeyword) REFERENCES keyword_table (keywordName));"""
            
            self.cursor.execute(create_command)
            self.connection.commit()
            print('Table created succesfully!')
        except Exception as error:
            print('Error trying to create table: {}'.format(error))    
    
    
    
        
    def insert_rec(self):
        try:
            command_line = '''INSERT INTO keyword_table(name, age) VALUES ('bana', 10.8);'''
            self.cursor.execute(command_line)
            self.connection.commit()
        except Exception as error:
            print('Error occured: {}'.format(error))
    
    
    def insert_new_record_from_csv(self, table_name, path_csv):
        try:
            #with open(path_csv, 'r') as csv_file:
            #    self.cursor.copy_from(csv_file, table_name, sep='\t', null='')
            with open(path_csv, 'r') as csv_file:
                command_line = '''COPY {} FROM STDIN WITH (FORMAT CSV, HEADER false, DELIMITER '\t', NULL '', ENCODING 'utf-8'); '''.format(table_name)
                self.cursor.copy_expert(command_line, csv_file)
                self.connection.commit()
            print('Values inserted in {}'.format(table_name))
        except Exception as error:
            print('Error trying to insert values: {}'.format(error))
            
    def join_js_tables(self):
        command_line = '''SELECT * FROM products_database_table JOIN keyword_table ON products_database_table.parentkeyword = keyword_table.keywordname;'''
        self.cursor.execute(command_line)
        return self.cursor.fetchall()
        
    def query_data(self, table_name):
        command_line = '''SELECT * FROM {} WHERE keywordName = 'face masks skincare';'''.format(table_name)
        self.cursor.execute(command_line)
        return self.cursor.fetchall()
    
    def delete_table(self, table_name):
        try:
            command_line = '''DROP TABLE {}'''.format(table_name)
            self.cursor.execute(command_line)
            self.connection.commit()
            print('Table {} deleted'.format(table_name))
        except:
            pass
    
    def close(self):
        if self.cursor is not None:
            self.cursor.close()
        self.connection.close()
        

FILTERS = {
    'NbrReviews' : lambda df: (df['nreviews'] < 5),
    'Weight': lambda df: (df['weight'] >= 2.5),
    'Country': lambda df: (df['productcountry'] == 'us'),
    'Price': lambda df: (df['price'] >= 15) & (df['price'] <= 200),
    'Nsellers': lambda df: (df['nsellers'] == 1),
    'SellerType': lambda df: (df['sellertype'] == 'FBA'),
    'ExactSearchVolume': lambda df: (df['exactsearchvolume'] <= 100),
    'EasetoRank': lambda df: (df['easeofrankingscore'] >= 75),
    'TotalNOP': lambda df: (df['totalnopforkeyword'] <= 20)  
}

def filter_dataframe(dataframe, dicFilter):
    # df = dataframe[dicFilter['NbrReviews'](dataframe) & dicFilter['Weight'](dataframe) & dicFilter['Country'](dataframe)
    #                 & dicFilter['Price'](dataframe) & dicFilter['Nsellers'](dataframe) & dicFilter['SellerType'](dataframe)
    #                  & dicFilter['TotalNOP'](dataframe)]
    df = dataframe[dicFilter['ExactSearchVolume'](dataframe) & dicFilter['EasetoRank'](dataframe)
                    & dicFilter['TotalNOP'](dataframe)]
    return df       

def string_to_amzonUrl(names):
    if isinstance(names, list) or isinstance(names, np.ndarray):
        return [string_to_amzonUrl(name) for name in names]
    #in_b = names.replace(' ', '-')
    names = names.replace(' ', '+').replace(',', '%2C')
    url = ''.join(['https://www.amazon.com/s?k=', names])
    return url

def string_to_googleUrl(names):
    if isinstance(names, list) or isinstance(names, np.ndarray):
        return [string_to_googleUrl(name) for name in names]
    Google_Image = 'https://www.google.com/search?site=&tbm=isch&source=hp&biw=1873&bih=990&q='
    names = names.replace(' ', '+').replace(',', '%2C')
    url = ''.join([Google_Image, names])
    return url
      
        
if __name__ == "__main__":
    database = DataBaseConnection()
    #database.create_keyword_table()
    #database.create_products_database_table()
    
    #database.delete_table('product_database_table')
    #database.delete_table('keyword_table')
    #database.create_keyword_table()
    #database.create_product_database_table()
    #database.insert_rec()
    #database.insert_new_record_from_csv('keyword_table','/Users/Gustavo/.spyder-py3/keywords_0_to_160.csv')
    database.insert_new_record_from_csv('products_database_table','/Users/Gustavo/.spyder-py3/products_106500_to_120500.csv')
    #x = database.query_data('keyword_table')
    #sql = '''SELECT * FROM products_database_table JOIN keyword_table ON products_database_table.parentkeyword = keyword_table.keywordname;'''
    #x = pd.read_sql_query(sql, database.connection)
    #f = filter_dataframe(x, FILTERS)
    #y = database.query_data('product_database_table')
    database.close()
    