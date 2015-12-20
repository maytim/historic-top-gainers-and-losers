# imports
import time
import json
import requests
from decimal import *
from datetime import date, datetime, timedelta
from heapq import nsmallest, nlargest
from operator import itemgetter

class sector:
    # Constructor
    # Accepts a dictionary of the sector mapping the industry IDs to industry names
    def __init__(self,sector_dict, start, end):
        # initialize returns data
        self.initialize_returns(start,end)
        # get a list of the keys (IDs) of the industries
        industry_ids = list(sector_dict.keys())
        # load the industry data
        self.data_scope = self.get_industry_data(industry_ids)
        # add the names to the industries
        self.name_industries(sector_dict)
        # load returns data
        self.get_returns_data(start,end)


    def get_industry_data(self, industry_list, result=None):
        result = {}

        query_data = self.yahoo_finance_industry_query(industry_list)

        # set up the sector by grabbing the companies for each industry
        for industry in query_data['query']['results']['industry']:
            id = industry['id']
            result[id] = {}
            result[id]['name'] = ""
            result[id]['companies'] = []
            result[id]['quotes'] = {}
            if 'company' in industry:
                for company in industry['company']:
                    result[id]['companies'].append(company['symbol'])
        return result

    def yahoo_finance_query(self, query):
        query_start = 'https://query.yahooapis.com/v1/public/yql?q='
        query_end = '&format=json&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback='
        url = query_start + query + query_end
        return requests.get(url).json()

    def yahoo_finance_industry_query(self,industries):
        # generate the list of industries for the yahoo query
        query = '('
        for i in industries:
            query += str(i)+'%2C'
        query = query[:-3] + ')'
        return self.yahoo_finance_query('select%20*%20from%20yahoo.finance.industry%20where%20id%20in%20'+query)

    def yahoo_finance_historical_query(self, company, start, end):
        query_1 = "select%20*%20from%20yahoo.finance.historicaldata%20where%20symbol%20%3D%20%22"
        query_2 = "%22%20and%20startDate%20%3D%20%22"
        query_3 = "%22%20and%20endDate%20%3D%20%22"
        query_4 = "%22"
        return self.yahoo_finance_query(query_1 + company + query_2 + start + query_3 + end + query_4)

    # a quick function to label the industries in the sector
    def name_industries(self, sector_dict):
        for key in self.data_scope:
            self.data_scope[key]['name'] = sector_dict[int(key)]

    # a quick function to save the scraped data to a .txt
    def save_to_txt(self, filename):
        pass
        ### TODO
        #with open(str(filename)+'.txt','w') as outfile:
            #json.dump(self.data,outfile)

    def get_returns_data(self, start, end):
        # get each industry within the sector database
        for key in self.data_scope:
            # get the industry data with the key
            industry = self.data_scope[key]

            # get the list of companies within each industry
            for company in industry['companies']:

                raw_data = self.yahoo_finance_historical_query(company,start,end)

                # check that the quote data was correctly loaded
                # a possiblity for an error could be lack of data for stock X in the given timeframe
                if raw_data['query']['results'] is not None:
                    # parse the 'raw_data' to get the quotes for each day
                    for data in raw_data['query']['results']['quote']:
                        # for the case where a single quote is returned then don't iterate through the list
                        if isinstance(data,str):
                            data = raw_data['query']['results']['quote']
                        # cache the relevant data
                        date = data['Date']
                        price_open = data['Open']
                        price_close = data['Close']
                        # calculate the daily return
                        # Note: used Decimal() in order to get precise results that can be rounded to 2 places
                        price_return = round((Decimal(price_close)-Decimal(price_open))/Decimal(price_open) * 100,2)

                        # check if return needs to be generated for date and then add company
                        if price_return not in self.returns[date]:
                            self.returns[date][price_return] = []
                        self.returns[date][price_return].append(company)

    # helper function to generate empty dictionaries of dates (designed for gainers / losers)
    def initialize_returns(self,start,end):
        # takes dates (start, stop) in format '%Y-%m-%d'
        start_date = datetime.strptime(start,'%Y-%m-%d')
        end_date = datetime.strptime(end,'%Y-%m-%d')
        self.returns = {}

        # iterate through dates from start to end
        current = start_date
        while current <= end_date:
            # create keys in the dict for each day initialized with []
            self.returns[current.strftime('%Y-%m-%d')] = {}
            current += timedelta(days=1)

    def get_gainers(self,date,count):
        return nlargest(count, self.returns[date].items())

    def get_losers(self,date,count):
        return nsmallest(count, self.returns[date].items())
