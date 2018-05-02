
# coding: utf-8

# In[122]:


from __future__ import print_function

import argparse
import json
import pprint
from botocore.vendored import requests
import sys
import urllib
import boto3
from datetime import date
from time import mktime
import time
from urllib.parse import quote_plus
import datetime
import pandas as pd
import os


# This client code can run on Python 2.x or 3.x.  Your imports can be
# simpler if you only need one of those.
try:
    # For Python 3.0 and later
    from urllib.error import HTTPError
    from urllib.parse import quote
    from urllib.parse import urlencode
except ImportError:
    # Fall back to Python 2's urllib2 and urllib
    from urllib2 import HTTPError
    from urllib import quote
    from urllib import urlencode


# Yelp Fusion no longer uses OAuth as of December 7, 2017.
# You no longer need to provide Client ID to fetch Data
# It now uses private keys to authenticate requests (API Key)
# You can find it on
# https://www.yelp.com/developers/v3/manage_app
API_KEY= "9qjWx4wu4uhmju3vIFMHtSqVSUaRsVCN5hcpR16_hCHIuCf8uFViu07uvvwhm5tLCeuQJayXdO0pyLhfgxwjlBHuLx6JEYq3GXiiybMb2T05inwg-5Wm87DaWp7fWnYx"

# API constants, you shouldn't have to change these.
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.

MAX_BUSINESSES = 1000


def request(host, path, api_key, url_params=None):
    """Given your API_KEY, send a GET request to the API.

    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        API_KEY (str): Your API Key.
        url_params (dict): An optional set of query parameters in the request.

    Returns:
        dict: The JSON response from the request.

    Raises:
        HTTPError: An error occurs from the HTTP request.
    """
    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }

    print(u'Querying {0} ...'.format(url))

    response = requests.request('GET', url, headers=headers, params=url_params)

    return response.json()


def search(api_key, event, offset = 0, dataset = []):
    """Query the Search API by a search term and location.

    Args:
        term (str): The search term passed to the API.
        location (str): The search location passed to the API.

    Returns:
        dict: The JSON response from the request.
    """
    term = event['RestaurantType']
    location = event['Location']
    limitPerRequest = 50
    
    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': limitPerRequest   #SEARCH_LIMIT
    }
    
    if (offset):
        url_params["offset"] = offset
    
    response = request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)
    businesses = response.get('businesses')
    if businesses:
        dataset += businesses
        offset += len(businesses)
        if (len(dataset) <= MAX_BUSINESSES and len(businesses) == limitPerRequest):
            search(api_key, event, offset = offset, dataset = dataset)
        else:
            print("Stopped collecting businesses, API doesn\'t have any more.")
    return (dataset)

def get_business(api_key, business_id):
    """Query the Business API by a business ID.

    Args:
        business_id (str): The ID of the business to query.

    Returns:
        dict: The JSON response from the request.
    """
    business_path = BUSINESS_PATH + business_id

    return request(API_HOST, business_path, api_key)


def query_api(event):
    """Queries the API by the input values from the user.

    Args:
        term (str): The search term to query.
        location (str): The location of the business to query.
    """
    output = search(API_KEY, event)

#     if not businesses:
#         print(u'No businesses for {0} in {1} found.'.format(term, location))
#         return
    
    print(len(output))       
    print(output)
    for i in range(len(output)):
        a = event["RestaurantType"]
        b = output[i]['review_count']
        c = output[i]['rating']
        d = output[i]['id']
        e = output[i]['name']
        f = output[i]['location']
        g = output[i]['coordinates']
    
        df = pd.DataFrame({'Cuisine':[a], "NumberOfReviews":[b], "Rating":[c], "RestaurantID":[d], "Name":[e], "Location":[f], "Coordinates":[g]})
        # if file does not exist write header 
        if not os.path.isfile('FILE.csv'):
            with open('FILE.csv', 'a') as f:
                df.to_csv(f, header=True, index=False)
        else: # else it exists so append without writing the header
            with open('FILE.csv', 'a') as f:
                df.to_csv(f, header=False, index=False)
        
    
    # print(u'{0} businesses found, querying business info ' \
    #     'for the top result "{1}" ...'.format(
    #         len(businesses), business_id))
    #response = get_business(API_KEY, business_id)

    
    #pprint.pprint(response, indent=2)


    
def getLocalUnix(event):
    #calculate the dining time in local Unix format
    time = event['DiningDate'].split("-")
    start = date(int(time[0]), int(time[1]), int(time[2]))
    dayTime = mktime(start.timetuple())  #loss 4 hours, 14400 seconds should be added
    hm = event['DiningTime'].split(":")
    local_unixTime = round(int(dayTime) + int(hm[0])*3600 + int(hm[1])*60 + 14400) #This is local Unix time
    #local_unixTime = 1522595640
    print(local_unixTime)
    return local_unixTime
    
    

def lambda_handler():

    try:
        event = {
            "DiningDate": "2018-04-25",
            "DiningTime": "16:00",
            "RestaurantType": "Vietnamese",
            "PeopleNum": 5,
            "Taboo": "None",
            "Location": "Manhattan",
            "PhoneNum": "1234567890"      
        }
        query_api(event)
        
        
    except HTTPError as error:
        sys.exit(
            'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
                error.code,
                error.url,
                error.read(),
            )
        )

lambda_handler()

