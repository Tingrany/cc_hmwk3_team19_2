from __future__ import print_function
# from elasticsearch import Elasticsearch, RequestsHttpConnection
# from requests_aws4auth import AWS4Auth
import glob
from csv import reader
import argparse
import json
import pprint
from botocore.vendored import requests
import sys
import urllib
import urllib.request
import boto3
from datetime import date
from time import mktime
import time
from urllib.parse import quote_plus
import csv, datetime
from boto3.dynamodb.conditions import Key


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
API_KEY= "QO1mvO3lzL14Zt6jpGF0OT8D87MB8is2iKS69acuAqcLC7anwazh97SpIP6I0ieE3PBPubaH0AvqjNppRS3hgGFmJIG9_dNazvbv1uhU52T93IsQZyAPFhMgIKq-WnYx" 


# API constants, you shouldn't have to change these.
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.



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


def search(api_key, term, location, unixTime, price):
    """Query the Search API by a search term and location.

    Args:
        term (str): The search term passed to the API.
        location (str): The search location passed to the API.

    Returns:
        dict: The JSON response from the request.
    """
    host = 'https://search-test-c4yyz42af2dgbospe67euc27um.us-east-1.es.amazonaws.com' # For example, my-test-domain.us-east-1.es.amazonaws.com

    
    query = '/predictions/_search?q=' + term
    url = host + query
    with urllib.request.urlopen(url) as url:
        res = url.read()
    #res = urllib.request.openurl(url).read()
    res = json.loads(res.decode())
    ans = []
    for item in res['hits']['hits'][:3]:
        ans.append(item['_source']['RestaurantID'])
    return ans
    


def get_business(api_key, business_id):
    """Query the Business API by a business ID.

    Args:
        business_id (str): The ID of the business to query.

    Returns:
        dict: The JSON response from the request.
    """
    business_path = BUSINESS_PATH + business_id

    return request(API_HOST, business_path, api_key)


def query_api(event, unixTime):
    """Queries the API by the input values from the user.

    Args:
        term (str): The search term to query.
        location (str): The location of the business to query.
    """

    term = event['RestaurantType']
    location = event['Location']

    businesses = search(API_KEY, term, location, unixTime, event['Price'])
    print("here is the response of elasticsearch:")
    print(businesses)

    # businesses = response.get('businesses')

    if not businesses:
        print(u'No businesses for {0} in {1} found.'.format(term, location))
        return
    output = "Hello! Here are my {0} restaurant suggestions for {1} people, for {2} at {3}: ".format(event['RestaurantType'], event['PeopleNum'], event['DiningDate'], event['DiningTime'])
    for i in range(len(businesses)):
        #business_id = businesses[0]['id']
        queryRes = csv_reader(businesses[i])[0]
        business_name = queryRes['Name']
        business_address = queryRes['Location'].strip("{}").split(":")[-1].replace("['", "")
        ########
        #alias = businesses[i]['categories'][0]['alias'].title()
        #expectPrice = businesses[i]['price']
        #########
        rate = queryRes['Rating']
        print(u'Result for business "{0}" found,'.format(business_name)+u'address is "{0}"'.format(business_address))
        output = output + "Restaurant {0}: {1}, rating: {2}, located at {3}. ".format(i+1, business_name, rate, str(business_address).split(",")[0].strip("['']") + "," + str(business_address).split(",")[1].replace("'", "")+","+str(business_address).split(",")[2].replace("']", ""))   
    print(output)
    return(output)
    

    # print(u'{0} businesses found, querying business info ' \
    #     'for the top result "{1}" ...'.format(
    #         len(businesses), business_id))
    #response = get_business(API_KEY, business_id)

    
    #pprint.pprint(response, indent=2)
    
def sqsMessage():
    sqs = boto3.client('sqs')  #create SQS client
    queue_url = 'https://sqs.us-east-1.amazonaws.com/952621368311/DiningSuggestion'
    #queue_url = 'https://sqs.us-east-1.amazonaws.com/153684740928/DiningSuggestion'

    #use url to receive message from SQS queue
    response_sqs = sqs.receive_message(
        QueueUrl = queue_url,
        AttributeNames= ['SentTimestamp'],
        MaxNumberOfMessages = 1,
        MessageAttributeNames=['All'],
        VisibilityTimeout = 0,
        WaitTimeSeconds = 20
        )
    print(response_sqs)
    event1 = response_sqs['Messages'][0]['Body']
    receipt_handle = response_sqs['Messages'][0]['ReceiptHandle']
    #This is Temp.
    response_sqs = sqs.delete_message(
        QueueUrl = queue_url,
        ReceiptHandle = receipt_handle
        )
    return event1
    #RestaurantType = response_sqs['Messages'][0]['Body']['RestaurantType']
    #Location = response_sqs['Messages'][0]['Body']['Location']
    #

    
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
    
def send_to_sns(message, phone_num):
    sns = boto3.client('sns')
    sns.publish(
        #TopicArn=message['topic'],
        #Subject=message['subject'],
        Message=message,
        PhoneNumber = phone_num
    )

    return ('Sent a message to an Amazon SNS topic.')
    
def csv_reader(target):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants8')
    response = table.query(
        KeyConditionExpression=Key('RestaurantID').eq(target)
        )
    items = response['Items']
    return(items)
    

def lambda_handler(event, context):

    try:
        event = json.loads(sqsMessage())
        #event['DiningDate'] = "2018-04-03"
        query_api(event, getLocalUnix(event))
        phone_num = event['PhoneNum']
        if len(phone_num) == 10:
            phone_num = "1" + phone_num
            print("append 1 to the head of the phone number")
        #This is Temp.
        send_to_sns(query_api(event, getLocalUnix(event)), phone_num)
    except HTTPError as error:
        sys.exit(
            'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
                error.code,
                error.url,
                error.read(),
            )
        )


        