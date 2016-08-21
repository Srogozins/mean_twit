#!/usr/bin/env python
# Usage: ./twitget.py twitget.ini

import configparser
import time
import sys

from pymongo import MongoClient
import TwitterSearch

import requests
import logging
logging.basicConfig(level=logging.DEBUG)

DEFAULT_INTERVAL = 15 * 60
DEFAULT_LANGUAGE = 'en'

def get_tweets(tweets_col, config):
    try:
        newest_tweet = tweets_col.find_one({}, {'id': True}, sort=[('id', -1)])
        if newest_tweet is None:
            newest_id = int(config['query']['default_since_id'])
        else:
            newest_id = newest_tweet['id']

        tso = TwitterSearch.TwitterSearchOrder()
        tso.set_keywords(config['query']['keywords'].split(','), bool(config['query']['or']))
        tso.set_language(DEFAULT_LANGUAGE)
        tso.set_include_entities(False)
        tso.set_since_id(newest_id)

        ts = TwitterSearch.TwitterSearch(consumer_key=config['auth']['consumer_key'],
                                         consumer_secret=config['auth']['consumer_secret'],
                                         access_token=config['auth']['access_token'],
                                         access_token_secret=config['auth']['access_token_secret'],
                                         verify=True)
        tweets_col.insert_many(ts.search_tweets_iterable(tso))
    except TwitterSearch.TwitterSearchException as e:
        print(e)

def init_config(config_path):
    config = configparser.ConfigParser()
    config.read(config_path)
    if not config.has_option('query', 'interval'):
        config.set('query', 'interval', DEFAULT_INTERVAL)

    return config

if __name__ == '__main__':
    config = init_config(sys.argv[1])
    client = MongoClient(config['db']['host'], int(config['db']['port']))
    db = client.twit

    while True:
        get_tweets(db.tweets, config)
        time.sleep(int(config['query']['interval']))
