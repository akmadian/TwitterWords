# -*- coding: utf-8 -*-
"""
    File Name: main.py
    Author: Ari Madian
    Created: August 10, 2017 9:09 PM
    Python Version: 3.6

    main.py - Part of TwitterWords
    Repo: github.com/akmadian/TwitterWords


    Data to gather -

    status.id
    status.text
    status.time_zone
    status.created_at
    status.entities['hashtags']

"""

import arrow
import tweepy
import configparser
import sqlite3
import requests.packages.urllib3.exceptions
from timezonefinder import TimezoneFinder


config = configparser.ConfigParser()
config.read('config.ini')
auth = tweepy.OAuthHandler(config['twitterauth']['c_key'],
                           config['twitterauth']['c_secret'])
auth.set_access_token(config['twitterauth']['a_token'],
                      config['twitterauth']['a_token_secret'])
api = tweepy.API(auth)


def tweetops(status):
    has_link = None
    # print(status)

    tokenized_tweet = status.text.split()
    tf = TimezoneFinder()

    # Check to see if the tweet is a retweet and take out 'RT' related tokens
    tokenized_tweet = tokenized_tweet[2:] \
        if tokenized_tweet[0] == 'RT' \
        else tokenized_tweet

    # Check to see if any hashtags and make a list of them
    hashtags = [word for word in tokenized_tweet if word[0] == '#']

    # Check to see if tweet has link
    for word in tokenized_tweet:
        if word[:4] == 'http':
            has_link = 1
            break
        else:
            has_link = 0

    # Tweet Timestamp
    timestamp = arrow.get(status.created_at).to('US/Pacific')
    conn = sqlite3.connect('TwitterWords.db')
    c = conn.cursor()

    # Add tweet to DB
    c.execute(
        'INSERT INTO Tweets(tweet_text, tweet_id, timeoftweet, timezone, hashtags, has_link, tweet_length, coordinates) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        (' '.join(tokenized_tweet),
         status.id_str,
         str(timestamp),
         tf.timezone_at(lat=status.coordinates['coordinates'][1], lng=status.coordinates['coordinates'][0]),
         str(hashtags),
         has_link,
         len(' '.join(tokenized_tweet)),
         str(status.coordinates['coordinates'])))
    print('inserted')

    # Get data from words DB, either update count or add new word
    c.execute('SELECT * from Words')
    knownwords = dict(c.fetchall())
    # print(knownwords)
    for word in tokenized_tweet:
        if word[0] == '#' or word[:4] == 'http' or word.isalpha() is False:
            pass
        else:
            lowercase_word = word.lower()
            if lowercase_word in knownwords:
                c.execute('UPDATE Words SET uses=? WHERE word=?',
                          (knownwords[lowercase_word] + 1,
                           lowercase_word))
            else:
                # print('Unknown Word')
                c.execute('INSERT INTO Words(word, uses) VALUES(?, ?)',
                          (lowercase_word,
                           1))

    # Check for duplicates and resolve duplicate issues
    c.execute('SELECT word, COUNT(*) c FROM Words GROUP BY word HAVING c > 1')
    fetchalllist = c.fetchall()
    if len(fetchalllist) > 1:
        for word in fetchalllist:
            c.execute('SELECT * FROM Words WHERE word=?', (word[0],))
            fetchlist = c.fetchall()
            if all(x == fetchlist[0] for x in fetchlist):
                c.execute('DELETE FROM Words WHERE word=?', (fetchlist[0][0],))
                c.execute('INSERT INTO Words (word, uses) VALUES(?, ?)',
                          (word[0],
                           word[1]))
            else:
                pass
                # print('not all same')

    conn.commit()
    c.close()
    conn.close()
    print('_' * 30)
    return True


def create_db():
    """Create an SQLite DB if it doesn't exist and add Tables if they don't exist"""
    conn = sqlite3.connect('TwitterWords.db')
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS Tweets(tweet_text TEXT, tweet_id TEXT, timeoftweet TEXT, timezone TEXT, hashtags TEXT, has_link INTEGER, tweet_length INTEGER, coordinates TEXT)')
    c.execute('CREATE TABLE IF NOT EXISTS Words(word TEXT, uses INTEGER)')
    conn.commit()
    c.close()
    conn.close()


class TwitterStream(tweepy.StreamListener):
    """A twitter stream"""

    def on_status(self, status):
        """What to do on new status"""
        if status.coordinates is not None and 'RT' not in status.text:
            print(status.coordinates)
            tweetops(status)


    def on_error(self, status_code):
        """What to do on error"""
        print('Error -- SC >> ' + str(status_code))
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False
        if status_code == 406:
            print('Status Code - 406 - Returned when an invalid format is specified in the request.')
            return True

    def on_exception(self, exception):
        """What to do on exception"""
        print('Exception -- ' + str(exception))
        if 'Connection broken' in str(exception):
            print('Connection broken exception')
            pass
        return True


try:
    create_db()
    myStreamListener = TwitterStream()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
    myStream.filter(locations=[-180,-90,180,90], languages=['en'])
except BaseException as e:
    print(e)
except requests.packages.urllib3.exceptions.ProtocolError as e:
    print(e)
    pass

