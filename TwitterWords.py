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


config = configparser.ConfigParser()
config.read('config.ini')
auth = tweepy.OAuthHandler(config['twitterauth']['c_key'],
                           config['twitterauth']['c_secret'])
auth.set_access_token(config['twitterauth']['a_token'],
                      config['twitterauth']['a_token_secret'])
api = tweepy.API(auth)


def all_same(items):
    """Checks to see if all the items in a list are the same"""
    return all(x == items[0] for x in items)


class TwitterStream(tweepy.StreamListener):
    """A twitter stream"""

    def on_status(self, status):
        """What to do on new status"""
        # Check if the tweet is a retweet, if yes, break, if no, continue
        print('_______________ New Tweet _______________')
        print(status.text)
        tokenized_tweet = status.text.split()
        if tokenized_tweet[0] == 'RT':
            print('Stopping')
            return True
        else:
            self.tweetops(status)

    def on_error(self, status_code):
        """What to do on error"""
        print('Error -- SC >> ' + str(status_code))
        if status_code == 420:
            print('Status Code - 420 - Returned when an application is being rate limited.')
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


    @staticmethod
    def tweetops(status):
        has_link = None
        print(status.text)
        tokenized_tweet = status.text.split()
        #Check to see if the tweet is a retweet and take out 'RT' related tokens
        print('Continuing')
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
        timestamp = arrow.get(status.created_at)


        conn = sqlite3.connect('TwitterWords.db')
        c = conn.cursor()

        # Add tweet to DB
        c.execute('INSERT INTO Tweets(tweet_text, tweet_id, timeoftweet, timezone, hashtags, has_link, tweet_length) VALUES (?, ?, ?, ?, ?, ?, ?)',
                  (' '.join(tokenized_tweet),
                   status.id_str,
                   str(timestamp),
                   '',
                   str(hashtags),
                   has_link,
                   len(' '.join(tokenized_tweet))))

        # Get data from words DB, either update count or add new word
        c.execute('SELECT * from Words')
        knownwords = dict(c.fetchall())
        print(knownwords)
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
                    c.execute('INSERT INTO Words(word, uses) VALUES(?, ?)',
                              (lowercase_word,
                               1))

        # Check for duplicates and resolve duplicate issues
        c.execute('SELECT word, COUNT(*) c FROM Words GROUP BY word HAVING c > 1')
        fetchalllist = c.fetchall()
        if len(fetchalllist) > 1:
            for word in fetchalllist:
                c.execute('SELECT * FROM Words WHERE word=?', (word[0], ))
                fetchlist = c.fetchall()
                if all_same(fetchlist):
                    c.execute('DELETE FROM Words WHERE word=?', (fetchlist[0][0], ))
                    c.execute('INSERT INTO Words (word, uses) VALUES(?, ?)',
                              (word[0],
                               word[1]))
                else:
                    print('not all same')

        conn.commit()
        c.close()
        conn.close()
        return True


def create_db():
    """Create an SQLite DB if it doesn't exist and add Tables if they don't exist"""
    conn = sqlite3.connect('TwitterWords.db')
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS Tweets(tweet_text TEXT, tweet_id TEXT, timeoftweet TEXT, timezone TEXT, hashtags TEXT, has_link INTEGER, tweet_length INTEGER)')
    c.execute('CREATE TABLE IF NOT EXISTS Words(word TEXT, uses INTEGER)')
    conn.commit()
    c.close()
    conn.close()


try:
    create_db()
    myStreamListener = TwitterStream()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
    myStream.filter(track=['python', 'machine'], languages=['en'])
except BaseException as e:
    print(e)
except requests.packages.urllib3.exceptions.ProtocolError as e:
    print(e)
    pass

