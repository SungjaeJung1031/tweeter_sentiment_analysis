# set chdir to current dir
from textblob import TextBlob
import itertools
import pickle
import string
from collections import Counter
import regex as re
from config import stop_words
import pandas as pd
from threading import Lock, Timer
import time
from unidecode import unidecode
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import sqlite3
import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import os
import sys
sys.path.insert(0, os.path.realpath(os.path.dirname(__file__)))
os.chdir(os.path.realpath(os.path.dirname(__file__)))


analyzer = SentimentIntensityAnalyzer()

# consumer key, consumer secret, access token, access secret.
ckey = "H1uCbkmrRHCYfZJdss4sOiees"
csecret = "kRn2CSdozYb1yqLEkSdZ59XBjIAI9SV4EfJ9eHUER4TXe4jB3n"
atoken = "991302229972860930-6pcAUmE1tqwhvkn8ovCudchJLnCqk3x"
asecret = "fsmNVS2bcdEirXBSRZ45LdaA5Faq0XLeFvsuc4ofKLz5j"
# isolation lever disables automatic transactions,
# we are disabling thread check as we are creating connection here, but we'll be inserting from a separate thread (no need for serialization)
conn = sqlite3.connect('/home/sungjae/Documents/git/twitter-sentiment-analysis/twitter_live_bak.db',
                       isolation_level=None, check_same_thread=False)
c = conn.cursor()


def create_table():
    try:

        # http://www.sqlite.org/pragma.html#pragma_journal_mode
        # for us - it allows concurrent write and reads
        c.execute("PRAGMA journal_mode=wal")
        c.execute("PRAGMA wal_checkpoint=TRUNCATE")
        #c.execute("PRAGMA journal_mode=PERSIST")

        # changed unix to INTEGER (it is integer, sqlite can use up to 8-byte long integers)
        c.execute("CREATE TABLE IF NOT EXISTS sentiment(id INTEGER PRIMARY KEY AUTOINCREMENT, unix INTEGER, tweet TEXT, sentiment REAL)")
        # key-value table for random stuff
        c.execute(
            "CREATE TABLE IF NOT EXISTS misc(key TEXT PRIMARY KEY, value TEXT)")
        # id on index, both as DESC (as you are sorting in DESC order)
        c.execute("CREATE INDEX id_unix ON sentiment (id DESC, unix DESC)")
        # out full-text search table, i choosed creating data from external (content) table - sentiment
        # instead of directly inserting to that table, as we are saving more data than just text
        # https://sqlite.org/fts5.html - 4.4.2
        c.execute("CREATE VIRTUAL TABLE sentiment_fts USING fts5(tweet, content=sentiment, content_rowid=id, prefix=1, prefix=2, prefix=3)")
        # that trigger will automagically update out table when row is interted
        # (requires additional triggers on update and delete)
        c.execute("""
            CREATE TRIGGER sentiment_insert AFTER INSERT ON sentiment BEGIN
                INSERT INTO sentiment_fts(rowid, tweet) VALUES (new.id, new.tweet);
            END
        """)
    except Exception as e:
        print(str(e))


create_table()

# create lock
lock = Lock()


class listener(StreamListener):

    data = []
    lock = None

    def __init__(self, lock):

        # create lock
        self.lock = lock

        # init timer for database save
        self.save_in_database()

        # call __inint__ of super class
        super().__init__()

    def save_in_database(self):

        # set a timer (1 second)
        Timer(1, self.save_in_database).start()

        # with lock, if there's data, save in transaction using one bulk query
        with self.lock:
            if len(self.data):
                c.execute('BEGIN TRANSACTION')
                try:
                    c.executemany(
                        "INSERT INTO sentiment (unix, tweet, sentiment) VALUES (?, ?, ?)", self.data)
                except:
                    pass
                c.execute('COMMIT')

                self.data = []

    def on_data(self, data):
        try:
            # print('data')
            data = json.loads(data)
            # there are records like that:
            # {'limit': {'track': 14667, 'timestamp_ms': '1520216832822'}}
            if 'truncated' not in data:
                # print(data)
                return True
            if data['truncated']:
                tweet = unidecode(data['extended_tweet']['full_text'])
            else:
                tweet = unidecode(data['text'])
            time_ms = data['timestamp_ms']
            vs = analyzer.polarity_scores(tweet)
            sentiment = vs['compound']
            #print(time_ms, tweet, sentiment)

            # append to data list (to be saved every 1 second)
            with self.lock:
                self.data.append((time_ms, tweet, sentiment))

        except KeyError as e:
            # print(data)
            print(str(e))
        return True

    def on_error(self, status):
        print(status)


# make a counter with blacklist words and empty word with some big value - we'll use it later to filter counter
stop_words.append('')
blacklist_counter = Counter(dict(zip(stop_words, [1000000] * len(stop_words))))

# complie a regex for split operations (punctuation list, plus space and new line)
punctuation = [str(i) for i in string.punctuation]
split_regex = re.compile("[ \n" + re.escape("".join(punctuation)) + ']')


def map_nouns(col):
    return [word[0] for word in TextBlob(col).tags if word[1] == u'NNP']

# generate "trending"


def generate_trending():

    try:
        # select last 10k tweets
        df = pd.read_sql(
            "SELECT * FROM sentiment ORDER BY id DESC, unix DESC LIMIT 10000", conn)
        df['nouns'] = list(map(map_nouns, df['tweet']))

        # make tokens
        tokens = split_regex.split(' '.join(
            list(itertools.chain.from_iterable(df['nouns'].values.tolist()))).lower())
        # clean and get top 10
        trending = (Counter(tokens) - blacklist_counter).most_common(10)

        # get sentiments
        trending_with_sentiment = {}
        for term, count in trending:
            df = pd.read_sql(
                "SELECT sentiment.* FROM  sentiment_fts fts LEFT JOIN sentiment ON fts.rowid = sentiment.id WHERE fts.sentiment_fts MATCH ? ORDER BY fts.rowid DESC LIMIT 1000", conn, params=(term,))
            trending_with_sentiment[term] = [df['sentiment'].mean(), count]

        # save in a database
        with lock:
            c.execute('BEGIN TRANSACTION')
            try:
                c.execute("REPLACE INTO misc (key, value) VALUES ('trending', ?)",
                          (pickle.dumps(trending_with_sentiment),))
            except:
                pass
            c.execute('COMMIT')

    except Exception as e:
        with open('errors.txt', 'a') as f:
            f.write(str(e))
            f.write('\n')
    finally:
        Timer(5, generate_trending).start()


Timer(1, generate_trending).start()

while True:

    try:
        auth = OAuthHandler(ckey, csecret)
        auth.set_access_token(atoken, asecret)
        twitterStream = Stream(auth, listener(lock))
        twitterStream.filter(track=["a", "e", "i", "o", "u"])
    except Exception as e:
        print(str(e))
        time.sleep(5)


# def create_table():
#     try:
#         # (unix, tweet, sentiment, loc_pnt_long, loc_pnt_lat, lang, friends_cnt, followers_cnt, retweet_cnt)
#         c.execute(
#             "CREATE TABLE IF NOT EXISTS sentiment(unix REAL, tweet TEXT, sentiment REAL, sentiment_pos REAL, sentiment_neg REAL, sentiment_neu REAL, loc_pnt_long REAL, loc_pnt_lat REAL, lang TEXT, friends_cnt INT, followers_cnt INT, retweet_cnt INT)")
#         c.execute("CREATE INDEX fast_unix ON sentiment(unix)")
#         c.execute("CREATE INDEX fast_tweet ON sentiment(tweet)")
#         c.execute("CREATE INDEX fast_sentiment ON sentiment(sentiment)")
#         c.execute("CREATE INDEX fast_sentiment_pos ON sentiment(sentiment_pos)")
#         c.execute("CREATE INDEX fast_sentiment_neg ON sentiment(sentiment_neg)")
#         c.execute("CREATE INDEX fast_sentiment_neu ON sentiment(sentiment_neu)")
#         c.execute("CREATE INDEX fast_loc_pnt_long ON sentiment(loc_pnt_long)")
#         c.execute("CREATE INDEX fast_loc_pnt_lat ON sentiment(loc_pnt_lat)")
#         c.execute("CREATE INDEX fast_lang ON sentiment(lang)")
#         c.execute("CREATE INDEX fast_friends_cnt ON sentiment(friends_cnt)")
#         c.execute("CREATE INDEX fast_followers_cnt ON sentiment(follwers_cnt)")
#         c.execute("CREATE INDEX fast_retweet_cnt ON sentiment(retweet_cnt)")
#         conn.commit()
#     except Exception as e:
#         print(str(e))


# create_table()


# class listener(StreamListener):

#     def on_data(self, data):
#         try:
#             data = json.loads(data)

#             time_ms = data['timestamp_ms']
#             tweet = unidecode(data['text'])
#             vs = analyzer.polarity_scores(tweet)
#             sentiment = vs['compound']
#             sentiment_pos = vs['pos']
#             sentiment_neg = vs['neg']
#             sentiment_neu = vs['neu']
#             coordinate_obj = data['coordinates']
#             loc_pnt_long = 1024.0
#             loc_pnt_lat = 1024.0

#             if coordinate_obj != None:
#                 loc_pnt_long, loc_pnt_lat = data['coordinates']['coordinates']

#             lang = data['lang']
#             friends_cnt = data['user']['friends_count']
#             followers_cnt = data['user']['followers_count']
#             retweet_cnt = data['retweet_count']

#             print(time_ms, tweet, sentiment, loc_pnt_long, loc_pnt_lat, lang,
#                   friends_cnt, retweet_cnt)

#             c.execute("INSERT INTO sentiment (unix, tweet, sentiment, sentiment_pos, sentiment_neg, sentiment_neu, loc_pnt_long, loc_pnt_lat, lang, friends_cnt, followers_cnt, retweet_cnt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
#                       (time_ms, tweet, sentiment, sentiment_pos, sentiment_neg, sentiment_neu, loc_pnt_long, loc_pnt_lat, lang, friends_cnt, followers_cnt, retweet_cnt))

#             conn.commit()
#         except KeyError as e:
#             print(str(e))

#         return True

#     def on_error(self, status):
#         print(status)


# while True:

#     try:
#         auth = OAuthHandler(ckey, csecret)
#         auth.set_access_token(atoken, asecret)
#         twitterStream = Stream(auth, listener())
#         twitterStream.filter(track=["a", "e", "i", "o", "u"])
#     except Exception as e:
#         print(str(e))
#         time.sleep(5)
