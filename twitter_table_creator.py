from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import sqlite3
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from unidecode import unidecode
import time

analyzer = SentimentIntensityAnalyzer()

# consumer key, consumer secret, access token, access secret.
ckey = "H1uCbkmrRHCYfZJdss4sOiees"
csecret = "kRn2CSdozYb1yqLEkSdZ59XBjIAI9SV4EfJ9eHUER4TXe4jB3n"
atoken = "991302229972860930-6pcAUmE1tqwhvkn8ovCudchJLnCqk3x"
asecret = "fsmNVS2bcdEirXBSRZ45LdaA5Faq0XLeFvsuc4ofKLz5j"

conn = sqlite3.connect(
    '/home/sungjae/Documents/git/twitter-sentiment-analysis/twitter_live.db')
c = conn.cursor()


def create_table():
    try:
        # (unix, tweet, sentiment, loc_pnt_long, loc_pnt_lat, lang, friends_cnt, followers_cnt, retweet_cnt)
        c.execute(
            "CREATE TABLE IF NOT EXISTS sentiment(unix REAL, tweet TEXT, sentiment REAL, sentiment_pos REAL, sentiment_neg REAL, sentiment_neu REAL, loc_pnt_long REAL, loc_pnt_lat REAL, lang TEXT, friends_cnt INT, followers_cnt INT, retweet_cnt INT)")
        c.execute("CREATE INDEX fast_unix ON sentiment(unix)")
        c.execute("CREATE INDEX fast_tweet ON sentiment(tweet)")
        c.execute("CREATE INDEX fast_sentiment ON sentiment(sentiment)")
        c.execute("CREATE INDEX fast_sentiment_pos ON sentiment(sentiment_pos)")
        c.execute("CREATE INDEX fast_sentiment_neg ON sentiment(sentiment_neg)")
        c.execute("CREATE INDEX fast_sentiment_neu ON sentiment(sentiment_neu)")
        c.execute("CREATE INDEX fast_loc_pnt_long ON sentiment(loc_pnt_long)")
        c.execute("CREATE INDEX fast_loc_pnt_lat ON sentiment(loc_pnt_lat)")
        c.execute("CREATE INDEX fast_lang ON sentiment(lang)")
        c.execute("CREATE INDEX fast_friends_cnt ON sentiment(friends_cnt)")
        c.execute("CREATE INDEX fast_followers_cnt ON sentiment(follwers_cnt)")
        c.execute("CREATE INDEX fast_retweet_cnt ON sentiment(retweet_cnt)")
        conn.commit()
    except Exception as e:
        print(str(e))


create_table()


class listener(StreamListener):

    def on_data(self, data):
        try:
            data = json.loads(data)

            time_ms = data['timestamp_ms']
            tweet = unidecode(data['text'])
            vs = analyzer.polarity_scores(tweet)
            sentiment = vs['compound']
            sentiment_pos = vs['pos']
            sentiment_neg = vs['neg']
            sentiment_neu = vs['neu']
            coordinate_obj = data['coordinates']
            loc_pnt_long = 1024.0
            loc_pnt_lat = 1024.0

            if coordinate_obj != None:
                loc_pnt_long, loc_pnt_lat = data['coordinates']['coordinates']

            lang = data['lang']
            friends_cnt = data['user']['friends_count']
            followers_cnt = data['user']['followers_count']
            retweet_cnt = data['retweet_count']

            print(time_ms, tweet, sentiment, loc_pnt_long, loc_pnt_lat, lang,
                  friends_cnt, retweet_cnt)

            c.execute("INSERT INTO sentiment (unix, tweet, sentiment, sentiment_pos, sentiment_neg, sentiment_neu, loc_pnt_long, loc_pnt_lat, lang, friends_cnt, followers_cnt, retweet_cnt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                      (time_ms, tweet, sentiment, sentiment_pos, sentiment_neg, sentiment_neu, loc_pnt_long, loc_pnt_lat, lang, friends_cnt, followers_cnt, retweet_cnt))

            conn.commit()
        except KeyError as e:
            print(str(e))

        return True

    def on_error(self, status):
        print(status)


while True:

    try:
        auth = OAuthHandler(ckey, csecret)
        auth.set_access_token(atoken, asecret)
        twitterStream = Stream(auth, listener())
        twitterStream.filter(track=["a", "e", "i", "o", "u"])
    except Exception as e:
        print(str(e))
        time.sleep(5)
