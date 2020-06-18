from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import sqlite3
from textblob import TextBlob
from unidecode import unidecode
import time

conn = sqlite3.connect(
    '/home/sungjae/Documents/git/twitter-sentiment-analysis/twitter.db')
c = conn.cursor()


def create_table():
    c.execute(
        "CREATE TABLE IF NOT EXISTS sentiment(unix REAL, tweet TEXT, sentiment REAL)")
    c.execute("CREATE INDEX fast_unix ON sentiment(unix)")
    c.execute("CREATE INDEX fast_tweet ON sentiment(tweet)")
    c.execute("CREATE INDEX fast_sentiment ON sentiment(sentiment)")

    conn.commit()


create_table()


# consumer key, consumer secret, access token, access secret.
ckey = "H1uCbkmrRHCYfZJdss4sOiees"
csecret = "kRn2CSdozYb1yqLEkSdZ59XBjIAI9SV4EfJ9eHUER4TXe4jB3n"
atoken = "991302229972860930-6pcAUmE1tqwhvkn8ovCudchJLnCqk3x"
asecret = "fsmNVS2bcdEirXBSRZ45LdaA5Faq0XLeFvsuc4ofKLz5j"


class listener(StreamListener):

    def on_data(self, data):
        try:
            data = json.loads(data)
            tweet = unidecode(data["text"])
            time_ms = data['timestamp_ms']

            analysis = TextBlob(tweet)

            sentiment = analysis.sentiment.polarity

            print(time_ms, tweet, sentiment)

            c.execute("INSERT INTO sentiment (unix, tweet, sentiment) VALUES (?, ?, ?)",
                      (time_ms, tweet, sentiment))

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
        twitterStream.filter(track=["car"])
    except Exception as e:
        print(str(e))
        time.sleep(5)
