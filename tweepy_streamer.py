#Copied from: https://www.youtube.com/watch?v=wlnx-7cm4Gg
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import twitterAuth

class TwitterStreamer():
    def stream_tweets(self, found_tweets):
        listener = StdOutListener(found_tweets)
        auth = OAuthHandler(twitterAuth.CONSUMER_KEY, twitterAuth.CONSUMER_SECRET)
        auth.set_access_token(twitterAuth.ACCESS_TOKEN, twitterAuth.ACCESS_TOCKEN_SECRET)

        stream = Stream(auth, listener)
        stream.filter(locations = [-125.214491, 24.086284, -60.351216, 49.524991])

class StdOutListener(StreamListener):
    def __init__(self,found_tweets):
        self.found_tweets = found_tweets

    def on_data(self,data):
        try:
            with open(self.found_tweets , 'a') as tf:
                tf.write(data)
            return True
        except BaseException as e:
            print("Error on_data : %s" % str(e))
        return True

    def on_error(self,status):
        print(status)

if __name__ == "__main__":
    found_tweets = "foundTweets.json"
    twitter_streamer = TwitterStreamer()
    twitter_streamer.stream_tweets(found_tweets)
