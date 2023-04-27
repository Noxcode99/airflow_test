import tweepy
import pandas as pd 
import json 
from datetime import datetime
import s3fs


def run_twitter_etl():
        
        
        access_key = "cjJ6HQBOnZJJvTBeLKzg7Iqzp"
        access_secret = "2GiykZl4YTBAiyfmNIuUBggsNxopGb7nS8lWH0AkEeGdvmqZk2"
        consumer_key = "1298817809049219072-tL54EZdcdCmhTXSMmjVuHGS4kCQXQl"
        consumer_secret = "uXOk00epG0mSnxEYLhnJm5nsYT4FH8USXvWBhTi3wFstg"

        #Twitter authentication (This will create a connection between my code and twitter api )
        auth = tweepy.OAuthHandler(access_key, access_secret)
        auth.set_access_token(consumer_key, consumer_secret)

        #creating an API object
        api = tweepy.API(auth)

        tweets = api.user_timeline(screen_name='@elonmusk',
                                #  200 is the maximum allowed scan 
                                count=200,
                                include_rts = False,
                                # Necessary to Keep full_text
                                # otherwise only the first 140 words are 
                                tweet_mode = 'extended'
                                )
        tweet_list = []
        for tweet in tweets:
                text = tweet._json['full_text']

                refined_tweet = {"user": tweet.user.screen_name,
                                'text' : text,
                                'favorite_count' : tweet.favorite_count,
                                'retweet_count' : tweet.retweet_count,
                                'created_at' : tweet.created_at}
                
                tweet_list.append(refined_tweet)

        df = pd.DataFrame(tweet_list)
        df.to_csv("s3://gourab-airflow-test-bucket/elonmusk_twitter_data.csv")