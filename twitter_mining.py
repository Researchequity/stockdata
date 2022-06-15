import tweepy
import pandas as pd
import icecream as ic
import os

consumer_key = '5jyzlKVX4JQcsZFmGyNvTSLFG'
consumer_secret = '9aorBUKETKQCuIAUKB4eyUtIUmIDUi58rezx2HUBrSOJRre7zy'
access_token = '1481203594833854465-SYAqrpIrLQLGGIqUH5T4z3s6dVtXWw'
access_token_secret = 'FrvsHvWqkSjEQTn62xhiiBTuCUFNtd42pcVhnIoQ1fBZS'

# online authentication
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

number_of_tweets = 100
tweets = []
likes = []
time = []
retweet = []
retweet_count = []
retweeted = []
retweets = []
id_num = []
user_name = []

user_list = ['nid_rockz', 'ValivetiMurali', 'Abhi4Research', 'abhishekcjain', '1shankarsharma', 'sahil_vi',
             'NiftyGranmaster']

# user_list = ['Alok_dharia']

# for user timeline tweets
for i in user_list:
    for i in tweepy.Cursor(api.user_timeline, id=i, tweet_mode='extended').items(number_of_tweets):
        tweets.append(i.full_text)
        likes.append(i.favorite_count)
        time.append(i.created_at)
        id_num.append(i.id)
        retweet.append(i.retweet_count)
        user_name.append(i.author.screen_name)
    df_to_csv = pd.DataFrame({'id': id_num, 'tweets': tweets, 'likes': likes, 'time': time, 'retweet': retweet,
                          'user_name': user_name})
    print(df_to_csv['tweets'].head(20))
    df_to_csv.to_csv(r'\\192.168.41.190\nilesh\Alok_dharia.csv', index=None)
    exit()
    if not os.path.exists(r'\\192.168.41.190\nilesh\twitter_tweets.csv'):
        df_to_csv.to_csv(r'\\192.168.41.190\nilesh\twitter_tweets.csv', index=None)
    else:
        tweets_df = pd.read_csv(r'\\192.168.41.190\nilesh\twitter_tweets.csv')
        df_csv = pd.concat([df_to_csv, tweets_df])
        df_csv = df_csv.drop_duplicates(subset=['id'])
        df_csv.to_csv(r'\\192.168.41.190\nilesh\twitter_tweets.csv', index=None)

# cursor = tweepy.Cursor(api.search_tweets, q='Bitcoin', tweet_mode='extended').items(1)
# for i in cursor:
#     print(i.full_text)