import tweepy
import pandas as pd
import xlwings as xw

consumer_key = '5jyzlKVX4JQcsZFmGyNvTSLFG'
consumer_secret = '9aorBUKETKQCuIAUKB4eyUtIUmIDUi58rezx2HUBrSOJRre7zy'
access_token = '1481203594833854465-SYAqrpIrLQLGGIqUH5T4z3s6dVtXWw'
access_token_secret = 'FrvsHvWqkSjEQTn62xhiiBTuCUFNtd42pcVhnIoQ1fBZS'

# online authentication
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

number_of_tweets = 1000
tweets = []
likes = []
time = []
retweet = []
retweet_count = []
retweeted = []
retweets = []
id_num = []
user_name = []

data_excel_file = r"\\192.168.41.190\report\twitter_search.xlsm"
wb = xw.Book(data_excel_file)
wb_input = wb.sheets("Input")
wb_output = wb.sheets("TweetSearch")
wb_user = wb.sheets("userTimeline")
wb_app = wb.sheets("WhatsApp")
wb_output.clear()
wb_user.clear()
wb_app.clear()
query = wb.sheets("Input").range('B2').options(index=False).value


# to search for a specific keyword
search_tweets = []
for i in tweepy.Cursor(api.search_tweets, q=query + '-filter:retweets', tweet_mode='extended', lang="en").items(number_of_tweets):
    tweets.append(i.full_text)
    likes.append(i.favorite_count)
    time.append(i.created_at)
    id_num.append(i.id)
    retweet.append(i.retweet_count)
    user_name.append(i.author.screen_name)
df_to_csv = pd.DataFrame({'id': id_num, 'tweets': tweets, 'likes': likes, 'time': time, 'retweet': retweet,
                          'user_name': user_name})
wb_output.range("A1").options().value = df_to_csv

# search for user timeline
user_timeline = pd.read_csv(r'\\192.168.41.190\nilesh\twitter_tweets.csv')
dump = user_timeline[user_timeline['tweets'].str.contains(query)]
wb_user.range("A1").options().value = dump

# search for WhatsApp
whatsapp = pd.read_csv(r'\\192.168.41.190\nilesh\Whatsapp_Backup\wappp.csv', encoding='cp1252')
whatsapp = whatsapp[whatsapp['text'].notna()]
app_search = whatsapp[whatsapp['text'].str.contains(query)]
wb_app.range("A1").options().value = app_search
wb.save()
