#!/usr/bin/env python
# coding: utf-8

# # <font color = blue> JSON primer </font>

# In[14]:


#import json module
import json


# # <font color = blue> Twitter app creation </font>
# 

# In[15]:


#create the key and token variables and assign them values
consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''


# In[16]:


pip install tweepy


# In[17]:


#import tweepy
import tweepy


# # <font color = blue> MongoDB primer with pymongo

# In[18]:


pip install pymongo


# In[19]:


import pymongo as pm


# In[21]:


client = pm.MongoClient('localhost',27017)


# # <font color = blue>Downloading tweets with streaming API and saving to mongoDB

# <font color = purple> We will now use Twitter Streaming API ([Documentation](https://developer.twitter.com/en/docs/tweets/filter-realtime/overview)) to create a class StreamListener. It inherits from tweepy's StreamListener and adds definition to Method stubs to fit to our purpose.<br>
#     
# StreamListner also needs a client to save the twitter data. We will use mongodb as the client and create a database in it.<br><br>
#     
# you may find this URL useful https://github.com/tweepy/tweepy/blob/master/tweepy/streaming.py#L45   

# In[22]:


#now set authentication
#new method to authenticate
auth = tweepy.OAuth1UserHandler(consumer_key, consumer_secret)


# In[23]:


#note - wait on rate limit
api = tweepy.API(auth, wait_on_rate_limit = True)


# In[24]:


class myStreamListener(tweepy.Stream):
        def on_connect(self):
            # Called initially to connect to the Streaming API
            print("You are now connected to the streaming API.")
            print("------------------")
        
        def __init__(self, consumer_key, consumer_secret, access_token, access_token_secret):
            super(myStreamListener, self).__init__(consumer_key, consumer_secret, access_token, access_token_secret)
            self.num_tweets = 0
        
        def on_request_error(status_code):
        #def on_error(self, status_code):
            # On error - if an error occurs, display the error / status code
            print('An Error has occured: ' + repr(status_code))
            return False
    

        def on_data(self, data):
            #it connects to your mongoDB and stores the tweet
            try:
                client = pm.MongoClient('localhost',27017)

                #Use twitterdb database. If it doesn't exist, it will be created.
                db = client.cloudtweets

                # Decode the JSON from Twitter
                datajson = json.loads(data)

                #grab the 'created_at' data from the Tweet to use for display
                created_at = datajson['created_at']

                #print out a message to the screen that we have collected a tweet
                print("Tweet collected at " + created_at)
                print('Tweet Text: ' + datajson['text'])
                print('----------')
                

                #insert the data into the mongoDB into a collection called twitter_search
                #if twitter_search doesn't exist, it will be created.
                db.tweets.insert_one(datajson)
                
                self.num_tweets += 1
                print("Tweet # ", self.num_tweets)
                print("------------")
        
                #disocnnects the stream when desired counter is reached
                if self.num_tweets == 500000:
                    test = datajson['text']
                    self.disconnect()
                    return False
              
                
            except Exception as e:
                   print(e)
            
            


# In[25]:


keyword = ['#azure','#alicloud','#gcp']


# <font color = purple> https://boundingbox.klokantech.com/

# In[26]:


#the init method as per new versio needs the keys and tokens
streamer = myStreamListener(consumer_key = '',
                            consumer_secret = '',
                            access_token = '',
                            access_token_secret = '')

#you can add this line if you are filtering stream for a location
# print("Tracking location: " + str('Wales'))

print("Tracking tweets with the following keyword")

#to filter stream with a list of keywords - in this case only one keyword
streamer.filter(track=keyword, languages=['en'])

# you can add this line if you want to download tweets from a specific location - these cordinaets are for wales
#locations=[-5.4977,51.229,-2.7182,53.4774],


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


# In[169]:


import pandas as pd


# In[195]:


df = pd.read_csv('new_tweet_filter.csv')


# In[196]:


df.head()


# In[197]:


df = df.fillna("No Attributes")


# In[198]:


df.head()


# In[199]:


df.rename(columns = {'created_at':'Time_and_Date', 'entities.hashtags':'Hashtags', 'entities.user_mentions':'User_Mentions', 'extended_tweet.full_text':'Extended_Tweets', 'text':'Tweet', 'user.location':'Location'}, inplace = True)


# In[200]:


df.head()


# In[201]:


df['Hashtag'] = df['Tweet'].apply(lambda x: re.findall(r"#(\w+)", x))


# In[202]:


df.head()


# In[203]:


from nltk.corpus import stopwords
additional  = ['rt','rts','retweet']
swords = set().union(stopwords.words('english'),additional)


# In[204]:


df['Clean_Tweets'] = df['Tweet'].str.lower()          .str.replace('(@[a-z0-9]+)\w+',' ')          .str.replace('(http\S+)', ' ')          .str.replace('([^09a-z \t])',' ')          .str.replace(' +',' ')          .apply(lambda x: [i for i in x.split() if not i in swords])


# In[205]:


df.head()


# In[206]:


from nltk.stem import PorterStemmer
ps = PorterStemmer()
df['Stemmed'] = df['Clean_Tweets'].apply(lambda x: [ps.stem(i) for i in x if i != ''])


# In[207]:


df.head()


# In[218]:


additional2  = ['screen','name','id', 'id_str', 'indices', 'str']
swords = set().union(stopwords.words('english'),additional2)


# In[219]:


df['Clean_User_Mentions'] = df['User_Mentions'].str.lower()          .str.replace('(@[a-z0-9]+)\w+',' ')          .str.replace('(http\S+)', ' ')          .str.replace('([^09a-z \t])',' ')          .str.replace(' +',' ')          .apply(lambda x: [i for i in x.split() if not i in swords])


# In[223]:


df


# In[ ]:




