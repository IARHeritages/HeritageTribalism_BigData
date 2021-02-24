### Data Export ###
## This file contains the codes used to export data from Mongo Database into csv files, for further analysis in R and Python

## Exports for Section 1. Data collection and processing

# 01_07 Export tweets:
mongoexport --db IARH --collection cheddarman_tweets --type=csv --fields _id,text  --out /home/mk69/cheddarman/data/tweets.csv

# 01_08 Export user data:
mongoexport --db IARH --collection cheddarman_users --type=csv --fields _id,text  --out /home/mk69/cheddarman/data/users.csv


## Export for Section 2. Summary statistics and data extraction for qualitative analysis

# 02_01 Export basic info about the dataset:
mongoexport --db IARH --collection cheddarman_info --type=csv --fields _id,value  --noHeaderLine --out /home/mk69/cheddarman/outputs/02_01_Dataset_information.csv

# 02_02 Export link data:

# Export overall link frequencies
mongoexport --db IARH --collection cheddarmanLinks --type=csv --fields _id,count,display_url,url --out /home/mk69/cheddarman/data/links.csv
# Export link frequencies by date
mongoexport --db IARH --collection cheddarmanLinksByDate --type=csv --fields '_id.id','_id.created_at','_id.link.expanded_url','_id.link.display_url','_id.link.url' --out /home/mk69/cheddarman/data/linksByDate.csv


# 02_04 Impact tweetsextraction:


#Extract the tweets that received more than 10 interactons - for visualisation of the pattern:
mongoexport --db IARH --collection cheddarman_tweetsImpact --type=csv --fields _id,text,total,retweet,quote,reply,anonymised_id,dominant_topic,description,location,url,lang,followers_count,friends_count,favourites_count,statuses_count --query '{"total":{"$gt":10}}'  --out /home/mk69/cheddarman/data/impactTweetsAll.csv
# Extract the tweets that received more than 50 interations for the qualitaitve analysis:
mongoexport --db IARH --collection cheddarman_tweetsImpact --type=csv --fields _id,text,total,retweet,quote,reply,anonymised_id,dominant_topic,description,location,url,lang,followers_count,friends_count,favourites_count,statuses_count --query '{"total":{"$gt":50}}'  --out /home/mk69/cheddarman/data/impactTweets.csv

# Extract the most interacted with tweets per topic:

mongoexport --db IARH --collection cheddarman_tweetsImpact --type=csv --fields _id,text,total,retweet,quote,reply,anonymised_id,dominant_topic,description,location,url,lang,followers_count,friends_count,favourites_count,statuses_count --query '{"dominant_topic":"Topic 1"}'  --out /home/mk69/cheddarman/data/impactTweetsT1.csv

mongoexport --db IARH --collection cheddarman_tweetsImpact --type=csv --fields _id,text,total,retweet,quote,reply,anonymised_id,dominant_topic,description,location,url,lang,followers_count,friends_count,favourites_count,statuses_count --query '{"dominant_topic":"Topic 2"}'  --out /home/mk69/cheddarman/data/impactTweetsT2.csv

mongoexport --db IARH --collection cheddarman_tweetsImpact --type=csv --fields _id,text,total,retweet,quote,reply,anonymised_id,dominant_topic,description,location,url,lang,followers_count,friends_count,favourites_count,statuses_count --query '{"dominant_topic":"Topic 3"}'  --out /home/mk69/cheddarman/data/impactTweetsT3.csv

mongoexport --db IARH --collection cheddarman_tweetsImpact --type=csv --fields _id,text,total,retweet,quote,reply,anonymised_id,dominant_topic,description,location,url,lang,followers_count,friends_count,favourites_count,statuses_count --query '{"dominant_topic":"Topic 4"}'  --out /home/mk69/cheddarman/data/impactTweetsT4.csv

mongoexport --db IARH --collection cheddarman_tweetsImpact --type=csv --fields _id,text,total,retweet,quote,reply,anonymised_id,dominant_topic,description,location,url,lang,followers_count,friends_count,favourites_count,statuses_count --query '{"dominant_topic":"Topic 5"}'  --out /home/mk69/cheddarman/data/impactTweetsT5.csv

mongoexport --db IARH --collection cheddarman_tweetsImpact --type=csv --fields _id,text,total,retweet,quote,reply,anonymised_id,dominant_topic,description,location,url,lang,followers_count,friends_count,favourites_count,statuses_count --query '{"dominant_topic":"Topic 6"}'  --out /home/mk69/cheddarman/data/impactTweetsT6.csv

mongoexport --db IARH --collection cheddarman_tweetsImpact --type=csv --fields _id,text,total,retweet,quote,reply,anonymised_id,dominant_topic,description,location,url,lang,followers_count,friends_count,favourites_count,statuses_count --query '{"dominant_topic":"Topic 7"}'  --out /home/mk69/cheddarman/data/impactTweetsT7.csv

mongoexport --db IARH --collection cheddarman_tweetsImpact --type=csv --fields _id,text,total,retweet,quote,reply,anonymised_id,dominant_topic,description,location,url,lang,followers_count,friends_count,favourites_count,statuses_count --query '{"dominant_topic":"Topic 8"}'  --out /home/mk69/cheddarman/data/impactTweetsT8.csv

mongoexport --db IARH --collection cheddarman_tweetsImpact --type=csv --fields _id,text,total,retweet,quote,reply,anonymised_id,dominant_topic,description,location,url,lang,followers_count,friends_count,favourites_count,statuses_count --query '{"dominant_topic":"Topic 9"}'  --out /home/mk69/cheddarman/data/impactTweetsT9.csv

# Extract the random sample of tweets not involved in any interactions:
mongoexport --db IARH --collection cheddarman_tweetsNoImpactSample --type=csv --fields _id,text,created_at,anonymised_id,description,location,url,lang,followers_count,friends_count,favourites_count,statuses_count --out /home/mk69/cheddarman/data/noImpactTweetsSample.csv

# 03_05_Peaks_extraction:
# Extract the csvs with the most commonly retweeted/quoted tweets on selected dates.
mongoexport --db IARH --collection cheddarman_2302 --type=csv --fields _id,created_at,anonymised_tweet_id,text,user.anonymised_id,user.description,total,retweeted_count,quoted_count,replies_count  --out /home/mk69/cheddarman/data/s2302.csv
mongoexport --db IARH --collection cheddarman_1902 --type=csv --fields _id,created_at,anonymised_tweet_id,text,user.anonymised_id,user.description,total,retweeted_count,quoted_count,replies_count  --out /home/mk69/cheddarman/data/s1902.csv
mongoexport --db IARH --collection cheddarman_0303 --type=csv --fields _id,anonymised_tweet_id,created_at,text,user.anonymised_id,user.description,total,retweeted_count,quoted_count,replies_count  --out /home/mk69/cheddarman/data/s0303.csv

### 04_01 Boundary markers extraction (extract some collecitons again, after the bounary markers have been identified within them)

mongoexport --db IARH --collection cheddarman_users --type=csv --fields _id,description,phrases,tribes,positions  --out /home/mk69/cheddarman/data/users.csv