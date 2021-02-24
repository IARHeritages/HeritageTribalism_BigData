// Extract tweets from metadata
// Metadata from the streamed tweets that were retweets, qoutes or replies to other tweets also included information about the original tweets they referred to.
// This code extract those tweets, and puts them into a one collection with originally streamed tweets.

// Enter the database
use IARH

// Define the collection

collection="cheddarman"

// Get the tweets from the metadata, and extract the text, creation data, and id of all the tweets into a separate colleciton
//Loop over all the tweets
db[collection].find().forEach(function(document){ 
    //Iterate over each level of the tweet:
    var levels = ["",'quoted_status','retweeted_status','retweeted_status.quoted_status']
    for (i in levels){
        level = levels[i]
        if(level==""){var doc = document}
        else if(document['retweeted_status']!=null && level=='retweeted_status.quoted_status'){
            var doc = document['retweeted_status']['quoted_status']
            level=level+"."}
        else{var doc = document[level]
            level=level+"."
            }
        //If the level exists, try to insert the the key data about the tweets to a separate collection, called 'cheddarman_tweets'
        if(doc!=null){
            var id = doc['anonymised_tweet_id']
            var user = doc['user']['anonymised_id']
            var extended_tweet = doc['extended_tweet']
            if(extended_tweet!=null){var text = doc['extended_tweet']['full_text']}
            else{var text = doc['text']}
            var created_at = doc['created_at']
            try {db[collection + "_tweets"].insertOne({"_id": id, "text":text,"user":user,"created_at":created_at})}
            catch (e) {print (e)}
        }
    }
 })
