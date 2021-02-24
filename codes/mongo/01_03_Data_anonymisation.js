// Data anonymisation
// This code anonymises the user identifiers and tweet ids in the collection with cheddarman tweets

// Enter the database
use IARH 

// Anonymise user data:
// Create a collection to store anonymised user ids with a unique index on the user_id field:

db.tweetsUsers.createIndex( { "user_id": 1 }, { unique: true } )

// Assigne the collection to the variable for easier code reproducibility:
collection="cheddarman"

// Iterate through all the documents in the collection to anonymise them:
db[collection].find().noCursorTimeout().forEach(function(document){
    var id = document['_id']
    print("Procession document "+id)
    // Tweets that are retweets or quotes also contain metadata about the original tweets they refer to,
    // So go through each level of the tweet if they exists.
    var levels = ["",'quoted_status','retweeted_status','retweeted_status.quoted_status'] //define levels
    for (i in levels){ //iterate through all the levels to anonymise them
        level = levels[i]
        if(level==""){var doc = document}
        else if(document['retweeted_status']!=null && level=='retweeted_status.quoted_status'){
            var doc = document['retweeted_status']['quoted_status']
            level=level+"."}
        else{var doc = document[level]
            level=level+"."}      
        if(doc!=null){ //check if the level exists
            // If the level exists anonymise data within it
            // (Here's where the actual anonymisation begins)
            
            // Anonymises the author od the tweet:
            var user_id = doc['user']['id_str'] //Get user id
            var handle = doc['user']['screen_name'] //Get handle
            var name = doc['user']['name'] //Get name  
            //Try to update the document with user_id in the newly created tweetsUsers collection, and if it does not exists, create it:
            try{db.tweetsUsers.updateOne({"user_id":user_id},{$addToSet:{"handle":handle,"name":name}},{ upsert: true} )}catch(e){print(e)}
            //Use the _id from the new collection as an anonymised user id
            var anonymised_id = db.tweetsUsers.findOne({"user_id":user_id})['_id'] // Get the _id from the new collection
            //This anonymises the author of the tweet inside the user's subfield
            db[collection].updateOne({"_id":id},{$set:{[level + "user.anonymised_id"]:anonymised_id}}) // Use the _id from above as anonymised_id
            
            // If the tweet was a reply, also anonymises the author of the original tweet
            var user_id = doc['in_reply_to_user_id_str'] //get the id of the original tweer
            var handle=doc['in_reply_to_screen_name'] //get the handle of the original tweet
            if(handle != null){ //checks if tweet was a reply
                // If tweet was a reply, anonymise data related to the original tweet
                try{db.tweetsUsers.updateOne({"user_id":user_id},{$addToSet:{"handle":handle}},{ upsert: true} )}catch(e){print(e)}
                var anonymised_id = db.tweetsUsers.findOne({"user_id":user_id})['_id']
                db[collection].updateOne({"_id":id},{$set:{[level+"in_reply_to_anonymised_id"]:anonymised_id}})
            }
            
            // If the tweet mentioned other twitter users, anonymise those as well
            var user_ids = doc['entities']['user_mentions'] // Get the list of all users mentioned in the texts of the tweets
            var text = doc['text'] // Get the text of the tweets
            // If tweet text was not stored in the field text, get it from the full_text field.
            // (full_text was a new field introduced after Twitter expanded the maximum number of characters in the tweet):
            if(text==null){var text = doc['full_text']}
            // Iterate over all user mentions in the user_ids list and anonymise them within the entities field and within text:
            for (i=0; i < user_ids.length; i++){
                var user_id = (user_ids[i]['id_str']) // Get user id
                var handle = (user_ids[i]['screen_name']) // Get screen name
                var name = (user_ids[i]['name']) // Get name
                // Try to insert new user data into the collection with anonymised ids
                try{db.tweetsUsers.updateOne({"user_id":user_id},{$addToSet:{"handle":handle,"name":name}},{ upsert: true} )}catch(e){print(e)}
                var anonymised_id = db.tweetsUsers.findOne({"user_id":user_id})['_id'] // Get the anonymised id for the user
                // Anonymise the entities field:
                db[collection].updateOne({"_id":id},{$set:{[level+"entities.user_mentions."+i+".anonymised_id"]:anonymised_id}})
               // Identify the handle in the tweet's text and substitute it with the anonymised id:
                var text = text.replace(handle,anonymised_id)       
            }
            // Update the text in the collection after iterating over all the user mentions and replacing the handle in the text with anonymised ids:
            // Check if the text is in the text field or full_text field
            if(doc.text != null){db[collection].updateOne({"_id":id},{$set:{[level+"text"]:text}})}
            if(doc.full_tweet != null){db[collection].updateOne({"_id":id},{$set:{[level+"full_text"]:text}})}
            
            // Repeat the same procedure as above for the extended entities field, if it exists:
            if(doc.extended_tweet != null){
                var user_ids = doc['extended_tweet']['entities']['user_mentions']
                var text = doc['extended_tweet']['full_text']
                for (i=0; i < user_ids.length; i++){
                    var user_id = (user_ids[i]['id_str'])
                    var handle = (user_ids[i]['screen_name'])
                    var name = (user_ids[i]['name'])
                    try{db.tweetsUsers.updateOne({"user_id":user_id},{$addToSet:{"handle":handle,"name":name}},{ upsert: true} )}catch(e){print(e)}
                    var anonymised_id = db.tweetsUsers.findOne({"user_id":user_id})['_id']
                    db[collection].updateOne({"_id":id},{$set:{[level+"extended_tweet.entities.user_mentions."+i+".anonymised_id"]:anonymised_id}})
                    var text = text.replace(handle,anonymised_id)
                }
                db[collection].updateOne({"_id":id},{$set:{[level+"extended_tweet.full_text"]:text}})
            }
            //Check if the tweet had contributors, and if so also anonymise them
            if(doc.contributors != null){
                var contributors = doc['contributors']
                for (i=0; i < contributors.length; i++){
                    try{db.tweetsUsers.insertOne({"user_id":contributors[i]})}catch(e){print(e)}
                    var anonymised_id = db.tweetsUsers.findOne({"user_id":contributors[i]})['_id']
                    db[collection].updateOne({"_id":id},{$addToSet:{[level+"anonymised_contributors"]:anonymised_id}})
                }
            }                   
        }
    }
})

//Delete the orginal user ids, names and handle from the cheddarman collection:

db[collection].updateMany({},
    //Delete the field on the top level of the tweet
   { $unset: { "user.id": "", "user.id_str": "","user.name":"","user.screen_name":"",
     "in_reply_to_screen_name":"","in_reply_to_user_id":"","in_reply_to_user_id_str":"",
    //Delete user data from the qouted status
     "quoted_status.user.id": "", "quoted_status.user.id_str": "","quoted_status.user.name":"","quoted_status.user.screen_name":"",
     "quoted_status.in_reply_to_screen_name":"","quoted_status.in_reply_to_user_id":"","quoted_status.in_reply_to_user_id_str":"",
    // Delete user data from the retweeted status
     "retweeted_status.user.id": "", "retweeted_status.user.id_str": "","retweeted_status.user.name":"","retweeted_status.user.screen_name":"",
     "retweeted_status.in_reply_to_screen_name":"","retweeted_status.in_reply_to_user_id":"","retweeted_status.in_reply_to_user_id_str":"",
    // Delete user data from the quoted status within the retweeted status
    "retweeted_status.quoted_status.user.id": "", "retweeted_status.quoted_status.user.id_str": "","retweeted_status.quoted_status.user.name":"","retweeted_status.quoted_status.user.screen_name":"",
    "retweeted_status.quoted_status.in_reply_to_screen_name":"","retweeted_status.quoted_status.in_reply_to_user_id":"","retweeted_status.quoted_status.in_reply_to_user_id_str":""
    } }
)

// Delete the user ids, names and handles from the mentions fields 
// (go through that 100 times, to make sure none are left - none of the tweets would have more than 100 mentions
for (i=0; i <100; i++){
print(i)
db[collection].updateMany({},
    {$unset:{//Uset from the top level of the tweet
             ["entities.user_mentions."+i+".screen_name"]:"",["entities.user_mentions."+i+".name"]:"",
             ["entities.user_mentions."+i+".id_str"]:"",["entities.user_mentions."+i+".id"]:"",
             ["extended_tweet.entities.user_mentions."+i+".screen_name"]:"",["extended_tweet.entities.user_mentions."+i+".name"]:"",
             ["extended_tweet.entities.user_mentions."+i+".id_str"]:"",["extended_tweet.entities.user_mentions."+i+".id"]:"",
             //Unset from the quoted status:
             ["quoted_status.entities.user_mentions."+i+".screen_name"]:"",["quoted_status.entities.user_mentions."+i+".name"]:"",
             ["quoted_status.entities.user_mentions."+i+".id_str"]:"",["quoted_status.entities.user_mentions."+i+".id"]:"",
             ["quoted_status.extended_tweet.entities.user_mentions."+i+".screen_name"]:"",["quoted_status.extended_tweet.entities.user_mentions."+i+".name"]:"",
             ["quoted_status.extended_tweet.entities.user_mentions."+i+".id_str"]:"",["quoted_status.extended_tweet.entities.user_mentions."+i+".id"]:"",
             //Unset from the retweeted status:
             ["retweeted_status.entities.user_mentions."+i+".screen_name"]:"",["retweeted_status.entities.user_mentions."+i+".name"]:"",
             ["retweeted_status.entities.user_mentions."+i+".id_str"]:"",["retweeted_status.entities.user_mentions."+i+".id"]:"",
             ["retweeted_status.extended_tweet.entities.user_mentions."+i+".screen_name"]:"",["retweeted_status.extended_tweet.entities.user_mentions."+i+".name"]:"",
             ["retweeted_status.extended_tweet.entities.user_mentions."+i+".id_str"]:"",["retweeted_status.extended_tweet.entities.user_mentions."+i+".id"]:"",
             //Unset from the quoted within the retweeted status:
             ["retweeted_status.quoted_status.entities.user_mentions."+i+".screen_name"]:"",["retweeted_status.quoted_status.entities.user_mentions."+i+".name"]:"",
             ["retweeted_status.quoted_status.entities.user_mentions."+i+".id_str"]:"",["retweeted_status.quoted_status.entities.user_mentions."+i+".id"]:"",
             ["retweeted_status.quoted_status.extended_tweet.entities.user_mentions."+i+".screen_name"]:"",["retweeted_status.quoted_status.extended_tweet.entities.user_mentions."+i+".name"]:"",
             ["retweeted_status.quoted_status.extended_tweet.entities.user_mentions."+i+".id_str"]:"",["retweeted_status.quoted_status.extended_tweet.entities.user_mentions."+i+".id"]:""             
    }}
  )
}


// Tweet ids anonymisation
// Create a collection to store anonymised tweet ids with the unique index on the id field:

db.tweetsIDs.createIndex( { "id": 1 }, { unique: true } )

// Loop over all the tweets in the collection, on all levels to anonymise them:
db[collection].find().noCursorTimeout().forEach(function(document){
    var id = document['_id']
    print("Procession document: " +id)
    // Go through all the levals of the tweet,as with the user ids:
    var levels = ["",'quoted_status','retweeted_status','retweeted_status.quoted_status']
    for (i in levels){
        level = levels[i]
        if(level==""){var doc = document}
        else if(document['retweeted_status']!=null && level=='retweeted_status.quoted_status'){
            var doc = document['retweeted_status']['quoted_status']
            level=level+"."}
        else{var doc = document[level]
            level=level+"."}
        //If the level exists anonymise the tweet ids:
        if(doc!=null){ //Checks if the level exists
            print(level)
            //Here's where the actual anonymisation begins 
            //Anonymise the id of the tweet: 
            var tweet_id = doc['id_str']
            if(tweet_id==null){var tweet_id = doc['id']}
                try{db.tweetsIDs.insertOne({"id":tweet_id} )}catch(e){print(e)}
                var anonymised_tweet_id = db.tweetsIDs.findOne({"id":tweet_id})['_id']
                db[collection].updateOne({"_id":id},{$set:{[level + "anonymised_tweet_id"]:anonymised_tweet_id}})
            
            //Anonymizes the id of the original tweet if an original tweet was a reply
            var tweet_id = doc['in_reply_to_status_id_str']
            if(tweet_id==null){var tweet_id = doc['in_reply_to_status_id']}
            if(tweet_id != null){
                try{db.tweetsIDs.insertOne({"id":tweet_id})}catch(e){print(e)}
                var anonymised_tweet_id = db.tweetsIDs.findOne({"id":tweet_id})['_id']
                print(anonymised_tweet_id)
                db[collection].updateOne({"_id":id},{$set:{[level+"in_reply_to_anonymised_tweet_id"]:anonymised_tweet_id}})
            }
            
            //Anonymise the id of the quoted status if an original tweet was a quote// 
            var tweet_id = doc['quoted_status_id_str']
            if(tweet_id==null){var tweet_id = doc['quoted_status_id']}
            if(tweet_id != null){
                try{db.tweetsIDs.insertOne({"id":tweet_id})}catch(e){print(e)}
                var anonymised_tweet_id = db.tweetsIDs.findOne({"id":tweet_id})['_id']
                db[collection].updateOne({"_id":id},{$set:{[level+"quoted_status_anonymised_tweet_id"]:anonymised_tweet_id}})
            }
                    
        }
    }
})

// Delete the original ids from the tweets:
    
db[collection].updateMany({},
   { $unset: {//Delete ids from the top level
               "id": "", "id_str": "",
              "in_reply_to_status_id":"","in_reply_to_status_id_str":"",
              "quoted_status_id":"","quoted_status_id_str":"",      
    //This is for all the entities media             
    "entities.media.id": "", "entities.media.id_str": "",
    "extended_entities.media.id":"","extended_entities.media.id_str":"",
    //This is for enetities media in the extended tweet
    "extended_tweet.entities.media.id": "", "extended_tweet.entities.media.id_str": "",
    "extended_tweet.extended_entities.media.id_str":"","extended_tweet.extended_entities.media.id":"",                     
    //This is for quoted stauts
    "quoted_status.id": "", "quoted_status.id_str": "",
    "quoted_status.in_reply_to_status_id":"","quoted_status.in_reply_to_status_id_str":"",
    "quoted_status.quoted_status_id":"","quoted_status.quoted_status_id_str":"", 
        //This is for all the entities media             
        "quoted_status.entities.media.id": "", "quoted_status.entities.media.id_str": "",
        "quoted_status.extended_entities.media.id":"","quoted_status.extended_entities.media.id_str":"",
        //This is for enetities media in the extended tweet
        "quoted_status.extended_tweet.entities.media.id": "", "quoted_status.extended_tweet.entities.media.id_str": "",
        "quoted_status.extended_tweet.extended_entities.media.id":"","quoted_status.extended_tweet.extended_entities.media.id_str":"",   
    //This is fot the retweeted status:
    "retweeted_status.id": "", "retweeted_status.id_str": "",
    "retweeted_status.in_reply_to_status_id":"","retweeted_status.in_reply_to_status_id_str":"",
    "retweeted_status.quoted_status_id":"","retweeted_status.quoted_status_id_str":"", 
        //This is for all the entities media             
        "retweeted_status.entities.media.id": "", "retweeted_status.entities.media.id_str": "",
        "retweeted_status.extended_entities.media.id":"","retweeted_status.extended_entities.media.id_str":"",
        //This is for enetities media in the extended tweet
        "retweeted_status.extended_tweet.entities.media.id": "", "retweeted_status.extended_tweet.entities.media.id_str": "",
       "retweeted_status.extended_tweet.extended_entities.media.id":"","retweeted_status.extended_tweet.extended_entities.media.id_str":"",         //This is for retweeted quoted stauts
    "retweeted_status.quoted_status.id": "", "retweeted_status.quoted_status.id_str": "",
    "retweeted_status.quoted_status.in_reply_to_status_id":"","retweeted_status.quoted_status.in_reply_to_status_id_str":"",
    "retweeted_status.quoted_status.quoted_status_id":"","retweeted_status.quoted_status.quoted_status_id_str":"", 
        //This is for all the entities media             
        "retweeted_status.quoted_status.entities.media.id": "", "retweeted_status.quoted_status.entities.media.id_str": "",
        "retweeted_status.quoted_status.extended_entities.media.id":"","retweeted_status.quoted_status.extended_entities.media.id_str":"",
        //This is for enetities media in the extended tweet
        "retweeted_status.quoted_status.extended_tweet.entities.media.id": "", "retweeted_status.quoted_status.extended_tweet.entities.media.id_str": "",        "retweeted_status.quoted_status.extended_tweet.extended_entities.media.id":"","retweeted_status.quoted_status.extended_tweet.extended_entities.media.id_str":""  
    } }
)

//After that and delete them from the database, leaving only the information about the public profiles, selceted manually:

db.tweetsUsers.drop()
db.tweetsIDs.drop()