// User data extraction
// This extracts the user metadata, from the collection of tweets that mention cheddarman, into a separate collection

// Enter the database
use IARH 

// Define the collection

collection="cheddarman"

//Extract the user data from the tweets metadata at all levels, and insert them into a new collection:
//Iterate over all tweets
db[collection].find().forEach(function(document){ 
    //Iterate over all levels:
    var levels = ["",'quoted_status','retweeted_status','retweeted_status.quoted_status']
    for (i in levels){
        level = levels[i]
        if(level==""){var doc = document}
        else if(document['retweeted_status']!=null && level=='retweeted_status.quoted_status'){
            var doc = document['retweeted_status']['quoted_status']
            level=level+"."}
        else{var doc = document[level]
            level=level+"."}
        //If level exists, extract and try to insert user data to a separate collection:
        if(doc!=null){
            print(level)
            var user = doc['user'] //Get user data
            user ['_id'] = user['anonymised_id'] //Put user id into _id field to insert it into new collection
            delete user.anonymised_id // Delete original id field
            try {db[collection + "_users"].insertOne(user)} //Try to Inset user data, as a document in the new collection
            catch (e) {print (e)}
        }
    }
 })