// Links extraction
// This extracts the links embedded in the tweets from their metadata and counts their frequencies in the overall dataset and for each day of the data collection.

// Enter the database:
use IARH 

// Define the collection
collection="cheddarman"

//Extracts the links from each level to the separte collection:
db[collection].find().forEach(function(document){
    //Iterate over each level:
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
        //If level exists extract link and isert is as a new document with the date when it was tweeted:
        if(doc!=null){//Check if the document exists
        var links = doc['entities']['urls'] // get the links
        var id = doc['anonymised_tweet_id'] // get the id of the user who included the link
        var created_at = doc['created_at'] // get the time when tweet with the user was created
        for (l in links){ //iterate over all the links in the tweet leve
        link=links[l] //define link from the list of links
        db[collection+"_links"].insert({id:id,created_at:created_at,link})} //insert link with it's metadata into the databse
}}})

// Group by the same mention of the link to exclude duplicates (the same user, link and date)
db[collection+"_links"].aggregate([
    {$unwind:"$link"},
    {$group:{_id:{id:"$id",created_at:"$created_at",link:"$link"}}},
    {$out: collection+'LinksByDate'}], //ouptut the results to the new collection, formatted in a way that will allow to plot link frequencies by date.
    {allowDiskUse:true})

// Aggregate by link to get the total number of mentions for each link across the whole dataset:

db[collection+'LinksByDate'].aggregate([
    {$group: {_id:'$_id.link.expanded_url',
              url:{$addToSet: '$_id.link.url'}, 
              display_url:{$addToSet: '$_id.link.display_url'}, 
              count:{$sum:1} }},
              {$sort : { count : -1 }},
              {$out: collection+'Links'}],{allowDiskUse:true})

//Drop the original links collection:
db[collection + "_links"].drop()

// Get the total number of links in the dataset
db[collection+"Links"].count() // 22757 sources lines