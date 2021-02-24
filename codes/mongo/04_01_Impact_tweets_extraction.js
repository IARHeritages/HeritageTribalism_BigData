// Impact tweets extractions
// This aggregates the retweeted quouted and replied to tweets by their frequencies, orders them in a decreasing order, outputs into a separtate collection, and adds the user data to it.
// Subsequently it also extracts a random sample of 500 tweets without any interactions

//This pulls out the statistics about the most impactful tweets in the dataset:

// Aggregae by head_tweet in the edges collection (which indicates that the tweet was retweeted, quoted or replied to): 
db[collection + "_network"].aggregate([
   {$match : { "head_tweet_id" : {"$ne":null}} }, // match by head tweet id
   {$unwind: "$relationship_type" }, // unwind the relationship type, as it is an array that can contian more than 1 relationship
   { $group: { _id: {id:"$head_tweet_id","relationship_type":"$relationship_type"}, // group by id and relationship type
                text:{$first:"$head_text"}, // include the text of the tweet that was interacted with
                user_id:{$first:"$head"}, // include the (anonymised) user id of the author of the tweet
                interacted_with: { $sum: 1 } // count the number of interactions per relationship type
             }},
   {$match : { "_id.relationship_type" : {"$ne":"mention"}} }, // match only the relationship types that are not mentions
   {$group:{ // group by id and push the relationships with the number of interactions per relationship
            _id:"$_id.id", // take out id inside the _id field
            text:{$first:"$text"}, // define the text
            user_id:{$first:"$user_id"}, //define the user id
            interactions_stats: {$push: {k:"$_id.relationship_type",v:"$interacted_with"}}}}, //push the interaction type and the number of interactions
    // Reformat and simplify the metadata
    {$project:{text:1,user_id:1,count: { $arrayToObject: "$interactions_stats" }}},
    {$replaceRoot: { newRoot: { $mergeObjects: [ { _id: "$_id",text:"$text",user_id:"$user_id",total:"$total"}, "$count" ] } } },
    {$addFields: { total:{ $sum: [ "$quote", "$retweet", "$reply" ] } }},
    {$sort : { total : -1, retweet: -1,quote:-1,reply:-1 }}, //Sort the interactions by frequency
    {"$out": collection + "_tweetsImpact" }  // Output to the new collection  
  ],  {allowDiskUse:true} )


// Also add the relevant user data to the tweetsImpact collection:

db[collection+"_tweetsImpact"].aggregate([{$lookup:
                         {
                             from: collection+"_users", // lookup additional data from the user collection
                             localField: "user_id",
                             foreignField: "_id",
                             as: "metadata"}},
                        {$replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$metadata", 0 ] }, "$$ROOT" ] } }},
                        {$project: { metadata: 0 }},       
                        {$out:collection + "_tweetsImpact"}])


// Get the sample of the most impactful tweets:

// Get the tweets that were retweeted quoted or replied to 
var impactTweets = db[collection +"_network"].distinct('head_tweet_id',{"head_tweet_id":{$ne:null}})
// Get the ids of tweets that received any of the above interaction
var impactTweets = impactTweets.distinct('head_tweet_id')
// Push ids into the array, that can be used in an aggregate query:
var ids = []
for (var i = 0; i < impactTweets.length; i++) {
    print(impactTweets[i])
    ids.push(impactTweets[i])    
   
}

// Extract the tweets that received not interactions, and themselves were not retweests into a separate collection, keeping only the key metadata: 
db[collection].aggregate([
    {$match:{$and:[{"anonymised_tweet_id":{$nin:ids}}, // Select tweets that received no interactions (are not in interacted with ids list)
                  {"retweeted_status":{$exists:0}} // and are not retweets themselves
    ]}},
    {$project:{_id:1, //Include only key metadata
               created_at:1,
               user:1,
               text: {$ifNull: ['$extended_tweet.full_text', '$text'] },
               quotes_tweet: {$cond:{if:{$eq:["$is_quote_status",true]},then:true,else:false}}, //include information about whether the tweets is a quote
               is_reply: {$cond: [{$not:['$in_reply_to_anonymised_tweet_id']},false,true] } //include information whther the tweet is a reply
              }}, 
    {$replaceRoot: { newRoot: { $mergeObjects: [ "$$ROOT","$user" ] }}}, //get the user metadata out of the array
    {$project: { user: 0 }},
    {$sort : {date:-1}}, //sort the new collection by date
    {"$out": collection + "_tweetsNoImpact" }
],{allowDiskUse:true})

// Take the sample of tweets with no imact, excluding those that were quotes and replies to other tweets, as they were still engaged in the interactions
db[collection + "_tweetsNoImpact"].aggregate(
    {$match:{$and:[{"quotes_tweet":false}, //select tweets that are not quoted
                  {"is_reply":false}]}}, // and that are not replies
    { $sample: { size: 500 } }, // take the sample with the size of 500 tweets
    {"$out": collection + "_tweetsNoImpactSample" } //Output the sample to the new collection:
    
)
 
db[collection+"_tweetsNoImpactSample"].aggregate([{$lookup:
                         {
                             from: collection+"_tweetsNoImpact",
                             localField: "_id",
                             foreignField: "_id",
                             as: "metadata"}},
                        {$replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$metadata", 0 ] }, "$$ROOT" ] } }},
                        {$project: { metadata: 0 }},       
                        {$out:collection + "_tweetsNoImpactSample"}])