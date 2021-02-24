// Basisc statiistics
// This extracts basis information about the collection of cheddarman tweets.

// Enter the database
use IARH 

// Define the collection
collection="cheddarman"

// Get the date of the first streamed tweet

var first_tweet = db[collection].find({},{created_at:1}).sort({created_at:1}).limit(1)[0].created_at //2018-02-07T12:05:09Z

//Get the date of the last streamed tweet:

var last_tweet = db[collection].find({},{created_at:1}).sort({created_at:-1}).limit(1)[0].created_at //2018-03-28T19:35:43Z

//Get the number of tweets that were orignally streamed

var streamed_tweets = db[collection].count() // 201458

//Get the number of tweets including those extracted from the metadata:

var total_tweets = db[collection + "_tweets"].count() // 203872

// Get the number of tweets originally posted before 02.07.2018
var early_tweets = db[collection + "_tweets"].count({created_at:{"$lte":ISODate("2018-02-07T00:00:00Z")}})// 82

// Get the number of unique users in the dataset:

var users = db[collection +"_users"].count() // 140875

// Put the basic information into a separate collection for the export into csv:

db[collection +"_info"].insertMany([
    {_id: "First streamed tweet timestamp", value: first_tweet},
    {_id: "Last streamed tweet timestamp", value: last_tweet},
    {_id: "Number of streamed tweets", value: streamed_tweets},
    {_id: "Number of additional tweets extracted from metadata",value: total_tweets-streamed_tweets},
    {_id: "Tweets created before 07.02.2018",value: early_tweets},
    {_id: "Total number of tweets", value: total_tweets},
    {_id: "Total number of unique users", value: users}
])