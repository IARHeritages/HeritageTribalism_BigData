// Indexing
// This code creates indices on the frequently accessed fields in the database, to speed up search by these fields

// Enter the database
use IARH 

// Define collection:

collection="cheddarman"

//Create indicies on the fields that will need to be quickly parased multiple times

db[collection].createIndex( { "anonymised_tweet_id": 1 }, { unique: true } ) // anonymised_tweet_id
db[collection].createIndex( { "quoted_status.anonymised_tweet_id": 1 } ) // anonymised_tweet_id within the quoted_status
db[collection].createIndex( { "retweeted_status.anonymised_tweet_id": 1 } ) // anonymised_tweet_id within the retweeted status