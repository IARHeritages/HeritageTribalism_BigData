// Peaks extraction
// This retrives the most retweeted/quouted tweets on the selected dataset, with the high frequency of tweets (23.02.2018,19.02.2018 and 03.03.2018)

//Get the most retweeted tweets on the relevant dates:

//23rd of Febuary

// Aggregate data by retweeted status and sort by total count:

db[collection].aggregate([
    {$match:{$and:[{created_at: { $gte: ISODate("2018-02-22T23:59:59Z")}}, 
         {created_at: { $lt: ISODate("2018-02-23T23:59:59Z")}}]}}, //This selects only the tweets posted between the specified times
    {$group:{_id:"$retweeted_status.anonymised_tweet_id",retweeted_count:{$sum:1},
    text:{$first:"$retweeted_status.text"},
    created_at:{$first:"$retweeted_status.created_at"},
    user:{$first:{anonymised_id:"$retweeted_status.user.anonymised_id",
          description:"$retweeted_status.user.description"}}}},           
    {$sort : {retweeted_count:-1}},
    {$out: collection +"_frequent_rtweeted"}
])

// Aggregate data by quoted_status and sort by total count:
db[collection].aggregate([
    {$match:{$and:[{created_at: { $gte: ISODate("2018-02-22T23:59:59Z")}}, 
         {created_at: { $lt: ISODate("2018-02-23T23:59:59Z")}}]}},
    {$group:{_id:"$quoted_status.anonymised_tweet_id",quoted_count:{$sum:1},
    text:{$first:"$quoted_status.text"},
    created_at:{$first:"$quoted_status.created_at"},
    user:{$first:{anonymised_id:"$quoted_status.user.anonymised_id",
          description:"$quoted_status.user.description"}}}},           
    {$sort : {quoted_count:-1}},
    {$out: collection +"_frequent_qted"}
])

db[collection].aggregate([
     {$match:{$and:[{created_at: { $gte: ISODate("2018-02-22T23:59:59Z")}}, 
         {created_at: { $lt: ISODate("2018-02-23T23:59:59Z")}}]}},
         {$group:{_id:"$in_reply_to_anonymised_tweet_id",replies_count:{$sum:1}}},
           {$sort : {replies_count:-1}},
        {$out: collection +"_frequent_rep"}
])

db[collection + "_frequent_rep"].aggregate([{$lookup:
                         {
                             from: collection,
                             localField: "_id",    // field in the orders collection
                             foreignField: "anonymised_tweet_id",
                             as:"metadata"
                         }},      
                         {$project: {replies_count:"$replies_count",
                                     text: "$metadata.text",
                                    created_at:"$metadata.created_at",
                                    user:"$metadata.user.anonymised_id",
                                    description:"$metadata.user.description"}},   
                        { $unwind : "$text" },
                                            { $unwind : "$created_at" },
                                            { $unwind : "$user" },
                                            { $unwind : "$description" },
                        {$out:collection + "_frequent_rep"}])

// Combine the collections of frequently retweeted tweets with the frequently quoted ones:
// Iterate over all quoted tweets      
db[collection+"_frequent_qted"].find().forEach(function(doc){
    var id = doc['_id']
    print(id)
    var rec = db[collection+"_frequent_rtweeted"].findOne({_id:id}) //look for the tweet in retweeted collection
    if(rec != null){//if the tweet exist, update it with the information about the number of quotes
    db[collection +"_frequent_rtweeted"].updateOne({_id:doc['_id']},{$set:{quoted_count:doc['quoted_count']}})
    }else{db[collection +"_frequent_rtweeted"].insertOne(doc)} //if it does not exists, simply insert it as a new document
})

//The same but for replies
db[collection+"_frequent_rep"].find().forEach(function(doc){
    var id = doc['_id']
    print(id)
    var rec = db[collection+"_frequent_rtweeted"].findOne({_id:id}) //look for the tweet in retweeted collection
    if(rec != null){//if the tweet exist, update it with the information about the number of quotes
    db[collection +"_frequent_rtweeted"].updateOne({_id:doc['_id']},{$set:{replies_count:doc['replies_count']}})
    }else{db[collection +"_frequent_rtweeted"].insertOne(doc)} //if it does not exists, simply insert it as a new document
})

// Do the same for the replies:


// Get the total of quotes and retweets for each tweet:
db[collection+"_frequent_rtweeted"].aggregate([
    {$addFields:{total:{$sum:["$retweeted_count","$quoted_count","$replies_count"]}}},
    {$sort : {total:-1}},
    {$out: collection +"_2302"}
])


// 19.02.2018
// Repeat the above, aggregating by the new data

db[collection].aggregate([
    {$match:{$and:[{created_at: { $gte: ISODate("2018-02-18T23:59:59Z")}}, 
         {created_at: { $lt: ISODate("2018-02-19T23:59:59Z")}}]}},
    {$group:{_id:"$retweeted_status.anonymised_tweet_id",retweeted_count:{$sum:1},
    text:{$first:"$retweeted_status.text"},
    created_at:{$first:"$retweeted_status.created_at"},
    user:{$first:{anonymised_id:"$retweeted_status.user.anonymised_id",
          description:"$retweeted_status.user.description"}}}},           
    {$sort : {retweeted_count:-1}},
    {$out: collection +"_frequent_rtweeted"}
])

db[collection].aggregate([
    {$match:{$and:[{created_at: { $gte: ISODate("2018-02-18T23:59:59Z")}}, 
         {created_at: { $lt: ISODate("2018-02-19T23:59:59Z")}}]}},
    {$group:{_id:"$quoted_status.anonymised_tweet_id",quoted_count:{$sum:1},
    text:{$first:"$quoted_status.text"},
    created_at:{$first:"$quoted_status.created_at"},
    user:{$first:{anonymised_id:"$quoted_status.user.anonymised_id",
          description:"$quoted_status.user.description"}}}},           
    {$sort : {quoted_count:-1}},
    {$out: collection +"_frequent_qted"}
])

// Aggregate by replies
db[collection].aggregate([
     {$match:{$and:[{created_at: { $gte: ISODate("2018-02-18T23:59:59Z")}}, 
         {created_at: { $lt: ISODate("2018-02-19T23:59:59Z")}}]}},
         {$group:{_id:"$in_reply_to_anonymised_tweet_id",replies_count:{$sum:1}}},
           {$sort : {replies_count:-1}},
        {$out: collection +"_frequent_rep"}
])

db[collection + "_frequent_rep"].aggregate([{$lookup:
                         {
                             from: collection,
                             localField: "_id",    // field in the orders collection
                             foreignField: "anonymised_tweet_id",
                             as:"metadata"
                         }},      
                         {$project: {replies_count:"$replies_count",
                                     text: "$metadata.text",
                                    created_at:"$metadata.created_at",
                                    user:"$metadata.user.anonymised_id",
                                    description:"$metadata.user.description"}},   
                        { $unwind : "$text" },
                                            { $unwind : "$created_at" },
                                            { $unwind : "$user" },
                                            { $unwind : "$description" },
                        {$out:collection + "_frequent_rep"}])
      
db[collection+"_frequent_qted"].find().forEach(function(doc){
    var id = doc['_id']
    print(id)
    var rec = db[collection+"_frequent_rtweeted"].findOne({_id:id})
    if(rec != null){
    db[collection +"_frequent_rtweeted"].updateOne({_id:doc['_id']},{$set:{quoted_count:doc['quoted_count']}})
    }else{db[collection +"_frequent_rtweeted"].insertOne(doc)}
})

db[collection+"_frequent_rep"].find().forEach(function(doc){
    var id = doc['_id']
    print(id)
    var rec = db[collection+"_frequent_rtweeted"].findOne({_id:id}) //look for the tweet in retweeted collection
    if(rec != null){//if the tweet exist, update it with the information about the number of quotes
    db[collection +"_frequent_rtweeted"].updateOne({_id:doc['_id']},{$set:{replies_count:doc['replies_count']}})
    }else{db[collection +"_frequent_rtweeted"].insertOne(doc)} //if it does not exists, simply insert it as a new document
})

db[collection+"_frequent_rtweeted"].aggregate([
    {$addFields:{total:{$sum:["$retweeted_count","$quoted_count","$replies_count"]}}},
    {$sort : {total:-1}},
    {$out: collection +"_1902"}
])

//3rd of March:
// Repeat the above aggregating by the new date.

db[collection].aggregate([
    {$match:{$and:[{created_at: { $gte: ISODate("2018-03-02T23:59:59Z")}}, 
         {created_at: { $lt: ISODate("2018-03-03T23:59:59Z")}}]}},
    {$group:{_id:"$retweeted_status.anonymised_tweet_id",retweeted_count:{$sum:1},
    text:{$first:"$retweeted_status.text"},
    created_at:{$first:"$retweeted_status.created_at"},
    user:{$first:{anonymised_id:"$retweeted_status.user.anonymised_id",
          description:"$retweeted_status.user.description"}}}},           
    {$sort : {retweeted_count:-1}},
    {$out: collection +"_frequent_rtweeted"}
])

db[collection].aggregate([
    {$match:{$and:[{created_at: { $gte: ISODate("2018-03-02T23:59:59Z")}}, 
         {created_at: { $lt: ISODate("2018-03-03T23:59:59Z")}}]}},
    {$group:{_id:"$quoted_status.anonymised_tweet_id",quoted_count:{$sum:1},
    text:{$first:"$quoted_status.text"},
    created_at:{$first:"$quoted_status.created_at"},
    user:{$first:{anonymised_id:"$quoted_status.user.anonymised_id",
          description:"$quoted_status.user.description"}}}},           
    {$sort : {quoted_count:-1}},
    {$out: collection +"_frequent_qted"}
])


// Aggregate by replies
db[collection].aggregate([
     {$match:{$and:[{created_at: { $gte: ISODate("2018-03-02T23:59:59Z")}}, 
         {created_at: { $lt: ISODate("2018-03-03T23:59:59Z")}}]}},
         {$group:{_id:"$in_reply_to_anonymised_tweet_id",replies_count:{$sum:1}}},
           {$sort : {replies_count:-1}},
        {$out: collection +"_frequent_rep"}
])

db[collection + "_frequent_rep"].aggregate([{$lookup:
                         {
                             from: collection,
                             localField: "_id",    // field in the orders collection
                             foreignField: "anonymised_tweet_id",
                             as:"metadata"
                         }},      
                         {$project: {replies_count:"$replies_count",
                                     text: "$metadata.text",
                                    created_at:"$metadata.created_at",
                                    user:"$metadata.user.anonymised_id",
                                    description:"$metadata.user.description"}},   
                        { $unwind : "$text" },
                                            { $unwind : "$created_at" },
                                            { $unwind : "$user" },
                                            { $unwind : "$description" },
                        {$out:collection + "_frequent_rep"}])

      
db[collection+"_frequent_qted"].find().forEach(function(doc){
    var id = doc['_id']
    print(id)
    var rec = db[collection+"_frequent_rtweeted"].findOne({_id:id})
    if(rec != null){
    db[collection +"_frequent_rtweeted"].updateOne({_id:doc['_id']},{$set:{quoted_count:doc['quoted_count']}})
    }else{db[collection +"_frequent_rtweeted"].insertOne(doc)}
})

db[collection+"_frequent_rep"].find().forEach(function(doc){
    var id = doc['_id']
    print(id)
    var rec = db[collection+"_frequent_rtweeted"].findOne({_id:id}) //look for the tweet in retweeted collection
    if(rec != null){//if the tweet exist, update it with the information about the number of quotes
    db[collection +"_frequent_rtweeted"].updateOne({_id:doc['_id']},{$set:{replies_count:doc['replies_count']}})
    }else{db[collection +"_frequent_rtweeted"].insertOne(doc)} //if it does not exists, simply insert it as a new document
})

db[collection+"_frequent_rtweeted"].aggregate([
    {$addFields:{total:{$sum:["$retweeted_count","$quoted_count","$replies_count"]}}},
    {$sort : {total:-1}},
    {$out: collection +"_0303"}
])