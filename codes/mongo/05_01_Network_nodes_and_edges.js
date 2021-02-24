// Nodes and edges extraction for the network analysis
// This extract the list of edges and nodes for the network analysis, from the tweets collection.
// The connections between the nodes are defined as retweets, quotes, replies to tweets, and mentions

// Parase all the tweets to extract all the relationships into the edge list:
db[collection].find().noCursorTimeout().forEach(function(document){
    print("Documents_processed: "+k)
    var id = document['_id']
    // Extract data on interaction for all levels in the datase
    var levels = ["",'quoted_status','retweeted_status','retweeted_status.quoted_status']
    //Parase all levels:
    for (i in levels){
        level = levels[i]
        if(level==""){var doc = document}
        else if(document['retweeted_status']!=null && level=='retweeted_status.quoted_status'){
            var doc = document['retweeted_status']['quoted_status']
            level=level+"."}
        else{var doc = document[level]
            level=level+"."
            }
        //If the level exists extract all the information from it:
        if(doc!=null){
            var level_id = doc['anonymised_tweet_id']
            //Check whether the embedded tweet id is already in the levels above
            if(level=="quoted_status."){var date = db[collection].findOne({"anonymised_tweet_id":level_id})}
            else if(level=="retweeted_status."){var date = db[collection].findOne({$or:[{"anonymised_tweet_id":level_id},{"quoted_status.anonymised_tweet_id":level_id}]})}
            else if(level=="retweeted_status.quoted_status"){var date = db[collection].findOne({$or:[{"anonymised_tweet_id":level_id},{"quoted_status.anonymised_tweet_id":level_id},{"retweeted_status.anonymised_tweet_id":level_id}]})}
            print("The id " + date + " is already inclueded")
            //If it is not, get the check for the date:
            if(date==null){var date = db[collection].findOne({"$and":[{"_id":id},{[level+"created_at"]:{$gte:ISODate("2018-02-07T06:00:00Z")}}]})}
            //Otherwise set date to null
            else{var date = null}
            //Process the tweet if the date is not null: i.e. the tweet is not in the tweets level above and it is later than Friday, 7th of February 6.00 am, the time when cheddarman news was published.
            if(level=="" || date!=null){
                
                //Get the info about the tail (person who initiated interaction) 
                var tail = doc['user']['anonymised_id']
                var onset = doc['created_at']
                var extended_tweet = doc['extended_tweet']
                if(extended_tweet!=null){var text = doc['extended_tweet']['full_text']}
                else{var text = doc['text']}
                tail_tweet_id = doc['anonymised_tweet_id']

                //Get user mentions from the extended entities is they exists
                var extended_tweet = doc['extended_tweet']
                if(extended_tweet!=null){
                    var extended_mentions = doc['extended_tweet']['entities']['user_mentions']
                    for (i=0; i < extended_mentions.length; i++){
                        head = extended_mentions[i]['anonymised_id']
                        db[collection + "_network"].insertOne({"tail":tail,"head":head,"onset":onset, "relationship_type":"mention","head_onset":onset,"text":text,"tail_tweet_id":tail_tweet_id})}
                }else{ //Otherwise get user mentions from the entities user mentions field 
                    //List all the user mentions of the tweet
                    var mentions = doc['entities']['user_mentions']
                    var mentions_ids=[]
                    //Iterate over the mentions, to include the interaction in the new collection
                    for (i=0; i < mentions.length; i++){
                        head = mentions[i]['anonymised_id']
                        db[collection + "_network"].insertOne({"tail":tail,"head":head,"onset":onset, "relationship_type":"mention","head_onset":onset,"text":text,"tail_tweet_id":tail_tweet_id}) //insert the information about the interaction to the edge list
                 }
                }
                
                //This extracts reply links:
                var head = doc['in_reply_to_anonymised_id']
                var head_tweet_id=doc['in_reply_to_anonymised_tweet_id']
                if(head!=null){db[collection + "_network"].insertOne({"tail":tail,"head":head,"onset":onset, "relationship_type":"reply","head_onset":onset,"text":text,"head_tweet_id":head_tweet_id,"tail_tweet_id":tail_tweet_id})}
                
                //This checks for and extracts the 'quote' relationship:
                var head = doc['quoted_status']
                if(head!=null){
                        var head = doc['quoted_status']['user']['anonymised_id']
                        var head_onset = doc['quoted_status']['created_at']
                        var head_text = doc['quoted_status']['text']
                        var head_tweet_id = doc['quoted_status']['anonymised_tweet_id']
                        db[collection + "_network"].insertOne({"tail":tail,"head":head,"onset":onset, "relationship_type":"quote","head_onset":head_onset,"text":text,"tail_tweet_id":tail_tweet_id,"head_text":head_text,"head_tweet_id":head_tweet_id})
                }
                
                //This check for and extracts the retweet relationship:
                var head = doc['retweeted_status']
                if(head!=null){
                    var head = doc['retweeted_status']['user']['anonymised_id']
                    var head_onset = doc['retweeted_status']['created_at']
                    var head_tweet_id = doc['retweeted_status']['anonymised_tweet_id']
                    var head_text = doc['retweeted_status']['text']
                    db[collection + "_network"].insertOne({"tail":tail,"head":head,"onset":onset, "relationship_type":"retweet","head_onset":head_onset,"text":text,"tail_tweet_id":tail_tweet_id,"head_text":head_text,"head_tweet_id":head_tweet_id})
                }
            }
        }
    }
})

//Creat index on the head_tweet_id field, in the collection, for further searches:

db[collection + "_network"].createIndex( { "head_tweet_id": 1 } )
            
//If the relationship is a reply, get additional data about the original tweet to which the reply refered to:
//Iterate over each reply relationship:
db[collection+"_network"].find({"relationship_type":"reply"}).forEach(function(document){
            var id = document['_id']
            // For each reply find a tweet with id of the replied to tweet in the original collection and extract data on onset and text from it
            var tweet_id = document['head_tweet_id']
            if (tweet_id != null){
                var new_doc = db[collection].findOne({anonymised_tweet_id:tweet_id})
                if (new_doc != null){
                    var extended_tweet = new_doc['extended_tweet']
                    if(extended_tweet!=null){var new_text = new_doc['extended_tweet']['full_text']}
                    else{var new_text = new_doc['text']} 
                    new_onset=new_doc['created_at']
                    db[collection+"_network"].updateOne({"_id":id},{ $set:{head_text:new_text,head_onset:new_onset}})
                }
                //Now find other head tweets with the same id, in the network collection, and fill in the data data from there if they exist
                var new_doc =db[collection+"_network"].findOne({$and:[{"head_tweet_id":tweet_id},{"relationship_type":{$ne:"reply"}}]})
                if (new_doc != null){
                    var new_text = new_doc['head_text']
                    new_onset=new_doc['head_onset']
                    db[collection+"_network"].updateOne({"_id":id},{ $set:{"head_text":new_text,"head_onset":new_onset}})
                     }
            }
        })

// Make list of nodes with onset and terminus of each node 
//(onset = first time a node appears in the link, temrminus last time a node appears in the link)
//Gets the onset and terminus for head nodes and tail nodes:

db[collection + "_network"].aggregate([{ $group: {_id: "$tail", onset: { $min:"$onset" }, terminus:{$max:"$onset"},tail_count:{$sum:1},n_interactions:{$sum:1}}},{$out:collection + "_nodes"}])
db[collection + "_network"].aggregate([{ $group: {_id: "$head", onset: { $min:"$head_onset" }, terminus:{$max:"$head_onset"},head_count:{$sum:1},n_interactions:{$sum:1}}},{$out:collection + "_headnodes"}])

//This merges the headnodes and the tailnodes, when they are the same
db[collection+"_nodes"].aggregate([{$lookup:
                         {
                             from: collection+"_headnodes",
                             let: { onset: "$onset", terminus: "$terminus",id:"$_id"},
                             pipeline:[
                                 {$match:{ $expr:{$eq:["$_id","$$id"] }}},
                                 {$project: {
                                 n_interactions: "$n_interactions",
                                 onset: "$onset",
                                 terminus:"$terminus"
                             }}],
                             as: "head"}},
                            {$unwind:"$head"},
                            {$project:{
                                _id:1,
                                onset: {$min:["$onset","$head.onset"]},
                                terminus: {$max:["$terminus","$head.terminus"]},
                                tail_count:"$n_interactions",
                                head_count:"$head.n_interactions",
                                n_interaction:{$add: ["$n_interactions", "$head.n_interactions"] },
                            }},
                            {$out:collection + "_sharednodes"}])

//Copy all the nodes into one collection and delete the others:
db[collection + "_headnodes"].copyTo([collection + "_nodes"])
db[collection + "_sharednodes"].copyTo([collection + "_nodes"])
db[collection + "_headnodes"].drop()
db[collection + "_sharednodes"].drop()

//Add metadata to the nodes collection from the users collection:
db[collection+"_nodes"].aggregate([{$lookup:
                         {
                             from: collection+"_users",
                             localField: "_id",
                             foreignField: "_id",
                             as: "metadata"}},
                        {$replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$metadata", 0 ] }, "$$ROOT" ] } }},
                        {$project: { metadata: 0 }},       
                        {$out:collection + "_nodes"}])

//Also add the metadata related to topics, to the nodes metadata
db[collection+"_nodes"].aggregate([{$lookup:
                         {
                             from: collection+"_userTopics",
                             localField: "_id",
                             foreignField: "_id",
                             as: "metadata"}},
                        {$replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$metadata", 0 ] }, "$$ROOT" ] } }},
                        {$project: { metadata: 0 }},       
                        {$out:collection + "_nodes"}])

//Now also set terminus and onset in seconds for the nodes
var onset = ISODate("2018-02-07T06:00:00Z")

db[collection + "_nodes"].aggregate([
        { "$addFields": { 
            "duration": {"$divide": [{"$subtract": ["$terminus", "$onset"]}, 1000]},
            "onset_time":{"$divide": [{"$subtract": ["$onset", onset]}, 1000]},
            "terminus_time":{"$divide": [{"$subtract": ["$terminus", onset]}, 1000]}
        } },
        { "$out": collection + "_nodes" }])

//Transform the times for the network collection as well:

db[collection + "_network"].aggregate([
        { "$addFields": { 
            "onset_time":{"$divide": [{"$subtract": ["$onset", onset]}, 1000]},
            "head_onset_time":{"$divide": [{"$subtract": ["$head_onset", onset]}, 1000]}
        } },
        { "$out": collection + "_network" }])

//Add data about the topics to the network collection:

db[collection+"_network"].find({"dominant_topic":{"$exists":0}}).forEach(function(doc){
    id=doc["_id"]
    tail_tweet_id = doc['tail_tweet_id']
    //If the relationship is a quoted of a reply (use head_tweet_id)
    if(doc['relationship_type']=="quote" || (doc['relationship_type']=='reply' && doc['head_text']!=null)){
        head_tweet_id = doc['head_tweet_id']
        t_1 = db[collection + "_topics"].findOne({_id:tail_tweet_id})
        t_2 = db[collection + "_topics"].findOne({_id:head_tweet_id})
        t1=(t_1['Topic 1']+t_2["Topic 1"])/2
        t2=(t_1['Topic 2']+t_2["Topic 2"])/2
        t3=(t_1['Topic 3']+t_2["Topic 3"])/2
        t4=(t_1['Topic 4']+t_2["Topic 4"])/2
        t5=(t_1['Topic 5']+t_2["Topic 5"])/2
        t6=(t_1['Topic 6']+t_2["Topic 6"])/2
        t7=(t_1['Topic 7']+t_2["Topic 7"])/2
        t8=(t_1['Topic 8']+t_2["Topic 8"])/2
        t9=(t_1['Topic 9']+t_2["Topic 9"])/2
        topics_array=[t1,t2,t3,t4,t5,t6,t7,t8,t9]
        dominant_average_value=Math.max.apply(Math,topics_array)
        dominant_average_topic="Topic " + (topics_array.indexOf(dominant_value)+1)
        dominant_head_value = Math.max.apply(Math,topics_array)
        dominant_head_topic = t_2["dominant_topic"]
        dominant_head_value = t_2["dominant_value"]
        dominant_topic = t_1["dominant_topic"]
        dominant_value = t_1["dominant_value"]
        db[collection+"_network"].updateOne({"_id":id},{$set:{"Topic 1":t1,"Topic 2":t2,"Topic 3":t3,"Topic 4":t4,
                                                            "Topic 5":t5,"Topic 6":t6,"Topic 7":t7,"Topic 8":t8,
                                                            "Topic 9":t9,"dominant_average_topic":dominant_topic,
                                                             "dominant_average_value":dominant_value,
                                                             "dominant_head_topic":dominant_head_topic,
                                                             "dominant_head_value":dominant_head_value,
                                                             "dominant_topic":dominant_topic,
                                                             "dominant_value":dominant_value}})
        }
        else{//for retweet and mention, use the tail_tweet_id to find text
        topics = db[collection + "_topics"].findOne({"_id":tail_tweet_id})
        delete topics['_id']
        delete topics['text']
        db[collection+"_network"].updateOne({"_id":id},{"$set":{topics}})
        }
    
})

//Unwind the topics field for retweet and mentions relationships
db[collection+"_network"].aggregate([
                        {$replaceRoot: { newRoot: { $mergeObjects: [  "$topics","$$ROOT" ] } }},
                        {$project: { topics: 0 }},       
                        {$out:collection + "_network"}])

//Aggregate the information about the topics, into the list of static edges with metadata:

db[collection+"_network"].aggregate([
    { $group: { _id: {tail:"$tail",head:"$head"},
               frequency:{$sum:1},
               onset:{$min:"$onset"},
               relationship_type:{$addToSet:"$relationship_type"},
               tail_tweets_id:{$push:"$tail_tweet_id"},
               text:{$push:"$text"},
               head_text:{$push:"$head_text"},
               head_tweet_id:{$push:"$tail_tweet_id"},
               onset_time:{$min:"$onset_time"},
               terminus_time:{$max:"$onset_time"},
               "Topic 1": { $avg: "$Topic 1" },
               "Topic 2": { $avg: "$Topic 2" },
               "Topic 2": { $avg: "$Topic 2" },
               "Topic 3": { $avg: "$Topic 3" },
               "Topic 4": { $avg: "$Topic 4" },
               "Topic 5": { $avg: "$Topic 5" },
               "Topic 6": { $avg: "$Topic 6" },
               "Topic 7": { $avg: "$Topic 7" },
               "Topic 8": { $avg: "$Topic 8" },
               "Topic 9": { $avg: "$Topic 9" }}},
    { $addFields: {"dominant_value": {$max:["$Topic 1","$Topic 2","$Topic 3","$Topic 4","$Topic 5","$Topic 6","$Topic 7","$Topic 8","$Topic 9"]}}},
    { $addFields: {"dominant_topic": {$sum:[{ $indexOfArray: [ ["$Topic 1","$Topic 2","$Topic 3","$Topic 4","$Topic 5","$Topic 6","$Topic 7","$Topic 8","$Topic 9"], "$dominant_value" ] },1]}}},         
    {$out:collection + "_static_network"}
    ],{allowDiskUse:true})

//Group by head, tail and dominant topic, and get the most frequent dominant topic on the intratction:
db[collection+"_network"].aggregate([
    { $group: { _id: {tail:"$tail",head:"$head",topic:"$dominant_topic"}, count:{$sum:1}}},
    {"$sort": {"count":-1}},
    { $group: { _id: {tail:"$_id.tail",head:"$_id.head"},most_frequent_topic:{$first:"$_id.topic"}}},
    {$out:collection + "_SNT"}
    ],{allowDiskUse:true})

//Add information about the most frequent topic to the static network metdata

db[collection+"_static_network"].aggregate([{$lookup:
                         {
                             from: collection+"_SNT",
                             localField: "_id",
                             foreignField: "_id",
                             as: "metadata"}},
                        {$replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$metadata", 0 ] }, "$$ROOT" ] } }},
                        {$project: { metadata: 0 }},       
                        {$out:collection + "_static_network"}])