// Topic assignment
// This extracts topic assignment from the imported tweetsTopic.csv file and updates relevant collections with this information
// Subsequently, it finds the most frequent dominant topic for each user:

// This converts the ids in the topic collections to an appropriate format to match other collection:
// Iterate over the collection with tweets with assigned topics
db[collection+"_topics"].find().forEach(function(doc){ 
    var my_id=doc["_id"]
    print(my_id)
    var new_id=my_id.replace("ObjectId(", "") // Delete the ObjectId(), and leave only actual id
    var new_id=new_id.replace(")", "")
    doc["_id"]=ObjectId(new_id) // Transform the id, into ObjectID type of mongo database
    db[collection+"_topics"].insertOne(doc) // Insert the document, with the tranformed id
    db[collection+"_topics"].deleteOne({"_id":my_id}) //Delete the original document
})

//Merges the tweets collection with the information about the topics, and output the results to the nw collection.

db[collection+"_tweets"].aggregate([{$lookup:
                         {
                             from: collection+"_topics", // Merge wteets with topics collection, by tweet id
                             localField: "_id",
                             foreignField: "_id",
                             as: "metadata"}},
                        {$replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$metadata", 0 ] }, "$$ROOT" ] } }},
                        {$project: { metadata: 0 }},       
                        {$out:collection + "_tweetsTopics"}])

// Get the average dominant topic for each user:
db[collection+"_tweetsTopics"].aggregate([
    { $group: { _id: "$user", "Topic 1": { $avg: "$Topic 1" }, //Aggregate data by user id
                  "Topic 2": { $avg: "$Topic 2" }, //and get the average probability for each topics, from the tweets authored by the user
              "Topic 3": { $avg: "$Topic 3" },
              "Topic 4": { $avg: "$Topic 4" },
              "Topic 5": { $avg: "$Topic 5" },
              "Topic 6": { $avg: "$Topic 6" },
              "Topic 7": { $avg: "$Topic 7" },
              "Topic 8": { $avg: "$Topic 8" },
              "Topic 9": { $avg: "$Topic 9" },
              }},
     { $addFields: {"dominant_value": {$max:["$Topic 1","$Topic 2","$Topic 3","$Topic 4","$Topic 5","$Topic 6","$Topic 7","$Topic 8","$Topic 9"]}}}, //assign the highest topic probability value to the dominant value
     { $addFields: {"dominant_topic": {$sum:[{ $indexOfArray: [ ["$Topic 1","$Topic 2","$Topic 3","$Topic 4","$Topic 5","$Topic 6","$Topic 7","$Topic 8","$Topic 9"], "$dominant_value" ] },1]}}}, // assign the topic with highest average tweet probability value as a dominant topic
    {$out:collection + "_userTopics"} //output the assignment to the new collection
],{allowDiskUse:true})

// Get the most frequent dominant topic for each user (the most frequently assigned, as opposed to the one with the highest average probability
db[collection+"_tweetsTopics"].aggregate([
    { $group: { _id: {user:"$user",topic:"$dominant_topic"}, count:{$sum:1}}}, //aggregate by user and dominant topic
    {"$sort": {"count":-1}}, //sort by frequency of topics, so that the first one is the most frequent one
    { $group: { _id: "$_id.user",most_frequent_topic:{$first:"$_id.topic"}}}, //push the firs most frequent topic to the most frequent topic field
    {$out:collection + "_UFT"} //export the assignment to the temporary collection
    ],{allowDiskUse:true})

// Combine the info about the dominant topic and the most frequent topic for each user:

db[collection+"_userTopics"].aggregate([{$lookup:
                         {
                             from: collection+"_UFT", //lookup information about the most frequent topic from the temporary collection
                             localField: "_id",
                             foreignField: "_id",
                             as: "metadata"}},
                        {$replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$metadata", 0 ] }, "$$ROOT" ] } }},
                        {$project: { metadata: 0 }},       
                        {$out:collection + "_userTopics"}])