// Data cleaning.
// This code remove empty documents and duplicates from the cheddarman collection.

// Enter the database
use IARH 

//Get the original count of tweets in the database:

db.cheddarman.count() //#Results in: 234521

// Delete non-tweets from documents:
// Due to the occassionaly interruptions in the data extraction, the structure of json files included some
// 'non-tweet' entries that got imported into the database. These would not have the fields associated with 
// the tweets, such as "id_str", so delete all the documents where "id_str" is null:

db.cheddarman.deleteMany({ "id_str" : null }) // deleted 7 documents

// Check for the duplicates in the collection:
// Each tweet should have a unique id_str, so where multiple tweets with the same id_str were present, the duplicates were deleted

//Make a temporary collection (called "chmtmp"), with unique ids and their number in the cheddarman collection:
db.cheddarman.aggregate(
        {$group: {_id:'$id_str', count:{$sum:1} }},
        {$out: "chmtmp"}
    )
 
// Delete the documents from chmtmp, which contain ids that appear only one time in the main collection:
db.chmtmp.deleteMany({ "count" : 1 })

// Count the remaining documents:
db.chmtmp.count()
// There were 31638 ids that appeared more than once in the main collection

// For each document with duplicates delete the duplicates and leave only one document in the cheddarman collection:
db.chmtmp.find().forEach(function(doc) { //Loop over all the documents in the temporary collection
    var count = doc['count'] //Get the number of tweets with the same id, per id
    var id_str = doc['_id'] //Get the id of duplicated tweets
    var i = 1 
    while (i<count){ //Loop over and delete each duplicate, leaving only one tweet with this id
        db.cheddarman.deleteOne({ "id_str" : id_str })
        i=i+1
    }
    print(id_str) //Print the id string which is currently being processed
})

// Count the number of distinct documents and compare it with the overall document count to make sure all the duplicates have been delted:
db.cheddarman.distinct("id_str").length //# Results in: 201458
db.cheddarman.count() //# Now also results in: 201458

//Delete the temporary collection
db.chmptmp.drop()