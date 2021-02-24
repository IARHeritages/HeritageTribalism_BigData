// Boundary markers
// This codes identifies the terms that indicate boundary markers in the text of the tweets, and updates the relevant collections with the information about it

// Enter the database

use IARH

//Define collection

collection="cheddarman"

//Define the subcollection, to search for terms in:

subcollection="_tweetsTopics"


// Create the text index on the text field in the collection containing tweets with assigned topics:
db[collection + "_tweetsTopics"].createIndex({text:"text"})


//### Search for keywords that identify specific boundary markers:

///# Newspaper readership:
var phrases = ["\"Daily Mail\"","\"Daily Heil\"","\"Mail Online\"","\"Daily Comments\"","Guardian","\"Sun readers\""]

for (i in phrases){
    var p = phrases[i]
    print(p)
    var count=db[collection+subcollection].count({$text:{$search:p}},{"text":1})
    print(count)
    db[collection+subcollection].updateMany({$text:{$search:p}},{$addToSet:{phrases:p,tribe:"Newspaper Readership"}})
}


//anti-semitism
phrases = ["jew","jewish","\"anti-semitic\""]

for (i in phrases){
    var p = phrases[i]
    print(p)
    var count=db[collection+subcollection].count({$text:{$search:p}},{"text":1})
    print(count)
    db[collection+subcollection].updateMany({$text:{$search:p}},{$addToSet:{phrases:p,tribe:"anti-semitism"}})
}



//Political leaning
phrases=["\"Jacob Rees-Mogg\"","\"far-right\"","\"far right\"","\"Britain First\"","magats","Trump supporters","UKIP","\"altright\"","labour","left-liberalist","lefties","leftists","\"demented left\"","left","Marxist","Corbyn","\"twitter left\"","\"leftist twitter\"","Obama"
for (i in phrases){
    var p = phrases[i]
    print(p)
    var count=db[collection+subcollection].count({$text:{$search:p}},{"text":1})
    print(count)
    db[collection+subcollection].updateMany({$text:{$search:p}},{$addToSet:{phrases:p,tribe:"political leaning"}})
}
db[collection+subcollection].updateMany({$text:{$search:"BNP",$caseSensitive: true}},                          
                                              {$addToSet:{phrases:"BNP",tribe:"political leaning"}})
db[collection+subcollection].updateMany({$text:{$search:"EDL",$caseSensitive: true}},                          
                                              {$addToSet:{phrases:"EDL",tribe:"political leaning"}})

//EU-UK relationships:

phrases=['Brexit', 'Brexiteers',"\"pro EU\"","\"remain-er\""]
for (i in phrases){
    var p = phrases[i]
    print(p)
    var count=db[collection+subcollection].count({$text:{$search:p}},{"text":1})
    print(count)
    db[collection+subcollection].updateMany({$text:{$search:p}},{$addToSet:{phrases:p,tribe:"UK-EU position"}})
}


//Values
phrases=["bisexual","feminist","gender","\"climate change\"","vegan","weed","bigots"]
for (i in phrases){
    var p = phrases[i]
    print(p)
    var count=db[collection+subcollection].count({$text:{$search:p}},{"text":1})
    print(count)
    db[collection+subcollection].updateMany({$text:{$search:p}},{$addToSet:{phrases:p,tribe:"values"}})
}

//Racial views
phrases=["\"anti-white\"","\"non-white\"","\"Meghan Markle\"","\"Paul Nehlen\"","blacks","\"white supremacist\"","\"white man\"", "\"white genocide\"","\"pro-white\"","\"white racist\"","nationalist"]

for (i in phrases){
    var p = phrases[i]
    print(p)
    var count=db[collection+subcollection].count({$text:{$search:p}},{"text":1})
    print(count)
    db[collection+subcollection].updateMany({$text:{$search:p}},$addToSet:{phrases:p,tribe:"racial views"}})
}



//Information:

phrases=["\"junk science\"","\"pseudo-scientist\"","\"politically motivated\"","nonces","\"experts\"","establishment","\"Jewish researchers\"","\"skeptic community\""]

for (i in phrases){
    var p = phrases[i]
    print(p)
    var count=db[collection+subcollection].count({$text:{$search:p}},{"text":1})
    print(count)
    db[collection+subcollection].updateMany({$text:{$search:p}},{$addToSet:{phrases:p,tribe:"Trust in experts"}})
}
db[collection+subcollection].updateMany({$text:{$search:"MSM",$caseSensitive: true}},{$addToSet:{phrases:"MSM",tribe:"Trust in experts"}})
db[collection+subcollection].updateMany({$text:{$search:"BBC",$caseSensitive: true}},{$addToSet:{phrases:"BBC",tribe:"Trust in experts"}})


//Nazism
p="Nazis"
db[collection+subcollection].updateMany({$text:{$search:p}},{$addToSet:{phrases:p,tribe:"Neo-nazizm"}})
p="\"Richard Spencer\""
db[collection+subcollection].updateMany({$text:{$search:p}},{$addToSet:{phrases:p,tribe:"Neo-nazizm"}})

//### Create indexes for the search of tribes in other collections:
db[collection+"_static_network"].createIndex( { "tail_tweets_id": 1 } )
db[collection+"_static_network"].createIndex( { "head_tweet_id": 1 })

//### Update users, nosed, and network collections:

db[collection+"_tweetsTopics"].find({phrases:{$ne:[]}}).forEach(function(doc){
       id = doc['_id']
       user_id = doc['user']
       phrases = doc['phrases']
       tribes = doc['tribe']
       positions = doc['position']
       for (i in phrases){
            phrase=phrases[i]
           db[collection+"_nodes"].updateOne({_id:user_id},{$addToSet:{phrases:phrase}})
           db[collection+"_users"].updateOne({_id:user_id},{$addToSet:{phrases:phrase}})
           db[collection+"_static_network"].updateMany({$or:[{tail_tweets_id:id},{head_tweet_id:id}]},{$addToSet:{phrases:phrase}})
       }
       for (i in tribes){
           tribe = tribes[i]
           print(tribe)
           db[collection+"_nodes"].updateOne({_id:user_id},{$addToSet:{tribes:tribe}})
           db[collection+"_users"].updateOne({_id:user_id},{$addToSet:{tribes:tribe}})
           db[collection+"_static_network"].updateMany({$or:[{tail_tweets_id:id},{head_tweet_id:id}]},{$addToSet:{tribes:tribe}})
       }
    })