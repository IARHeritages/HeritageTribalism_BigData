// Aggregate by id field:

db[collection+"_tweetsImpact"].aggregate([{$lookup:
                         {
                             from: collection+"_tweetsTopics",
                             localField: "_id",
                             foreignField: "_id",
                             as: "metadata"}},
                        {$replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$metadata", 0 ] }, "$$ROOT" ] } }},  
                        {$project: { metadata: 0 }},
                        {$out:collection + "_tweetsImpact"}])

//