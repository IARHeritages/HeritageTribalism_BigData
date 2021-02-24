### Data import

## 1. Import json files with Cheddarman tweets:

# Get the names of file from the directrory (~path) where the json files have been saved
FILES=~path/*

for f in $FILES #Set up a loop to iterate over each file
do
  echo "Processing file..." #Display which file is being processed
  mongoimport --file $f --host localhost:27017 --db IARH --collection cheddarman #Import the file into the IARH database and collection called cheddarman, 
  # take action on each file. $f store current file name
  cat $f
done

## 2. Import the topic assignment:

mongoimport --db=IARH --collection=cheddarman_topics --type=csv --headerline --file=/~path/data/tweetsTopics.csv