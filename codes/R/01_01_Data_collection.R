### Data collection

#Set the working directory:
setwd(~path)

#Load required library
library(streamR)

# Create the token for authentication using details of your Twitter app 
# The app needs to be created on the twitter developer website first https://developer.twitter.com/en.
# More detail instructions for app creation are given in https://cran.r-project.org/web/packages/rtweet/vignettes/auth.html

my_oauth<-createOAuthToken(consumerKey, consumerSecret, accessToken, accessTokenSecret)

# For the future use, save and load the token:
save(my_oauth, file = "~token.RData")
load("~token.Rdata")

# Define keyword to stream:
keywords <- c("cheddar man", "cheddarman")

# Stream tweets with the keywords using into the file defined in file.name and created token.
# Set up the stream inside an infinite loop, so that in case there are interruptions it automatically resumes
# and starts writing to a new file (i.e. cheddarman_y_1,cheddarman_2... and so on):
 
i=1
j=2
while(i<j){
filterStream(file.name = paste("~cheddar_man",i,".json",sep=""),  #This will write tweetes to a new file after every interruption
             track = keywords, 
             oauth = my_oauth) 
i=i+1 
j=j+1 #This ensures infinite loop
}