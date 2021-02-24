### Links frequencies
## This code prepares visualisation of links frequencies, including:
# - barplots of most frequent links (link frequency >200)
# - animation of 10 most frequent links by day, over the course of the data collection
# - 10 most frequent links for choosen dates
# set working directory:

setwd("~path")

# Load libraries:

library(ggplot2)
library(lubridate)
library(viridis)
library(gganimate)
library(av)
# Source theme
source('codes/R/theme_cheddarman.R')

# set paths:
# inputs:

path2links<-'data/links.csv'
path2linksByDate<-'data/linksByDate.csv'
path2anonymisedLinks<-'data/anonymised_links.csv'


# outputs:
path2anonymisedByDate<-'data/anonymised_linksByDate.csv'
path2firstDay <-'data/l0207.csv'
path2peakDates <-'data/peak_links.csv'
path2linksDateFreqTable<-'outputs/02_linksByDate.csv'


# Read the data:
links<-read.csv(path2links)
lbd<-read.csv(path2linksByDate)
an_links<-read.csv(path2anonymisedLinks)

# Subset the data to match the anonymised links:

links<-subset(links,links$count>=20)

# Add the column with anonymised links for the visualisation
links$anonymised<-an_links$X_id
links$anonymised<-sub("anonymised","",links$anonymised)

# Also, save the equivalent table as csv to include in the paper
san_links<-subset(an_links,an_links$count>200)[c(2:3)]
colnames(san_links)<-c("Link","Frequency")
write.csv(san_links, path2linksDateFreqTable, row.names=FALSE)

## Prepare the figure of links by date
# Modify column names of links by date:
colnames(lbd)<-c("id","created_at","expanded_url","display_url","url")

# Get only unique records:
lbd<-unique(lbd)

# Transform the date field, and include only the tweets posted after the article was published:
lbd$created_at<-ymd_hms(lbd$created_at, tz = "UTC") # Transforms created_at column, into time format
lbd<-subset(lbd,created_at>="2018-02-07 06:00:00") # Subsets only links included after the cheddarman news was published
lbd$date<-date(lbd$created_at) # Transforms column created_at into a date

# Get the link frequencies by date:
lbd<-table(lbd$expanded_url,lbd$date) # Make a frequency table of urls by date
lbd<-as.data.frame(lbd) # Transform the frequency table into the data frame
lbd<-lbd[order(-lbd$Freq),] # Order the date frame by dates
colnames(lbd)<-c("Link","Date","Frequency") # Change the names on the columns to the more meaningful ones

# Assign ranks for each date:
dates<-unique(lbd$Date) # Get unique dates
lbd$Rank<-NA # Create a rank column
# Iterate over all dates, and for each assigne the rank for the url
for (date in dates){lbd[lbd$Date==date,]$Rank<-rank(-lbd[lbd$Date==date,]$Frequency,ties.method="first")} 

# Merge data frames, to get the anonymised column
anonymisedlbd<-merge(lbd, links,by.x = 'Link', by.y = "X_id", all.x =TRUE, all.y = FALSE,sort=FALSE)

# Anonymise the links that feature less than 20 times in the dataset overall
anonymisedlbd[!(anonymisedlbd$Link %in% links$X_id),]$anonymised<-''

# Subset links for specific dates (first day and peak dates)
dates<-c('2018-02-07','2018-02-19','2018-02-23','2018-03-03')

l0207<-lbd[lbd$Date==dates[1] ,]
l0219<-lbd[lbd$Date==dates[2] ,]
l0223<-lbd[lbd$Date==dates[3] ,]
l0303<-lbd[lbd$Date==dates[4] ,]

# Manually clean the top links to make the tables for the article:

# Qualitative analysis revealed that some website have multiple links pointing to them. List and aggregate these:

# The guardian article:
g_links<-c("https://www.theguardian.com/science/2018/feb/07/first-modern-britons-dark-black-skin-cheddar-man-dna-analysis-reveals","https://www.theguardian.com/science/2018/feb/07/first-modern-britons-dark-black-skin-cheddar-man-dna-analysis-reveals?CMP=share_btn_tw",
          "https://www.theguardian.com/science/2018/feb/07/first-modern-britons-dark-black-skin-cheddar-man-dna-analysis-reveals?CMP=Share_iOSApp_Other",
          "https://amp.theguardian.com/science/2018/feb/07/first-modern-britons-dark-black-skin-cheddar-man-dna-analysis-reveals?CMP=Share_AndroidApp_Tweet&__twitter_impression=true")

# The AFP tweet:
afp_links<-c("https://twitter.com/AFP/status/961122278707691520","https://twitter.com/afp/status/961122278707691520")

# The bbc article:
bbc_links<-c("http://bbc.in/2C14JbF","https://twitter.com/BBCNews/status/961156442672848896","http://bbc.in/2E7ylpJ")

# The bbc tweet:
bbcn_links<-c("https://twitter.com/BBCNews/status/961156442672848896","https://twitter.com/bbcnews/status/961156442672848896")

# Add additional bbc article
bbcs_links<-c("http://www.bbc.co.uk/news/science-environment-42939192","http://www.bbc.co.uk/news/science-environment-42939192")

# New scientist
ns_links<-c("https://www.newscientist.com/article/2161867-ancient-dark-skinned-briton-cheddar-man-find-may-not-be-true/",
            "http://bit.ly/2BIrULq",
            "http://bit.ly/2CcHGiz",
           "https://www.newscientist.com/article/2161867-ancient-dark-skinned-briton-cheddar-man-find-may-not-be-true/amp/?__twitter_impression=true",
           "http://bit.ly/2EPc84g",
           "http://bit.ly/2EL6Vuo",
           "https://www.newscientist.com/article/2161867-ancient-dark-skinned-briton-cheddar-man-find-may-not-be-true/?utm_campaign=Echobox&utm_medium=SOC&utm_source=Twitter#link_time=1519306987",
           "https://www.newscientist.com/article/2161867-ancient-dark-skinned-briton-cheddar-man-find-may-not-be-true/?utm_campaign=Echobox&utm_medium=SOC&utm_source=Twitter#link_time=1519296985")

# Republic standard
rs_links<-c("https://republicstandard.com/black-cheddar-man-war-white-british-identity/",
           "https://republicstandard.com/black-cheddar-man-war-white-british-identity/amp/?__twitter_impression=true")

# Daily mail
dm_links<-c("http://www.dailymail.co.uk/sciencetech/article-5453665/Was-Cheddar-man-white-all.html",
            "http://dailym.ai/2taEv7u")

# Prepare top 10 links on the first day of the data collection
# Aggregate the links in the dataset for 07.02:
l0207[l0207$Link==bbc_links[2],]$Frequency<-sum(l0207[l0207$Link %in% bbc_links,]$Frequency)
l0207<-l0207[!(l0207$Link %in% bbc_links[c(1,3)]),]
l0207[l0207$Link==bbcn_links[1],]$Frequency<-sum(l0207[l0207$Link %in% bbcn_links,]$Frequency)
l0207<-l0207[!(l0207$Link %in% bbcn_links[c(2:3)]),]
l0207[l0207$Link==g_links[1],]$Frequency<-sum(l0207[l0207$Link %in% g_links,]$Frequency)
l0207<-l0207[!(l0207$Link %in% g_links[c(2:4)]),]
l0207[l0207$Link==afp_links[1],]$Frequency<-sum(l0207[l0207$Link %in% afp_links,]$Frequency)
l0207<-l0207[!(l0207$Link %in% afp_links[c(2)]),]
l0207[l0207$Link==bbcs_links[1],]$Frequency<-sum(l0207[l0207$Link %in% bbcs_links,]$Frequency)
l0207<-l0207[!(l0207$Link %in% bbcs_links[c(2)]),]
l0207<-l0207[order(-l0207$Frequency),]

# Manually remove the tweets from individuals from the table and leave only websites/media organisation tweets:
l0207<-l0207[c(1:4,6:7,11:14),]

# Save the csv with the links
write.csv(l0207,file="data/l0207.csv")

# Get frequencies higher than 10 only, for the peak dates

# l0219
l0219<-l0219[l0219$Frequency>10,]
# Manually remove private individulas links from the table
l0219<-l0219[c(1:2,4,9),]
# For this date, a qualitative analysis of the links reaveled more links pointing to the first two websites, so manually add the count:
# This adds count from modified links
l0219$Frequency[1]<-76+8
l0219$Frequency[2]<-61+7


#l0223
# Aggregate the links for the same websites for l0223
l0223[l0223$Link==ns_links[1],]$Frequency<-sum(l0223[l0223$Link %in% ns_links,]$Frequency)
l0223<-l0223[!(l0223$Link %in% ns_links[c(2:8)]),]
l0223[l0223$Link==rs_links[1],]$Frequency<-sum(l0223[l0223$Link %in% rs_links,]$Frequency)
l0223<-l0223[!(l0223$Link %in% rs_links[c(2)]),]
# Subset only the links that featured more than 10 times, and order the data:
l0223<-l0223[l0223$Frequency>=10,]
l0223<-l0223[order(-l0223$Frequency),]
# Manually remove the links to private individual accounts:
l0223<-l0223[c(1:4,7,8:10,14),]


# l0303
# Aggregate by link
l0303[l0303$Link==dm_links[1],]$Frequency<-sum(l0303[l0303$Link %in% dm_links,]$Frequency)
l0303<-l0303[!(l0303$Link %in% dm_links[c(2)]),]
# Get only links featured >10 times and order the table
l0303<-l0303[l0303$Frequency>=10,]
l0303<-l0303[order(-l0303$Frequency),]
# Manually remove the links to the private individual's accounts
l0303<-l0303[c(1,3),]


# Bind the links into 1 table:
peak_links<-rbind(l0219,l0223,l0303)[,1:3]

# Save the table:
write.csv(peak_links,file="data/peak_links.csv")