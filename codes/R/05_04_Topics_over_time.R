### Topics over time ###
## This code prepares the visualisation of the frequencies of tweets assigned to specific topics over time.
## It also prepares the table showing the proportions of tweets assigned to specific topics for selected dates.

# set working directory
setwd("~path")

# load the library:
library(ggplot2)
library(lubridate)
library(reshape2)
library(viridis)
library(RColorBrewer)

# define the paths
#Inputs:
path2tweets<-"data/tweetsTopics.csv"
path2labels<-"outputs/06_Topic_labels.csv"

# Outputs:
path2time_series<-"outputs/09_time_series.png"

# Load the theme for plotting
source("codes/R/theme_cheddarman.R")

# load the data
tweets<-read.csv(path2tweets)
labels<-read.csv(path2labels)


# Time series:
topic_series<-tweets #assign tweets to a new variable for data transformation
topic_series$created_at<-ymd_hms(topic_series$created_at, tz = "UTC") #Reformat created_at field
topic_series<-subset(topic_series,created_at>="2018-02-07 06:00:00") #Only include tweets posted after 6 am on the 7th of Fabuary - the time of the article publication
topic_series$date<-date(topic_series$created_at) #Creat a data field to aggregate the tweets by:

# Reformat data for plotting
ts<-as.data.frame(cbind(table(topic_series$date,topic_series$dominant_topic)))
ts$total<-rowSums(ts)
ts$date<-as.Date(rownames(ts))
meltdf<-melt(ts[c(1:9,11)],id="date")

topic_labels<-paste("Topic",labels[,1],labels[,2])
# Define scale factor for the total number of tweets in comparison to the number of tweets per topic
scaleFactor <-max(ts$total)/ max(meltdf$value)

# Plot the time series:
# Plot the time series:
time_series<-ggplot()+
    geom_bar(data=ts,aes(x=date,y=total),stat = "identity",fill = "lightyellow",color="lightgrey")+
    geom_line(data=meltdf,aes(x=date,y=value*scaleFactor,colour=variable,group=variable),size=0.4,alpha=1)+
    theme_bw()+
    theme_cheddarman+
    labs(x="Date")+
    scale_y_continuous(name="Total number of tweets\n", sec.axis=sec_axis(~./scaleFactor, name="Number of tweets per topic\n"),expand=c(0,0))+
    scale_x_date(expand=c(0,0))+
    scale_color_manual(name = "Topics", labels = topic_labels,values=c(brewer.pal(9, "Set1")[1:5], "black",brewer.pal(9, "Set1")[7:9]))+
    # Add segments for dates we want to highligh:
    geom_segment(aes(x=ts$date[17],y=1,xend=ts$date[17],yend=40000), linetype=5, color="darkgrey", size=0.5)+
    geom_segment(aes(x=ts$date[13],y=1,xend=ts$date[13],yend=25000), linetype=5, color="darkgrey", size=0.5)+
    geom_segment(aes(x=ts$date[26],y=1,xend=ts$date[26],yend=25000), linetype=5, color="darkgrey", size=0.5)+
    geom_text(aes(x=ts$date[17], label="23rd February", y=42000), colour="black")+
    geom_text(aes(x=ts$date[13], label="19th February", y=27000), colour="black")+
    geom_text(aes(x=ts$date[26], label="3rd March", y=27000), colour="black")+
    theme(legend.position=c(0.7,0.785),
         legend.background=element_rect(color="black",size=0.2))

# Save the time series:
width=21
height=18
ggsave(path2time_series,time_series,width = width, height = height, units = "cm",device='png',dpi=600)