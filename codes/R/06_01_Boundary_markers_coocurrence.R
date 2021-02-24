### Boundary markers cooccurrence ###
## This code creates and plots the feature co-occurance matrix for different categories of boundary markers within tweets written by the same authors and within the communities, in the largest network component:

#Load libraries:
library(lubridate)
library(stringr)
library(sna)
library(tsna)
library(ndtv)
library(plyr)
library(ggnetwork)
library(ggplot2)
library(ggraph)
library(igraph)
library("intergraph")# for community structure
library("data.table")# for binding the list of lists
library("tidyr") # For gather function
library(ggpubr)
library(quanteda)
library(viridis)

setwd("~path")

#paths to inputs:

path2users<-'data/users.csv'
path2tweetsTopics<-'data/tweetsTopics.csv'
path2labels<-"outputs/06_Topic_labels.csv"

# paths to outputs:

path2tribes<-'outputs/10_userTribes.csv'
path2tribes_network<-'outputs/11_userTribes.png'


# source theme

source('codes/R/theme_cheddarman.R')

# load data
labels<-read.csv(path2labels)
users<-read.csv(path2users)
tweets<-read.csv(path2tweetsTopics)
impactTweets<-read.csv(path2impactTweets)


### Tribes-users co-occurrence

tribes_names<-c("racial views","political leaning","Trust in experts","anti-semitism","Newspaper Readership",
                "values","Neo-nazizm","UK-EU position")# Clean the tribes column
users$tribes<-gsub("\\[","",users$tribes)
users$tribes<-gsub("\\]","",users$tribes)
users$tribes<-gsub(",","",users$tribes)
users$tribes<-gsub(" ","",users$tribes)
users$tribes<-gsub('"'," ",users$tribes)
# Construct the fcm and matrix of co-occurance for tribes
tribes_fcm<-fcm(users$tribes,context = c("document"))
colnames(tribes_fcm)<-toupper(tribes_names)
rownames(tribes_fcm)<-toupper(tribes_names)
tribes.df<-as.data.frame(as.matrix(tribes_fcm))

#Save the matrix of co-occurance:
write.csv(tribes.df,path2tribes)

# Plot the tribes
png(path2tribes_network)
textplot_network(tribes_fcm,edge_color =plasma(10)[5])
dev.off()