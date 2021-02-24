### Topic model diagnostics ###
## This codes plots:
## 1. The coherence scores for lda topic models of tweets
## 2. The distribution of probabilities for dominant topics assigned to tweets

# set working directory
setwd("~path")

# load the library:
library(ggplot2)
library("viridis") 

# define the paths

#Inputs:
path2coherence<-'outputs/07_coherenceScores.csv'
path2tweets<-"data/tweetsTopics.csv"
# Outputs:
path2coherencePlot<-"outputs/03_03_01_coherenceScores.png"
path2tp<-"outputs/08_topic_probabilities.png"

# load the data
coherence<-read.csv(path2coherence)
tweets<-read.csv(path2tweets)

# Load the theme:

source('codes/R/theme_cheddarman.R')

## 1. Plot coherence scores

# Define the plot for coherence scores:
# Define the plot for coherence scores:
cplot<-ggplot(coherence,aes(x=n_topics,y=coherence_score))+
    geom_line()+
    theme_bw()+
    theme_cheddarman+
    scale_x_continuous(breaks=c(2:29),limits = c(2,29),expand=c(0,0))+
    scale_y_continuous(breaks=seq(0.32,0.48, by=0.02),limits = c(0.32,0.48),expand=c(0,0))+
    labs(y="Coherence score\n",x="\nNumber of topics")

#### Save plot:

height=21/2
width=21
ggsave(path2coherencePlot,cplot,width = width, height = height, units = "cm",device='png',dpi = 600)

## 2. Plot the topic assignment ptobabilities
tp<-ggplot(tweets, aes(x=dominant_topic,y=dominant_value)) + 
  geom_boxplot(width=0.1,fill=plasma(10)[9])+
  theme_bw()+
  theme_cheddarman+
  labs(y="Topic probability\n",x="")+
  geom_violin(data=tweets, aes(x="topic probability",y=dominant_value),fill=plasma(10)[9])+
  geom_boxplot(data=tweets, aes(x="topic probability",y=dominant_value),width=0.1,fill="white")+
  scale_x_discrete(labels=c("Topic 1","Topic 2", "Topic 3", "Topic 4","Topic 5", "Topic 6", "Topic 7", "Topic 8", "Topic 9","All topics"))

### Save plot:
### Save plot:
height=21/2
width=21
ggsave(path2tp,tp,width = width, height = height, units = "cm",device='png',dpi = 600)