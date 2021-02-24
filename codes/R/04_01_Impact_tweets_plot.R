# Set working directory:
setwd("~path")

# Load libraries:
library(ggplot2)
library(viridis)

#Define input paths:
path2impactTweets<-"data/impactTweetsAll.csv"

#Define output paths:
path2tf<-"outputs/03_ImpactTweets.png"

# Read the data:
tweets<-read.csv("data/impactTweetsAll.csv")

# Source theme:

source("codes/R/theme_cheddarman.R")
# Plot the most impactful tweets:
tg<-ggplot(data=tweets, aes(x=reorder(X_id,total),y=total))+
    geom_point(size=0.5)+
    coord_flip()+
    theme_bw()+
    theme_cheddarman+
    scale_y_continuous(expand=c(0.01,0))+
    labs(x="Tweet\n",y="\nTotal number of interactions received by the tweet")+
    theme(axis.text.y=element_blank(),
           panel.grid.major.y=element_blank())+
    geom_text(aes(x=nrow(tweets)-nrow(tweets[tweets$total>50,])+20, label="threshold of 50 interaction", y=5000))+
    geom_vline(xintercept = nrow(tweets)-nrow(tweets[tweets$total>50,]),linetype='dashed',color=plasma(10)[5])

# Save the most impactful tweets:
width=21
height=21
ggsave(path2tf,tg,width = width, height = height, units = "cm",device='png',dpi=600)