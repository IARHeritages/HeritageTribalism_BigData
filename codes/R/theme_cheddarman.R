### Themes and labels ###
## This file defines themes use for plots:

#Load the libraries:
library(ggplot2)

theme_cheddarman<-theme(axis.text=element_text(size=11),
      axis.title.y=element_text(size=12),
      plot.title=element_blank(),
      panel.grid.minor.x=element_blank(),
      panel.border=element_rect(),
      axis.line = element_line(),
      legend.spacing.y = unit(0.5, 'cm'),
      strip.background = element_rect(
      color="black", fill=NA, size=1.5, linetype="solid"
     ))