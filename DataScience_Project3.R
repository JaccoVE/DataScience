library(DBI)
library(RPostgreSQL)
library(readr)
library(dplyr)
library(lubridate)

data_main <- read_delim(file = "Users/Duncan/Desktop/Studie/18-19 1A/Data Science/Project/R",
                        delim = ";", 
                        col_names = TRUE, 
                        col_types = NULL)