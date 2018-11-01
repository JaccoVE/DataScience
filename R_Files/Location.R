# Load libraries
library(DBI)
library(RPostgreSQL)
library(readr)
library(dplyr)
library(lubridate)
library(xml2)
library(rgdal)
library(data.table)
library(gtools)
library(foreach)
library(doMC)

# --------------------------------------------------
# Settings -----------------------------------------

# Number of threads to use when performing the for loop
registerDoMC(8)

# Seperated for csv save
sep_symbol <- ","

# Folder Locations
f_database <- "/home/jacco/Documents/Git/DataScience/Database/"
f_output <- "/home/jacco/Documents/Git/DataScience/Database/"

# Load metaRoad
metaRoad = data.table::fread(file = paste(f_database, "metaRoad", ".csv", sep="", collapse=NULL),
                               nThread = 24)

# Load metaFlow
metaFlow = data.table::fread(file = paste(f_database, "metaFlow", ".csv", sep="", collapse=NULL),
                             nThread = 24)

# Load metaSpeed
metaSpeed = data.table::fread(file = paste(f_database, "metaSpeed", ".csv", sep="", collapse=NULL),
                             nThread = 24)

