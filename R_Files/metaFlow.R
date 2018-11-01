# Load libraries
library(DBI)
library(RPostgreSQL)
library(readr)
library(dplyr)
library(lubridate)
library(xml2)
library(data.table)
library(gtools)
library(foreach)
library(doMC)

# --------------------------------------------------
# Settings -----------------------------------------

# Number of threads to use when performing the for loop
registerDoMC(9)

# Seperated for csv save
sep_symbol <- ","

# Folder Locations
#f_main <- "/media/jacco/HDD/DataScienceData/Data/NDW/"
f_main <- "/home/jacco/Documents/DataScienceData/Data/NDW/"
f_flow_meta <- "utwente intensiteiten groot amsterdam 1 dag met metadata (2) 20160916T104708 197/utwente intensiteiten groot amsterdam  1 dag met metadata (2)_intensiteit_00001.csv"
f_output <- "/home/jacco/Documents/Git/DataScience/Database/"

# --------------------------------------------------
# Create UniqueSiteReference tables flow ------

# Collect garbage
gc()

# Load meta data flow
metaFlow <- data.table::fread(file = paste(f_main, f_flow_meta, sep="", collapse=NULL),
                                        nThread = 24)

# Unique Sites
metaFlow <- metaFlow %>%
  select(
    "measurementSiteReference",
    "index",
    "specificVehicleCharacteristics",
    "startLocatieForDisplayLat",
    "startLocatieForDisplayLong",
    "ROADNUMBER") %>%
  arrange(
    measurementSiteReference) %>%
  group_by(
    measurementSiteReference) %>%
  distinct() %>%
  ungroup() %>%
  mutate(
    flowID = row_number())

# Save to csv file
data.table::fwrite(metaFlow,
                   nThread = 24,
                   file = paste(f_output, "metaFlow.csv", sep="", collapse=NULL),
                   sep = sep_symbol)

# Collect garbage
gc()

