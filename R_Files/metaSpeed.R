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
registerDoMC(6)

# Seperated for csv save
sep_symbol <- ","

# Folder Locations
#f_main <- "/media/jacco/HDD/DataScienceData/Data/NDW/"
f_main <- "/home/jacco/Documents/DataScienceData/Data/NDW/"
f_speed_meta <- "utwente snelheden groot amsterdam 1 dag met metadata 20160916T105028 197/utwente snelheden groot amsterdam  1 dag met metadata_snelheid_00001.csv"
f_output <- "/home/jacco/Documents/Git/DataScience/Database/"

# --------------------------------------------------
# Create UniqueSiteReference tables speed ----------

# Collect garbage
gc()

# Load meta data speed
metaSpeed <- data.table::fread(file = paste(f_main, f_speed_meta, sep="", collapse=NULL),
                                        nThread = 24)

# Unique Sites
metaSpeed <- metaSpeed %>%
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
    speedID = row_number())

# Save to csv file
data.table::fwrite(metaSpeed,
                   nThread = 24,
                   file = paste(f_output, "metaSpeed.csv", sep="", collapse=NULL),
                   sep = sep_symbol)

# Collect garbage
gc()

