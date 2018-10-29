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
#f_main <- "/media/jacco/HDD/DataScienceData/Data/Status/"
f_main <- "/home/jacco/Documents/DataScienceData/Data/Status/"
f_road <- "/home/jacco/Documents/Git/DataScience/Status/Tableau Input Files/road_data.csv"
f_output <- "/home/jacco/Documents/Git/DataScience/Status/Tableau Input Files/"

# ----------------------------------------------------
# Select all relevant RoadMaintenance's for Amsterdam

# Collect garbage
gc()

# Load meta data intensity
road_data <- data.table::fread(file = paste(f_road, sep="", collapse=NULL),
                                        nThread = 24)

# Unique Sites
intensity_unique <- intensity_unique %>%
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
    trafficID = row_number())

# Save to csv file
data.table::fwrite(intensity_unique,
                   nThread = 24,
                   file = paste(f_output, "intensity_unique.csv", sep="", collapse=NULL),
                   sep = sep_symbol)

# Collect garbage
gc()
