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
metaRoad <- data.table::fread(file = paste(f_road, sep="", collapse=NULL),
                                        nThread = 24)

# Select coordinates in range of Amsterdam area
metaRoad <- metaRoad[metaRoad$location_locationForDisplay_latitude >= 52.2602 & 
                       metaRoad$location_locationForDisplay_latitude <= 52.5066 &
                       metaRoad$location_locationForDisplay_longitude >= 4.5689 &
                       metaRoad$location_locationForDisplay_longitude <= 5.0558,]

# Collect garbage
gc()

# Select columns
metaRoad = metaRoad %>%
  select(
    "location_locationForDisplay_latitude",
    "location_locationForDisplay_longitude",
    "location_carriageway") %>%
  rename(
    startLocatieForDisplayLat = "location_locationForDisplay_latitude",
    startLocatieForDisplayLong = "location_locationForDisplay_longitude",
    carriageway = "location_carriageway") %>%
  unique() %>%
  mutate(roadID = row_number())

# Save to csv file
data.table::fwrite(metaRoad,
                   nThread = 24,
                   file = paste(f_output, "metaRoad.csv", sep="", collapse=NULL),
                   sep = sep_symbol)

# Collect garbage
gc()

