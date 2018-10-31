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
f_meta <- "/home/jacco/Documents/Git/DataScience/Status/Tableau Input Files/metaRoad.csv"
f_output <- "/home/jacco/Documents/Git/DataScience/Status/Tableau Input Files/"

# ----------------------------------------------------
# Generate dataRoad.csv

# Collect garbage
gc()

# Load meta data road
metaRoad <- data.table::fread(file = paste(f_meta, sep="", collapse=NULL),
                              nThread = 24)

# Load data road
dataRoad <- data.table::fread(file = paste(f_road, sep="", collapse=NULL),
                              nThread = 24)

# Select coordinates in range of Amsterdam area
dataRoad <- dataRoad[dataRoad$location_locationForDisplay_latitude >= 52.2602 & 
                       dataRoad$location_locationForDisplay_latitude <= 52.5066 &
                       dataRoad$location_locationForDisplay_longitude >= 4.5689 &
                       dataRoad$location_locationForDisplay_longitude <= 5.0558,]

# Collect garbage
gc()

# Select columns
dataRoad = dataRoad %>%
  select(
    "location_locationForDisplay_latitude",
    "location_locationForDisplay_longitude",
    "location_carriageway",
    "validity_overallStartTime",
    "validity_overallEndTime",
    "probabilityOfOccurrence",
    "sourceName",
    "operatorActionStatus") %>%
  rename(
    Lat = "location_locationForDisplay_latitude",
    Long = "location_locationForDisplay_longitude",
    carriageway_data = "location_carriageway") %>%
  unique()

# Change time format
dataRoad <- dataRoad %>%
  mutate(
    date_start = format(strptime(dataRoad$validity_overallStartTime,format="%Y-%m-%dT%H:%M"), "%Y-%m-%d-%H"),
    date_end   = format(strptime(dataRoad$validity_overallEndTime,format="%Y-%m-%dT%H:%M"), "%Y-%m-%d-%H"))%>%
  select( 
    -validity_overallStartTime,
    -validity_overallEndTime)

# Collect garbage
gc()

# Add day of the week
dataRoad <- dataRoad %>%
  mutate(
    week_day_start = weekdays(as.Date(dataRoad$date_start,'%Y-%m-%d')),
    week_day_end = weekdays(as.Date(dataRoad$date_end,'%Y-%m-%d')))

# Combine data with meta data
dataRoad = dataRoad %>%
  full_join(
    metaRoad, 
    by = c(
      "Lat" = "startLocatieForDisplayLat",
      "Long" = "startLocatieForDisplayLong",
      "carriageway_data" = "carriageway")) %>%
  select( 
    -Lat, 
    -Long,
    -carriageway_data)

dataRoad = dataRoad %>%
  select(
    "date_start",
    "date_end",
    "week_day_start",
    "week_day_end",
    "roadID",
    "probabilityOfOccurrence",
    "operatorActionStatus",
    "sourceName") %>%
  arrange(
    date_start,
    date_end)

# Save to csv file
data.table::fwrite(dataRoad,
                   nThread = 24,
                   file = paste(f_output, "dataRoad.csv", sep="", collapse=NULL),
                   sep = sep_symbol)

# Collect garbage
gc()

