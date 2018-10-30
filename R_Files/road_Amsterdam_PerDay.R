# Load libraries
library(DBI)
library(RPostgreSQL)
library(readr)
library(dplyr)
library(lubridate)
library(xml2)
library(ggplot2)
library(sp)
library(rgdal)
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

# Select coordinates in range of Amsterdam area
road_data <- road_data[road_data$location_locationForDisplay_latitude >= 52.2602 & 
                       road_data$location_locationForDisplay_latitude <= 52.5066 &
                       road_data$location_locationForDisplay_longitude >= 4.5689 &
                       road_data$location_locationForDisplay_longitude <= 5.0558,]

# Collect garbage
gc()

# Change time format
road_data <- road_data %>%
  mutate(
    date_start = format(strptime(road_data$validity_overallStartTime,format="%Y-%m-%dT%H:%M"), "%Y-%m-%d"),
    date_end   = format(strptime(road_data$validity_overallEndTime,format="%Y-%m-%dT%H:%M"), "%Y-%m-%d"))%>%
  select( 
    -validity_overallStartTime,
    -validity_overallEndTime)

# Collect garbage
gc()

# Add day of the week
road_data <- road_data %>%
  mutate(
    week_day_start = weekdays(as.Date(road_data$date_start,'%Y-%m-%d')),
    week_day_end   = weekdays(as.Date(road_data$date_end,'%Y-%m-%d')))

# Save to csv file
data.table::fwrite(road_data,
                   nThread = 24,
                   file = paste(f_output, "road_data_Amsterdam_PerDay.csv", sep="", collapse=NULL),
                   sep = sep_symbol)

# Collect garbage
gc()

