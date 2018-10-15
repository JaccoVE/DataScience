# Load libraries
library(DBI)
library(RPostgreSQL)
library(readr)
library(dplyr)
library(lubridate)
library(xml2)
library(data.table)
library(gtools)

if(.Platform$OS.type == "unix") {
    # Load meta data
    meta_data_main <- data.table::fread(file = "/media/jacco/HDD/DataScienceData/Data/NDW/utwente snelheden groot amsterdam 1 dag met metadata 20160916T105028 197/utwente snelheden groot amsterdam  1 dag met metadata_snelheid_00001.csv",
                                        nThread = getDTthreads())
  
    # Load data of one day
    data_main <- data.table::fread(file = "/media/jacco/HDD/DataScienceData/Data/NDW/utwente snelheden groot amsterdam/utwente snelheden groot amsterdam _snelheid_00001.csv",
                                   nrows = 7898604,
                                   nThread = getDTthreads())
} else {
	# Load meta data
    meta_data_main_speed <- data.table::fread(file = "D:/DataScienceData/Data/NDW/utwente snelheden groot amsterdam 1 dag met metadata 20160916T105028 197/utwente snelheden groot amsterdam  1 dag met metadata_snelheid_00001.csv")
    
    # Load data of one day
    data_main_speed <- data.table::fread(file = "D:/DataScienceData/Data/NDW/utwente snelheden groot amsterdam/utwente snelheden groot amsterdam _snelheid_00001.csv",
                                         nrows = 7898604)
    
}

# Generate msiteUniqueInfo table
msiteUniqueInfo <- meta_data_main %>%
  select(
    "measurementSiteReference",
    "generatedSiteName",
    "startLocatieForDisplayLat",
    "startLocatieForDisplayLong",
    "specificLocation",
    "alertCDirectionCoded") %>%
  arrange(measurementSiteReference) %>%
  group_by(
    measurementSiteReference) %>%
  distinct() %>%
  ungroup()

# Generate index table
indexUniqueInfo <- meta_data_main %>%
  select("index",
         "specificLane",
         "specificVehicleCharacteristics") %>%
  arrange(
        index) %>%
  group_by(
        index) %>%
  distinct() %>%
  ungroup()

# Tableau table for Duncan-chan witch coordinates
coordinates <- meta_data_main %>%
  select("startLocatieForDisplayLat",
         "startLocatieForDisplayLong") %>%
  arrange(
    startLocatieForDisplayLat) %>%
  group_by(
    startLocatieForDisplayLat) %>%
  distinct() %>%
  ungroup()

data.table::fwrite(coordinates, file = "coordinates.csv")

# Join meta data and data
data_main <- data_main %>%
  full_join(meta_data_main, by = c("prod_name" = "name",
                            "prod_category" = "category",
                            "prod_subcategory" = "subcategory")) %>%
  select( -prod_name, 
          -prod_category, 
          -prod_subcategory)


