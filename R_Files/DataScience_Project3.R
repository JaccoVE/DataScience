# Load libraries
library(DBI)
library(RPostgreSQL)
library(readr)
library(dplyr)
library(lubridate)
library(xml2)

# Load meta data
meta_data_main <- read.csv(file = "D:/DataScienceData/Data/NDW/utwente snelheden groot amsterdam 1 dag met metadata 20160916T105028 197/utwente snelheden groot amsterdam  1 dag met metadata_snelheid_00001.csv", nrows = 10000)

# Select required columns from meta data
meta_data_main <- meta_data_main %>%
  select(
      "measurementSiteReference",
      "measurementSiteVersion",
      "index",
      "computationMethod",
      "measurementSiteName1",
      "measurementSiteNumberOfLanes",
      "measurementSide",
      "specificLane",
      "specificVehicleCharacteristics",
      "startLocatieForDisplayLat",
      "startLocatieForDisplayLong",
      "specificLocation") %>%
  arrange(
      "measurementSiteReference",
      "index") %>%
  group_by(
      "measurementSiteReference",
      "index") %>%
  distinct() %>%
  ungroup() %>%

# Load data
data_main <- read.csv(file = "D:/DataScienceData/Data/NDW/utwente snelheden groot amsterdam/utwente snelheden groot amsterdam _snelheid_00001.csv", nrows = 10000)

# Select required columns from data
data_main <- data_main %>%
  select("measurementSiteReference",
         "index",
         "periodStart",
         "periodEnd",
         "numberOfIncompleteInputs",
         "avgVehicleSpeed",
         "generatedSiteName"
  )

# Join meta data and data
data_main <- data_main %>%
  full_join(meta_data_main, by = c("prod_name" = "name",
                            "prod_category" = "category",
                            "prod_subcategory" = "subcategory")) %>%
  select( -prod_name, 
          -prod_category, 
          -prod_subcategory)


