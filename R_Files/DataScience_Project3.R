# Load libraries
library(DBI)
library(RPostgreSQL)
library(readr)
library(dplyr)
library(lubridate)
library(xml2)
library(data.table)
library(gtools)
library(parallel)

# --------------------------------------------------
# Settings -----------------------------------------

# Folder Locations
f_main <- "/media/jacco/HDD/DataScienceData/Data/NDW/"
f_intensity_meta <- "utwente intensiteiten groot amsterdam 1 dag met metadata (2) 20160916T104708 197/utwente intensiteiten groot amsterdam  1 dag met metadata (2)_intensiteit_00001.csv"
f_intensity <- "utwente intensiteiten groot amsterdam/utwente intensiteiten groot amsterdam _intensiteit_000"
f_time_meta <- "utwente reistijden groot amsterdam 1 dag met metadata 20160916T103803 197/utwente reistijden groot amsterdam  1 dag met metadata_reistijd_00001.csv"
f_time <- "utwente reistijden groot amsterdam 20160916T115957 197/utwente reistijden groot amsterdam  _reistijd_000"
f_speed_meta <- "utwente snelheden groot amsterdam 1 dag met metadata 20160916T105028 197/utwente snelheden groot amsterdam  1 dag met metadata_snelheid_00001.csv"
f_speed <- "utwente snelheden groot amsterdam/utwente snelheden groot amsterdam _snelheid_000"

# --------------------------------------------------
# Load meta data -----------------------------------

# Load meta data intensity
data_intensity_meta <- data.table::fread(file = paste(f_main, f_intensity_meta, sep="", collapse=NULL),
                                    nThread = getDTthreads())

# Load meta data travel time
data_time_meta <- data.table::fread(file = paste(f_main, f_time_meta, sep="", collapse=NULL),
                                          nThread = getDTthreads())

# Load meta data speed
data_speed_meta <- data.table::fread(file = paste(f_main, f_speed_meta, sep="", collapse=NULL),
                                          nThread = getDTthreads())

# --------------------------------------------------
# Load data ----------------------------------------

# Load data of one day intensity (nrows=8176606)
data_intensity <- data.table::fread(file = paste(f_main, f_intensity, "01.csv", sep="", collapse=NULL),
                               nThread = getDTthreads())

# Load data of one day travel time (nrows=1207547)
data_time <- data.table::fread(file = paste(f_main, f_time, "01.csv", sep="", collapse=NULL),
                                     nThread = getDTthreads())

# Load data of one day speed (nrows=7898604)
data_speed <- data.table::fread(file = paste(f_main, f_speed, "01.csv", sep="", collapse=NULL),
                                     nThread = getDTthreads())

# --------------------------------------------------
# Select required data -----------------------------

# Generate msiteUniqueInfo table
msiteUniqueInfo <- data_intensity_meta %>%
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
indexUniqueInfo <- data_intensity_meta %>%
  select("index",
         "specificLane",
         "specificVehicleCharacteristics") %>%
  arrange(
        index) %>%
  group_by(
        index) %>%
  distinct() %>%
  ungroup()

# --------------------------------------------------
# Combine data -------------------------------------

# Join meta data and data
data_main <- data_intensity %>%
  full_join(data_speed, by = c("prod_name" = "name",
                               "prod_category" = "category",
                               "prod_subcategory" = "subcategory")) %>%
  select( -prod_name, 
          -prod_category, 
          -prod_subcategory)







# Tableau table for Duncan-chan witch coordinates
coordinates <- data_intensity_meta %>%
  select("startLocatieForDisplayLat",
         "startLocatieForDisplayLong") %>%
  arrange(
    startLocatieForDisplayLat) %>%
  group_by(
    startLocatieForDisplayLat) %>%
  distinct() %>%
  ungroup()

data.table::fwrite(coordinates, file = "coordinates.csv")
