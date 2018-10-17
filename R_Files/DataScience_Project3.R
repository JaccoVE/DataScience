# Load libraries
library(DBI)
library(RPostgreSQL)
library(readr)
library(dplyr)
library(lubridate)
library(xml2)
library(data.table)
library(gtools)
#library(parallel)

# --------------------------------------------------
# Settings -----------------------------------------
# Number of Threads to use when reading files from disk
num_threads <- 24

# Folder Locations
#f_main <- "/media/jacco/HDD/DataScienceData/Data/NDW/"
f_main <- "/home/jacco/Documents/DataScienceData/Data/NDW/"
f_intensity_meta <- "utwente intensiteiten groot amsterdam 1 dag met metadata (2) 20160916T104708 197/utwente intensiteiten groot amsterdam  1 dag met metadata (2)_intensiteit_00001.csv"
f_intensity <- "utwente intensiteiten groot amsterdam/utwente intensiteiten groot amsterdam _intensiteit_000"
f_speed_meta <- "utwente snelheden groot amsterdam 1 dag met metadata 20160916T105028 197/utwente snelheden groot amsterdam  1 dag met metadata_snelheid_00001.csv"
f_speed <- "utwente snelheden groot amsterdam/utwente snelheden groot amsterdam _snelheid_000"

# --------------------------------------------------
# Load meta data -----------------------------------

# Collect garbage
gc()

# Load meta data intensity
csv_intensity_meta <- data.table::fread(file = paste(f_main, f_intensity_meta, sep="", collapse=NULL),
                                        nThread = num_threads)

# Load meta data speed
csv_speed_meta <- data.table::fread(file = paste(f_main, f_speed_meta, sep="", collapse=NULL),
                                    nThread = num_threads)

# --------------------------------------------------
# Load data ----------------------------------------

# Load data of one day intensity (nrows=8176606)
csv_intensity <- data.table::fread(file = paste(f_main, f_intensity, "02.csv", sep="", collapse=NULL),
                                   nrows = 10000,
                                   nThread = num_threads)#,
#                                    nrows = dim(csv_intensity_meta)[1])

# Load data of one day speed (nrows=7898604)
csv_speed <- data.table::fread(file = paste(f_main, f_speed, "02.csv", sep="", collapse=NULL),
                               nrows = 10000,
                               nThread = num_threads)#,
#                                nrows = dim(csv_speed_meta)[1])

# --------------------------------------------------
# Select required data -----------------------------

# Select meta data
data_intensity_meta <- csv_intensity_meta %>%
  select(
    "measurementSiteReference",
    "index",
    "generatedSiteName",
    "startLocatieForDisplayLat",
    "startLocatieForDisplayLong",
    "specificLocation",
    "alertCDirectionCoded") %>%
  arrange(
    measurementSiteReference,
    index) %>%
  group_by(
    measurementSiteReference,
    index) %>%
  distinct() %>%
  ungroup() %>%
  mutate(
    intensity_id = row_number())

data_speed_meta <- csv_speed_meta %>%
  select(
    "measurementSiteReference",
    "index",
    "generatedSiteName",
    "startLocatieForDisplayLat",
    "startLocatieForDisplayLong",
    "specificLocation",
    "alertCDirectionCoded") %>%
  arrange(
    measurementSiteReference,
    index) %>%
  group_by(
    measurementSiteReference,
    index) %>%
  distinct() %>%
  ungroup() %>%
  mutate(
    speed_id = row_number())

# Save to file
data.table::fwrite(data_intensity_meta, file = "data_intensity_meta.csv")
data.table::fwrite(data_speed_meta, file = "data_speed_meta.csv")

# Select data
data_intensity <- csv_intensity %>%
  select(
    "measurementSiteReference",
    "index",
    "generatedSiteName",
    "periodStart",
    "periodEnd",
    "numberOfInputValuesused",
    "numberOfIncompleteInputs",
    "avgVehicleFlow",
    "avgVehicleSpeed") %>%
  rename(
    meas_site_ref = "measurementSiteReference",
    ind = "index",
    gen_site_name = "generatedSiteName",
    per_start = "periodStart",
    per_end = "periodEnd",
    num_in_use = "numberOfInputValuesused",
    num_in_in = "numberOfIncompleteInputs",
    avg_flow = "avgVehicleFlow",
    avg_speed = "avgVehicleSpeed") %>%
  arrange(
    meas_site_ref,
    ind,
    gen_site_name) %>%
  group_by(
    meas_site_ref) %>%
  distinct() %>%
  ungroup()

data_speed <- csv_speed %>%
  select(
    "measurementSiteReference",
    "index",
    "generatedSiteName",
    "periodStart",
    "periodEnd",
    "numberOfInputValuesused",
    "numberOfIncompleteInputs",
    "avgVehicleFlow",
    "avgVehicleSpeed") %>%
  rename(
    meas_site_ref = "measurementSiteReference",
    ind = "index",
    gen_site_name = "generatedSiteName",
    per_start = "periodStart",
    per_end = "periodEnd",
    num_in_use = "numberOfInputValuesused",
    num_in_in = "numberOfIncompleteInputs",
    avg_flow = "avgVehicleFlow",
    avg_speed = "avgVehicleSpeed") %>%
  arrange(
    per_start,
    per_end,
    num_in_use,
    num_in_in) %>%
  group_by(
    meas_site_ref) %>%
  distinct() %>%
  ungroup()

# --------------------------------------------------
# Combine data with meta data ----------------------

data_com_intensity <- data_intensity_meta %>%
  full_join(
    data_intensity, 
    by = c(
      "measurementSiteReference" = "meas_site_ref",
      "index" = "ind",
      "numberOfInputValuesused" = "num_in_use",
      "numberOfIncompleteInputs" = "num_in_in",
      "generatedSiteName" = "gen_site_name",
      "periodStart" = "per_start",
      "periodEnd" = "per_end")) %>%
  select( 
    -avgVehicleFlow, 
    -avgVehicleSpeed)

data_com_speed <- data_speed_meta %>%
  full_join(
    data_speed, 
    by = c(
      "measurementSiteReference" = "meas_site_ref",
      "index" = "ind",
      "numberOfInputValuesused" = "num_in_use",
      "numberOfIncompleteInputs" = "num_in_in",
      "generatedSiteName" = "gen_site_name",
      "periodStart" = "per_start",
      "periodEnd" = "per_end")) %>%
  select( 
    -avgVehicleFlow, 
    -avgVehicleSpeed)

# --------------------------------------------------
# Remove unuseful rows ----------------------------- 

# Remove all rows that contain NA's for flow or speed
data_com_intensity <- data_com_intensity[!is.na(data_com_intensity$avg_flow),]
data_com_speed <- data_com_speed[!is.na(data_com_speed$avg_speed),]

# Remove all rows that contain NA's for the coordinates
data_com_intensity <- data_com_intensity[!is.na(data_com_intensity$startLocatieForDisplayLat),]
data_com_speed <- data_com_speed[!is.na(data_com_speed$startLocatieForDisplayLat),]


# --------------------------------------------------
# Other tables -------------------------------------

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
