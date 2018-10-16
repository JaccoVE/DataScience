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

# Collect garbage
gc()

# Load meta data intensity
csv_intensity_meta <- data.table::fread(file = paste(f_main, f_intensity_meta, sep="", collapse=NULL),
                                        nTread = getDTthreads())

# Load meta data travel time
csv_time_meta <- data.table::fread(file = paste(f_main, f_time_meta, sep="", collapse=NULL),
                                   nTread = getDTthreads())

# remove duplicated columns
csv_time_meta <- csv_time_meta[,unique(names(csv_time_meta)),with=FALSE]

# Load meta data speed
csv_speed_meta <- data.table::fread(file = paste(f_main, f_speed_meta, sep="", collapse=NULL),
                                    nTread = getDTthreads())

# --------------------------------------------------
# Load data ----------------------------------------

# Load data of one day intensity (nrows=8176606)
csv_intensity <- data.table::fread(file = paste(f_main, f_intensity, "01.csv", sep="", collapse=NULL),
                                   nTread = getDTthreads())#,
#                                    nrows = dim(csv_intensity_meta)[1])

# Load data of one day travel time (nrows=1207547)
csv_time <- data.table::fread(file = paste(f_main, f_time, "01.csv", sep="", collapse=NULL),
                              nTread = getDTthreads())#,
#                               nrows = dim(csv_time_meta)[1])

# Load data of one day speed (nrows=7898604)
csv_speed <- data.table::fread(file = paste(f_main, f_speed, "01.csv", sep="", collapse=NULL),
                               nTread = getDTthreads())#,
#                                nrows = dim(csv_speed_meta)[1])

# --------------------------------------------------
# Select required data -----------------------------

# Select meta data
data_intensity_meta <- csv_intensity_meta %>%
  select(
    "measurementSiteReference",
    "index",
    "generatedSiteName",
    "periodStart",
    "periodEnd",
    "startLocatieForDisplayLat",
    "startLocatieForDisplayLong",
    "specificLocation",
    "alertCDirectionCoded",
    "avgVehicleFlow",
    "avgVehicleSpeed",
    "avgTravelTime") %>%
  arrange(measurementSiteReference) %>%
  group_by(
    measurementSiteReference) %>%
  distinct() %>%
  ungroup()

data_time_meta <- csv_time_meta %>%
  select(
    "measurementSiteReference",
    "index",
    "generatedSiteName",
    "periodStart",
    "periodEnd",
    "startLocatieForDisplayLat",
    "startLocatieForDisplayLong",
    "specificLocation",
    "alertCDirectionCoded",
    "avgVehicleFlow",
    "avgVehicleSpeed",
    "avgTravelTime") %>%
  arrange(measurementSiteReference) %>%
  group_by(
    measurementSiteReference) %>%
  distinct() %>%
  ungroup()

data_speed_meta <- csv_speed_meta %>%
  select(
    "measurementSiteReference",
    "index",
    "generatedSiteName",
    "periodStart",
    "periodEnd",
    "startLocatieForDisplayLat",
    "startLocatieForDisplayLong",
    "specificLocation",
    "alertCDirectionCoded",
    "avgVehicleFlow",
    "avgVehicleSpeed",
    "avgTravelTime") %>%
  arrange(measurementSiteReference) %>%
  group_by(
    measurementSiteReference) %>%
  distinct() %>%
  ungroup()

# Select data
data_intensity <- csv_intensity %>%
  select(
    "measurementSiteReference",
    "index",
    "generatedSiteName",
    "periodStart",
    "periodEnd",
    "avgVehicleFlow",
    "avgVehicleSpeed",
    "avgTravelTime") %>%
  rename(
    meas_site_ref = "measurementSiteReference",
    ind = "index",
    gen_site_name = "generatedSiteName",
    per_start = "periodStart",
    per_end = "periodEnd",
    avg_flow = "avgVehicleFlow",
    avg_speed = "avgVehicleSpeed",
    avg_time = "avgTravelTime") %>%
  arrange(meas_site_ref) %>%
  group_by(
    meas_site_ref) %>%
  distinct() %>%
  ungroup()

data_time <- csv_time %>%
  select(
    "measurementSiteReference",
    "index",
    "generatedSiteName",
    "periodStart",
    "periodEnd",
    "avgVehicleFlow",
    "avgVehicleSpeed",
    "avgTravelTime") %>%
  rename(
    meas_site_ref = "measurementSiteReference",
    ind = "index",
    gen_site_name = "generatedSiteName",
    per_start = "periodStart",
    per_end = "periodEnd",
    avg_flow = "avgVehicleFlow",
    avg_speed = "avgVehicleSpeed",
    avg_time = "avgTravelTime") %>%
  arrange(meas_site_ref) %>%
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
    "avgVehicleFlow",
    "avgVehicleSpeed",
    "avgTravelTime") %>%
  rename(
    meas_site_ref = "measurementSiteReference",
    ind = "index",
    gen_site_name = "generatedSiteName",
    per_start = "periodStart",
    per_end = "periodEnd",
    avg_flow = "avgVehicleFlow",
    avg_speed = "avgVehicleSpeed",
    avg_time = "avgTravelTime") %>%
  arrange(meas_site_ref) %>%
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
      "generatedSiteName" = "gen_site_name",
      "periodStart" = "per_start",
      "periodEnd" = "per_end")) %>%
  select( 
    -avg_flow, 
    -avg_speed, 
    -avg_time)

data_com_time <- data_time_meta %>%
  full_join(
    data_time, 
    by = c(
      "measurementSiteReference" = "meas_site_ref",
      "index" = "ind",
      "generatedSiteName" = "gen_site_name",
      "periodStart" = "per_start",
      "periodEnd" = "per_end")) %>%
  select( 
    -avg_flow, 
    -avg_speed, 
    -avg_time)

data_com_speed <- data_speed_meta %>%
  full_join(
    data_speed, 
    by = c(
      "measurementSiteReference" = "meas_site_ref",
      "index" = "ind",
      "generatedSiteName" = "gen_site_name",
      "periodStart" = "per_start",
      "periodEnd" = "per_end")) %>%
  select( 
    -avg_flow, 
    -avg_speed, 
    -avg_time)

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
