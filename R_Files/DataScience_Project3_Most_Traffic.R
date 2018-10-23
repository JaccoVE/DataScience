# Load libraries
library(DBI)
library(RPostgreSQL)
library(readr)
library(dplyr)
library(lubridate)
library(xml2)
library(data.table)
library(gtools)
source("/home/jacco/Documents/Git/DataScience/R_Files/sort_points.R")
#library(parallel)

# --------------------------------------------------
# Settings -----------------------------------------

# Number of threads to use when reading files from disk
num_threads <- 24

# Number of rows to read from data csv file ("10000", "dim(csv_speed_meta)[1]" or "Inf")
num_rows <- Inf

# Seperated for csv save
sep_symbol <- ","

# Folder Locations
#f_main <- "/media/jacco/HDD/DataScienceData/Data/NDW/"
f_main <- "/home/jacco/Documents/DataScienceData/Data/NDW/"
f_intensity_meta <- "utwente intensiteiten groot amsterdam 1 dag met metadata (2) 20160916T104708 197/utwente intensiteiten groot amsterdam  1 dag met metadata (2)_intensiteit_00001.csv"
f_intensity <- "utwente intensiteiten groot amsterdam/utwente intensiteiten groot amsterdam _intensiteit_000"
f_time_meta <- "utwente reistijden groot amsterdam 1 dag met metadata 20160916T103803 197/utwente reistijden groot amsterdam  1 dag met metadata_reistijd_00001.csv"
f_time <- "utwente reistijden groot amsterdam 20160916T115957 197/utwente reistijden groot amsterdam  _reistijd_000"
f_speed_meta <- "utwente snelheden groot amsterdam 1 dag met metadata 20160916T105028 197/utwente snelheden groot amsterdam  1 dag met metadata_snelheid_00001.csv"
f_speed <- "utwente snelheden groot amsterdam/utwente snelheden groot amsterdam _snelheid_000"
f_output <- "/home/jacco/Documents/Git/DataScience/Tableau Input Files/"

# --------------------------------------------------
# Load meta data -----------------------------------

# Collect garbage
gc()

# Load meta data intensity
csv_intensity_meta <- data.table::fread(file = paste(f_main, f_intensity_meta, sep="", collapse=NULL),
                                        nThread = num_threads)

# --------------------------------------------------
# Load data ----------------------------------------

# Load data of one day intensity (nrows=8176606)
csv_intensity <- data.table::fread(file = paste(f_main, f_intensity, "01.csv", sep="", collapse=NULL),
                                   nrows = num_rows,
                                   nThread = num_threads)

# --------------------------------------------------
# Create UniqueSiteReference tables intensity ------

# Unique Sites (positive and negative still combined)
data_intensity_unique <- csv_intensity_meta %>%
  select(
    "measurementSiteReference",
    "generatedSiteName",
    "carriageway",
    "startLocatieForDisplayLat",
    "startLocatieForDisplayLong",
    "alertCDirectionCoded",
    "ROADNUMBER",
    "LocationTableNumber",
    "LocationTableVersion",
    "specificLocation",
    "offsetDistance",
    "LOC_TYPE",
    "LOC_DES") %>%
  arrange(
    measurementSiteReference) %>%
  group_by(
    measurementSiteReference) %>%
  distinct() %>%
  ungroup() %>%
  mutate(
    trafficID = row_number())

# Remove row if coordinate already exists
#data_intensity_unique <- data_intensity_unique[!duplicated(data_intensity_unique[
#  c("startLocatieForDisplayLat","startLocatieForDisplayLong")]),]

# Save to csv file
data.table::fwrite(data_intensity_unique,
                   nThread = num_threads,
                   file = paste(f_output, "data_intensity_unique.csv", sep="", collapse=NULL),
                   sep = sep_symbol)

# Remove tables that are no longer needed
#remove(data_intensity_unique)
gc()

# --------------------------------------------------
# Create UniqueSiteReference tables time -----------

# Unique Sites (positive and negative still combined)
data_time_unique <- csv_time_meta %>%
  select(
    "measurementSiteReference",
    "generatedSiteName",
    "carriageway",
    "startLocatieForDisplayLat",
    "startLocatieForDisplayLong",
    "eindLocatieForDisplayLat",
    "eindLocatieForDisplayLong",
    "alertCDirectionCoded",
    "ROADNUMBER",
    "LocationTableNumber",
    "LocationTableVersion",
    "specificLocation",
    "offsetDistance",
    "LOC_TYPE",
    "LOC_DES",
    "measurementSiteNumberOfLanes",
    "offsetDistance",
    "primaryLocation_specificLocation",
    "primaryLocation_offsetDistance",
    "secondaryLocation_specificLocation",
    "secondaryLocation_offsetDistance",
    "ROADNUMBER_PRIM",
    "lengthAffected") %>%
  arrange(
    measurementSiteReference) %>%
  group_by(
    measurementSiteReference) %>%
  distinct() %>%
  ungroup()

# Save to csv file
data.table::fwrite(data_time_unique,
                   nThread = num_threads,
                   file = paste(f_output, "data_time_unique.csv", sep="", collapse=NULL),
                   sep = sep_symbol)

# Select only A1, A2, A4, A5, A8, A9 and A10
data_time_unique <- data_time_unique[data_time_unique$ROADNUMBER %in% 
                                                 c("A1", "A2", "A4", "A5", "A8", "A9", "A10"), ]

# Select only mainCarriageway
data_time_unique <- data_time_unique[data_time_unique$carriageway %in% 
                                                 c("mainCarriageway"), ]

# Remove mainCarriageway from table
data_time_unique <- data_time_unique %>%
  select(
    -carriageway)

# Create all positive and negative UniqueSites
data_time_uniqueP <- data_time_unique[grep("positive", data_time_unique$alertCDirectionCoded), ]
data_time_uniqueN <- data_time_unique[grep("negative", data_time_unique$alertCDirectionCoded), ]

# Remove row if coordinate already exists
data_time_uniqueP <- data_time_uniqueP[!duplicated(data_time_uniqueP[
  c("startLocatieForDisplayLat","startLocatieForDisplayLong")]),]

data_time_uniqueN <- data_time_uniqueN[!duplicated(data_time_uniqueN[
  c("startLocatieForDisplayLat","startLocatieForDisplayLong")]),]

# Add trafficID's to positive and negative UniqueSites
data_time_uniqueP <- data_time_uniqueP %>%
  mutate(
    trafficID = row_number())

data_time_uniqueN <- data_time_uniqueN %>%
  mutate(
    trafficID = row_number())

# Save to csv file
data.table::fwrite(data_time_uniqueP,
                   nThread = num_threads,
                   file = paste(f_output, "data_time_uniqueP.csv", sep="", collapse=NULL),
                   sep = sep_symbol)

data.table::fwrite(data_time_uniqueN,
                   nThread = num_threads,
                   file = paste(f_output, "data_time_uniqueN.csv", sep="", collapse=NULL),
                   sep = sep_symbol)

# Remove tables that are no longer needed
remove(data_time_unique)
gc()

# --------------------------------------------------
# Create traffic tables --------------------------

# Select meta data
data_traffic <- csv_intensity %>%
  select(
    "measurementSiteReference",
    "generatedSiteName",
    "startLocatieForDisplayLat",
    "startLocatieForDisplayLong",
    "specificLocation",
    "alertCDirectionCoded",
    "ROADNUMBER") %>%
  arrange(
    measurementSiteReference) %>%
  group_by(
    measurementSiteReference) %>%
  distinct() %>%
  ungroup() %>%
  mutate(
    intensity_id = row_number())

data_speed_meta <- csv_speed_meta %>%
  select(
    "measurementSiteReference",
    "generatedSiteName",
    "startLocatieForDisplayLat",
    "startLocatieForDisplayLong",
    "specificLocation",
    "alertCDirectionCoded",
    "ROADNUMBER") %>%
  arrange(
    measurementSiteReference) %>%
  group_by(
    measurementSiteReference) %>%
  distinct() %>%
  ungroup() %>%
  mutate(
    speed_id = row_number())

# Save to file
data.table::fwrite(data_intensity_meta,
                   nThread = num_threads,
                   file = paste(f_output, "data_intensity_meta.csv", sep="", collapse=NULL),
                   sep = sep_symbol)

data.table::fwrite(data_speed_meta,
                   nThread = num_threads,
                   file = paste(f_output, "data_speed_meta.csv", sep="", collapse=NULL),
                   sep = sep_symbol)

# --------------------------------------------------
# Create flowID tables -----------------------------

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
    "avgVehicleFlow") %>%
  rename(
    meas_site_ref = "measurementSiteReference",
    ind = "index",
    gen_site_name = "generatedSiteName",
    per_start = "periodStart",
    per_end = "periodEnd",
    num_in_use = "numberOfInputValuesused",
    num_in_in = "numberOfIncompleteInputs",
    avg_flow = "avgVehicleFlow") %>%
  arrange(
    per_start)

data_speed <- csv_speed %>%
  select(
    "measurementSiteReference",
    "index",
    "generatedSiteName",
    "periodStart",
    "periodEnd",
    "numberOfInputValuesused",
    "numberOfIncompleteInputs",
    "avgVehicleSpeed") %>%
  rename(
    meas_site_ref = "measurementSiteReference",
    ind = "index",
    gen_site_name = "generatedSiteName",
    per_start = "periodStart",
    per_end = "periodEnd",
    num_in_use = "numberOfInputValuesused",
    num_in_in = "numberOfIncompleteInputs",
    avg_speed = "avgVehicleSpeed") %>%
  arrange(
    per_start)

# --------------------------------------------------
# Combine data with meta data ----------------------

data_com_intensity <- data_intensity %>%
  full_join(
    data_intensity_meta, 
    by = c(
      "meas_site_ref" = "measurementSiteReference",
      "ind" = "index",
      "gen_site_name" = "generatedSiteName")) %>%
  select( 
    -meas_site_ref, 
    -ind,
    -gen_site_name,
    -startLocatieForDisplayLat,
    -startLocatieForDisplayLong,
    -specificLocation,
    -alertCDirectionCoded,
    -ROADNUMBER)

data_com_speed <- data_speed %>%
  full_join(
    data_speed_meta, 
    by = c(
      "meas_site_ref" = "measurementSiteReference",
      "ind" = "index",
      "gen_site_name" = "generatedSiteName")) %>%
  select( 
    -meas_site_ref, 
    -ind,
    -gen_site_name,
    -startLocatieForDisplayLat,
    -startLocatieForDisplayLong,
    -specificLocation,
    -alertCDirectionCoded,
    -ROADNUMBER)

# --------------------------------------------------
# Remove unuseful rows -----------------------------

# Collect garbage
gc()

# Remove all rows that contain NA's for flow or speed
data_com_intensity <- data_com_intensity[!is.na(data_com_intensity$avg_flow),]
data_com_speed <- data_com_speed[!is.na(data_com_speed$avg_speed),]

# Remove all rows that contain 0's for flow and do not have 0's for numberOfIncompleteInputs
data_com_intensity <- data_com_intensity[!(
  identical(data_com_intensity$avg_flow, as.numeric(0)) && 
  identical(data_com_intensity$num_in_in, as.numberic(0))),]

# Remove all rows that contain -1's for speed and do not have 0's for numberOfInputValuesUsed and numberOfIncompleteInputs
data_com_speed <- data_com_speed[!(
  identical(data_com_intensity$avg_speed, as.numeric(-1)) && 
  identical(data_com_intensity$num_in_use, as.numberic(0)) &&
  identical(data_com_intensity$num_in_in, as.numberic(0))),]

# Collect garbage
gc()

# --------------------------------------------------
# Remove unuseful columns --------------------------

data_com_intensity <- data_com_intensity %>%
  select( 
    -num_in_use,
    -num_in_in)

data_com_speed <- data_com_speed %>%
  select( 
    -num_in_use,
    -num_in_in)

# --------------------------------------------------
# Replace -1 and 0 by "no traffic" -----------------


# --------------------------------------------------
# Save to file -------------------------------------

# Save to file
data.table::fwrite(
  data_com_intensity,
  nThread = num_threads,
  file = paste(f_output, "data_com_intensity.csv", sep="", collapse=NULL),
  sep = sep_symbol)

data.table::fwrite(
  data_com_speed, 
  nThread = num_threads,
  file = paste(f_output, "data_com_speed.csv", sep="", collapse=NULL),
  sep = sep_symbol)

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


