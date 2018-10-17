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

# Seperated for csv save
sep_symbol = ","

# Folder Locations
#f_main <- "/media/jacco/HDD/DataScienceData/Data/NDW/"
f_main <- "/home/jacco/Documents/DataScienceData/Data/NDW/"
f_intensity_meta <- "utwente intensiteiten groot amsterdam 1 dag met metadata (2) 20160916T104708 197/utwente intensiteiten groot amsterdam  1 dag met metadata (2)_intensiteit_00001.csv"
f_intensity <- "utwente intensiteiten groot amsterdam/utwente intensiteiten groot amsterdam _intensiteit_000"
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

# Load meta data speed
csv_speed_meta <- data.table::fread(file = paste(f_main, f_speed_meta, sep="", collapse=NULL),
                                    nThread = num_threads)

# --------------------------------------------------
# Load data ----------------------------------------

# Load data of one day intensity (nrows=8176606)
csv_intensity <- data.table::fread(file = paste(f_main, f_intensity, "01.csv", sep="", collapse=NULL),
#                                   nrows = 10000,
                                   nThread = num_threads)#,
#                                    nrows = dim(csv_intensity_meta)[1])

# Load data of one day speed (nrows=7898604)
csv_speed <- data.table::fread(file = paste(f_main, f_speed, "01.csv", sep="", collapse=NULL),
#                               nrows = 10000,
                               nThread = num_threads)#,
#                                nrows = dim(csv_speed_meta)[1])

# --------------------------------------------------
# Select required data -----------------------------

# Tableau table for Duncan-chan witch coordinates
coordinates_intensity <- csv_intensity_meta %>%
  select("startLocatieForDisplayLat",
         "startLocatieForDisplayLong",
         "ROADNUMBER") %>%
  arrange(
    startLocatieForDisplayLat) %>%
  group_by(
    startLocatieForDisplayLat) %>%
  distinct() %>%
  ungroup() %>%
  mutate(
    cor_id = row_number())

coordinates_speed <- csv_speed_meta %>%
  select("startLocatieForDisplayLat",
         "startLocatieForDisplayLong",
         "ROADNUMBER") %>%
  arrange(
    startLocatieForDisplayLat) %>%
  group_by(
    startLocatieForDisplayLat) %>%
  distinct() %>%
  ungroup() %>%
  mutate(
    cor_id = row_number())

data.table::fwrite(coordinates_intensity,
                   nThread = num_threads,
                   file = paste(f_output, "coordinates_intensity.csv", sep="", collapse=NULL),
                   sep = sep_symbol)

data.table::fwrite(coordinates_speed,
                   nThread = num_threads,
                   file = paste(f_output, "coordinates_speed.csv", sep="", collapse=NULL),
                   sep = sep_symbol)

# Select meta data
data_intensity_meta <- csv_intensity_meta %>%
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


