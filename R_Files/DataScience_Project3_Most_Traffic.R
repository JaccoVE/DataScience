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
f_output <- "/home/jacco/Documents/Git/DataScience/NDW/Tableau Input Files/"

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
                                   nrows = dim(csv_intensity_meta)[1],
                                   nThread = num_threads)

# --------------------------------------------------
# Create UniqueSiteReference tables intensity ------

# Unique Sites
data_intensity_unique <- csv_intensity_meta %>%
  select(
    "measurementSiteReference",
    "generatedSiteName",
    "index",
    "carriageway",
    "specificLane",
    "specificVehicleCharacteristics",
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
# Create flowID tables -----------------------------

# Select data
data_intensity <- csv_intensity %>%
  select(
    "measurementSiteReference",
    "generatedSiteName",
    "index",
    "periodStart",
    "periodEnd",
    "numberOfInputValuesused",
    "numberOfIncompleteInputs",
    "avgVehicleFlow") %>%
  rename(
    meas_site_ref = "measurementSiteReference",
    gen_site_name = "generatedSiteName",
    ind = "index",
    per_start = "periodStart",
    per_end = "periodEnd",
    num_in_use = "numberOfInputValuesused",
    num_in_in = "numberOfIncompleteInputs",
    avg_flow = "avgVehicleFlow") %>%
  arrange(
    per_start)

# --------------------------------------------------
# Combine data with meta data ----------------------

data_com_intensity <- data_intensity %>%
  full_join(
    data_intensity_unique, 
    by = c(
      "meas_site_ref" = "measurementSiteReference",
      "gen_site_name" = "generatedSiteName",
      "ind" = "index")) %>%
  select( 
    -meas_site_ref, 
    -gen_site_name,
    -ind,
    -startLocatieForDisplayLat,
    -startLocatieForDisplayLong,
    -specificLocation,
    -alertCDirectionCoded,
    -ROADNUMBER,
    -carriageway,
    -specificLane,
    -LocationTableNumber,
    -LocationTableVersion,
    -offsetDistance,
    -LOC_TYPE,
    -LOC_DES)

# --------------------------------------------------
# Remove unuseful rows and columns -----------------

# Collect garbage
gc()

# Remove all rows that contain NA's for flow
data_com_intensity <- data_com_intensity[!is.na(data_com_intensity$avg_flow),]

# Remove all rows that contain 0's for flow and do not have 0's for numberOfIncompleteInputs
data_com_intensity <- data_com_intensity[!(
  identical(data_com_intensity$avg_flow, as.numeric(0)) && 
  identical(data_com_intensity$num_in_in, as.numberic(0))),]

# Remove unuseful columns
data_com_intensity <- data_com_intensity %>%
  select( 
    -num_in_use,
    -num_in_in)

# Remove all flows smaller than zero because we don't 
# want to sum negative flows
data_com_intensity <- data_com_intensity[which(data_com_intensity$avg_flow>=0),]

# Collect garbage
gc()

# --------------------------------------------------
# Sum flows for each measurement point per day -----
#test <- aggregate(data_com_intensity$avg_flow, by=list(avg_flow=data_com_intensity$avg_flow), FUN=sum)
data_com_intensity <- data_com_intensity %>%
  group_by(trafficID) %>%
  summarise(avg_flow = sum(avg_flow))

#arrange(
#    trafficID) %>%
#  group_by(
#    trafficID) %>%
#  distinct() %>%
#  ungroup()

# --------------------------------------------------
# Save to file -------------------------------------

# Save to file
data.table::fwrite(
  data_com_intensity,
  nThread = num_threads,
  file = paste(f_output, "data_com_intensity.csv", sep="", collapse=NULL),
  sep = sep_symbol)

