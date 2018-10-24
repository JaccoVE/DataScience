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
#f_main <- "/media/jacco/HDD/DataScienceData/Data/NDW/"
f_main <- "/home/jacco/Documents/DataScienceData/Data/NDW/"
f_intensity_meta <- "utwente intensiteiten groot amsterdam 1 dag met metadata (2) 20160916T104708 197/utwente intensiteiten groot amsterdam  1 dag met metadata (2)_intensiteit_00001.csv"
f_intensity <- "utwente intensiteiten groot amsterdam/utwente intensiteiten groot amsterdam _intensiteit_000"
f_output <- "/home/jacco/Documents/Git/DataScience/NDW/Tableau Input Files/"

# --------------------------------------------------
# Create UniqueSiteReference tables intensity ------

# Collect garbage
gc()

# Load meta data intensity
csv_intensity_meta <- data.table::fread(file = paste(f_main, f_intensity_meta, sep="", collapse=NULL),
                                        nThread = 24)

# Unique Sites
data_intensity_unique <- csv_intensity_meta %>%
  select(
    "measurementSiteReference",
    "index",
    "specificVehicleCharacteristics",
    "startLocatieForDisplayLat",
    "startLocatieForDisplayLong",
    "ROADNUMBER") %>%
  arrange(
    measurementSiteReference) %>%
  group_by(
    measurementSiteReference) %>%
  distinct() %>%
  ungroup() %>%
  mutate(
    trafficID = row_number())

# Save to csv file
data.table::fwrite(data_intensity_unique,
                   nThread = 24,
                   file = paste(f_output, "data_intensity_unique.csv", sep="", collapse=NULL),
                   sep = sep_symbol)

# Remove tables that are no longer needed
remove(csv_intensity_meta)
gc()

# --------------------------------------------------
# Create trafficID table ---------------------------

# List of data.table's
data_intensity_list <- list()

# Iterate over each file in parallel
data_intensity_list <- foreach(i=1:27) %dopar% {
  
  # Collect garbage
  gc()
  
  # Convert loop integer to string format with pre-zeros
  s_file_num = formatC(i, width = 2, format = "d", flag = "0")

  # Load data
  data_intensity = data.table::fread(file = paste(f_main, f_intensity, s_file_num, ".csv", sep="", collapse=NULL),
                                     nThread = 1)
  
  # Select data
  data_intensity = data_intensity %>%
    select(
      "measurementSiteReference",
      "index",
      "periodStart",
      "periodEnd",
      "numberOfInputValuesused",
      "numberOfIncompleteInputs",
      "avgVehicleFlow") %>%
    rename(
      meas_site_ref = "measurementSiteReference",
      ind = "index",
      per_start = "periodStart",
      per_end = "periodEnd",
      num_in_use = "numberOfInputValuesused",
      num_in_in = "numberOfIncompleteInputs",
      avg_flow = "avgVehicleFlow") %>%
    arrange(
      per_start)
  
  # Collect garbage
  gc()
  
  # Combine data with meta data
  data_intensity = data_intensity %>%
    full_join(
      data_intensity_unique, 
      by = c(
        "meas_site_ref" = "measurementSiteReference",
        "ind" = "index")) %>%
    select( 
      -meas_site_ref, 
      -ind,
      -startLocatieForDisplayLat,
      -startLocatieForDisplayLong,
      -ROADNUMBER,
      -specificVehicleCharacteristics)
  
  # Collect garbage
  gc()
  
  # Add date column and remove period_start and period_end columns
  data_intensity = data_intensity %>%
    mutate(
      date = format(as.Date(data_intensity$per_start,format="%Y-%m-%d"), "%Y-%m-%d")) %>%
    select( 
      -per_start,
      -per_end)
  
  # Collect garbage
  gc()
  
  # Remove all rows that contain NA's for flow
  data_intensity = data_intensity[!is.na(data_intensity$avg_flow),]
  
  # Collect garbage
  gc()
  
  # Remove all rows that contain 0's for flow and do not have 0's for numberOfIncompleteInputs
  data_intensity = data_intensity[!(
    identical(data_intensity$avg_flow, as.numeric(0)) && 
    identical(data_intensity$num_in_in, as.numberic(0))),]
  
  # Collect garbage
  gc()
  
  # Remove unuseful columns
  data_intensity = data_intensity %>%
    select( 
      -num_in_use,
      -num_in_in)
  
  # Collect garbage
  gc()
  
  # Remove all flows smaller than zero because we don't 
  # want to sum negative flows
  data_intensity = data_intensity[which(data_intensity$avg_flow>=0),]
  
  # Collect garbage
  gc()
  
  # Sum flows for each measurement point per day and add to list
  data_intensity = data_intensity %>%
    group_by(trafficID,
             date) %>%
    summarise(avg_flow = sum(avg_flow))%>%
    ungroup()
  
  # Collect garbage
  gc()
  
  returnValue(data_intensity)

}


# Combine the list of data.table's to one data.table
data_intensity <- rbindlist(data_intensity_list)

# Remove overlappings in lists, meaning:
# sum flows when date and trafficID occurs double
data_intensity <- data_intensity %>%
  group_by(trafficID,
           date) %>%
  summarise(avg_flow = sum(avg_flow))%>%
  ungroup()

# Remove all rows that contain NA's for trafficID
data_intensity = data_intensity[!is.na(data_intensity$trafficID),]

# Collect garbage
gc()

# Save to file
data.table::fwrite(
  data_intensity,
  nThread = 24,
  file = paste(f_output, "data_intensity.csv", sep="", collapse=NULL),
  sep = sep_symbol)
