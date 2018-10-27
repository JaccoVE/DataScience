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
registerDoMC(6)

# Seperated for csv save
sep_symbol <- ","

# Folder Locations
#f_main <- "/media/jacco/HDD/DataScienceData/Data/NDW/"
f_main <- "/home/jacco/Documents/DataScienceData/Data/NDW/"
f_speed_meta <- "utwente snelheden groot amsterdam 1 dag met metadata 20160916T105028 197/utwente snelheden groot amsterdam  1 dag met metadata_snelheid_00001.csv"
f_speed <- "utwente snelheden groot amsterdam/utwente snelheden groot amsterdam _snelheid_000"
f_output <- "/home/jacco/Documents/Git/DataScience/NDW/Tableau Input Files/"

# --------------------------------------------------
# Create UniqueSiteReference tables speed ------

# Collect garbage
gc()

# Load meta data speed
speed_unique <- data.table::fread(file = paste(f_main, f_speed_meta, sep="", collapse=NULL),
                                        nThread = 24)

# Unique Sites
speed_unique <- speed_unique %>%
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
data.table::fwrite(speed_unique,
                   nThread = 24,
                   file = paste(f_output, "speed_unique.csv", sep="", collapse=NULL),
                   sep = sep_symbol)

# Collect garbage
gc()

# --------------------------------------------------
# Create trafficID table ---------------------------

# List of data.table's
speed_data_list <- list()

# Iterate over each file in parallel
speed_data_list <- foreach(i=1:25) %dopar% {
  
  # Collect garbage
  gc()
  
  # Convert loop integer to string format with pre-zeros
  s_file_num = formatC(i, width = 2, format = "d", flag = "0")

  # Load data
  speed_data = data.table::fread(file = paste(f_main, f_speed, s_file_num, ".csv", sep="", collapse=NULL),
                                     nThread = 1)
  
  # Select data
  speed_data = speed_data %>%
    select(
      "measurementSiteReference",
      "index",
      "periodStart",
      "periodEnd",
      "numberOfInputValuesused",
      "numberOfIncompleteInputs",
      "dataError",
      "avgVehicleSpeed") %>%
    rename(
      meas_site_ref = "measurementSiteReference",
      ind = "index",
      per_start = "periodStart",
      per_end = "periodEnd",
      num_in_use = "numberOfInputValuesused",
      num_in_in = "numberOfIncompleteInputs",
      error = "dataError",
      avg_speed = "avgVehicleSpeed") %>%
    arrange(
      per_start)
  
  # Collect garbage
  gc()
  
  # Remove all rows that contain an error
  speed_data = speed_data[!grepl("1", speed_data$error),]
  
  # Remove all rows that contain NA's for speed
  speed_data = speed_data[!is.na(speed_data$avg_speed),]
  
  # Collect garbage
  gc()
  
  # Remove all rows that contain -1's for speed and do not have 0's for numberOfIncompleteInputs and numberOfInputValuesUsed
  speed_data = speed_data[!(speed_data$avg_speed == -1 & speed_data$num_in_use != 0 & speed_data$num_in_in != 0),]
  
  # Collect garbage
  gc()
  
  # Remove unuseful columns
  speed_data = speed_data %>%
    select( 
      -num_in_use,
      -num_in_in,
      -error)
  
  # Collect garbage
  gc()
  
  # Remove remaining rows with NA's
  speed_data = na.omit(speed_data) 
  
  # Collect garbage
  gc()
  
  # Combine data with meta data
  speed_data = speed_data %>%
    full_join(
      speed_unique, 
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
  speed_data = speed_data %>%
    mutate(
      date = format(strptime(speed_data$per_start,format="%Y-%m-%d %H:%M"), "%Y-%m-%d")) %>%
    select( 
      -per_start,
      -per_end)
  
  # Collect garbage
  gc()
  
  # Remove all speeds smaller than zero because we don't 
  # want to sum negative speeds
  speed_data = speed_data[which(speed_data$avg_speed>=0),]
  
  # Collect garbage
  gc()
  
  # Average speed (km/h) for each measurement point per day and add to list
  speed_data = speed_data %>%
    group_by(trafficID,
             date) %>%
    summarise(avg_speed = mean(avg_speed))%>%
    ungroup()
  
  # Collect garbage
  gc()
  
  returnValue(speed_data)

}


# Combine the list of data.table's to one data.table
speed_data <- rbindlist(speed_data_list)

# Remove overlappings in lists, meaning:
# sum speeds when date and trafficID occurs double
speed_data <- speed_data %>%
  group_by(trafficID,
           date) %>%
  summarise(avg_speed = mean(avg_speed))%>%
  ungroup()

# Remove all rows that contain NA's for trafficID
speed_data = speed_data[!is.na(speed_data$trafficID),]

# Collect garbage
gc()

# Save to file
data.table::fwrite(
  speed_data,
  nThread = 24,
  file = paste(f_output, "speed_data_PerDay.csv", sep="", collapse=NULL),
  sep = sep_symbol)
