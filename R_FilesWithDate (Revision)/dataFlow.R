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
f_main <- "/home/jacco/Documents/DataScienceData/Data/NDW/"
f_metaFlow <- "/home/jacco/Documents/Git/DataScience/DatabaseWithDate (Revision)/metaFlow.csv"
f_flow_meta <- "utwente intensiteiten groot amsterdam 1 dag met metadata (2) 20160916T104708 197/utwente intensiteiten groot amsterdam  1 dag met metadata (2)_intensiteit_00001.csv"
f_dataFlow <- "/home/jacco/Documents/DataScienceData/Data/NDW/utwente intensiteiten groot amsterdam/utwente intensiteiten groot amsterdam _intensiteit_000"
f_output <- "/home/jacco/Documents/Git/DataScience/DatabaseWithDate (Revision)/"

# Load metaFlow
metaFlow <- data.table::fread(file = paste(f_metaFlow, sep="", collapse=NULL),
                                        nThread = 24)

# Load meta data flow
metaFlowFull <- data.table::fread(file = paste(f_main, f_flow_meta, sep="", collapse=NULL),
                               nThread = 24)

# Get all indexes where specificVehicleCharacteristics equals anyVehicle
anyVehicleIndexes <- metaFlowFull$index[metaFlowFull$specificVehicleCharacteristics == "anyVehicle"] %>%
  unique()

# Remove unused tables
remove(metaFlowFull)

# Collect garbage
gc()

# --------------------------------------------------
# Create dataFlow table ---------------------------

# List of data.table's
dataFlow_list <- list()

# Iterate over each file in parallel
dataFlow_list <- foreach(i=1:27) %dopar% {
  
  # Collect garbage
  gc()
  
  # Convert loop integer to string format with pre-zeros
  s_file_num = formatC(i, width = 2, format = "d", flag = "0")

  # Load data
  dataFlow = data.table::fread(file = paste(f_dataFlow, s_file_num, ".csv", sep="", collapse=NULL),
                                     nThread = 1)
  
  # Remove all rows that are not anyVehicle
  dataFlow = dataFlow[ dataFlow$index %in% anyVehicleIndexes, ]
  
  # Collect garbage
  gc()
  
  # Select data
  dataFlow = dataFlow %>%
    select(
      "measurementSiteReference",
      "periodStart",
      "periodEnd",
      "numberOfInputValuesused",
      "numberOfIncompleteInputs",
      "dataError",
      "avgVehicleFlow") %>%
    rename(
      meas_site_ref = "measurementSiteReference",
      per_start = "periodStart",
      per_end = "periodEnd",
      num_in_use = "numberOfInputValuesused",
      num_in_in = "numberOfIncompleteInputs",
      error = "dataError",
      avg_flow = "avgVehicleFlow") %>%
    arrange(
      per_start)
  
  # Collect garbage
  gc()
  
  # Remove all rows that contain an error
  dataFlow = dataFlow[!grepl("1", dataFlow$error),]
  
  # Remove all rows that contain NA's for flow
  dataFlow = dataFlow[!is.na(dataFlow$avg_flow),]
  
  # Collect garbage
  gc()
  
  # Remove all rows that contain 0's for flow and do not have 0's for numberOfIncompleteInputs
  dataFlow = dataFlow[!(dataFlow$avg_flow == 0 & dataFlow$num_in_in != 0),]
  
  # Collect garbage
  gc()
  
  # Remove unuseful columns
  dataFlow = dataFlow %>%
    select( 
      -num_in_in,
      -error)
  
  # Collect garbage
  gc()
  
  # Combine data with meta data
  dataFlow = dataFlow %>%
    full_join(
      metaFlow, 
      by = c(
        "meas_site_ref" = "measurementSiteReference")) %>%
    select( 
      -meas_site_ref, 
      -startLocatieForDisplayLat,
      -startLocatieForDisplayLong,
      -ROADNUMBER)
  
  # Collect garbage
  gc()
  
  # Add date column and remove period_start and period_end columns
  dataFlow = dataFlow %>%
    mutate(
      date = format(strptime(dataFlow$per_start,format="%Y-%m-%d %H:%M"), "%Y-%m-%d-%H")) %>%
    select( 
      -per_start,
      -per_end) %>%
    arrange(flowID,
            date)
  
  # Collect garbage
  gc()
  
  # Calculate mean of the flow and the standard deviation in km/h for each 
  # measurement point per hour and add to list
  dataFlow = dataFlow %>%
    group_by(flowID,
             date) %>%
    summarise(std = sd(avg_flow), avg_flow = mean(avg_flow)) %>%
    ungroup()
  
  # Collect garbage
  gc()
  
  returnValue(dataFlow)

}

# Combine the list of data.table's to one data.table
dataFlow <- rbindlist(dataFlow_list)

# Remove overlappings in lists, meaning:
# Calculate mean of the flow and the mean of the standard deviation in km/h for each 
# measurement point per hour
dataFlow = dataFlow %>%
  group_by(flowID,
           date) %>%
  summarise(std = mean(std), avg_flow = mean(avg_flow)) %>%
  ungroup()

# Remove all rows that contain NA's for flowID
dataFlow = dataFlow[!is.na(dataFlow$flowID),]

# Collect garbage
gc()

# Add day of the week
dataFlow = dataFlow %>%
  mutate(
    week_day = weekdays(as.Date(dataFlow$date,'%Y-%m-%d')))

# Add seperate hour and day column
dataFlow = dataFlow %>%
  mutate(
    hour = format(strptime(dataFlow$date,format="%Y-%m-%d-%H"), "%H"),
    day = format(strptime(dataFlow$date,format="%Y-%m-%d-%H"), "%Y-%m-%d"))

# Add reference column (average for each flowID, day of the week and hour of the day - the standard deviation)
dataFlow = dataFlow %>%
  mutate(ref_flowID_dayWeek         = ave(dataFlow$avg_flow, dataFlow$flowID, dataFlow$week_day, FUN = mean) - std,
         ref_flowID_dayWeek_hourDay = ave(dataFlow$avg_flow, dataFlow$flowID, dataFlow$week_day, dataFlow$hour, FUN = mean) - std) %>%
  select(-hour)

# Add difference column
dataFlow = dataFlow %>%
  mutate(dif_flowID_dayWeek         = dataFlow$avg_flow - dataFlow$ref_flowID_dayWeek,
         dif_flowID_dayWeek_hourDay = dataFlow$avg_flow - dataFlow$ref_flowID_dayWeek_hourDay)

# Save to file
data.table::fwrite(
  dataFlow,
  nThread = 1,
  file = paste(f_output, "dataFlow", ".csv", sep="", collapse=NULL),
  sep = sep_symbol)
