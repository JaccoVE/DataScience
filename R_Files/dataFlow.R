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
registerDoMC(8)

# Seperated for csv save
sep_symbol <- ","

# Folder Locations
f_metaFlow <- "/home/jacco/Documents/Git/DataScience/NDW/Tableau Input Files/metaFlow.csv"
f_dataFlow <- "/home/jacco/Documents/DataScienceData/Data/NDW/utwente intensiteiten groot amsterdam/utwente intensiteiten groot amsterdam _intensiteit_000"
f_output <- "/home/jacco/Documents/Git/DataScience/NDW/Tableau Input Files/dataFlow/"

# Load metaFlow
metaFlow <- data.table::fread(file = paste(f_metaFlow, sep="", collapse=NULL),
                                        nThread = 24)

# Collect garbage
gc()

# --------------------------------------------------
# Create flowID table ---------------------------

# Get all flowID's where specificVehicleCharacteristics equals anyVehicle
anyVehicleIDs <- metaFlow$flowID[metaFlow$specificVehicleCharacteristics == "anyVehicle"]

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
  
  # Select data
  dataFlow = dataFlow %>%
    select(
      "measurementSiteReference",
      "index",
      "periodStart",
      "periodEnd",
      "numberOfInputValuesused",
      "numberOfIncompleteInputs",
      "dataError",
      "avgVehicleFlow") %>%
    rename(
      meas_site_ref = "measurementSiteReference",
      ind = "index",
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
      -num_in_use,
      -num_in_in,
      -error)
  
  # Collect garbage
  gc()
  
  # Remove remaining rows with NA's
  dataFlow = na.omit(dataFlow) 
  
  # Collect garbage
  gc()
  
  # Combine data with meta data
  dataFlow = dataFlow %>%
    full_join(
      metaFlow, 
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
  
  # Remove all rows that are not anyVehicle
  dataFlow = dataFlow[ dataFlow$flowID %in% anyVehicleIDs, ]
  
  # Add date column and remove period_start and period_end columns
  dataFlow = dataFlow %>%
    mutate(
      date = format(strptime(dataFlow$per_start,format="%Y-%m-%d %H:%M"), "%Y-%m-%d-%H")) %>%
    select( 
      -per_start,
      -per_end)
  
  # Collect garbage
  gc()
  
  # Remove all flows smaller or equal to zero because we don't 
  # want to sum negative flows
  dataFlow = dataFlow[which(dataFlow$avg_flow>0),]
  
  # Collect garbage
  gc()
  
  # Average flow (cars/h) for each measurement point per hour and add to list
  dataFlow = dataFlow %>%
    group_by(flowID,
             date) %>%
    summarise(avg_flow = mean(avg_flow))%>%
    ungroup()
  
  # Collect garbage
  gc()
  
  returnValue(dataFlow)

}

# Combine the list of data.table's to one data.table
dataFlow <- rbindlist(dataFlow_list)

# Remove overlappings in lists, meaning:
# sum flows when date and flowID occurs double
dataFlow <- dataFlow %>%
  group_by(flowID,
           date) %>%
  summarise(avg_flow = mean(avg_flow))%>%
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

# Add reference column (average for each flowID, day of the week and hour of the day)
dataFlow = dataFlow %>%
  mutate(ref_flowID_dayWeek         = ave(dataFlow$avg_flow, dataFlow$flowID, dataFlow$week_day, FUN = mean),
         ref_flowID_dayWeek_hourDay = ave(dataFlow$avg_flow, dataFlow$flowID, dataFlow$week_day, dataFlow$hour, FUN = mean)) %>%
  select(-hour)

# Add difference column
dataFlow = dataFlow %>%
  mutate(dif_flowID_dayWeek         = dataFlow$avg_flow - dataFlow$ref_flowID_dayWeek,
         dif_flowID_dayWeek_hourDay = dataFlow$avg_flow - dataFlow$ref_flowID_dayWeek_hourDay)

# Determine unique days
uniqueDays = unique(dataFlow$day)

# Iterate over each file in parallel
foreach(i=1:length(uniqueDays)) %dopar% {
  
  dataFlow_Splitted = dataFlow[dataFlow$day == uniqueDays[i], ]
  
  dataFlow_Splitted = dataFlow_Splitted %>%
    select(-day)
  
  # Save to file
  data.table::fwrite(
    dataFlow_Splitted,
    nThread = 1,
    file = paste(f_output, "dataFlow_", uniqueDays[i], ".csv", sep="", collapse=NULL),
    sep = sep_symbol)
}
