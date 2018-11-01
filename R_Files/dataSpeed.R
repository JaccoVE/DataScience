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
f_metaSpeed <- "/home/jacco/Documents/Git/DataScience/Database/metaSpeed.csv"
f_dataSpeed <- "/home/jacco/Documents/DataScienceData/Data/NDW/utwente snelheden groot amsterdam/utwente snelheden groot amsterdam _snelheid_000"
f_output <- "/home/jacco/Documents/Git/DataScience/Database/dataSpeed/"

# Load metaSpeed
metaSpeed <- data.table::fread(file = paste(f_metaSpeed, sep="", collapse=NULL),
                                        nThread = 24)

# Collect garbage
gc()

# --------------------------------------------------
# Create dataSpeed table ---------------------------

# Get all speedID's where specificVehicleCharacteristics equals anyVehicle
anyVehicleIDs <- metaSpeed$speedID[metaSpeed$specificVehicleCharacteristics == "anyVehicle"]

# List of data.table's
dataSpeed_list <- list()

# Iterate over each file in parallel
dataSpeed_list <- foreach(i=1:25) %dopar% {
  
  # Collect garbage
  gc()
  
  # Convert loop integer to string format with pre-zeros
  s_file_num = formatC(i, width = 2, format = "d", flag = "0")

  # Load data
  dataSpeed = data.table::fread(file = paste(f_dataSpeed, s_file_num, ".csv", sep="", collapse=NULL),
                                     nThread = 1)
  
  # Select data
  dataSpeed = dataSpeed %>%
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
  dataSpeed = dataSpeed[!grepl("1", dataSpeed$error),]
  
  # Remove all rows that contain NA's for speed
  dataSpeed = dataSpeed[!is.na(dataSpeed$avg_speed),]
  
  # Collect garbage
  gc()
  
  # Remove all rows that contain -1's for speed and do not have 0's for numberOfIncompleteInputs and numberOfInputValuesUsed
  dataSpeed = dataSpeed[!(dataSpeed$avg_speed == -1 & dataSpeed$num_in_use != 0 & dataSpeed$num_in_in != 0),]
  
  # Collect garbage
  gc()
  
  # Remove unuseful columns
  dataSpeed = dataSpeed %>%
    select( 
      -num_in_use,
      -num_in_in,
      -error)
  
  # Collect garbage
  gc()
  
  # Remove remaining rows with NA's
  dataSpeed = na.omit(dataSpeed) 
  
  # Collect garbage
  gc()
  
  # Combine data with meta data
  dataSpeed = dataSpeed %>%
    full_join(
      metaSpeed, 
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
  dataSpeed = dataSpeed[ dataSpeed$speedID %in% anyVehicleIDs, ]
  
  # Add date column and remove period_start and period_end columns
  dataSpeed = dataSpeed %>%
    mutate(
      date = format(strptime(dataSpeed$per_start,format="%Y-%m-%d %H:%M"), "%Y-%m-%d-%H")) %>%
    select( 
      -per_start,
      -per_end)
  
  # Collect garbage
  gc()
  
  # Remove all speeds smaller or equal to zero because we don't 
  # want to sum negative speeds
  dataSpeed = dataSpeed[which(dataSpeed$avg_speed>0),]
  
  # Collect garbage
  gc()
  
  # Average speed (km/h) for each measurement point per hour and add to list
  dataSpeed = dataSpeed %>%
    group_by(speedID,
             date) %>%
    summarise(avg_speed = mean(avg_speed))%>%
    ungroup()
  
  # Collect garbage
  gc()
  
  returnValue(dataSpeed)

}

# Combine the list of data.table's to one data.table
dataSpeed <- rbindlist(dataSpeed_list)

# Remove overlappings in lists, meaning:
# sum speeds when date and speedID occurs double
dataSpeed <- dataSpeed %>%
  group_by(speedID,
           date) %>%
  summarise(avg_speed = mean(avg_speed))%>%
  ungroup()

# Remove all rows that contain NA's for speedID
dataSpeed = dataSpeed[!is.na(dataSpeed$speedID),]

# Collect garbage
gc()

# Add day of the week
dataSpeed = dataSpeed %>%
  mutate(
    week_day = weekdays(as.Date(dataSpeed$date,'%Y-%m-%d')))

# Add seperate hour and date column
dataSpeed = dataSpeed %>%
  mutate(
    hour = format(strptime(dataSpeed$date,format="%Y-%m-%d-%H"), "%H"),
    day = format(strptime(dataSpeed$date,format="%Y-%m-%d-%H"), "%Y-%m-%d"))

# Add reference column (average for each speedID, day of the week and hour of the day)
dataSpeed = dataSpeed %>%
  mutate(ref_speedID_dayWeek         = ave(dataSpeed$avg_speed, dataSpeed$speedID, dataSpeed$week_day, FUN = mean),
         ref_speedID_dayWeek_hourDay = ave(dataSpeed$avg_speed, dataSpeed$speedID, dataSpeed$week_day, dataSpeed$hour, FUN = mean)) %>%
  select(-hour)

# Add difference column
dataSpeed = dataSpeed %>%
  mutate(dif_speedID_dayWeek         = dataSpeed$avg_speed - dataSpeed$ref_speedID_dayWeek,
         dif_speedID_dayWeek_hourDay = dataSpeed$avg_speed - dataSpeed$ref_speedID_dayWeek_hourDay)

# Determine unique days
uniqueDays = unique(dataSpeed$day)

# Iterate over each file in parallel
foreach(i=1:length(uniqueDays)) %dopar% {
  
  dataSpeed_Splitted = dataSpeed[dataSpeed$day == uniqueDays[i], ]
  
  dataSpeed_Splitted = dataSpeed_Splitted %>%
    select(-day)
  
  # Save to file
  data.table::fwrite(
    dataSpeed_Splitted,
    nThread = 1,
    file = paste(f_output, "dataSpeed_", uniqueDays[i], ".csv", sep="", collapse=NULL),
    sep = sep_symbol)
}
