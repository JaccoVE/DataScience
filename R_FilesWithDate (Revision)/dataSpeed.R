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
f_metaSpeed <- "/home/jacco/Documents/Git/DataScience/DatabaseWithDate (Revision)/metaSpeed.csv"
f_speed_meta <- "utwente snelheden groot amsterdam 1 dag met metadata 20160916T105028 197/utwente snelheden groot amsterdam  1 dag met metadata_snelheid_00001.csv"
f_dataSpeed <- "/home/jacco/Documents/DataScienceData/Data/NDW/utwente snelheden groot amsterdam/utwente snelheden groot amsterdam _snelheid_000"
f_output <- "/home/jacco/Documents/Git/DataScience/DatabaseWithDate (Revision)/"

# Load metaSpeed
metaSpeed <- data.table::fread(file = paste(f_metaSpeed, sep="", collapse=NULL),
                                        nThread = 24)

# Load meta data speed
metaSpeedFull <- data.table::fread(file = paste(f_main, f_speed_meta, sep="", collapse=NULL),
                               nThread = 24)

# Get all indexes where specificVehicleCharacteristics equals anyVehicle
anyVehicleIndexes <- metaSpeedFull$index[metaSpeedFull$specificVehicleCharacteristics == "anyVehicle"] %>%
  unique()

# Remove unused tables
remove(metaSpeedFull)

# Collect garbage
gc()

# --------------------------------------------------
# Functions ----------------------------------------
func_mean <- function(x, w ){
  if(anyNA(w)) # Return Harmonic Mean if we have no weight value
  {
    return(length(x)/sum(1/x))
  }
  else # Return Weighed Harmonic Mean
  {
    return(sum(w)/(sum(w/x)))
  }
}

func_sd <- function(x, w ){
  if(length(x) - 1 == 0) # Return zero for the Standard Deviation if only one value is used
  {
    return(0)
  }
  else # Return Standard Deviation
  {
    return(sqrt(sum((x - func_mean(x,w))^2) / (length(x) - 1))) 
  }
}

# --------------------------------------------------
# Create dataSpeed table ---------------------------

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
  
  # Remove all rows that are not anyVehicle
  dataSpeed = dataSpeed[ dataSpeed$index %in% anyVehicleIndexes, ]
  
  # Collect garbage
  gc()
  
  # Select data
  dataSpeed = dataSpeed %>%
    select(
      "measurementSiteReference",
      "periodStart",
      "periodEnd",
      "numberOfInputValuesused",
      "numberOfIncompleteInputs",
      "dataError",
      "avgVehicleSpeed") %>%
    rename(
      meas_site_ref = "measurementSiteReference",
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
  
  # Remove all speeds smaller or equal to zero (invalid speeds)
  dataSpeed = dataSpeed[which(dataSpeed$avg_speed>0),]
  
  # Collect garbage
  gc()
  
  # Remove unuseful columns
  dataSpeed = dataSpeed %>%
    select( 
      -num_in_in,
      -error)
  
  # Collect garbage
  gc()
  
  # Combine data with meta data
  dataSpeed = dataSpeed %>%
    full_join(
      metaSpeed, 
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
  dataSpeed = dataSpeed %>%
    mutate(
      date = format(strptime(dataSpeed$per_start,format="%Y-%m-%d %H:%M"), "%Y-%m-%d-%H")) %>%
    select( 
      -per_start,
      -per_end) %>%
    arrange(speedID,
            date)
  
  # Collect garbage
  gc()
  
  # Calculate weighted harmonic mean of the speed and the standard deviation in km/h for each 
  # measurement point per hour and add to list
  dataSpeed = dataSpeed %>%
    group_by(speedID,
             date) %>%
    summarise(std = func_sd(avg_speed, num_in_use), avg_speed = func_mean(avg_speed, num_in_use)) %>%
    ungroup()
  
  # Collect garbage
  gc()
  
  returnValue(dataSpeed)

}

# Combine the list of data.table's to one data.table
dataSpeed <- rbindlist(dataSpeed_list)

# Remove overlappings in lists, meaning:
# Calculate weighted mean of the speed and the mean of the standard deviation in km/h for each 
# measurement point per hour
dataSpeed = dataSpeed %>%
  group_by(speedID,
           date) %>%
  summarise(std = mean(std), avg_speed = mean(avg_speed)) %>%
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

# Add reference column (average for each speedID, day of the week and hour of the day - standard deviation)
dataSpeed = dataSpeed %>%
  mutate(dif_speedID_dayWeek         = ave(dataSpeed$avg_speed, dataSpeed$speedID, dataSpeed$week_day, FUN = mean) - dataSpeed$avg_speed,
         dif_speedID_dayWeek_hourDay = ave(dataSpeed$avg_speed, dataSpeed$speedID, dataSpeed$week_day, dataSpeed$hour, FUN = mean) - dataSpeed$avg_speed) %>%
  select(-hour)

# Add difference column
dataSpeed = dataSpeed %>%
  mutate(sig_dif_speedID_dayWeek         = dataSpeed$dif_speedID_dayWeek - dataSpeed$std,
         sig_dif_speedID_dayWeek_hourDay = dataSpeed$dif_speedID_dayWeek_hourDay - dataSpeed$std)

# Save to file
data.table::fwrite(
  dataSpeed,
  nThread = 1,
  file = paste(f_output, "dataSpeed", ".csv", sep="", collapse=NULL),
  sep = sep_symbol)
