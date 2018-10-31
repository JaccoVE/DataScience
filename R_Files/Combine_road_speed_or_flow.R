# Load libraries
library(DBI)
library(RPostgreSQL)
library(readr)
library(dplyr)
library(lubridate)
library(xml2)
library(rgdal)
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
f_NDW <- "/home/jacco/Documents/Git/DataScience/NDW/Tableau Input Files/"
f_Status <- "/home/jacco/Documents/Git/DataScience/Status/Tableau Input Files/"

f_input <- c("intensity_data_PerDay", 
             "intensity_data_PerHour",
             "speed_data_PerDay", 
             "speed_data_PerHour",
             "road_data_Amsterdam_PerDay",
             "road_data_Amsterdam_PerHour")

f_output <- "/home/jacco/Documents/Git/DataScience/NDW/Tableau Input Files/"

# ----------------------------------------------------
# Load and prepare road data

foreach(i=5:6) %do% {
  
  # Load road data
  road = data.table::fread(file = paste(f_Status, f_input[i], ".csv", sep="", collapse=NULL),
                                  nThread = 24)
  
  # Select columns
  road = road %>%
    select(
      "date_start",
      "date_end",
      "week_day_start",
      "week_day_end",
      "location_locationForDisplay_latitude",
      "location_locationForDisplay_longitude") %>%
    arrange(
      date_start) %>%
    unique()
  
  if(i == 5) # perDay
  {
    road_perDay <- road 
  }
  else # perHour
  {
    road_perHour <- road
  }
}

# Remove road data because it is renamed
remove(road)

# Collect garbage
gc()

# ----------------------------------------------------
# Combine road with speed/flow

# Iterate over each file in parallel
foreach(i=1:4) %dopar% {
  
  i <- 1

  # Load meta data intensity
  data = data.table::fread(file = paste(f_NDW, f_input[i], ".csv", sep="", collapse=NULL),
                                          nThread = 1)
  
  # Collect garbage
  gc()
  
  # Add label wheter have road data or flow/speed data
  data = data %>%
    mutate(
      roadORdata = "data")
  
  # Add end date column
  data = data %>%
    mutate(
      date_end = "data")
    
  # Save to csv file
  data.table::fwrite(data,
                     nThread = 1,
                     file = paste(f_output, f_input[i], "_Combined.csv", sep="", collapse=NULL),
                     sep = sep_symbol)
  
  # Collect garbage
  gc()
}

