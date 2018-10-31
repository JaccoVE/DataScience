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
registerDoMC(4)

# Seperated for csv save
sep_symbol <- ","

# Folder Locations
f_NDW <- "/home/jacco/Documents/Git/DataScience/NDW/Tableau Input Files/"
f_Status <- "/home/jacco/Documents/Git/DataScience/Status/Tableau Input Files/"

f_input <- c("intensity_data_PerDay", 
             "intensity_data_PerHour",
             "speed_data_PerDay", 
             "speed_data_PerHour",
             "road_data_Amsterdam_PerDay.csv",
             "road_data_Amsterdam_PerHour.csv")

f_output <- "/home/jacco/Documents/Git/DataScience/NDW/Tableau Input Files/"

# ----------------------------------------------------
# Add day of the week to data

# Collect garbage
gc()

# Iterate over each file in parallel
foreach(i=1:4) %dopar% {
  
  i <- 1

  # Load meta data intensity
  data = data.table::fread(file = paste(f_NDW, f_input[i], ".csv", sep="", collapse=NULL),
                                          nThread = 1)
  
  # Collect garbage
  gc()
  
  # Add day of the week
  data = data %>%
    mutate(
      week_day = weekdays(as.Date(data$date,'%Y-%m-%d')))
  
  # Handle difference in flow and speed column names
  if(i == 1 | i == 2) # Flow 
  {
    # Add reference column
    data = data %>%
      mutate(reference = ave(data$avg_flow, data$week_day))
    
    # Add difference column
    data_test = data %>%
      mutate(difference = data$avg_flow - data$reference)
    
    # Save to csv file
    data.table::fwrite(road_data,
                       nThread = 1,
                       file = paste(f_output, f_input[i], "2.csv", sep="", collapse=NULL),
                       sep = sep_symbol)
  }
  else # Speed
  {
    
  }
  
  # Collect garbage
  gc()
}

