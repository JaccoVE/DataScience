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
registerDoMC(20)

# Seperated for csv save
sep_symbol <- ","

# Folder Locations
f_database <- "/home/jacco/Documents/Git/DataScience/DatabaseWithDate (Revision)/"
f_output <- "/home/jacco/Documents/Git/DataScience/DatabaseWithDate (Revision)/"

# --------------------------------------------------
# Generate date table --------------------------

# Load dataRoad
dataRoad = data.table::fread(file = paste(f_database, "dataRoad", ".csv", sep="", collapse=NULL),
                             nThread = 24)

# Load dataFlow
dataFlow = data.table::fread(file = paste(f_database, "dataFlow", ".csv", sep="", collapse=NULL),
                             nThread = 24)

# Load dataSpeed
dataSpeed = data.table::fread(file = paste(f_database, "dataSpeed", ".csv", sep="", collapse=NULL),
                             nThread = 24)

# Select coordinates of metaFlow
dateFlow = dataFlow %>%
  select("date") %>%
  mutate(dataType = "Flow")

# Select coordinates of metaSpeed
dateSpeed = dataSpeed %>%
  select("date") %>%
  mutate(dataType = "Speed")

# Combine metaRoad, metaFlow and metaSpeed coordinates in 1 data.table
Date <- rbind(dateFlow, dateSpeed)

# Remove data.tables that are no longer needed
remove(dateFlow)
remove(dateSpeed)

# Collect garbage
gc()

# Remove duplicated rows
Date <- Date %>%
  unique()

# Split date-time column in date and time column
Date <- Date %>%
  mutate(
    date = format(strptime(Date$date,format="%Y-%m-%d-%H"), "%Y-%m-%d"),
    hour = format(strptime(Date$date,format="%Y-%m-%d-%H"), "%H")) %>%
  arrange(date, hour)

# Remove all rows that contain NA's for date
Date = Date[!is.na(Date$date),]

# Collect garbage
gc()

# Add dateID
Date <- Date %>%
  mutate(dateID = row_number())

# Save to csv file
data.table::fwrite(Date,
                   nThread = 24,
                   file = paste(f_output, "Date.csv", sep="", collapse=NULL),
                   sep = sep_symbol)

# --------------------------------------------------
# Join Date table with dataFlow and dataSpeed files 

# Combine Date with dataFlow
dataFlow = dataFlow %>%
  mutate(date = format(strptime(dataFlow$date,format="%Y-%m-%d-%H"), "%Y-%m-%d"),
         hour = format(strptime(dataFlow$date,format="%Y-%m-%d-%H"), "%H"),
         dataType = "Flow") %>%
  rename(
    dateFlow = "date",
    hourFlow = "hour",
    dataTypeFlow = "dataType") %>%
  inner_join(
    Date,
    by = c(
      "dateFlow" = "date",
      "hourFlow" = "hour",
      "dataTypeFlow" = "dataType")) %>%
  select(
    -dateFlow,
    -hourFlow,
    -dataTypeFlow,
    -week_day)

# Combine Date with dataSpeed
dataSpeed = dataSpeed %>%
  mutate(date = format(strptime(dataSpeed$date,format="%Y-%m-%d-%H"), "%Y-%m-%d"),
         hour = format(strptime(dataSpeed$date,format="%Y-%m-%d-%H"), "%H"),
         dataType = "Speed") %>%
  rename(
    dateSpeed = "date",
    hourSpeed = "hour",
    dataTypeSpeed = "dataType") %>%
  inner_join(
    Date,
    by = c(
      "dateSpeed" = "date",
      "hourSpeed" = "hour",
      "dataTypeSpeed" = "dataType")) %>%
  select(
    -dateSpeed,
    -hourSpeed,
    -dataTypeSpeed,
    -week_day)

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
    file = paste(f_output, "dataSpeed/dataSpeed_", uniqueDays[i], ".csv", sep="", collapse=NULL),
    sep = sep_symbol)
}

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
    file = paste(f_output, "dataFlow/dataFlow_", uniqueDays[i], ".csv", sep="", collapse=NULL),
    sep = sep_symbol)
}

# --------------------------------------------------
# Remove dates that are not in range and adjust range

# Remove day column from dataSpeed
dataSpeed = dataSpeed %>%
  select(-day)

# Remove day column from dataFlow
dataFlow = dataFlow %>%
  select(-day)

# Remove week columns from dataRoad and change date-time format
dataRoad = dataRoad %>%
  select(-week_day_start,
         -week_day_end) %>%
  mutate(hour_start = format(strptime(dataRoad$date_start,format="%Y-%m-%d-%H"), "%H"),
         hour_end = format(strptime(dataRoad$date_end,format="%Y-%m-%d-%H"), "%H"),
         date_start = format(strptime(dataRoad$date_start,format="%Y-%m-%d-%H"), "%Y-%m-%d"),
         date_end = format(strptime(dataRoad$date_end,format="%Y-%m-%d-%H"), "%Y-%m-%d"))

# Delete rows that are not within range
dataRoad = dataRoad[!(dataRoad$date_end < min(Date$date)), ]
dataRoad = dataRoad[!(dataRoad$date_start > max(Date$date)), ]

# Remove remaining rows with NA's
dataRoad = na.omit(dataRoad) 

# Iterate over all rows of dataRoad
foreach(i=1:nrow(dataRoad)) %do% {
  
  # Change ranges of dates to min of Date table
  if(dataRoad$date_start[i] < min(Date$date))
  {
    dataRoad$date_start[i] <- min(Date$date)
    dataRoad$hour_start[i] <- Date$hour[1]
  }
  else
  {
    dataRoad$date_start[i] <- dataRoad$date_start[i]
    dataRoad$hour_start[i] <- dataRoad$hour_start[i]
  }
  
  # Change ranges of dates to max of Date table
  if(is.na(dataRoad$date_end[i]))
  {
    dataRoad$date_end[i] <- max(Date$date)
    dataRoad$hour_end[i] <- Date$hour[nrow(Date)]
  }
  else if(dataRoad$date_end[i] > max(Date$date))
  {
    dataRoad$date_end[i] <- max(Date$date)
    dataRoad$hour_end[i] <- Date$hour[nrow(Date)]
  }
  else
  {
    dataRoad$date_end[i] <- dataRoad$date_end[i]
    dataRoad$hour_end[i] <- dataRoad$hour_end[i]
  }
  
}

# --------------------------------------------------
# Generate the dates between the date range --------

# List for the loop
dataRoad_list = list()

# Iterate over all rows of dataRoad to generate the dates between the date range
dataRoad_list <- foreach(i=1:nrow(dataRoad)) %dopar% {
  
  sequence = seq(
    from=as.Date(dataRoad$date_start[i]),
    to=as.Date(dataRoad$date_end[i]),
    by="day"
  )
  
  dataRoad_expand = dataRoad[i,]
  
  dataRoad_expand = dataRoad_expand[rep(seq_len(nrow(dataRoad_expand)), each=length(sequence)),]
  
  dataRoad_expand = dataRoad_expand %>%
    mutate(date = sequence)
  
  returnValue(dataRoad_expand)
  
}

# Combine the list of data.table's to one data.table
dataRoad <- rbindlist(dataRoad_list)

# Remove columns that are no longer needed
dataRoad = dataRoad %>%
  select(-date_start,
         -date_end)

# --------------------------------------------------
# Generate the hours between the hour range --------

# Change hour format
dataRoad = dataRoad %>%
  mutate(hour_start = paste0(date, " ", hour_start, ":00"),
         hour_end = paste0(date, " ", hour_end, ":00"))

# Remove rows where hour_end is smaller than hour_start
dataRoad = dataRoad[!(dataRoad$hour_end < dataRoad$hour_start) ,]

# List for the loop
dataRoad_list = list()

# Iterate over all rows of dataRoad to generate the hours between the hour range
dataRoad_list <- foreach(i=1:nrow(dataRoad)) %dopar% {
  
  sequence = seq(
    from=as.POSIXct(dataRoad$hour_start[i], tz="UTC"),
    to=as.POSIXct(dataRoad$hour_end[i], tz="UTC"),
    by="hour"
  )
  
  dataRoad_expand = dataRoad[i,]
  
  dataRoad_expand = dataRoad_expand[rep(seq_len(nrow(dataRoad_expand)), each=length(sequence)),]
  
  dataRoad_expand = dataRoad_expand %>%
    mutate(hour = sequence)
  
  returnValue(dataRoad_expand)
  
}

# Combine the list of data.table's to one data.table
dataRoad <- rbindlist(dataRoad_list)

# Remove columns that are no longer needed
dataRoad = dataRoad %>%
  select(-hour_start,
         -hour_end)

# Change hour format
dataRoad = dataRoad %>%
  mutate(hour = format(strptime(dataRoad$hour,format="%Y-%m-%d %H"), "%H"))

# --------------------------------------------------
# Save to file -------------------------------------

# Determine unique days
uniqueDays = unique(dataRoad$date)

# Iterate over each file in parallel
foreach(i=1:length(uniqueDays)) %dopar% {
  
  dataRoad_Splitted = dataRoad[dataRoad$date == uniqueDays[i], ]
  
  # Save to file
  data.table::fwrite(
    dataRoad_Splitted,
    nThread = 1,
    file = paste(f_output, "dataRoad/dataRoad_", uniqueDays[i], ".csv", sep="", collapse=NULL),
    sep = sep_symbol)
}





