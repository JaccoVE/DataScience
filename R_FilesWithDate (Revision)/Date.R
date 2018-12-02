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
# Generate a first version of the date table -------

# Load dataRoad
dataRoad = data.table::fread(file = paste(f_database, "dataRoad", ".csv", sep="", collapse=NULL),
                             nThread = 24)

# Load dataFlow
dataFlow = data.table::fread(file = paste(f_database, "dataFlow", ".csv", sep="", collapse=NULL),
                             nThread = 24)

# Load dataSpeed
dataSpeed = data.table::fread(file = paste(f_database, "dataSpeed", ".csv", sep="", collapse=NULL),
                             nThread = 24)

# Select date of dataFlow
dateFlow = dataFlow %>%
  select("date") %>%
  mutate(dataType = "Flow")

# Select date of dataSpeed
dateSpeed = dataSpeed %>%
  select("date") %>%
  mutate(dataType = "Speed")

# Combine dataFlow and dataSpeed dates in 1 data.table
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
    mutate(date = as.character(sequence))
    
  
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

# Remove unused data
remove(dataRoad_list)

# Collect garbage
gc()

# ------------------------------------------------------
# Add dataRoad dates to Date table ---------------------

# Select date of dataRoad
dateRoad = dataRoad %>%
  select("date",
         "hour") %>%
  mutate(dataType = "Road") %>%
  unique()

# Add dateRoad to Date table
Date <- rbind(dateRoad, Date)

# Arrange by date and hour and add dateID
Date <- Date %>%
  arrange(date, hour) %>%
  mutate(dateID = row_number())

# Remove unused data
remove(dateRoad)

# Collect garbage
gc()

# ------------------------------------------------------
# Join Date table with dataFlow, dataSpeed and dataRoad 

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
    -hourSpeed,
    -dataTypeSpeed,
    -week_day)

# Combine Date with dataRoad
dataRoad = dataRoad %>%
  mutate(dataType = "Road") %>%
  rename(
    dateRoad = "date",
    hourRoad = "hour",
    dataTypeRoad = "dataType") %>%
  inner_join(
    Date,
    by = c(
      "dateRoad" = "date",
      "hourRoad" = "hour",
      "dataTypeRoad" = "dataType")) %>%
  select(
    -hourRoad,
    -dataTypeRoad)

# ------------------------------------------------------------------
# Save dataSpeed, dataFlow, dataRoad and Date tables to file -------

# Determine unique days
uniqueDays = unique(dataSpeed$dateSpeed)

# Iterate over each file in parallel
foreach(i=1:length(uniqueDays)) %dopar% {
  
  dataSpeed_Splitted = dataSpeed[dataSpeed$dateSpeed == uniqueDays[i], ]
  
  dataSpeed_Splitted = dataSpeed_Splitted %>%
    select(-dateSpeed)
  
  # Save to file
  data.table::fwrite(
    dataSpeed_Splitted,
    nThread = 1,
    file = paste(f_output, "dataSpeed/dataSpeed_", uniqueDays[i], ".csv", sep="", collapse=NULL),
    sep = sep_symbol)
}

# Determine unique days
uniqueDays = unique(dataFlow$dateFlow)

# Iterate over each file in parallel
foreach(i=1:length(uniqueDays)) %dopar% {
  
  dataFlow_Splitted = dataFlow[dataFlow$dateFlow == uniqueDays[i], ]
  
  dataFlow_Splitted = dataFlow_Splitted %>%
    select(-dateFlow)
  
  # Save to file
  data.table::fwrite(
    dataFlow_Splitted,
    nThread = 1,
    file = paste(f_output, "dataFlow/dataFlow_", uniqueDays[i], ".csv", sep="", collapse=NULL),
    sep = sep_symbol)
}

# Determine unique days
uniqueDays = unique(dataRoad$dateRoad)

# Iterate over each file in parallel
foreach(i=1:length(uniqueDays)) %dopar% {
  
  dataRoad_Splitted = dataRoad[dataRoad$dateRoad == uniqueDays[i], ]
  
  dataRoad_Splitted = dataRoad_Splitted %>%
    select(-dateRoad)
  
  # Save to file
  data.table::fwrite(
    dataRoad_Splitted,
    nThread = 1,
    file = paste(f_output, "dataRoad/dataRoad_", uniqueDays[i], ".csv", sep="", collapse=NULL),
    sep = sep_symbol)
}

# Save Date to csv file
data.table::fwrite(Date,
                   nThread = 24,
                   file = paste(f_output, "Date.csv", sep="", collapse=NULL),
                   sep = sep_symbol)

