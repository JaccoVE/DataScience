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
f_database <- "/home/jacco/Documents/Git/DataScience/Database/"
f_output <- "/home/jacco/Documents/Git/DataScience/Database/"

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
    hour = format(strptime(Date$date,format="%Y-%m-%d-%H"), "%H"))

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
# Join Date table with dataRoad file ---------------

dataSpeed = dataSpeed %>%
  select(-day)

dataFlow = dataFlow %>%
  select(-day)

dataRoad = dataRoad %>%
  select(-week_day_start,
         -week_day_end) %>%
  mutate(hour_start = format(strptime(dataRoad$date_start,format="%Y-%m-%d-%H"), "%H"),
         hour_end = format(strptime(dataRoad$date_end,format="%Y-%m-%d-%H"), "%H"),
         date_start = format(strptime(dataRoad$date_start,format="%Y-%m-%d-%H"), "%Y-%m-%d"),
         date_end = format(strptime(dataRoad$date_end,format="%Y-%m-%d-%H"), "%Y-%m-%d"))

# Function
replaceByMin <- function(input)
{
  if(input < min(Date$date))
  {
    return(min(Date$date))
  }
  else
  {
    return(input)
  }
}

replaceByMax <- function(input)
{
  
  if(is.na(input))
  {
    return(max(Date$date))
  }
  else if(input > max(Date$date))
  {
    return(max(Date$date))
  }
  else
  {
    return(input)
  }
}

# Iterate over all rows of dataRoad
foreach(i=1:nrow(dataRoad)) %dopar% {
  
  # Change ranges of dates to min and max of Date table
  dataRoad$date_start[i] = replaceByMin(dataRoad$date_start[i])
  dataRoad$date_end[i] = replaceByMax(dataRoad$date_end[i])
  
}

# Iterate over all rows of dataRoad
foreach(i=1:nrow(dataRoad)) %dopar% {
  
  # Change ranges of dates to min and max of Date table
  dataRoad$date_start[i] = replaceByMin(dataRoad$date_start[i])
  dataRoad$date_end[i] = replaceByMax(dataRoad$date_end[i])
  
}







dataRoadtest = dataRoad%>%
  rowwise() %>%
  do(data.frame(idnum=.$roadNumber, month=seq(.$date_start,.$date_end,by="1 month")))


test = setDT(dataRoad)[, .(date = seq(dataRoad[i]$date_start, dataRoad[i]$date_end, by = '1 day')), 
                 by = .(roadID, ID2 = seq_len(nrow(dataRoad)))]  

# Iterate over all rows of dataRoad
foreach(i=1:length(uniqueDays)) %dopar% {
  
  i <- 1
  
  test = seq(
    from=as.POSIXct(dataRoad[i]$date_start, tz="UTC"),
    to=as.POSIXct(dataRoad[i]$date_end, tz="UTC"),
    by="hour"
  ) 
  
  test = format(strptime(test,format="%Y-%m-%d %H"), "%Y-%m-%d-%H")
  
}

# Combine Date with dataFlow
dataRoadTest = dataRoad %>%
  mutate(dataType = "Road") %>%
  seq(dataRoad$date_start, dataRoad$date_end, by="day")
  rename(
    dateRoad = "date",
    dataTypeRoad = "dataType") %>%
  inner_join(
    Date,
    by = c(
      "dateFlow" = "date",
      "dataTypeFlow" = "dataType")) %>%
  select(
    -dateFlow,
    -dataTypeFlow)

seq(strptime(dataFlow$date,format="%Y-%m-%d-%H"), strptime(dataFlow$date,format="%Y-%m-%d-%H"), by="days")


mutate(
  hour = format(strptime(dataFlow$date,format="%Y-%m-%d-%H"), "%H"),
  day = format(strptime(dataFlow$date,format="%Y-%m-%d-%H"), "%Y-%m-%d"))


