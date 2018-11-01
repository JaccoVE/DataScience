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
# Generate location table --------------------------

# Load metaRoad
metaRoad = data.table::fread(file = paste(f_database, "metaRoad", ".csv", sep="", collapse=NULL),
                               nThread = 24)

# Load metaFlow
metaFlow = data.table::fread(file = paste(f_database, "metaFlow", ".csv", sep="", collapse=NULL),
                             nThread = 24)

# Load metaSpeed
metaSpeed = data.table::fread(file = paste(f_database, "metaSpeed", ".csv", sep="", collapse=NULL),
                             nThread = 24)

# Select coordinates of metaRoad
locationRoad = metaRoad %>%
  select("startLocatieForDisplayLat",
         "startLocatieForDisplayLong") %>%
  mutate(metaType = "Road")

# Select coordinates of metaFlow
locationFlow = metaFlow %>%
  select("startLocatieForDisplayLat",
         "startLocatieForDisplayLong") %>%
  mutate(metaType = "Flow")

# Select coordinates of metaSpeed
locationSpeed = metaSpeed %>%
  select("startLocatieForDisplayLat",
         "startLocatieForDisplayLong") %>%
  mutate(metaType = "Speed")

# Combine metaRoad, metaFlow and metaSpeed coordinates in 1 data.table
Location <- rbind(locationRoad, locationFlow, locationSpeed)

# Remove data.tables that are no longer needed
remove(locationFlow)
remove(locationRoad)
remove(locationSpeed)

# Collect garbage
gc()

# Remove duplicated rows
Location <- Location %>%
  unique()

# Collect garbage
gc()

# Add coordinate ID
Location <- Location %>%
  mutate(coordinateID = row_number())

# Save to csv file
data.table::fwrite(Location,
                   nThread = 24,
                   file = paste(f_output, "Location.csv", sep="", collapse=NULL),
                   sep = sep_symbol)

# --------------------------------------------------
# Join location table with meta files --------------

# Combine Location with metaFlow
metaFlow = metaFlow %>%
  mutate(metaType = "Flow") %>%
  rename(
    Lat = "startLocatieForDisplayLat",
    Long = "startLocatieForDisplayLong",
    metaTypeFlow = "metaType") %>%
  inner_join(
    Location,
    by = c(
      "Lat" = "startLocatieForDisplayLat",
      "Long" = "startLocatieForDisplayLong",
      "metaTypeFlow" = "metaType")) %>%
  select(
    -Lat,
    -Long,
    -metaTypeFlow)

# Combine Location with metaRoad
metaRoad = metaRoad %>%
  mutate(metaType = "Road") %>%
  rename(
    Lat = "startLocatieForDisplayLat",
    Long = "startLocatieForDisplayLong",
    metaTypeRoad = "metaType") %>%
  inner_join(
    Location,
    by = c(
      "Lat" = "startLocatieForDisplayLat",
      "Long" = "startLocatieForDisplayLong",
      "metaTypeRoad" = "metaType")) %>%
  select(
    -Lat,
    -Long,
    -metaTypeRoad)

# Combine Location with metaSpeed
metaSpeed = metaSpeed %>%
  mutate(metaType = "Flow") %>%
  rename(
    Lat = "startLocatieForDisplayLat",
    Long = "startLocatieForDisplayLong",
    metaTypeSpeed = "metaType") %>%
  inner_join(
    Location,
    by = c(
      "Lat" = "startLocatieForDisplayLat",
      "Long" = "startLocatieForDisplayLong",
      "metaTypeSpeed" = "metaType")) %>%
  select(
    -Lat,
    -Long,
    -metaTypeSpeed)

# Save to csv file
data.table::fwrite(metaFlow,
                   nThread = 24,
                   file = paste(f_output, "metaFlow.csv", sep="", collapse=NULL),
                   sep = sep_symbol)

# Save to csv file
data.table::fwrite(metaRoad,
                   nThread = 24,
                   file = paste(f_output, "metaRoad.csv", sep="", collapse=NULL),
                   sep = sep_symbol)

# Save to csv file
data.table::fwrite(metaSpeed,
                   nThread = 24,
                   file = paste(f_output, "metaSpeed.csv", sep="", collapse=NULL),
                   sep = sep_symbol)
