# Load libraries
library(DBI)
library(RPostgreSQL)
library(readr)
library(dplyr)
library(lubridate)
library(xml2)

# Load data
meta_data_main <- read.csv(file = "C:/Users/Duncan/Desktop/Studie/18-19 1A/Data Science/Project/R/utwente snelheden groot amsterdam  1 dag met metadata_snelheid_00001.csv", nrows = 10)
data_main <- read.csv(file = "C:/Users/Duncan/Desktop/Studie/18-19 1A/Data Science/Project/R/utwente snelheden groot amsterdam _snelheid_00001.csv", nrows = 10)

# Select data
data_select <- data_main %>%
  select("measurementSiteReference",
         "index",
         "periodStart",
         "periodEnd",
         "avgVehicleSpeed",
         "publicationTime",
         "generatedSiteName")

# Find bridge not bombarded in WW2 a.k.a best bridges in the fucking Netherlands (heil hitler)

# Read file names
loc_files <- "C:\\Users\\Duncan\\Desktop\\Studie\\18-19 1A\\Data Science\\Project\\R\\20160601"
list_files <- list.files(loc_files)

# Open file
loc_xml = paste(loc_files,"\\", list_files[3], sep = "", collapse = NULL)
xml <- read_xml(loc_xml)
xml_root <- xmlRoot(xml)
print(xml_attr(xml, "exchange"))

# Scan for elements
# Save content elements

