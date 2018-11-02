# Load libraries
library(DBI)
library(RPostgreSQL)
library(readr)
library(dplyr)
library(lubridate)
library(XML)
library(methods)
library(xml2)
library(data.table)
library(gtools)
library(foreach)
library(doMC)

# --------------------------------------------------
# Settings -----------------------------------------

# Number of threads to use when performing the for loop
registerDoMC(22)

# Seperated for csv save
sep_symbol <- ","

# Folder Locations
#f_main <- "/media/jacco/HDD/DataScienceData/Data/Status/"
f_main <- "/home/jacco/Documents/DataScienceData/Data/Status/"
f_road <- "RoadMaintenance/"
f_output <- "/home/jacco/Documents/Git/DataScience/Database/"

# --------------------------------------------------------
# Functions ----------------------------------------------
NulltoNA <- function(input)
{
  if(is.null(input))
  {
    return(NA)
  }
  else
  {
    return(input)
  }
}

# --------------------------------------------------------
# Create UniqueSiteReference tables RoadMaintenance ------

# List of all the xml-files
filenames <- list.files(paste(f_main, f_road, sep="", collapse=NULL), pattern="*.xml", full.names=FALSE)

# List of data.table's
road_data_list <- list()

# Iterate over each file in parallel
road_data_list <- foreach(i=1:length(filenames)) %dopar% {
  
  # Collect garbage
  gc()
  
  # Name of the file to process
  file = paste(f_main, f_road, filenames[i], sep="", collapse=NULL)
  
  # Parsed xml data
  data = xmlParse(file)
  
  # List of the parsed xml
  xml_data = xmlToList(data)
  
  # Check whether the object contains useable data, otherwise skip it
  res = try(xmlRoot(data)[["payloadPublication"]])
  if(inherits(res, "try-error") | is.null(res))
  {
    returnValue(NA)
  }
  else
  {
    # Get the number of situation nodes
    numberOfSituation = length(xmlChildren(xmlRoot(data)[["payloadPublication"]])) - 2
  
    # List for all the situations
    situation_list = list()
    
    # Iterate over each situation 
    situation_list <- foreach(j=1:numberOfSituation) %do% {
      
      # Get the number of situationRecord nodes
      numberOfSituationRecord = length(xmlChildren(xmlRoot(data)[["payloadPublication"]][[j+2]])) - 3
      
      # List for all the situationRecords
      situationRecord_list = list()
      
      # Iterate over each situationRecord
      situationRecord_list <- foreach(k=1:numberOfSituationRecord) %do% {
        
        # Check whether the object contains a location, otherwise skip it
        res = try(xml_data[["payloadPublication"]][[j+2]][[k+3]][["groupOfLocations"]][["locationContainedInItinerary"]][["location"]][["locationForDisplay"]][["latitude"]])
        if(inherits(res, "try-error") | is.null(res))
        {
          returnValue(NA)
        }
        else
        {
          road_data <- data.table(
            filename                                       = filenames[i],
            situationNumber                                = j,
            situationRecordNumber                          = k,
            situationRecordCreationTime                    = NulltoNA(xml_data[["payloadPublication"]][[j+2]][[k+3]][["situationRecordCreationTime"]]),
            situationRecordVersionTime                     = NulltoNA(xml_data[["payloadPublication"]][[j+2]][[k+3]][["situationRecordVersionTime"]]),
            probabilityOfOccurrence                        = NulltoNA(xml_data[["payloadPublication"]][[j+2]][[k+3]][["probabilityOfOccurrence"]]),
            sourceName                                     = NulltoNA(xml_data[["payloadPublication"]][[j+2]][[k+3]][["source"]][["sourceName"]][["values"]][["value"]][["text"]]),
            validityStatus                                 = NulltoNA(xml_data[["payloadPublication"]][[j+2]][[k+3]][["validity"]][["validityStatus"]]),
            validity_overallStartTime                      = NulltoNA(xml_data[["payloadPublication"]][[j+2]][[k+3]][["validity"]][["validityTimeSpecification"]][["overallStartTime"]]),
            validity_overallEndTime                        = NulltoNA(xml_data[["payloadPublication"]][[j+2]][[k+3]][["validity"]][["validityTimeSpecification"]][["overallEndTime"]]),
            location_locationForDisplay_latitude           = NulltoNA(xml_data[["payloadPublication"]][[j+2]][[k+3]][["groupOfLocations"]][["locationContainedInItinerary"]][["location"]][["locationForDisplay"]][["latitude"]]),
            location_locationForDisplay_longitude          = NulltoNA(xml_data[["payloadPublication"]][[j+2]][[k+3]][["groupOfLocations"]][["locationContainedInItinerary"]][["location"]][["locationForDisplay"]][["longitude"]]),
            location_carriageway                           = NulltoNA(xml_data[["payloadPublication"]][[j+2]][[k+3]][["groupOfLocations"]][["locationContainedInItinerary"]][["location"]][["supplementaryPositionalDescription"]][["affectedCarriagewayAndLanes"]][["carriageway"]]),
            management_lifeCycleManagement_cancel          = NulltoNA(xml_data[["payloadPublication"]][[j+2]][[k+3]][["management"]][["lifeCycleManagement"]][["cancel"]]),
            operatorActionStatus                           = NulltoNA(xml_data[["payloadPublication"]][[j+2]][[k+3]][["operatorActionStatus"]])
          )
          
          # Return value
          returnValue(road_data)
        }
      }
      
      # Remove NA's from list
      situationRecord_list = situationRecord_list[!is.na(situationRecord_list)]
      
      # Combine the list of data.table's to one data.table
      road_data <- rbindlist(situationRecord_list)
      
      # Return value
      returnValue(road_data)
    }
    
    # Combine the list of data.table's to one data.table
    road_data <- rbindlist(situation_list)
    
    # Collect garbage
    gc()
    
    # Return value
    returnValue(road_data)
  }
}

# Remove NA's from list
road_data_list = road_data_list[!is.na(road_data_list)]

# Combine the list of data.table's to one data.table
road_data <- rbindlist(road_data_list)

# Collect garbage
gc()

# Save to file
data.table::fwrite(
  road_data,
  nThread = 24,
  file = paste(f_output, "road_data.csv", sep="", collapse=NULL),
  sep = sep_symbol)
