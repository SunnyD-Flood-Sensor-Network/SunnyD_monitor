# Packages to load
library(dplyr)
library(dbplyr)
library(lubridate)
library(RPostgres)
library(DBI)
library(pool)
library(foreach)
library(dbx)
library(readr)
library(httr)
library(later)
library(googledrive)
library(googlesheets4)
library(tidyr)
library(stringr)
library(jsonlite)
library(crul)
library(MASS)


#-------- Description --------------
# This code converts raw pressure data from a storm drain in
# Beaufort, NC to water level data. The water level data
# are then drift-corrected. Drift-corrected water level data
# are monitored, and any water level measurements above the road
# elevation trigger email alerts using Mailchimp.

# Github repo: https://github.com/SunnyD-Flood-Sensor-Network/SunnyD_monitor
# Project data viewer: http://go.unc.edu/flood-data


#-------- Setup ----------
# Source env variables if working on desktop
source("/Users/adam/Documents/SunnyD/sunnyday_postgres_keys.R")

# Connect to database
con <- dbPool(
  drv = RPostgres::Postgres(),
  dbname = Sys.getenv("POSTGRESQL_DATABASE"),
  host = Sys.getenv("POSTGRESQL_HOSTNAME"),
  port = Sys.getenv("POSTGRESQL_PORT"),
  password = Sys.getenv("POSTGRESQL_PASSWORD"),
  user = Sys.getenv("POSTGRESQL_USER")
)

# Connect to sensor location table
sensor_locations <- con %>%
  tbl("sensor_locations")

sensor_surveys <- con %>%
  tbl("sensor_surveys")

# These are just connections for reading/writing
raw_data <- con %>%
  tbl("sensor_data")

processed_data <- con %>%
  tbl("sensor_water_depth")

drift_corrected_data <- con %>%
  tbl("data_for_display")


#--------------- populating the new databases -----------------
raw_data_collected <- raw_data %>%
  collect()

old_processed_data <- con %>% 
  tbl("sensor_data_processed") %>% 
  collect() %>% 
  mutate(place = tools::toTitleCase(place))

new_processed_data <- con %>% 
  tbl("sensor_water_depth") %>% 
  collect()

new_colnames <- new_processed_data %>% colnames()

missing_processed_data <- old_processed_data[!old_processed_data$date %in% new_processed_data$date,]

missing_processed_data <- missing_processed_data %>% 
  mutate(tag = "new_data",
         atm_data_src = NA,
         atm_station_id = NA,
         sensor_water_depth = ((((sensor_pressure - atm_pressure) * 100
         ) / (1020 * 9.81)) * 3.28084)) %>% 
  dplyr::select(all_of(new_colnames))

dbx::dbxUpsert(
  conn = con,
  table = "sensor_water_depth",
  records = missing_processed_data,
  where_cols = c("place", "sensor_ID", "date"),
  skip_existing = F
)


#-------------- loop to reanalyze record ----------

first_survey_date <- sensor_surveys %>% 
  pull(date_surveyed) %>% 
  min() 

min_date <- (first_survey_date + weeks(1)) %>% 
  as_datetime()

max_date <- processed_data %>% 
  pull(date) %>% 
  max(na.rm=T)

number_of_iterations <- difftime(max_date, min_date, units = "days") %>% 
  ceiling() %>% 
  as.numeric()

data_for_display_collected <- drift_corrected_data %>% 
  collect()

for(i in 1:(number_of_iterations+1)){
  cat(i, "\n")
  
  adjusted_wl <- adjust_wl(time = min_date + days(i - 1), 
                           processed_data_db = processed_data %>%
                             filter(qa_qc_flag == F,
                                    date >= first_survey_date))
  
  data_for_display_collected <<- data_for_display_collected %>% 
    dplyr::rows_upsert(adjusted_wl, by = c("place", "sensor_ID", "date"))
}

data_for_display_collected <- data_for_display_collected %>% 
  filter(sensor_ID != "CB_02")

dbx::dbxUpsert(conn = con,
               table = "data_for_display",
               records = data_for_display_collected[25000:nrow(data_for_display_collected),],
               where_cols = c("place","sensor_ID","date")
)

# DBI::dbCreateTable(conn = con,
#                    name = "data_for_display",
#                    fields = data_for_display_collected)

nrow(data_for_display_collected)
