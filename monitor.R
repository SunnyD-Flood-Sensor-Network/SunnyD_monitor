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

# Source env variables if working on desktop
# source("C:/Users/Adam Gold/Desktop/postgres_keys.R")
# source("C:/Users/Adam Gold/Desktop/noaa_api_token.R")
# source("postgres_keys.R")

# Connect to database
con <- dbPool(
  drv = RPostgres::Postgres(),
  dbname = Sys.getenv("POSTGRESQL_DATABASE"),
  host = Sys.getenv("POSTGRESQL_HOST"),
  port = Sys.getenv("POSTGRESQL_PORT"),
  password = Sys.getenv("POSTGRESQL_PASSWORD"),
  user = Sys.getenv("POSTGRESQL_USER")
)

# download the sensor locations so we can use it easier
sensor_locations <- con %>%
  tbl("sensor_locations") %>%
  collect() %>%
  dplyr::select(place, sensor_ID, sensor_elevation, road_elevation)

# These are just connections for reading/writing
raw_data <- con %>%
  tbl("sensor_data")

# Read the latest atmospheric pressure readings
atm_pressure_tibble <- readr::read_csv("atm_pressure.csv")


#---------------- alternative loop -------------
# minutes <- seq(from = 4, to = 59, by = 6)
# 
# if(as.numeric(format(Sys.time(),format =  "%M")) %in% minutes)
# i=TRUE

# while(i = TRUE){
  # tictoc::tic()
# start_time <- Sys.time()

# monitor_function <- function(debug = T){
#   timestamp()



#------------------------ Functions to retrieve atm pressure -------------------
# Should return a tibble with columns: location | date | pressure_mb | notes

beaufort_atm <- function() {
  request <-
    httr::GET(
      url = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter/",
      query = list(
        "station" = "8656483",
        "date" = "latest",
        "product" = "air_pressure",
        "units" = "metric",
        "time_zone" = "gmt",
        "format" = "json",
        "application" = "UNC_Institute_for_the_Environment, https://github.com/acgold"
      )
    )
  
  latest_atm_pressure <-
    tibble::as_tibble(jsonlite::fromJSON(rawToChar(request$content))$data)
  colnames(latest_atm_pressure) <- c("date", "pressure_mb", "codes")
  
  latest_atm_pressure <- latest_atm_pressure %>%
    transmute(
      location = "Beaufort, North Carolina",
      date = ymd_hm(date),
      pressure_mb = as.numeric(pressure_mb),
      notes = "coop"
    )
  return(latest_atm_pressure)
}

#-------------------- Process the data ---------------------------
# Find the latest atmospheric pressure reading in .csv file
latest_date <- max(atm_pressure_tibble$date, na.rm = T)

# New data available from the API?
new_data_available <- (beaufort_atm()$date > latest_date)

# If no new data, say so
if (new_data_available == F) {
  if(debug == T){
    cat("No new data!", "\n")
  }
}

# If new data is available, process raw data within the time span between the two latest atmospheric pressure readings
if (new_data_available == T) {
  # Get the latest atm pressure reading and drop old ones so there are only the 2 latest measurements for each location. Filter out any repeated time points, and make sure there are 2 time points
  atm_pressure_tibble <- atm_pressure_tibble  %>%
    bind_rows(beaufort_atm()) %>%
    group_by(location) %>%
    arrange(desc(date)) %>%
    distinct(location, date, .keep_all = T) %>%
    slice(1:2) %>%
    mutate(n = n()) %>%
    filter(n > 1)
  
  # Write the latest atm pressures to the csv, overwriting old values
  readr::write_csv(atm_pressure_tibble, file = "atm_pressure.csv", append = F)
  
  # If there are two time points at a location, we will process that time period between them here
  if (nrow(atm_pressure_tibble) > 0) {
    
    # Get the difference in pressure and time between atm pressure readings,
    # getting the slope so we can linearly estimate the pressure for raw data points between those times
    pressure_diff <- atm_pressure_tibble %>%
      group_by(location) %>%
      summarise(min_date = min(date, na.rm = T),
                max_date = max(date, na.rm = T)) %>%
      mutate(
        min_date_pressure = as.numeric(atm_pressure_tibble$pressure_mb[atm_pressure_tibble$date == min_date]),
        max_date_pressure = as.numeric(atm_pressure_tibble$pressure_mb[atm_pressure_tibble$date == max_date]),
        slope = (max_date_pressure - min_date_pressure) / (as.numeric(max_date -
                                                                        min_date) * 60)
      )
    
    # For each location (row), process the data 
    combined_data <-
      foreach(i = 1:nrow(pressure_diff), .combine = "bind_rows") %do% {
        pressure_diff_row <- pressure_diff[i, ]
        
        raw_data_selected <- raw_data %>%
          filter(
            place == !!pressure_diff_row$location,
            date >= !!pressure_diff_row$min_date,
            date < !!pressure_diff_row$max_date
          ) %>%
          collect() %>%
          left_join(sensor_locations, by = c("place", "sensor_ID")) %>%
          mutate(
            min_date = !!pressure_diff_row$min_date,
            min_date_pressure = !!pressure_diff_row$min_date_pressure,
            slope = !!pressure_diff_row$slope
          )
        
        # Calculate the water elevation, sensor water level, and road water level using data from "sensor_locations" table
        processing_data <- raw_data_selected %>%
          transmute(
            place = place,
            sensor_ID = sensor_ID,
            date = date,
            road_water_level = NA,
            road_elevation = road_elevation,
            sensor_water_level = NA,
            sensor_elevation = sensor_elevation,
            atm_pressure = min_date_pressure + (as.numeric(date - min_date) * 60 * slope),
            sensor_pressure = pressure*10,
            voltage = voltage,
            notes = notes
          ) %>%
          mutate(
            sensor_water_level = ((((sensor_pressure - atm_pressure) * 100
            ) / (1020 * 9.81)) * 3.28084) + sensor_elevation,
            road_water_level = sensor_water_level - road_elevation
          )
        
        processing_data
      }
    
    # If there are no rows in combined data, there was no raw data between that time period
    if (nrow(combined_data) == 0) {
      if(debug == T){
        cat("No new raw data!", "\n")
      }
    }
    
    # If there are rows of processed data, upsert them into the database
    if (nrow(combined_data) > 0) {
      dbx::dbxUpsert(
        conn = con,
        table = "sensor_data_processed",
        records = combined_data,
        where_cols = c("place", "sensor_ID", "date") # Need to designate primary key on pgAdmin first, then put column name here
      )
      if(debug == T){
        cat("Wrote to database!", "\n")
      }
    }
  }
}

# later::later(monitor_function, 60*1)

# }


# monitor_function(debug = T)

# Close the pool connection to clean up
poolClose(con)

#---------------- Wunderground web scraping ----------------------
# cb_weather <- xml2::read_html("https://www.wunderground.com/weather/us/nc/carolina-beach")
# 
# timestamp <- cb_weather %>% 
#   # html_node(".timestamp") %>%
#   html_node(".timestamp :nth-child(2)") %>%
#   # html_node(".wu-unit-pressure .wu-value-to") %>%
#   html_text() %>%
#   stringr::str_split(.,pattern = " on ") %>% 
#   unlist()
# 
# timestamp[1] %>% lubridate::hms()
# 
# wu_pressure <- cb_weather %>% 
#   # html_node(".timestamp") %>%
#   # html_node(".timestamp :nth-child(2)") %>%
#   html_node(".wu-unit-pressure .wu-value-to") %>%
#   html_text() %>%
#   as.numeric() 
# 
# round(wu_pressure * 33.8639)
# 
# tictoc::toc()

#------------ Carolina Beach openweathermap --------------------
# owm_city_codes <- tibble::tibble("location" = c("Carolina Beach, North Carolina"),
# "owm_code" = c(4459261))

# owm_request <- httr::GET(url="https://api.openweathermap.org/data/2.5/weather",
#                          query=list(
#                            id = 4459261,
#                            appid= APP_ID))
# 
# owm_latest_atm_pressure <- tibble::tibble(location = "Carolina Beach, North Carolina",
#                                           date = lubridate::floor_date(Sys.time(), "1 minutes"),
#                                           pressure_mb = jsonlite::fromJSON(rawToChar(owm_request$content))$main$pressure,
#                                           notes = "owm.org")
# if(atm_in_db$date[atm_in_db$location == owm_latest_atm_pressure$location] < owm_latest_atm_pressure$date){
#   
#   DBI::dbAppendTable(conn = con,
#                      name = "atm_pressure",
#                      value = owm_latest_atm_pressure)
# }

#------------------ nws atm ---------------------------------
# latest_atm <- foreach(i = 1:nrow(nws_stationIDs), .combine = "bind_rows") %do% {
#   nws_request <- httr::GET(url = paste0("https://api.weather.gov/stations/",nws_stationIDs$stationID[i],"/observations/latest?require_qc=true"))
#   nws_request_parsed <- jsonlite::fromJSON(rawToChar(nws_request$content))
#   
#   latest_atm_pressure <- tibble::tibble("location" = nws_stationIDs$location[i],
#                                         "nws_code" = nws_request_parsed$properties$station,
#                                         "date" = lubridate::ymd_hms(nws_request_parsed$properties$timestamp),
#                                         "pressure_pa" = nws_request_parsed$properties$barometricPressure$value,
#                                         "pressure_mb" = pressure_pa/100)
#   latest_atm_pressure
# }
# 
# atm_in_db <- atm_pressure %>%
#   group_by(location) %>%
#   filter(date == max(date, na.rm = T)) %>% 
#   collect()
# 
# foreach(i = 1:nrow(latest_atm), .combine = "bind_rows") %do% {
#   if(atm_in_db$date[atm_in_db$location == latest_atm$location[i]] < latest_atm$date[i]){
#     
#     DBI::dbAppendTable(conn = con,
#                        name = "atm_pressure",
#                        value = latest_atm[i,])
#   }
# }




