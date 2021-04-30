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

# Source env variables if working on desktop
# source("C:/Users/Adam Gold/Desktop/postgres_keys.R")
# source("C:/Users/Adam Gold/Desktop/noaa_api_token.R")
# source("postgres_keys.R")

# Connect to database
con <- dbPool(
  drv = RPostgres::Postgres(),
  dbname = Sys.getenv("POSTGRESQL_DATABASE"),
  host = Sys.getenv("POSTGRESQL_HOSTNAME"),
  port = Sys.getenv("POSTGRESQL_PORT"),
  password = Sys.getenv("POSTGRESQL_PASSWORD"),
  user = Sys.getenv("POSTGRESQL_USER")
)

# download the sensor locations so we can use it easier
sensor_locations <- con %>%
  tbl("sensor_locations") %>%
  collect() 

# These are just connections for reading/writing
raw_data <- con %>%
  tbl("sensor_data")

processed_data <- con %>% 
  tbl("sensor_data_processed")

#------------------------ Functions to retrieve atm pressure -------------------

beaufort_atm <- function(begin_date, end_date) {
  # Should return a tibble with columns: place | date | pressure_mb | notes
  request <-
    httr::GET(
      url = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter/",
      query = list(
        "station" = "8656483",
        "begin_date" = format(begin_date, "%Y%m%d %H:%M"),
        "end_date" = format(end_date, "%Y%m%d %H:%M"),
        "product" = "air_pressure",
        "units" = "metric",
        "time_zone" = "gmt",
        "format" = "json",
        "application" = "UNC_Institute_for_the_Environment, https://github.com/acgold"
      )
    )
  
  latest_atm_pressure <-
    tibble::as_tibble(jsonlite::fromJSON(rawToChar(request$content))$data)
  colnames(latest_atm_pressure) <-
    c("date", "pressure_mb", "codes")
  
  latest_atm_pressure <- latest_atm_pressure %>%
    transmute(
      place = "Beaufort, North Carolina",
      date = ymd_hm(date),
      pressure_mb = as.numeric(pressure_mb),
      notes = "coop"
    )
  return(latest_atm_pressure)
}

get_atm_pressure <- function(place, begin_date, end_date) {
  # Each location will have its own function called within this larger function
  if(place == "Beaufort, North Carolina") {
    beaufort_atm(begin_date = begin_date,
                 end_date = end_date)
  }
}

#-------------------- Process the data ---------------------------
monitor_function <- function(debug = T) {
  
  new_data <- raw_data %>% 
    filter(processed == F) %>% 
    collect()
  
  if(nrow(new_data) == 0){
    if (debug == T) {
      cat("- No new raw data!", "\n")
    }
  }
  
  if(nrow(new_data) > 0){
    
    # Create a column for the atm pressure, fill it with NAs, remove any duplicated rows
    pre_interpolated_data <- new_data %>%
      dplyr::mutate("pressure_mb" = NA_real_) %>%
      group_by(place) %>% 
      arrange(date) %>%
      distinct() %>% 
      ungroup()
    
    # Get all of the place names of the new data so we can iterate through
    place_names <- unique(new_data$place)
    
    # process atm data and new data for each place   
    interpolated_data <- foreach(i = 1:length(place_names), .combine = "rbind") %do% {
      
      selected_place_name <- place_names[i]
      
      # select new data for each place
      pre_interpolated_data_filtered <- pre_interpolated_data %>% 
        filter(place == selected_place_name)
      
      # extract the date range and duration
      new_data_date_range <- c(min(pre_interpolated_data_filtered$date, na.rm=T)-minutes(30), max(pre_interpolated_data_filtered$date, na.rm=T)+minutes(30))
      new_data_date_duration <- time_length(diff(new_data_date_range), unit = "days")
      
      if(new_data_date_duration >= 30){
        chunks <- ceiling(new_data_date_duration / 30)
        span <- duration(new_data_date_duration / chunks, units = "days")
        
        atm_tibble <- foreach(j = 1:chunks, .combine = "rbind") %do% {
          range_min <- new_data_date_range[1] + (span * (j - 1))
          range_max <- new_data_date_range[1] + (span * j)
          
          get_atm_pressure(place = selected_place_name,
                           begin_date = range_min,
                           end_date = range_max) 
        }
        
        atm_tibble <- atm_tibble %>% 
          distinct(date, .keep_all=T)
      }
      
      if(new_data_date_duration < 30){
        # get atm pressure from NOAA
        atm_tibble <- get_atm_pressure(
          place = selected_place_name,
          begin_date = new_data_date_range[1],
          end_date = new_data_date_range[2]
        ) %>% 
          distinct()
      }
      
      interpolated_data_filtered <- pre_interpolated_data_filtered %>% 
        filter(date > min(atm_tibble$date, na.rm=T) & date < max(atm_tibble$date, na.rm=T)) %>% 
        mutate(pressure_mb = approxfun(atm_tibble$date, atm_tibble$pressure_mb)(date)) 
      
      if(debug == T) {
        cat("- New raw data detected for:", selected_place_name, "\n")
        cat("-",pre_interpolated_data_filtered %>% nrow(),"new rows", "\n")
        cat("- Date duration is",round(new_data_date_duration,digits = 2),"days", "\n")
        cat("-",(pre_interpolated_data_filtered %>% nrow())-(interpolated_data_filtered %>% nrow()),"new observations filtered out b/c not within atm pressure range","\n")
      }
      
      processing_data <- interpolated_data_filtered %>%
        left_join(sensor_locations, by = c("place", "sensor_ID")) %>% 
        transmute(
          place = place,
          sensor_ID = sensor_ID,
          date = date,
          road_water_level = NA,
          road_elevation = road_elevation,
          sensor_water_level = NA,
          sensor_elevation = sensor_elevation,
          atm_pressure = pressure_mb,
          sensor_pressure = ifelse(pressure < 500, pressure/10 * 68.9476, pressure),
          voltage = voltage,
          notes = notes.x
        ) %>%
        mutate(
          sensor_water_level = ((((sensor_pressure - atm_pressure) * 100
          ) / (1020 * 9.81)) * 3.28084) + sensor_elevation,
          road_water_level = sensor_water_level - road_elevation,
          qa_qc_flag = F,
          tag = "new_data"
        ) 
      
      final_data <- processing_data %>% 
        rbind(processed_data %>% 
                filter(date >= !!new_data_date_range[1] & date <= !!new_data_date_range[2]) %>% 
                collect() %>% 
                mutate(tag = "processed_data")) %>% 
        mutate(diff_lag = sensor_water_level - lag(sensor_water_level),
               time_lag = time_length(date - lag(date), unit = "minute"),
               diff_per_time_lag = diff_lag/time_lag,
               qa_qc_flag = ifelse((diff_per_time_lag >= abs(.1)) ,T,F)) %>% 
        filter(tag == "new_data") %>% 
        dplyr::select(-c(tag,diff_lag, time_lag, diff_per_time_lag))
      
      final_data
    }
    
    dbx::dbxUpsert(
      conn = con,
      table = "sensor_data_processed",
      records = interpolated_data,
      where_cols = c("place", "sensor_ID", "date"),
      skip_existing = T
    )
    
    dbx::dbxUpdate(conn = con,
                   table="sensor_data",
                   records = new_data %>% mutate(processed = T),
                   where_cols = c("place", "sensor_ID", "date")
                   )
    
    if (debug == T) {
      cat("- Wrote to database!", "\n")
    }
  }
}

# Run infinite loop that updates atm data and processes it
run = T

while(run ==T){
  start_time <- Sys.time()
  print(start_time)
  monitor_function(debug = T)
  
  delay <- difftime(Sys.time(),start_time, units = "secs")
  
  Sys.sleep((60*6) - delay)
}


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