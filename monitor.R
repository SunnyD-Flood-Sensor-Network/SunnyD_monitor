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

# Source env variables if working on desktop
# source("/Users/adam/Documents/SunnyD/sunnyday_postgres_keys.R")

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

adjust_wl <- function(time = Sys.time(), processed_data_db){
  
  min_date_x <-  time - days(14)
  min_date_x_shorter <- time - days(7)
  max_date_x <- time
  
  db_df_collected <- processed_data_db %>% 
    filter(date >= min_date_x & date <= max_date_x) %>% 
    collect()
  
  sensor_list <- unique(db_df_collected$sensor_ID)
  
  aggregate_smooth_wl <- foreach(j = 1:length(sensor_list), .combine = "bind_rows") %do% {
    
    min_wl <- foreach(i = 1:nrow(db_df_collected), .combine = "bind_rows") %do% {
      
      the_date <- db_df_collected$date[i]
      
      vals <- db_df_collected$road_water_level[(db_df_collected$date >= (the_date-days(1))) & (db_df_collected$date <= the_date)]
      
      min_val <- min(vals,na.rm=T)
      # min_val <- quantile(vals,0.02,na.rm=T)
      
      tibble::tibble("sensor_ID" = sensor_list[j], "date" = the_date, "min_wl" = min_val)
    }
    
    min_wl <- min_wl %>% 
      mutate(deriv = c(NA,diff(min_wl)),
             change_pt = ifelse(!is.na(deriv), ifelse(deriv != 0, T, F), F)) %>% 
      filter(deriv < 0.1)
    
    smoothed_min_wl <- tibble("date" = min_wl %>% filter(change_pt == T) %>% pull(date),
                              "smoothed_min_wl" = loess(min_wl~as.numeric(date), data = min_wl %>% filter(change_pt == T))$fitted
    )
    
    smoothed_wl_df <- min_wl %>%
      left_join(smoothed_min_wl, by = "date") %>%
      tidyr::fill(smoothed_min_wl,.direction = "downup") %>%
      dplyr::select(-c(deriv, change_pt)) %>% 
      filter(date >= min_date_x_shorter)
    
  }
  
  adjusted_df <- db_df_collected %>% 
    left_join(aggregate_smooth_wl, by = c("sensor_ID","date")) %>% 
    mutate(baseline_min_wl = sensor_elevation - road_elevation,
           drift = smoothed_min_wl - baseline_min_wl,
           road_water_level_adj = road_water_level - drift,
           sensor_water_level_adj = sensor_water_level - drift) %>% 
    filter(date >= min_date_x_shorter)
  
  return(adjusted_df)
}
  

flood_counter <- function(dates, start_number = 0, lag_hrs = 8){
  
  lagged_time <-  dates - dplyr::lag(dates)
  lead_time <-  dplyr::lead(dates) - dates
  
  lagged_time <- replace_na(lagged_time, 0)
  lead_time <- replace_na(lead_time, 0)
  
  group_change_vector <- foreach(i = 1:length(dates), .combine = "c") %do% {
    x <- 0
    
    if(lagged_time[i] > hours(lag_hrs)){
      x <- 1
    }
  
    x
  }
  
  group_vector <- cumsum(group_change_vector) + 1 + start_number
  return(group_vector)
}

is_it_flooding_now <- function(x, lag_hrs = 4){
  latest_flood <- x %>% 
    filter(flood_event == max(flood_event, na.rm=T)) %>% 
    filter(date == max(date, na.rm=T))
  
  return((Sys.time() - latest_flood$date) <= hours(lag_hrs))
}

find_flood_events <- function(x, existing_flood_events, flood_cutoff = 0){
  n_flooding_measurements <- x %>% 
    filter(road_water_level_adj >= flood_cutoff) %>% 
    nrow()
  
  if(n_flooding_measurements == 0){
    cat("No flooding detected!\n")
    return()
  }
  
  last_flood_number <- ifelse(nrow(existing_flood_events) > 0, max(existing_flood_events$flood_event, na.rm=T), 0)
  
  flooded_measurements <- x %>% 
    filter(road_water_level_adj >= flood_cutoff) %>% 
    group_by(sensor_ID) %>% 
    # mutate(flood_group = flood_counter(date, start_number = last_flood_number, lag_hrs = 2)) %>% 
    ungroup() %>% 
    mutate(min_date = date - minutes(1),
           max_date = date + minutes(1)) %>% 
    dplyr::select(place, sensor_ID, date, road_water_level_adj, road_water_level, drift, voltage, min_date, max_date)
  
  new_flood_events_df <- foreach(i = 1:nrow(flooded_measurements), .combine = "bind_rows") %do% {
    is_it_duplicate <- sum(flooded_measurements$min_date[i] < existing_flood_events$date & flooded_measurements$max_date[i] > existing_flood_events$date & flooded_measurements$sensor_ID[i] == existing_flood_events$sensor_ID) > 0 
    
    if(!is_it_duplicate){
      return(flooded_measurements[i,])
    }
    
    if(is_it_duplicate){
      return(NULL)
    }
  }
  
  if(nrow(new_flood_events_df) == 0){
    return(cat("No *NEW* flood events!\n"))
  }
  
  new_flood_events_df <- new_flood_events_df %>% 
    mutate(flood_event = flood_counter(date, start_number = last_flood_number, lag_hrs = 2), .before = "date")
    
  return(new_flood_events_df)
}


document_flood_events <- function(time = Sys.time(), processed_data_db){
  # correct for drift
  adjusted_wl <- adjust_wl(time = time, processed_data_db = processed_data_db)
  
  # Read in existing flood event data from Google Sheets
  existing_flood_events <- suppressMessages(googlesheets4::read_sheet(ss = sheets_ID))
  
  # segment the adjusted water level measurements into flood events
  flood_events_df <- find_flood_events(x = adjusted_wl, existing_flood_events = existing_flood_events)
  
  # No flood events detected, return the function
  if(is.null(flood_events_df)){
    return(cat("No flooding detected!\n"))
  }
  
  # check if it is currently flooding. If it is, add code to notify and remove the latest flood group
  # so it doesn't add unfinished flood event to the database
  if(is_it_flooding_now(flood_events_df) == T){
    # Add notification code here if it is currently flooding, but don't write it to the spreadsheet yet until it is over
    
    flood_events_df <- flood_events_df %>% 
      filter(flood_group < max(flood_group, na.rm = T))
  }
  
  # The time data seemed to be slightly off after being saved/read from Google Sheets, so
  # this gives a 1 minute buffer on either side of the time for comparison of new data to
  # existing data

  
  if(nrow(flood_events_df) == 0){
    return(cat("No new flood events!\n"))
  }
  
  if(nrow(flood_events_df) > 0){
    
    flood_event_name <- unique(flood_events_df$flood_event)
    
    flood_events_df$pic_link <- "NA"
    
    flood_events_w_pic <- foreach(k = 1:length(flood_event_name), .combine = "bind_rows") %do% {
      selected_flood <- flood_events_df %>% 
        filter(flood_event == flood_event_name[k]) 
      
      days_of_flood <- selected_flood %>% 
        pull(date) %>% 
        as_date() %>% 
        unique()
      
      folder_info <- suppressMessages(googledrive::drive_get(path = paste0("Images/","CAM_",unique(selected_flood$sensor_ID),"/",days_of_flood,"/"),shared_drive = as_id(Sys.getenv("GOOGLE_SHARED_DRIVE_ID"))))

      image_list <- suppressMessages(drive_ls(folder_info$id)) %>% 
        mutate(pic_time = ymd_hms(sapply(stringr::str_split(name, pattern = "_"), tail, 1)))
      
      selected_flood_w_pic <- foreach(j = 1:nrow(selected_flood), .combine = "bind_rows") %do% {
        
        min_flood_time <- min(selected_flood$date[j], na.rm=T) - minutes(5)
        max_flood_time <- max(selected_flood$date[j], na.rm=T) + minutes(5)
        
        filtered_image_list <- image_list %>% 
          filter(pic_time > min_flood_time & pic_time < max_flood_time)
        
        if(nrow(filtered_image_list) == 0){
          return(selected_flood[j,] %>% 
                   mutate(pic_link = NA))
        }
        
        filtered_image_list <- filtered_image_list %>% 
          mutate(diff_to_pic_time = abs(selected_flood$date[j] - pic_time)) %>% 
          filter(diff_to_pic_time == min(diff_to_pic_time)) %>% 
          slice(1) %>% 
          mutate(pic_link = drive_resource[[1]]$webViewLink)
        
        return(selected_flood[j,] %>% 
          mutate(pic_link = filtered_image_list$pic_link))
        }
      }
    }
    
    suppressMessages(googlesheets4::sheet_append(ss = sheets_ID,
                                                 data = flood_events_w_pic %>% 
                                                   dplyr::select(-c(min_date,max_date))))
    return(cat("Wrote new flood events!\n"))

  }

# load google authentications
folder_ID <- Sys.getenv("GOOGLE_DRIVE_FOLDER_ID")
sheets_ID <- Sys.getenv("GOOGLE_SHEET_ID")

googledrive::drive_auth(path = Sys.getenv("GOOGLE_JSON_KEY"))
googlesheets4::gs4_auth(token = googledrive::drive_token())




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
        cat("-",(pre_interpolated_data_filtered %>% nrow())-(interpolated_data_filtered %>% nrow()),"new observation(s) filtered out b/c not within atm pressure date range","\n")
      }
      
      processing_data <- interpolated_data_filtered %>%
        left_join(sensor_locations %>% collect(), by = c("place", "sensor_ID")) %>% 
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
               qa_qc_flag = ifelse(is.na(diff_per_time_lag), F, ifelse((diff_per_time_lag >= abs(.1)) , T, F))
               ) %>% 
        filter(tag == "new_data") %>% 
        dplyr::select(-c(tag,diff_lag, time_lag, diff_per_time_lag))
      
      final_data
    }
    
    dbx::dbxUpsert(
      conn = con,
      table = "sensor_data_processed",
      records = interpolated_data,
      where_cols = c("place", "sensor_ID", "date"),
      skip_existing = F
    )
    
    dbx::dbxUpdate(conn = con,
                   table="sensor_data",
                   records = new_data %>% 
                     semi_join(interpolated_data, by = c("place","sensor_ID","date")) %>% 
                     mutate(processed = T),
                   where_cols = c("place", "sensor_ID", "date")
                   )
    
    if (debug == T) {
      cat("- Wrote to database!", "\n")
    }
  }
}

# Run infinite loop that updates atm data and processes it
run = T
flood_tracker = 0

while(run ==T){
  start_time <- Sys.time()
  print(start_time)
  
  # calculate water level and write data
  monitor_function(debug = T)
  
  if(flood_tracker == 10){
    flood_tracker <- 0
  }
  
  if(flood_tracker == 0){
    # Extract flood events and write to Google Sheets
    document_flood_events(processed_data_db = processed_data)
  }
  
  flood_tracker <- flood_tracker + 1
  
  # Wait to make the delay 6 minutes
  delay <- difftime(Sys.time(),start_time, units = "secs")
  
  Sys.sleep((60*6) - delay)
}
