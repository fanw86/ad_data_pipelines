
rm(list=ls())
library(lubridate)
library(data.table)
library(tidyverse)
library(stringr)
library(ggplot2)
library(sf)
library(tictoc)


tic()
# ads_passenger_travel_time_di

# ads_passenger_travel_time_di	4	换乘量	bigint	transfer_num	FALSE
# ads_passenger_travel_time_di	4	行程量	bigint	journey_num	FALSE
# ads_passenger_travel_time_di	4	出行量	bigint	passenger_trips_num	FALSE
# ads_passenger_travel_time_di	4	创建时间	timestamp without time zone	create_time	FALSE
# ads_passenger_travel_time_di	4	更新者用户名	character varying(255)	update_by	FALSE
# ads_passenger_travel_time_di	4	创建者用户名	character varying(255)	create_by	FALSE
# ads_passenger_travel_time_di	4	更新时间	timestamp without time zone	update_time	FALSE
# ads_passenger_travel_time_di	4	时间范围类型，0-10min,10-20min,20-30min,30-40min,40-50min,50-60min,60-90min,90-120-min,>120min	character varying(255)	time_range_type	FALSE
# ads_passenger_travel_time_di	4	区域编号	character varying(255)	region_id	FALSE
# ads_passenger_travel_time_di	4	日期类型	character varying(255)	date_type	FALSE
# ads_passenger_travel_time_di	4	日期	integer	t_date	FALSE
# ads_ridership_passenger_trips_di	5	创建者用户名	character varying(255)	create_by	FALSE


convert_to_decimal <- function(x) {
  # Convert string to numeric for calculations, retaining the sign
  x <- as.numeric(x)
  
  # Extract degrees, minutes, seconds, and milliseconds
  sign <- ifelse(x < 0, -1, 1)  # Determine the sign for each value
  x <- abs(x)  # Work with absolute value for calculations
  
  degrees <- floor(x / 10000000)
  minutes <- floor((x - degrees * 10000000) / 100000)
  seconds <- floor((x - degrees * 10000000 - minutes * 100000) / 1000)
  milliseconds <- x - degrees * 10000000 - minutes * 100000 - seconds * 1000
  
  # Convert to decimal degrees
  decimal_degrees <- sign * (degrees + minutes / 60 + (seconds + milliseconds / 1000) / 3600)
  
  rounded_decimal_degrees <- round(decimal_degrees, 6)
  
  return(rounded_decimal_degrees)
}

# Load VDV tables with error handling, discarding the last 2 rows
load_vdv_table <- function(folder, path) {
  path <- paste0(folder, "/", path)
  if (!file.exists(path)) {
    stop(paste("File not found:", path))
  }
  tryCatch({
    # Read column names from line 11
    col_names <- read_lines(path, skip = 10, n_max = 1) %>%
      str_split(";") %>%
      unlist() %>%
      str_trim()
    
    # Read data starting from line 13, then remove the last 2 rows
    data <- read_delim(path, delim = ";", col_names = col_names, trim_ws = TRUE, skip = 12)
    data <- data[-((nrow(data)-1):nrow(data)),]  # Remove last 2 rows
    
    # Select all columns except the first one
    data %>% select(-1)
  }, error = function(e) {
    stop(paste("Error loading", path, ":", e$message))
  })
}

folder="data\\vdv_202505080912"

# Load all required tables
stop <- load_vdv_table(folder,"i2531280.x10")
stop_point <- load_vdv_table(folder,"i2291280.x10")
journey <- load_vdv_table(folder,"i7151280.x10")
travel_time <- load_vdv_table(folder,"i2821280.x10")
route_sequence <- load_vdv_table(folder,"i2461280.x10")
link <- load_vdv_table(folder,"i2991280.x10")
point_on_link <- load_vdv_table(folder,"i9951280.x10")
routes <- load_vdv_table(folder,"i2261280.x10")


day_types <-  load_vdv_table(folder,"i2901280.x10")

journey_types <-  load_vdv_table(folder,"i3321280.x10")

op_department <- load_vdv_table(folder,"i3331280.x10")

# Process AFC data for travel time analysis
afc_data <- arrow::read_parquet("data/2025_H1_afc_data_endstop_imputed.parquet")

#afc_data <- head(afc_data_raw,n=1000)


# Map AFC routes to regions using VDV route mapping
route_region_mapping <- routes %>%
  select(LINE_ABBR, OP_DEP_NO) %>%
  distinct() %>%
  mutate(
    region_id = case_when(
      as.numeric(substr(as.character(abs(OP_DEP_NO)), 1, 1)) == 1 ~ "Abu Dhabi",
      as.numeric(substr(as.character(abs(OP_DEP_NO)), 1, 1)) == 2 ~ "Al Ain", 
      as.numeric(substr(as.character(abs(OP_DEP_NO)), 1, 1)) == 3 ~ "Al Dhafra"
    )
  ) %>% select(-OP_DEP_NO)


afc_data <- afc_data %>% left_join(
  route_region_mapping,by=c('route'='LINE_ABBR')
) %>% 
  mutate(
    ope_date = as.Date(start_time)
  ) 


agg_summary <- afc_data %>% group_by(
  ope_date
) %>% summarise(
  trips=sum(boarding)
)


toc()

# Calculate travel times and group into time ranges
trip_data <- afc_data %>%
  mutate(
    start_time= lubridate::ymd_hms(start_time),
    end_time= lubridate::ymd_hms(end_time)
  ) %>%
  mutate(
    travel_time_minutes = tryCatch(
      as.numeric(difftime(end_time, start_time, units = "mins")),
      error = function(e) as.POSIXct(NA)
    ),
    time_range_type = case_when(
      travel_time_minutes <= 10 ~ "0-10min",
      travel_time_minutes <= 20 ~ "10-20min", 
      travel_time_minutes <= 30 ~ "20-30min",
      travel_time_minutes <= 40 ~ "30-40min",
      travel_time_minutes <= 50 ~ "40-50min",
      travel_time_minutes <= 60 ~ "50-60min",
      travel_time_minutes <= 90 ~ "60-90min",
      travel_time_minutes <= 120 ~ "90-120min",
      travel_time_minutes > 120 ~ ">120min",
      TRUE ~ "NA"
    )
  ) 
toc()

# get the transfer legs data

transfer_data <- trip_data %>%
  filter(is_multi_leg_journey_leg==1) %>%
  mutate(
    is_transfer = case_when(
      leg_id >1 ~ 1,
      .default =0
    )
  ) %>%
  group_by(
    uid,journey_id
  ) %>% 
  arrange (
    uid,journey_id,leg_id
  ) %>%
  mutate(
    last_end_time=lag(end_time)
  ) %>%
  filter(is_transfer==1) %>%
  mutate(
    transfer_time_minutes = tryCatch(
      as.numeric(difftime(start_time, last_end_time, units = "mins")),
      error = function(e) as.POSIXct(NA)
    )
  ) %>%
  mutate(
    time_range_type = case_when(
      travel_time_minutes <= 10 ~ "0-10min",
      travel_time_minutes <= 20 ~ "10-20min", 
      travel_time_minutes <= 30 ~ "20-30min",
      travel_time_minutes <= 40 ~ "30-40min",
      travel_time_minutes <= 50 ~ "40-50min",
      travel_time_minutes <= 60 ~ "50-60min",
      travel_time_minutes <= 90 ~ "60-90min",
      travel_time_minutes <= 120 ~ "90-120min",
      travel_time_minutes > 120 ~ ">120min",
      TRUE ~ "NA"
    )
  )

toc()

# journey data

journey_data <- trip_data %>%
  group_by(ope_date,uid,journey_id,region_id) %>%
  summarise(
    boardings=sum(boarding),
    travel_time_minutes= sum(travel_time_minutes),
    .groups = 'drop'
  )  %>%
  mutate(
    time_range_type = case_when(
      travel_time_minutes <= 10 ~ "0-10min",
      travel_time_minutes <= 20 ~ "10-20min", 
      travel_time_minutes <= 30 ~ "20-30min",
      travel_time_minutes <= 40 ~ "30-40min",
      travel_time_minutes <= 50 ~ "40-50min",
      travel_time_minutes <= 60 ~ "50-60min",
      travel_time_minutes <= 90 ~ "60-90min",
      travel_time_minutes <= 120 ~ "90-120min",
      travel_time_minutes > 120 ~ ">120min",
      TRUE ~ "NA"
    )
  ) %>% mutate(
    journey=1
  )

toc()


# aggregations


trip_by_ttime <- 
  trip_data %>% group_by(ope_date, time_range_type,region_id) %>%
  summarise(
    passenger_trips_num = sum(boarding,na.rm=T),
    .groups = 'drop'
  ) 


journey_by_ttime <- 
  journey_data %>% group_by(ope_date, time_range_type,region_id) %>%
  summarise(
    journey_num = sum(journey),
    .groups = 'drop'
    
  ) 

transfer_by_ttime <- 
  transfer_data %>% filter(
    is_transfer==1
  ) %>%
  group_by(ope_date, time_range_type,region_id) %>%
  summarise(
    transfer_num = sum(is_transfer),
    .groups = 'drop'
  ) 



ads_passenger_travel_time_di <- trip_by_ttime %>% full_join(
  journey_by_ttime,by=c('ope_date','region_id','time_range_type')
) %>% full_join(
  transfer_by_ttime,by=c('ope_date','region_id','time_range_type')
)


ads_passenger_travel_time_di_sum <- ads_passenger_travel_time_di %>% 
  group_by(
    ope_date,time_range_type,
  ) %>%
  summarise(
    passenger_trips_num=sum(passenger_trips_num,na.rm=T),
    journey_num=sum(journey_num,na.rm=T),
    transfer_num=sum(transfer_num,na.rm=T)
  ) %>% mutate(region_id='Overall') %>% 
  select(ope_date,time_range_type,region_id,passenger_trips_num,journey_num,transfer_num)



ads_passenger_travel_time_di <- rbind(
  ads_passenger_travel_time_di,ads_passenger_travel_time_di_sum
)  %>% arrange(
  ope_date,time_range_type,region_id
)




# Write the travel time analysis table
write.csv(ads_passenger_travel_time_di, 'ads_passenger_travel_time_di.csv', row.names = FALSE)
