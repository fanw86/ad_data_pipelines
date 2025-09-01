# ads_fare_revenue_region_mi	日期	integer	t_date
# ads_fare_revenue_region_mi	日期类型	character varying(255)	date_type
# ads_fare_revenue_region_mi	区域id，需要包含Overall区域	character varying(255)	region_id
# ads_fare_revenue_region_mi	票价收入，保留两位小数	numeric(15,2)	fare_revenue
# ads_fare_revenue_region_mi	日均票价收入，保留两位小数	numeric(14,2)	average_daily_revenue
# ads_fare_revenue_region_mi	平均票价，保留两位小数	numeric(14,2)	average_fare
# ads_fare_revenue_region_mi	为核对票价合规性而检查的乘客数量	bigint	passengers_inspection_num
# ads_fare_revenue_region_mi	逃票乘客数量	bigint	fare_evasion_passenger_num
# ads_fare_revenue_region_mi	乘客数量	bigint	passenger_num
# ads_fare_revenue_region_mi	总载客量	bigint	passenger_carried
# ads_fare_revenue_region_mi	乘客检查比例，=passengers_inspection_num/passenger_carried	numeric(10,4)	passenger_inspection_ratio
# ads_fare_revenue_region_mi	受检班次数	bigint	inspected_trips
# ads_fare_revenue_region_mi	计划总班次数	bigint	total_scheduled_trips
# ads_fare_revenue_region_mi	实际检查次数	bigint	actual_inspections_num
# ads_fare_revenue_region_mi	计划检查数	bigint	planned_inspections_num
# ads_fare_revenue_region_mi	逃票率=fare_evasion_passenger_num/passenger_num	numeric(10,4)	fare_evasion_ratio
# ads_fare_revenue_region_mi	公交行程检查率=inspected_trips/total_scheduled_trips	numeric(10,4)	bus_trip_inspection_ratio
# ads_fare_revenue_region_mi	遵守检查计划的比例=actual_inspections_num/planned_inspections_num	numeric(10,4)	inspection_plan_adherence_ratio
# ads_fare_revenue_region_mi	创建者用户名	character varying(255)	create_by
# ads_fare_revenue_region_mi	更新者用户名	character varying(255)	update_by
# ads_fare_revenue_region_mi	创建时间	timestamp without time zone	create_time
# ads_fare_revenue_region_mi	更新时间	timestamp without time zone	update_time

rm(list=ls())
library(lubridate)
library(data.table)
library(tidyverse)
library(stringr)
library(ggplot2)
library(sf)


inspection_data <- read.csv('./data/afc_inspection_list.csv')


fine_data <- read.csv('./data/afc_fines_list.csv') 


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




#afc_monthly_sales_devices <- read.csv('./data/afc_monthly_sales_devices.csv')


afc_monthly_sales_eqp <- read.csv('./data/afc_monthly_sales_eqp.csv')


ads_csc_sales_revenue_region_mi <- afc_monthly_sales_eqp %>%
  group_by(V_MONTH,PRODUCT) %>%
  summarise(
    sales_quantity=sum(QTY),
    sales_revenue=sum(AMOUNT),
    .groups = 'drop'
  )  %>% mutate(
    is_csc=grepl('CSC',PRODUCT)
  ) %>% group_by(V_MONTH) %>%
  summarise(
    sales_quantity=sum(sales_quantity),
    sales_revenue=sum(sales_revenue),
    contactless_smart_tickets_num=sum(sales_quantity*is_csc)
  ) %>%
  mutate(
    t_date=V_MONTH,
  ) %>% mutate(
    create_by='wufan',
    update_by='wufan',
    create_time='2025-09-01 11:07:58',
    update_time='2025-09-01 11:07:58') %>% select(-V_MONTH)


write.csv(ads_csc_sales_revenue_region_mi,'./resutls/ads_csc_sales_revenue_region_mi.csv',row.names = F)






