# dwd_bus_route_info_mf	10	更新时间	timestamp without time zone	update_time	FALSE
# dwd_bus_route_info_mf	10	线路编号	integer	line_no	FALSE
# dwd_bus_route_info_mf	10	线路选项编号	integer	route_no	FALSE
# dwd_bus_route_info_mf	10	线路类型,1: Normal profile 2: Depot access 3: Depot departure 4: Access route	integer	route_type	FALSE
# dwd_bus_route_info_mf	10	行程距离，单位米	numeric(8,2)	route_length	FALSE
# dwd_bus_route_info_mf	10	方向，1-正向，2-反向	integer	direction	FALSE
# dwd_bus_route_info_mf	10	线路简称	character varying(255)	line_abbreviation	FALSE
# dwd_bus_route_info_mf	10	线路描述	character varying(255)	line_description	FALSE
# dwd_bus_route_info_mf	10	线路代码	integer	line_code	FALSE
# dwd_bus_route_info_mf	10	线路颜色(RGB值)	character varying(255)	line_color	FALSE
# dwd_bus_route_info_mf	10	始发站编号	character varying(255)	from_stop_id	FALSE
# dwd_bus_route_info_mf	10	始发站名称	character varying(255)	from_stop_name	FALSE
# dwd_bus_route_info_mf	10	终点站编号	character varying(255)	to_stop_id	FALSE
# dwd_bus_route_info_mf	10	终点站名称	character varying(255)	to_stop_name	FALSE
# dwd_bus_route_info_mf	10	运营部门编号，可用于关联运营部门信息表	integer	operation_department_no	FALSE
# dwd_bus_route_info_mf	10	线路所属区域id	character varying(255)	region_id	FALSE
# dwd_bus_route_info_mf	10	版本号	integer	version	FALSE
# dwd_bus_route_info_mf	10	图层数据的地理位置数据	character varying(255)	geom	FALSE
# dwd_bus_route_info_mf	10	创建者用户名	character varying(255)	create_by	FALSE
# dwd_bus_route_info_mf	10	更新者用户名	character varying(255)	update_by	FALSE
# dwd_bus_route_info_mf	10	创建时间	timestamp without time zone	create_time	FALSE

rm(list=ls())
library(lubridate)
library(data.table)
library(tidyverse)
library(stringr)
library(ggplot2)
library(sf)


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

# Process point_on_link to create route geometries
link_geometries <- point_on_link %>%
  # Join with stop table to get coordinates
  left_join(stop, by = c("BASE_VERSION" = "BASE_VERSION",
                         "POINT_TO_LINK_NO" = "POINT_NO",
                         "POINT_TO_LINK_TYPE" = "POINT_TYPE")) %>%
  # Convert coordinates to decimal degrees
  mutate(
    POINT_LONGITUDE = convert_to_decimal(POINT_LONGITUDE),
    POINT_LATITUDE = convert_to_decimal(POINT_LATITUDE)
  ) %>%
  filter(!is.na(POINT_LONGITUDE)) %>%
  # Group by link to create LineStrings
  group_by(OP_DEP_NO, POINT_TYPE, POINT_NO, TO_POINT_NO, TO_POINT_TYPE) %>%
  arrange(POINT_ON_LINK_SERIAL_NO) %>%
  summarize(
    coords = list(cbind(POINT_LONGITUDE, POINT_LATITUDE)),
    .groups = 'drop'
  ) %>%
  mutate(geometry = map(coords, st_linestring)) %>%
  st_as_sf() %>%
  select(-coords) 

link_geometries <- st_set_crs(link_geometries, 4326) 



link_geometries <- link_geometries %>% left_join(
  link,
  by = c('OP_DEP_NO', 'POINT_TYPE', 'POINT_NO', 'TO_POINT_NO', 'TO_POINT_TYPE'))

#st_write(link_geometries, "route_shapes/links.shp",append = FALSE)


stop_only <- stop %>%
  left_join(
    stop_point,
    by = c('BASE_VERSION', 'POINT_NO', 'POINT_TYPE')
  ) %>%
  filter(STOP_TYPE>0) %>%
  mutate(
    POINT_LONGITUDE = convert_to_decimal(POINT_LONGITUDE),
    POINT_LATITUDE = convert_to_decimal(POINT_LATITUDE)
  )  %>%
  rename(
    STOP_PT_N = STOP_POINT_NO,
    STOP_PT_D = STOP_POINT_DESC,
    BAY_BEF_PO = BAY_BEVOR_POLE  # Assuming BAY_BEVOR_POLE might also cause issues
  ) %>%
  select(-BASE_VERSION)




stop_only_sf <- st_as_sf(stop_only, coords = c("POINT_LONGITUDE", "POINT_LATITUDE"), crs = 4326) %>%
  select(POINT_NO,STOP_DESC,STOP_TYPE,BAY_SIZE)



#st_write(stop_only_sf, "route_shapes/current_stops.shp",append = FALSE)

#st_write(updated_points, "route_shapes/stops.shp",append = FALSE)


processed_route_sequence <- route_sequence %>%
  group_by(LINE_NO, ROUTE_ABBR) %>%
  mutate(
    TO_POINT_TYPE = lead(POINT_TYPE),
    TO_POINT_NO = lead(POINT_NO)
  ) %>%
  ungroup() %>%
  left_join(
    routes,
    by = c('BASE_VERSION',"LINE_NO", "ROUTE_ABBR")
  ) %>%
  filter(!is.na(TO_POINT_NO))


route_lengths <- processed_route_sequence %>%
  left_join(link %>% select(OP_DEP_NO, POINT_TYPE, POINT_NO, TO_POINT_NO, TO_POINT_TYPE,
                            contains("LENGTH"), contains("DISTANCE")),
            by = c('OP_DEP_NO', 'POINT_TYPE', 'POINT_NO', 'TO_POINT_NO', 'TO_POINT_TYPE')) %>%
  group_by(LINE_NO, ROUTE_ABBR) %>%
  summarise(
    route_length_m = sum(LINK_DISTANCE, na.rm = TRUE),  # Adjust column name as needed
    .groups = 'drop'
  )


first_last_stops <- route_sequence %>%
  group_by(LINE_NO, ROUTE_ABBR) %>%
  arrange(SEQUENCE_NO) %>%
  summarise(
    first_stop_no = first(POINT_NO),
    last_stop_no = last(POINT_NO),
    .groups = 'drop'
  )


first_last_stops <- first_last_stops %>%
  left_join(route_lengths, by = c("LINE_NO", "ROUTE_ABBR"))


stop_names <- stop_only %>%
  select(POINT_NO, STOP_DESC) %>%
  rename(stop_name = STOP_DESC)


# Join route sequence with link geometries

route_shapes <-processed_route_sequence %>% left_join(
  link_geometries,
  by = c( 'OP_DEP_NO',"POINT_TYPE", "POINT_NO", "TO_POINT_NO", "TO_POINT_TYPE")
)  %>%
  group_by(LINE_NO,ROUTE_ABBR) %>% # Assuming 'ROUTE_ID' is your route identifier in route_sequence
  arrange(SEQUENCE_NO) %>%  # Assuming SEQUENCE_NO defines the order of links in a route
  summarize(geometry = st_combine(geometry), .groups = "drop") %>%
  # mutate(geometry = st_line_merge(geometry)) %>%
  left_join(
    routes,
    by = c("LINE_NO", "ROUTE_ABBR")
  ) %>%
  left_join(
    first_last_stops,
    by = c("LINE_NO", "ROUTE_ABBR")
  ) %>% left_join(
    stop_names,
    by = c("first_stop_no" = "POINT_NO")
  ) %>% rename(
    'from_stop_name'='stop_name'
  ) %>%
  left_join(
    stop_names,
    by = c("last_stop_no" = "POINT_NO")
  ) %>% rename(
    'to_stop_name'='stop_name'
  )




dwd_bus_route_info_mf <- route_shapes %>% select(
  line_no=LINE_NO,
  route_no=ROUTE_ABBR,
  route_type=ROUTE_TYPE,
  route_length=route_length_m,
  direction=DIRECTION,
  line_abbreviation=LINE_ABBR,
  line_description=LINE_DESC,
  line_code=LINE_CODE,
  line_color=LINE_COLOUR,
  from_stop_id=first_stop_no,
  from_stop_name=from_stop_name,
  to_stop_id=last_stop_no,
  to_stop_name=to_stop_name,
  operation_department_no=OP_DEP_NO,
  # region_id=
  version=BASE_VERSION,
  geom=geometry) %>% mutate(
    create_by='wufan',
    update_by='wufan',
    create_time='20250807_11:07:58',
    update_time='20250807_11:07:58',
    geom=st_as_text(geom)
  ) %>%
  mutate(
    region_id=case_when(
      as.numeric(substr(as.character(abs(operation_department_no)), 1, 1))==1 ~'Abu Dhabi',
      as.numeric(substr(as.character(abs(operation_department_no)), 1, 1))==2 ~'Al Ain',
      as.numeric(substr(as.character(abs(operation_department_no)), 1, 1))==3 ~'Al Dhafra'
    )
  )


write.csv(dwd_bus_route_info_mf,'dwd_bus_route_info_mf.csv')





# dwd_bus_route_stop_info_mf	11	线路编号，参考dwd_bus_route_info_mf表	character varying(255)	route_no	FALSE
# dwd_bus_route_stop_info_mf	11	方向，参考dwd_bus_route_info_mf表	character varying(255)	direction	FALSE
# dwd_bus_route_stop_info_mf	11	版本号	integer	version	FALSE
# dwd_bus_route_stop_info_mf	11	创建者用户名	character varying(255)	create_by	FALSE
# dwd_bus_route_stop_info_mf	11	更新者用户名	character varying(255)	update_by	FALSE
# dwd_bus_route_stop_info_mf	11	创建时间	timestamp without time zone	create_time	FALSE
# dwd_bus_route_stop_info_mf	11	更新时间	timestamp without time zone	update_time	FALSE
# dwd_bus_route_stop_info_mf	11	站点编号，参考dwd_bus_stop_info表	character varying(255)	stop_no	FALSE
# dwd_bus_route_stop_info_mf	11	站点索引	character varying(255)	stop_index	FALSE
# dwd_bus_route_stop_info_mf	11	线路编号，参考dwd_bus_route_info_mf表	integer	line_no	FALSE

route_lkp <- routes %>%
  select(LINE_NO,ROUTE_NO,direction=DIRECTION)


dwd_bus_route_stop_info_mf <- route_sequence %>% select(
  route_no=ROUTE_ABBR,
  # direction=DIRECTION,
  version=BASE_VERSION,
  stop_no=POINT_NO,
  stop_index=SEQUENCE_NO,
  line_no=LINE_NO
) %>% mutate(
  create_by='wufan',
  update_by='wufan',
  create_time='20250807_11:07:58',
  update_time='20250807_11:07:58',
) %>% left_join(
  route_lkp,by=c('line_no'='LINE_NO','route_no'='ROUTE_NO')
) %>%
  arrange(
    line_no,route_no,stop_index
  )



write.csv(dwd_bus_route_stop_info_mf,'dwd_bus_route_stop_info_mf.csv')

# 
# dwd_bus_stop_info_mf	12	更新者用户名	character varying(255)	update_by	FALSE
# dwd_bus_stop_info_mf	12	创建者用户名	character varying(255)	create_by	FALSE
# dwd_bus_stop_info_mf	12	版本号	integer	version	FALSE
# dwd_bus_stop_info_mf	12	图层数据的地理位置数据	character varying(255)	geom	FALSE
# dwd_bus_stop_info_mf	12	区域id	character varying(255)	region_id	FALSE
# dwd_bus_stop_info_mf	12	停车位长度，待定	integer	bay_size	FALSE
# dwd_bus_stop_info_mf	12	站点类型	character varying(255)	stop_type	FALSE
# dwd_bus_stop_info_mf	12	是否包含AFC设备，1-是，0-否	integer	is_afc	FALSE
# dwd_bus_stop_info_mf	12	是否有候车亭，1-是，0-否	integer	bus_shelter	FALSE
# dwd_bus_stop_info_mf	12	站台类型，1-永久，0-临时	integer	platform_type	FALSE
# dwd_bus_stop_info_mf	12	激活状态，1-激活，0-未激活	integer	active_status	FALSE
# dwd_bus_stop_info_mf	12	站点方向唯一标识符	character varying(255)	direction	FALSE
# dwd_bus_stop_info_mf	12	纬度，WGS坐标系	numeric	latitude	FALSE
# dwd_bus_stop_info_mf	12	经度，WGS坐标系	numeric(10,6)	longitude	FALSE
# dwd_bus_stop_info_mf	12	站点名称	character varying(255)	stop_name	FALSE
# dwd_bus_stop_info_mf	12	站点编号	character varying(255)	stop_no	FALSE
# dwd_bus_stop_info_mf	12	更新时间	timestamp without time zone	update_time	FALSE
# dwd_bus_stop_info_mf	12	创建时间	timestamp without time zone	create_time	FALSE

# Process stop_only_sf to create dwd_bus_stop_info_mf

# read geojson

ad_regions_sf <- st_read("data/ad_regions.geojson") %>% select(region_id=NAME_2)

stop_region_sf <- st_join(stop_only_sf,ad_regions_sf, join = st_within)


dwd_bus_stop_info_mf <- stop_region_sf %>%
  mutate(
    create_by='wufan',
    update_by='wufan',
    create_time='20250807_11:07:58',
    update_time='20250807_11:07:58',
    geom=st_as_text(geometry),
    is_afc=NA,
    bus_shelter=NA,
    platform_type=NA,
    active_status=NA,
    BASE_VERSION='202505080912'
  ) %>%
  select(
    stop_no=POINT_NO,
    stop_name=STOP_DESC,
    bay_size=BAY_SIZE,
    stop_type=STOP_TYPE,
    is_afc,
    bus_shelter,
    platform_type,
    active_status,
    geom,
    region_id,
    version=BASE_VERSION,
    create_by,update_by,create_time,update_time
  )

write.csv(dwd_bus_stop_info_mf,'dwd_bus_stop_info_mf.csv')









