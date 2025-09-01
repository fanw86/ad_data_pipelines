# relname	comment	type	name
# ads_csc_sales_revenue_region_mi	日期	integer	t_date
# ads_csc_sales_revenue_region_mi	日期类型	character varying(255)	date_type
# ads_csc_sales_revenue_region_mi	区域id，需要三个地区的，不包含Overall	character varying(255)	region_id
# ads_csc_sales_revenue_region_mi	销售收入	numeric(14,2)	sales_revenue
# ads_csc_sales_revenue_region_mi	销售数量	bigint	sales_quantity
# ads_csc_sales_revenue_region_mi	非接触式智能票数	bigint	contactless_smart_tickets_num
# ads_csc_sales_revenue_region_mi	激活的产品数量	bigint	activated_ticket_product_num
# ads_csc_sales_revenue_region_mi	创建者用户名	character varying(255)	create_by
# ads_csc_sales_revenue_region_mi	更新者用户名	character varying(255)	update_by
# ads_csc_sales_revenue_region_mi	创建时间	timestamp without time zone	create_time
# ads_csc_sales_revenue_region_mi	更新时间	timestamp without time zone	update_time


rm(list=ls())
library(lubridate)
library(data.table)
library(tidyverse)
library(stringr)
library(ggplot2)
library(sf)

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






